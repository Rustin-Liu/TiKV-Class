use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::{KvsError, Result};
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};

use serde::{Deserialize, Serialize};

/// The `KvStore` stores string key/value pairs.
///
/// Key/value pairs are persisted to disk in log file.
///
/// ```rust
/// # use kvs::{KvStore, Result};
/// # fn try_main() -> Result<()> {
/// use std::env::current_dir;
/// let mut store = KvStore::open(current_dir()?)?;
/// store.set("key".to_owned(), "value".to_owned())?;
/// let val = store.get("key".to_owned())?;
/// assert_eq!(val, Some("value".to_owned()));
/// # Ok(())
/// # }
/// ```
pub struct KvStore {
    // path for the log.
    path: PathBuf,
    // the file reader.
    reader: BufReader<File>,
    // writer of the current log.
    writer: BufWriter<File>,
    // the kv data.
    data: HashMap<String, String>,
}

impl KvStore {
    /// Opens a `KvStore` with the given path.
    ///
    /// This will create a new directory if the given one does not exist.
    ///
    /// # Errors
    ///
    /// It propagates I/O or deserialization errors during the log replay.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        fs::create_dir_all(&path)?;

        let writer = new_log_file(&path.join("kvs.log"))?;

        let mut reader = BufReader::new(File::open(&path.join("kvs.log"))?);

        let data = load(&mut reader)?;
        Ok(KvStore {
            path,
            reader,
            writer,
            data,
        })
    }

    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    ///
    /// # Errors
    ///
    /// It propagates I/O or serialization errors during writing the log.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::set(key, value);
        let cmd = serde_json::to_string(&cmd)?;
        self.writer.write_fmt(format_args!("{}\n", cmd));
        self.writer.flush();
        Ok(())
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if self.data.contains_key(&key) {
            Ok(self.data.get(&key).cloned())
        } else {
            Ok(None)
        }
    }

    /// Remove a given key.
    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.data.contains_key(&key) {
            let cmd = Command::remove(key.clone());
            let cmd = serde_json::to_string(&cmd)?;
            self.writer.write_fmt(format_args!("{}\n", cmd));
            self.writer.flush();
            self.data.remove(&key);
            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }
}

/// Create a new log file.
///
/// Returns the writer to the log.
fn new_log_file(path: &Path) -> Result<BufWriter<File>> {
    let writer = BufWriter::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&path)?,
    );
    Ok(writer)
}

/// Load the whole log file and store hash data.
///
/// Returns hash data.
fn load(reader: &mut BufReader<File>) -> Result<HashMap<String, String>> {
    let mut result = HashMap::new();
    for line in reader.lines() {
        let cmd: Command = serde_json::from_str(&line?)?;
        match cmd {
            Command::Set { key, value } => {
                result.insert(key, value);
            }
            Command::Remove { key } => {
                result.remove(&key);
            }
        }
    }
    Ok(result)
}

/// Struct representing a command
#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

impl Command {
    fn set(key: String, value: String) -> Command {
        Command::Set { key, value }
    }

    fn remove(key: String) -> Command {
        Command::Remove { key }
    }
}
