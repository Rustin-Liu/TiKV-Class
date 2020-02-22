use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crossbeam_skiplist::SkipMap;
use failure::_core::cell::RefCell;
use serde_json::Deserializer;

use crate::engine_kvs::kvs_command::{Command, CommandPos};
use crate::engine_kvs::kvs_reader::{BufReaderWithPos, KvStoreReader};
use crate::engine_kvs::kvs_writer::{BufWriterWithPos, KvStoreWriter};
use crate::{KvEngine, KvsError, Result};

pub const LOG_FILE_NAME: &str = "kvs.log";

/// The `KvStore` stores string key/value pairs.
///
/// Key/value pairs are persisted to disk in log files.
/// A `SkipMap` in memory stores the keys and the value locations for fast query.
///
/// ```rust
/// # use kvs::{MyKvStore, Result, KvEngine};
/// # fn try_main() -> Result<()> {
/// use std::env::current_dir;
/// let mut store = MyKvStore::open(current_dir()?)?;
/// store.set("key".to_owned(), "value".to_owned())?;
/// let val = store.get("key".to_owned())?;
/// assert_eq!(val, Some("value".to_owned()));
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct MyKvStore {
    // The current log file director path.
    path: Arc<PathBuf>,
    // Reader of the current log.
    reader: KvStoreReader,
    // Writer of the current log.
    writer: Arc<Mutex<KvStoreWriter>>,
    // The command position index.
    index: Arc<SkipMap<String, CommandPos>>,
}

impl MyKvStore {
    /// Opens a `KvStore` with the given path.
    ///
    /// This will create a new directory if the given one does not exist.
    ///
    /// # Errors
    ///
    /// It propagates I/O or deserialization errors during the log replay.
    pub fn open(path: impl Into<PathBuf>) -> Result<MyKvStore> {
        let path = Arc::new(path.into());
        fs::create_dir_all(&*path)?;
        let log_path = path.join(LOG_FILE_NAME);

        // Use log path init the reader and writer.
        let writer = new_log_file(&log_path)?;
        let mut reader = BufReaderWithPos::new(File::open(&log_path)?)?;

        let mut index = SkipMap::new();
        let mut need_compacted = 0;
        need_compacted += load(&mut reader, &mut index)?;

        let index = Arc::new(index);

        let writer = KvStoreWriter {
            path: Arc::clone(&path),
            reader,
            writer,
            index: Arc::clone(&index),
            need_compacted,
        };

        let reader = KvStoreReader {
            path: Arc::clone(&path),
            reader: RefCell::new(BufReaderWithPos::new(File::open(&log_path)?)?),
        };

        Ok(MyKvStore {
            path,
            reader,
            writer: Arc::new(Mutex::new(writer)),
            index,
        })
    }
}

impl KvEngine for MyKvStore {
    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    ///
    /// # Errors
    ///
    /// It propagates I/O or serialization errors during writing the log.
    fn set(&self, key: String, value: String) -> Result<()> {
        self.writer.lock().unwrap().set(key, value)
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    ///
    /// # Errors
    ///
    /// It returns `KvsError::IncorrectCommandType` if the given command is incorrect.
    fn get(&self, key: String) -> Result<Option<String>> {
        if let Some(cmd_pos) = self.index.get(&key) {
            if let Command::Set { value, .. } = self.reader.read_command(*cmd_pos.value())? {
                Ok(Some(value))
            } else {
                Err(KvsError::IncorrectCommandType)
            }
        } else {
            Ok(None)
        }
    }

    /// Remove a given key.
    ///
    /// # Errors
    ///
    /// It returns `KvsError::KeyNotFound` if the given key is not found.
    ///
    /// It propagates I/O or serialization errors during writing the log.
    fn remove(&self, key: String) -> Result<()> {
        self.writer.lock().unwrap().remove(key)
    }
}

/// Load the whole log file and store value locations in the index map.
///
/// Returns how many bytes can be saved after a compaction.
fn load(
    reader: &mut BufReaderWithPos<File>,
    index: &mut SkipMap<String, CommandPos>,
) -> Result<u64> {
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();
    let mut need_compacted = 0;
    while let Some(cmd) = stream.next() {
        let new_pos = stream.byte_offset() as u64;
        match cmd? {
            Command::Set { key, .. } => {
                if let Some(old_cmd) = index.get(&key) {
                    need_compacted += old_cmd.value().len;
                }
                index.insert(key, (pos..new_pos).into());
            }
            Command::Remove { key } => {
                if let Some(old_cmd) = index.remove(&key) {
                    need_compacted += old_cmd.value().len;
                }
                need_compacted += new_pos - pos;
            }
        }
        pos = new_pos;
    }
    Ok(need_compacted)
}

/// Create a new log file.
///
/// Returns the writer to the log.
pub fn new_log_file(path: &Path) -> Result<BufWriterWithPos<File>> {
    let writer = BufWriterWithPos::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&path)?,
    )?;
    Ok(writer)
}
