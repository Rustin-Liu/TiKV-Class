use std::borrow::BorrowMut;
use std::fs::File;
use std::io::Read;
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::{fs, io};

use crossbeam_skiplist::SkipMap;

use crate::engine_kvs::kvs_command::{Command, CommandPos};
use crate::engine_kvs::kvs_reader::BufReaderWithPos;
use crate::engine_kvs::my_kvs::{new_log_file, LOG_FILE_NAME};
use crate::{KvsError, Result};

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

pub struct KvStoreWriter {
    // The current log file director path.
    pub path: Arc<PathBuf>,
    pub reader: BufReaderWithPos<File>,
    pub writer: BufWriterWithPos<File>,
    // The number of bytes representing "stale" commands that could be
    // deleted during a compaction.
    pub need_compacted: u64,
    // The command position index.
    pub index: Arc<SkipMap<String, CommandPos>>,
}
impl KvStoreWriter {
    /// Sets the value of a string key into log file as string.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::set(key, value);
        let pos = self.writer.pos;
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;
        if let Command::Set { key, .. } = cmd {
            if let Some(old_cmd) = self.index.get(&key) {
                self.need_compacted += old_cmd.value().len;
            }
            self.index.insert(key, (pos..self.writer.pos).into());
        }

        if self.need_compacted > COMPACTION_THRESHOLD {
            self.compact()?;
        }
        Ok(())
    }

    /// Remove a given key from log file.
    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {
            let cmd = Command::remove(key);
            serde_json::to_writer(&mut self.writer, &cmd)?;
            self.writer.flush()?;
            if let Command::Remove { key } = cmd {
                let old_cmd = self.index.remove(&key).expect("key not found");
                self.need_compacted += old_cmd.value().len;
            }
            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }

    /// Clears stale entries in the log.
    pub fn compact(&mut self) -> Result<()> {
        let path = self.path.join(LOG_FILE_NAME.to_owned() + ".temp");
        let mut temp_writer = new_log_file(&path)?;

        let mut new_pos = 0;

        for entry in self.index.iter() {
            let reader = self.reader.borrow_mut();
            if reader.pos != entry.value().pos {
                reader.seek(SeekFrom::Start(entry.value().pos))?;
            }

            let mut entry_reader = reader.take(entry.value().len);
            let len = io::copy(&mut entry_reader, &mut temp_writer)?;
            self.index
                .insert(entry.key().clone(), (new_pos..new_pos + len).into());
            new_pos += len;
        }

        temp_writer.flush()?;

        let old_file_path = &self.get_log_file_path()?;
        fs::remove_file(old_file_path)?;
        fs::rename(path, old_file_path)?;
        self.need_compacted = 0;
        // need reset writer.
        self.writer = new_log_file(&old_file_path)?;
        Ok(())
    }

    /// Get log file path.
    ///
    /// Returns the path of the log file.
    fn get_log_file_path(&mut self) -> Result<PathBuf> {
        Ok(self.path.join(LOG_FILE_NAME))
    }
}

pub struct BufWriterWithPos<W: Write + Seek> {
    writer: BufWriter<W>,
    pos: u64,
}

impl<W: Write + Seek> BufWriterWithPos<W> {
    pub fn new(mut inner: W) -> Result<Self> {
        // because add the log at end so need start at file end position.
        let pos = inner.seek(SeekFrom::End(0))?;
        Ok(BufWriterWithPos {
            writer: BufWriter::new(inner),
            pos,
        })
    }
}

impl<W: Write + Seek> Write for BufWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write + Seek> Seek for BufWriterWithPos<W> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}
