use std::cell::RefCell;
use std::fs::File;
use std::io;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::Arc;

use crate::engine_kvs::kvs_command::{Command, CommandPos};
use crate::engine_kvs::my_kvs::LOG_FILE_NAME;
use crate::Result;

pub struct KvStoreReader {
    pub path: Arc<PathBuf>,
    pub reader: RefCell<BufReaderWithPos<File>>,
}

impl KvStoreReader {
    /// Read the log file at the given `CommandPos` and deserialize it to `Command`.
    pub fn read_command(&self, cmd_pos: CommandPos) -> Result<Command> {
        let reader = &mut self.reader.borrow_mut().reader;
        reader.seek(SeekFrom::Start(cmd_pos.pos))?;
        let cmd_reader = reader.take(cmd_pos.len);
        Ok(serde_json::from_reader(cmd_reader)?)
    }
}

impl Clone for KvStoreReader {
    fn clone(&self) -> KvStoreReader {
        let reader =
            BufReaderWithPos::new(File::open(&self.path.join(LOG_FILE_NAME)).unwrap()).unwrap();
        KvStoreReader {
            path: Arc::clone(&self.path),
            reader: RefCell::new(reader),
        }
    }
}

pub struct BufReaderWithPos<R: Read + Seek> {
    reader: BufReader<R>,
    pub pos: u64,
}

impl<R: Read + Seek> BufReaderWithPos<R> {
    pub fn new(mut inner: R) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufReaderWithPos {
            reader: BufReader::new(inner),
            pos,
        })
    }
}

impl<R: Read + Seek> Read for BufReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}

impl<R: Read + Seek> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}
