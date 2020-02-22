use std::io::{BufReader, BufWriter, Write};
use std::net::{TcpStream, ToSocketAddrs};

use crate::error::KvsError;
use crate::request::Request;
use crate::response::{GetResponse, RemoveResponse, SetResponse};
use crate::Result;
use serde::Deserialize;
use serde_json::de::IoRead;
use serde_json::Deserializer;

/// The client of key value store.
pub struct KvsClient {
    reader: Deserializer<IoRead<BufReader<TcpStream>>>,
    writer: BufWriter<TcpStream>,
}

impl KvsClient {
    /// Init client connect.
    pub fn init<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        let reader_streaming = TcpStream::connect(addr)?;
        let writer_streaming = reader_streaming.try_clone()?;
        Ok(KvsClient {
            reader: Deserializer::from_reader(BufReader::new(reader_streaming)),
            writer: BufWriter::new(writer_streaming),
        })
    }

    /// Get the value of key from server.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        self.send_request(&Request::Get { key })?;
        let response = GetResponse::deserialize(&mut self.reader)?;
        match response {
            GetResponse::Ok(value) => Ok(value),
            GetResponse::Err(msg) => Err(KvsError::ResponseError(msg)),
        }
    }

    /// Set the value to server.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.send_request(&Request::Set { key, value })?;
        let response = SetResponse::deserialize(&mut self.reader)?;
        match response {
            SetResponse::Ok(()) => Ok(()),
            SetResponse::Err(msg) => Err(KvsError::ResponseError(msg)),
        }
    }

    /// Remove the key value from server.
    pub fn remove(&mut self, key: String) -> Result<()> {
        self.send_request(&Request::Remove { key })?;
        self.writer.flush()?;
        let response = RemoveResponse::deserialize(&mut self.reader)?;
        match response {
            RemoveResponse::Ok(()) => Ok(()),
            RemoveResponse::Err(msg) => Err(KvsError::ResponseError(msg)),
        }
    }
    // Send request to server.
    fn send_request(&mut self, request: &Request) -> Result<()> {
        serde_json::to_writer(&mut self.writer, request)?;
        self.writer.flush()?;
        Ok(())
    }
}
