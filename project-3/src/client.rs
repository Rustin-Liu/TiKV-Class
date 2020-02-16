use std::io::Write;
use std::net::{TcpStream, ToSocketAddrs};

use crate::Result;

/// The client of key value store.
pub struct KvsClient;

impl KvsClient {
    /// Init client connect.
    pub fn init<A: ToSocketAddrs>(addr: A) -> Result<()> {
        let tcp_reader = TcpStream::connect(addr)?;
        let mut tcp_writer = tcp_reader.try_clone()?;
        tcp_writer.write("Hello Kvs".as_bytes());
        tcp_writer.flush();
        Ok(())
    }
}
