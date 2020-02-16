use slog::*;
use std::io::{BufRead, BufReader, BufWriter, Read};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::str;

use crate::{KvsError, Result};

/// The server of key value store.
pub struct KvsServer {
    logger: Logger,
}

impl KvsServer {
    /// Create a serve with logger.
    pub fn new(logger: Logger) -> Self {
        KvsServer { logger }
    }

    /// Init the listener.
    pub fn init<A: ToSocketAddrs>(mut self, addr: A) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        info!(self.logger, "Server started...");
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    if let Err(e) = self.handle(stream) {
                        error!(self.logger, "Error on serving client: {}", e);
                    }
                }
                Err(e) => error!(self.logger, "Connection failed: {}", e),
            }
        }
        Ok(())
    }

    /// Handle the stream.
    pub fn handle(&mut self, tcp: TcpStream) -> Result<()> {
        let peer_addr = tcp.peer_addr()?;
        let mut reader = BufReader::new(&tcp);
        let msg: &mut [u8] = &mut [0x0; 400];
        reader.read(msg).unwrap();
        info!(
            self.logger,
            "Get TCP stream msg {} form {}.",
            str::from_utf8(msg).unwrap(),
            peer_addr
        );
        Ok(())
    }
}
