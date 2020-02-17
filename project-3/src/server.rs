use serde_json::Deserializer;
use slog::*;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::str;

use crate::request::Request;
use crate::response::{GetResponse, RemoveResponse, SetResponse};
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
        debug!(self.logger, "Get the tcp stream form {}", peer_addr);
        let reader = BufReader::new(&tcp);
        let mut writer = BufWriter::new(&tcp);
        let request_reader = Deserializer::from_reader(reader).into_iter::<Request>();

        for request_item in request_reader {
            match request_item? {
                Request::Get { key } => {
                    info!(
                        self.logger,
                        "Get get {} command form client {}", key, peer_addr
                    );
                    serde_json::to_writer(
                        &mut writer,
                        &GetResponse::Err("unimplemented".to_string()),
                    )?;
                    writer.flush()?;
                }
                Request::Set { key, value } => {
                    info!(
                        self.logger,
                        "Get set {}:{} command form client {}", key, value, peer_addr
                    );
                    serde_json::to_writer(
                        &mut writer,
                        &SetResponse::Err("unimplemented".to_string()),
                    )?;
                    writer.flush()?;
                }
                Request::Remove { key } => {
                    info!(
                        self.logger,
                        "Get remove key {} command form client {}", key, peer_addr
                    );
                    serde_json::to_writer(
                        &mut writer,
                        &RemoveResponse::Err("unimplemented".to_string()),
                    )?;
                    writer.flush()?;
                }
            }
        }
        Ok(())
    }
}
