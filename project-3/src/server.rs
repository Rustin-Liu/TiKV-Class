use crate::{KvsError, Result};
use slog::*;
use std::net::{TcpListener, TcpStream, ToSocketAddrs};

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
        info!(self.logger, "Get TCP stream success.");
        Ok(())
    }
}
