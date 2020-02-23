use serde_json::Deserializer;
use std::io::{BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};

use crate::request::Request;
use crate::response::{GetResponse, RemoveResponse, SetResponse};
use crate::{KvEngine, Result, ThreadPool};

/// The server of key value store.
pub struct KvsServer<E: KvEngine, P: ThreadPool> {
    engine: E,
    thread_pool: P,
}

impl<E: KvEngine, P: ThreadPool> KvsServer<E, P> {
    /// Create a serve with logger.
    pub fn new(engine: E, thread_pool: P) -> Self {
        KvsServer {
            engine,
            thread_pool,
        }
    }

    /// Init the listener.
    pub fn start<A: ToSocketAddrs>(&self, addr: A) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        info!("Server started...");
        for stream in listener.incoming() {
            let engine = self.engine.clone();
            self.thread_pool.spawn(move || match stream {
                Ok(stream) => {
                    if let Err(e) = handle(engine, stream) {
                        error!("Error on serving client: {}", e);
                    }
                }
                Err(e) => error!("Connection failed: {}", e),
            })
        }
        Ok(())
    }
}

/// Handle the stream.
pub fn handle<E: KvEngine>(engine: E, tcp: TcpStream) -> Result<()> {
    let peer_addr = tcp.peer_addr()?;
    debug!("Get the tcp stream form {}", peer_addr);
    let reader = BufReader::new(&tcp);
    let mut writer = BufWriter::new(&tcp);
    let request_reader = Deserializer::from_reader(reader).into_iter::<Request>();

    for request_item in request_reader {
        match request_item? {
            Request::Get { key } => {
                info!("Get get {} command form client {}", key, peer_addr);
                let response = match engine.get(key) {
                    Ok(value) => GetResponse::Ok(value),
                    Err(e) => GetResponse::Err(format!("{}", e)),
                };
                serde_json::to_writer(&mut writer, &response)?;
                writer.flush()?;
            }
            Request::Set { key, value } => {
                info!(
                    "Get set {}:{} command form client {}",
                    key, value, peer_addr
                );
                let response = match engine.set(key, value) {
                    Ok(_) => SetResponse::Ok(()),
                    Err(e) => SetResponse::Err(format!("{}", e)),
                };
                serde_json::to_writer(&mut writer, &response)?;
                writer.flush()?;
            }
            Request::Remove { key } => {
                info!("Get remove key {} command form client {}", key, peer_addr);
                let response = match engine.remove(key) {
                    Ok(_) => RemoveResponse::Ok(()),
                    Err(e) => RemoveResponse::Err(format!("{}", e)),
                };
                serde_json::to_writer(&mut writer, &response)?;
                writer.flush()?;
            }
        }
    }
    Ok(())
}
