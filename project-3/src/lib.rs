#![deny(missing_docs)]
//! A simple key/value store.

pub use client::KvsClient;
pub use error::{KvsError, Result};
pub use kv::KvStore;
pub use server::KvsServer;

mod client;
mod error;
mod kv;
mod request;
mod response;
mod server;
