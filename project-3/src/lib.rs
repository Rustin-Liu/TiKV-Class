#![deny(missing_docs)]
//! A simple key/value store.

pub use error::{KvsError, Result};
pub use kv::KvStore;
pub use server::KvsServer;

mod error;
mod kv;
mod server;
