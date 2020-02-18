#![deny(missing_docs)]
//! A simple key/value store.

pub use client::KvsClient;
pub use engine_kvs::KvStore;
pub use engine_trait::KvEngine;
pub use error::{KvsError, Result};
pub use server::KvsServer;

mod client;
mod engine_kvs;
mod engine_trait;
mod error;
mod request;
mod response;
mod server;
