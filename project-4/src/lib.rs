#![deny(missing_docs)]
//! A simple key/value store.

extern crate slog;
#[macro_use]
extern crate slog_scope;

pub use client::KvsClient;
pub use engine_kvs::MyKvStore;
pub use engine_sled::SledKvs;
pub use engine_trait::KvEngine;
pub use error::{KvsError, Result};
pub use server::KvsServer;
pub use thread_pool::*;
mod client;
mod engine_kvs;
mod engine_sled;
mod engine_trait;
mod error;
mod request;
mod response;
mod server;
pub mod thread_pool;
