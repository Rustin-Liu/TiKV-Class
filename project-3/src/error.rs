use failure::Fail;
use std::io;

/// Custom Kvs Error type.
#[derive(Fail, Debug)]
pub enum KvsError {
    /// IO Error.
    #[fail(display = "{}", _0)]
    Io(#[cause] io::Error),
    /// Serialization or deserialization error
    #[fail(display = "{}", _0)]
    Serde(#[cause] serde_json::Error),
    /// Removing non-existent key error
    #[fail(display = "Key not found")]
    KeyNotFound,
    /// Incorrect command type error.
    #[fail(display = "Incorrect command type")]
    IncorrectCommandType,
    /// Response error.
    #[fail(display = "{}", _0)]
    ResponseError(String),
}

impl From<io::Error> for KvsError {
    fn from(err: io::Error) -> KvsError {
        KvsError::Io(err)
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(err: serde_json::Error) -> KvsError {
        KvsError::Serde(err)
    }
}

/// Result alias for Kvs.
pub type Result<T> = std::result::Result<T, KvsError>;
