use failure::Fail;
use std::io;

/// Custom Kvs Error type.
#[derive(Fail, Debug)]
pub enum KvsError {
    /// IO Error.
    #[fail(display = "{}", _0)]
    Io(#[cause] io::Error),
}

impl From<io::Error> for KvsError {
    fn from(err: io::Error) -> KvsError {
        KvsError::Io(err)
    }
}

/// Result alias for Kvs.
pub type Result<T> = std::result::Result<T, KvsError>;
