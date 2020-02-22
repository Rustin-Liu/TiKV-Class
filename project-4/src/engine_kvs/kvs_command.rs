use serde::{Deserialize, Serialize};
use std::ops::Range;

/// Struct representing a command.
#[derive(Serialize, Deserialize, Debug)]
pub enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

impl Command {
    pub fn set(key: String, value: String) -> Command {
        Command::Set { key, value }
    }

    pub fn remove(key: String) -> Command {
        Command::Remove { key }
    }
}

/// Represents the position and length of a json-serialized command in the log.
#[derive(Debug, Clone, Copy)]
pub struct CommandPos {
    pub pos: u64,
    pub len: u64,
}

impl From<Range<u64>> for CommandPos {
    fn from(range: Range<u64>) -> Self {
        CommandPos {
            pos: range.start,
            len: range.end - range.start,
        }
    }
}
