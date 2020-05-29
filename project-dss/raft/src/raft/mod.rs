#[cfg(test)]
pub mod config;
pub mod defs;
pub mod errors;
pub mod node;
pub mod persister;
pub mod raft_peer;
pub mod raft_server;
#[cfg(test)]
mod tests;

pub const APPLY_INTERVAL: u64 = 200;

pub const HEARTBEAT_INTERVAL: u64 = 100;
