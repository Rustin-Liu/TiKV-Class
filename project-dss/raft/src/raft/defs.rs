pub struct ApplyMsg {
    pub command_valid: bool,
    pub command: Vec<u8>,
    pub command_index: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Role {
    Follower,
    Leader,
    Candidate,
}

impl Default for Role {
    fn default() -> Self {
        Role::Follower
    }
}

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub(crate) term: u64,
    pub(crate) is_leader: bool,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}

pub const HEARTBEAT_INTERVAL: u64 = 50;
