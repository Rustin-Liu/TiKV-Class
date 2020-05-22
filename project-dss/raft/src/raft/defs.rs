use crate::proto::raftpb::*;
use crate::raft::errors::Result;
use futures::channel::oneshot::Sender;

pub struct ApplyMsg {
    pub command_valid: bool,
    pub command: Vec<u8>,
    pub command_index: u64,
}

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,
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

pub enum Action {
    RequestVote(RequestVoteArgs, Sender<RequestVoteReply>),
    AppendLogs(AppendLogsArgs, Sender<AppendLogsReply>),
    KickOffElection,
    Start(Vec<u8>, Sender<Result<(u64, u64)>>),
    Apply,
    StartAppendLogs,
    AppendLogsResult(AppendLogsReply),
    ElectionSuccess,
}
