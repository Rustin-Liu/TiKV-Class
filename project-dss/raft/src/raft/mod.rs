use std::sync::mpsc::{sync_channel, Receiver};

use futures::sync::mpsc::UnboundedSender;

#[cfg(test)]
pub mod config;
pub mod defs;
pub mod errors;
pub mod node;
pub mod persister;

#[cfg(test)]
mod tests;

use self::defs::*;
use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;
use futures::future::*;
use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::SystemTime;

/// State of a raft peer.
#[derive(Clone, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,
    pub voted_for: Option<usize>,
    // Peer current role.
    pub role: Role,
    pub leader_id: Option<usize>,
    pub last_receive_time: SystemTime,
}

impl Default for State {
    fn default() -> Self {
        State {
            term: 0,
            is_leader: false,
            voted_for: None,
            role: Default::default(),
            leader_id: None,
            last_receive_time: SystemTime::now(),
        }
    }
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

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: usize,
    state: RefCell<State>,
    dead: AtomicBool,
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        _apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();
        let mut rf = Raft {
            peers,
            persister,
            me,
            state: RefCell::from(State::default()),
            dead: AtomicBool::new(false),
        };
        // initialize from state persisted before a crash
        rf.restore(&raft_state);
        rf
    }

    fn convert_to_candidate(&self, voted_for: usize) {
        self.state.borrow_mut().role = Role::Candidate;
        self.state.borrow_mut().term += 1;
        self.state.borrow_mut().voted_for = Some(voted_for);
        self.state.borrow_mut().last_receive_time = SystemTime::now();
    }

    fn convert_to_follower(&self, new_term: u64) {
        self.state.borrow_mut().role = Role::Follower;
        self.state.borrow_mut().term = new_term;
        self.state.borrow_mut().voted_for = None;
        self.state.borrow_mut().last_receive_time = SystemTime::now();
    }

    fn convert_to_leader(&self, leader_id: usize) {
        self.state.borrow_mut().role = Role::Leader;
        self.state.borrow_mut().leader_id = Some(leader_id);
        self.state.borrow_mut().is_leader = true;
        self.state.borrow_mut().last_receive_time = SystemTime::now();
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
            return;
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
    }

    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns Ok(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/lib.rs for more details.
    fn send_request_vote(
        &self,
        server: usize,
        args: &RequestVoteArgs,
    ) -> Receiver<Result<RequestVoteReply>> {
        let (tx, rx) = sync_channel::<Result<RequestVoteReply>>(1);
        let peer = &self.peers[server];
        peer.spawn(
            peer.request_vote(&args)
                .map_err(Error::Rpc)
                .then(move |res| {
                    tx.send(res).unwrap();
                    Ok(())
                }),
        );
        rx
    }

    fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let index = 0;
        let term = 0;
        let is_leader = true;
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).

        if is_leader {
            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
    }

    fn kill(&self) {
        self.dead.store(true, Ordering::SeqCst);
    }
}

impl Raft {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = self.start(&0);
        let _ = self.send_request_vote(0, &Default::default());
        self.persist();
        let _ = &self.state;
        let _ = &self.me;
        let _ = &self.persister;
        let _ = &self.peers;
    }
}
