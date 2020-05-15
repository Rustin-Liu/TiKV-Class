use futures::channel::mpsc::UnboundedSender;

use crate::proto::raftpb::*;
use crate::raft::defs::{ApplyMsg, Role};
use crate::raft::errors::{Error, Result};
use crate::raft::persister::Persister;
use std::sync::atomic::AtomicBool;
use std::sync::Mutex;
use std::time::Instant;

// A single Raft peer.
pub struct RaftPeer {
    // RPC end points of all peers
    pub peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    pub persister: Mutex<Box<dyn Persister>>,
    // this peer's index into peers[]
    pub me: usize,
    pub term: u64,
    pub is_leader: bool,
    pub voted_for: Option<usize>,
    // Peer current role.
    pub role: Role,
    pub last_receive_time: Instant,
    pub dead: AtomicBool,
    pub apply_ch: UnboundedSender<ApplyMsg>,
}

impl RaftPeer {
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
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> RaftPeer {
        let raft_state = persister.raft_state();
        let mut rf = RaftPeer {
            peers,
            persister: Mutex::new(persister),
            me,
            term: 0,
            is_leader: false,
            voted_for: None,
            role: Default::default(),
            last_receive_time: Instant::now(),
            dead: Default::default(),
            apply_ch,
        };
        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        rf
    }

    pub fn convert_to_candidate(&mut self) {
        self.role = Role::Candidate;
        self.term += 1;
        self.voted_for = Some(self.me);
        self.is_leader = false;
        self.last_receive_time = Instant::now();
    }

    pub fn convert_to_follower(&mut self, new_term: u64) {
        self.role = Role::Follower;
        self.term = new_term;
        self.voted_for = None;
        self.is_leader = false;
        self.last_receive_time = Instant::now();
    }

    pub fn convert_to_leader(&mut self) {
        self.role = Role::Leader;
        self.is_leader = true;
        self.last_receive_time = Instant::now();
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
    async fn send_request_vote(
        &self,
        server: usize,
        args: &RequestVoteArgs,
    ) -> Result<RequestVoteReply> {
        let peer = &self.peers[server];
        let result = peer.request_vote(args).await;
        Ok(result.unwrap())
    }

    pub fn request_vote_handler(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        let mut reply = RequestVoteReply::default();
        reply.term = self.term;
        if args.term < self.term {
            reply.vote_granted = false;
        } else {
            if args.term > self.term {
                self.convert_to_follower(args.term);
            }
            if self.voted_for.is_none() {
                self.voted_for = Some(args.candidate_id as usize);
                self.last_receive_time = Instant::now();
                reply.vote_granted = true;
            }
        }
        reply
    }

    pub fn append_logs_handler(&mut self, args: AppendLogsArgs) -> AppendLogsReply {
        if args.term < self.term {
            return AppendLogsReply {
                term: self.term,
                success: false,
            };
        }
        self.last_receive_time = Instant::now();

        if args.term > self.term {
            self.convert_to_follower(args.term)
        }
        AppendLogsReply {
            term: self.term,
            success: true,
        }
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
}

impl RaftPeer {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = self.start(&0);
        let _ = self.send_request_vote(0, &Default::default());
        self.persist();
        let _ = &self.me;
        let _ = &self.persister;
        let _ = &self.peers;
    }
}
