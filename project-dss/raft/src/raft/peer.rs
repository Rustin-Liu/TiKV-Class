use crate::proto::raftpb::*;
use crate::raft::defs::*;
use crate::raft::errors::*;
use crate::raft::persister::*;
use futures::future::*;
use futures::sync::mpsc::UnboundedSender;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{sync_channel, Receiver};
use std::time::Instant;

// A single Raft peer.
pub struct RaftPeer {
    // RPC end points of all peers
    pub peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    pub persister: Box<dyn Persister>,
    // this peer's index into peers[]
    pub me: usize,
    pub term: u64,
    pub is_leader: bool,
    pub voted_for: Option<usize>,
    // Peer current role.
    pub role: Role,
    pub leader_id: Option<usize>,
    pub last_receive_time: Instant,
    pub dead: AtomicBool,
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
        _apply_ch: UnboundedSender<ApplyMsg>,
    ) -> RaftPeer {
        let raft_state = persister.raft_state();
        let mut rf = RaftPeer {
            peers,
            persister,
            me,
            term: 0,
            is_leader: false,
            voted_for: None,
            role: Default::default(),
            leader_id: None,
            last_receive_time: Instant::now(),
            dead: AtomicBool::new(false),
        };
        // initialize from state persisted before a crash
        rf.restore(&raft_state);
        rf
    }

    pub fn convert_to_candidate(&mut self) {
        self.role = Role::Candidate;
        self.term += 1;
        self.voted_for = Some(self.me);
        self.last_receive_time = Instant::now();
    }

    pub fn convert_to_follower(&mut self, new_term: u64) {
        self.role = Role::Follower;
        self.term = new_term;
        self.voted_for = None;
        self.last_receive_time = Instant::now();
    }

    pub fn convert_to_leader(&mut self) {
        self.role = Role::Leader;
        self.leader_id = Some(self.me);
        self.is_leader = true;
        self.last_receive_time = Instant::now();
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    pub fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
    }

    /// restore previously persisted state.
    pub fn restore(&mut self, data: &[u8]) {
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
    pub fn send_request_vote(
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

    pub fn send_append_log(
        &self,
        server: usize,
        args: &AppendLogArgs,
    ) -> Receiver<Result<AppendLogReply>> {
        let (tx, rx) = sync_channel::<Result<AppendLogReply>>(1);
        let peer = &self.peers[server];
        peer.spawn(peer.append_log(&args).map_err(Error::Rpc).then(move |res| {
            tx.send(res).unwrap();
            Ok(())
        }));
        rx
    }

    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
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

    pub fn kill(&self) {
        self.dead.store(true, Ordering::SeqCst);
    }
}

impl RaftPeer {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        self.persist();
        let _ = &self.persister;
        let _ = &self.peers;
    }
}
