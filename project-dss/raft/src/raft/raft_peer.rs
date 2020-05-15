use futures::channel::mpsc::UnboundedSender;

use crate::proto::raftpb::*;
use crate::raft::defs::{ApplyMsg, Role};
use crate::raft::errors::Error::Others;
use crate::raft::errors::{Error, Result};
use crate::raft::persister::Persister;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::runtime::Runtime;

// A single Raft peer.
pub struct RaftPeer {
    // RPC end points of all peers
    pub peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    pub persister: Mutex<Box<dyn Persister>>,
    // this peer's index into peers[]
    pub me: usize,
    pub current_term: Arc<AtomicU64>,
    pub is_leader: Arc<AtomicBool>,
    pub voted_for: Option<usize>,
    // Peer current role.
    pub role: Role,
    pub dead: Arc<AtomicBool>,
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
            current_term: Arc::new(AtomicU64::new(0)),
            is_leader: Arc::new(AtomicBool::new(false)),
            voted_for: None,
            role: Default::default(),
            dead: Arc::new(Default::default()),
            apply_ch,
        };
        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        rf
    }

    pub fn convert_to_candidate(&mut self) {
        self.role = Role::Candidate;
        self.current_term.fetch_add(1, Ordering::SeqCst);
        self.voted_for = Some(self.me);
        self.is_leader.store(false, Ordering::SeqCst);
    }

    pub fn convert_to_follower(&mut self, new_term: u64) {
        self.role = Role::Follower;
        self.current_term.store(new_term, Ordering::SeqCst);
        self.voted_for = None;
        self.is_leader.store(false, Ordering::SeqCst);
    }

    pub fn convert_to_leader(&mut self) {
        self.role = Role::Leader;
        self.is_leader.store(true, Ordering::SeqCst);
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

    async fn send_request_vote(
        &self,
        server: usize,
        args: &RequestVoteArgs,
    ) -> Result<RequestVoteReply> {
        let peer = &self.peers[server];
        let result = peer.request_vote(args).await;
        match result {
            Ok(result) => Ok(result),
            Err(_) => Err(Others(String::from("Request vote failed"))),
        }
    }

    async fn send_append_log(
        &self,
        server: usize,
        args: AppendLogsArgs,
    ) -> Result<AppendLogsReply> {
        let peer = &self.peers[server];
        let result = peer.append_logs(&args).await;
        match result {
            Ok(result) => Ok(result),
            Err(_) => Err(Others(String::from("Request vote failed"))),
        }
    }

    pub fn request_vote_handler(&mut self, args: &RequestVoteArgs) -> RequestVoteReply {
        let mut reply = RequestVoteReply::default();
        let current_term = self.current_term.load(Ordering::SeqCst);
        reply.term = current_term;
        if args.term < current_term {
            reply.vote_granted = false;
        } else {
            if args.term > current_term {
                self.convert_to_follower(args.term);
            }
            if self.voted_for.is_none() {
                self.voted_for = Some(args.candidate_id as usize);
                reply.vote_granted = true;
            }
        }
        reply
    }

    pub fn append_logs_handler(&mut self, args: &AppendLogsArgs) -> AppendLogsReply {
        let current_term = self.current_term.load(Ordering::SeqCst);
        if args.term < current_term {
            return AppendLogsReply {
                term: current_term,
                success: false,
            };
        }
        if args.term > current_term {
            self.convert_to_follower(args.term)
        }
        AppendLogsReply {
            term: current_term,
            success: true,
        }
    }

    pub fn kick_off_election(&mut self) -> bool {
        let me = self.me;
        let current_term = self.current_term.load(Ordering::SeqCst);
        let request_vote_args = RequestVoteArgs {
            term: current_term,
            candidate_id: me as u64,
        };
        let mut vote_count = 1 as usize;
        let peers_len = self.peers.len();
        let mut success = false;
        let mut runtime = Runtime::new().unwrap();
        // FIXME: maybe have better way.
        self.peers
            .iter()
            .enumerate()
            .filter(|(peer_id, _)| *peer_id != me)
            .for_each(|(peer_id, _)| {
                runtime.block_on(async {
                    if !success {
                        let reply = self.send_request_vote(peer_id, &request_vote_args).await;
                        if let Ok(reply) = reply {
                            if reply.vote_granted {
                                info!("{}: Got a granted from {}", self.me, peer_id);
                                vote_count += 1;
                                if vote_count * 2 > peers_len {
                                    success = true;
                                    info!("{}: became leader on {}", self.me, current_term);
                                }
                            }
                        }
                    }
                })
            });
        if success {
            self.convert_to_leader();
        }
        success
    }

    pub fn append_logs_to_peers(&mut self) {
        let me = self.me;
        let peers_len = self.peers.iter().len();
        let current_term = self.current_term.load(Ordering::SeqCst);
        let mut runtime = Runtime::new().unwrap();
        for peer_id in 0..peers_len {
            if peer_id != me {
                let append_logs_args = AppendLogsArgs {
                    term: current_term,
                    leader_id: me as u64,
                };
                runtime.block_on(async {
                    let reply = self.send_append_log(peer_id, append_logs_args).await;
                    if let Ok(reply) = reply {
                        if reply.term > current_term {
                            self.convert_to_follower(reply.term);
                        }
                        return;
                    }
                });
            }
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
