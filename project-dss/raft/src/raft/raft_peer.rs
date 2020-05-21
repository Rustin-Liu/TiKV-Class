use futures::channel::mpsc::UnboundedSender;

use crate::proto::raftpb::*;
use crate::raft::defs::{Action, ApplyMsg, Role};
use crate::raft::errors::Error::Others;
use crate::raft::errors::{Error, Result};
use crate::raft::persister::Persister;
use crate::raft::PRC_TIMEOUT;
use futures::channel::oneshot::{channel, Receiver};
use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryFutureExt};
use std::cmp;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::timeout;

// A single Raft peer.
pub struct RaftPeer {
    // RPC end points of all peers
    pub peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    pub persister: Mutex<Box<dyn Persister>>,
    // This peer's index into peers[]
    pub me: usize,
    // Current term.
    pub current_term: Arc<AtomicU64>,
    // Self is leader.
    pub is_leader: Arc<AtomicBool>,
    // Candidate that received in current term.
    pub voted_for: Option<usize>,
    // Peer current role.
    pub role: Role,
    // Is dead.
    pub dead: Arc<AtomicBool>,
    // Log entries.
    pub logs: Vec<LogEntry>,
    // Index of highest log entry known to be committed(initialized to 0, increases monotonically).
    pub committed_index: usize,
    // Index of highest log entry applied to state machine(initialized to 0, increases monotonically).
    pub last_applied_index: usize,
    // For each server, index of the next log entry to send to that server(initialized to leader last log index +1 ).
    pub next_indexes: Vec<usize>,
    // For each server, index of highest log entry to be replicated on server(initialized to 0, increases monotonically).
    pub matched_indexes: Vec<usize>,
    // Is a channel on which the tester or service expects Raft to send ApplyMsg messages.
    pub apply_ch: UnboundedSender<ApplyMsg>,
}

impl RaftPeer {
    /// The service or tester wants to create a Raft server. the ports
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
        let peers_len = peers.len();
        let mut rf = RaftPeer {
            peers,
            persister: Mutex::new(persister),
            me,
            current_term: Arc::new(AtomicU64::new(0)),
            is_leader: Arc::new(AtomicBool::new(false)),
            voted_for: None,
            role: Default::default(),
            dead: Arc::new(Default::default()),
            logs: vec![LogEntry {
                term: 0,
                index: 0,
                command_buf: vec![],
            }],
            committed_index: 0,
            last_applied_index: 0,
            next_indexes: (0..peers_len).map(|_| 0).collect(),
            matched_indexes: (0..peers_len).map(|_| 0).collect(),
            apply_ch,
        };
        rf.restore(&raft_state);
        rf
    }

    /// Convert it self to candidate.
    pub fn convert_to_candidate(&mut self) {
        self.role = Role::Candidate;
        self.current_term.fetch_add(1, Ordering::SeqCst);
        self.voted_for = Some(self.me);
        self.is_leader.store(false, Ordering::SeqCst);
    }

    /// Convert it self to follower.
    pub fn convert_to_follower(&mut self, new_term: u64) {
        self.role = Role::Follower;
        self.current_term.store(new_term, Ordering::SeqCst);
        self.voted_for = None;
        self.is_leader.store(false, Ordering::SeqCst);
    }

    /// Convert it self to leader and init index.
    pub fn convert_to_leader(&mut self) {
        self.init_index();
        self.role = Role::Leader;
        self.is_leader.store(true, Ordering::SeqCst);
    }

    // Init matched_indexes and next_indexes indexes.
    fn init_index(&mut self) {
        let logs_len = self.logs.len();
        for index in 0..self.peers.len() {
            self.matched_indexes[index] = 0;
            self.next_indexes[index] = logs_len;
        }
    }

    /// Save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
    }

    /// Restore previously persisted state.
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

    /// The service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns false. otherwise start the
    /// agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first return value is the index that the command will appear at
    /// if it's ever committed. the second return value is the current
    /// term. the third return value is true if this server believes it is
    /// the leader.
    ///
    pub fn start(&mut self, command_buf: Vec<u8>) -> Result<(u64, u64)> {
        let is_leader = self.is_leader.load(Ordering::SeqCst);
        // If not a leader, we can not do start this command.
        if !is_leader {
            return Err(Error::NotLeader);
        }

        let index = self.get_last_entry().index + 1;
        let me = self.me;
        let current_term = self.current_term.load(Ordering::SeqCst);
        // Init the log.
        let log = LogEntry {
            command_buf,
            term: current_term,
            index,
        };

        self.logs.push(log);
        // Update ourselves next indexes and matched indexes after we add a new log.
        // Because when we calculate the major index, we also need ourselves matched_indexes.
        self.next_indexes[me] = self.logs.len();
        self.matched_indexes[me] = self.logs.len() - 1;
        Ok((index as u64, current_term))
    }

    // Get peer's last entry.
    fn get_last_entry(&self) -> LogEntry {
        assert!(!self.logs.is_empty());
        self.logs[self.logs.len() - 1].clone()
    }

    // Update committed index.
    fn update_committed_index(&mut self) {
        let majority_index = RaftPeer::get_majority_same_index(self.matched_indexes.clone());
        let current_term = self.current_term.load(Ordering::SeqCst);
        if self.logs[majority_index].term == current_term && majority_index > self.committed_index {
            self.committed_index = majority_index;
        }
    }

    // Get majority same index.
    // Sort the indexed and get the middle one.
    fn get_majority_same_index(mut matched_index: Vec<usize>) -> usize {
        matched_index.sort();
        let index = matched_index.len() / 2;
        matched_index[index]
    }

    // Send request vote to peer.
    async fn send_request_vote(
        &self,
        peer_id: usize,
        args: &RequestVoteArgs,
    ) -> Result<RequestVoteReply> {
        let peer = &self.peers[peer_id];
        // We need to set timeout to prevent other requests from being blocked.
        let result = timeout(Duration::from_millis(PRC_TIMEOUT), peer.request_vote(args)).await;
        match result {
            Ok(result) => match result {
                Ok(result) => Ok(result),
                Err(_) => Err(Others(String::from("Request vote failed"))),
            },
            Err(_) => Err(Others(String::from("Request vote timeout"))),
        }
    }

    // Send append log to peer.
    fn send_append_log(
        &self,
        peer_id: usize,
        args: AppendLogsArgs,
    ) -> Receiver<Result<AppendLogsReply>> {
        let (sender, receiver) = channel::<Result<AppendLogsReply>>();
        let peer = &self.peers[peer_id];
        let client = peer.clone();
        peer.spawn(async move {
            let res = client.append_logs(&args).map_err(Error::Rpc).await;
            if !sender.is_canceled() {
                sender.send(res).unwrap_or_else(|_| ());
            }
        });
        receiver
    }

    /// Handle the vote request.
    pub fn request_vote_handler(&mut self, args: &RequestVoteArgs) -> RequestVoteReply {
        let mut reply = RequestVoteReply::default();
        let current_term = self.current_term.load(Ordering::SeqCst);

        // To check dose the candidate got a longer logs.
        let last_entry = self.get_last_entry();
        let is_more_update = (args.last_log_term == last_entry.term
            && args.last_log_index >= last_entry.index)
            || args.last_log_term > last_entry.term;

        reply.term = current_term;

        // If we get a term less than ourselves, we immediately refuse to vote for this candidate.
        if args.term < current_term {
            reply.vote_granted = false;
        } else if args.term >= current_term {
            self.convert_to_follower(args.term);
            if self.voted_for.is_none() && is_more_update {
                self.voted_for = Some(args.candidate_id as usize);
                reply.vote_granted = true;
            }
        }
        reply
    }

    /// Handler append logs request.
    pub fn append_logs_handler(&mut self, args: &AppendLogsArgs) -> AppendLogsReply {
        let me = self.me;
        let current_term = self.current_term.load(Ordering::SeqCst);
        let mut reply = AppendLogsReply {
            peer_id: me as u64,
            term: current_term,
            prev_log_index: args.prev_log_index,
            prev_log_term: args.prev_log_term,
            entries_len: args.entries.len() as u64,
            append_term: args.term,
            success: false,
        };

        // If we got a term less than ourselves term, we need reject the append request.
        if args.term < current_term {
            return reply;
        }

        // If we get a term more than ourselves term, we need covert ourselves be a follower.
        if args.term > current_term {
            self.convert_to_follower(args.term)
        }

        // Log got a conflict with leader log.
        if args.prev_log_index >= self.logs.len() as u64
            || self.logs[args.prev_log_index as usize].term != args.prev_log_term
        {
            // If our logs length longer than leader log, we need to delete the useless log.
            if args.prev_log_index < self.logs.len() as u64 {
                self.logs.truncate(args.prev_log_index as usize);
            }
            return reply;
        }
        // If no conflict with leader's log.
        // We just append the entries to our log.
        self.logs.truncate(args.prev_log_index as usize + 1);
        self.logs.extend_from_slice(&args.entries);
        // Update the committed index.
        if args.leader_committed_index > self.committed_index as u64 {
            self.committed_index =
                cmp::min(args.leader_committed_index, self.logs.len() as u64 - 1) as usize;
        }
        reply.success = true;
        reply
    }

    /// Kick off a election.
    ///
    /// Return is become leader.
    pub async fn kick_off_election(&mut self) -> bool {
        let me = self.me;
        let current_term = self.current_term.load(Ordering::SeqCst);

        // Get the last log index and term, we need these in request vote handler.
        let last_log = self.get_last_entry();
        let request_vote_args = RequestVoteArgs {
            term: current_term,
            candidate_id: me as u64,
            last_log_index: last_log.index,
            last_log_term: last_log.term,
        };

        // We default vote to ourselves.
        let mut vote_count = 1 as usize;
        let peers_len = self.peers.len();
        let mut success = false;

        for peer_id in 0..self.peers.len() {
            if peer_id != me && !success && self.role == Role::Candidate {
                let reply = self.send_request_vote(peer_id, &request_vote_args).await;
                if let Ok(reply) = reply {
                    if reply.vote_granted {
                        info!("{}: Got a granted from {}", self.me, peer_id);
                        vote_count += 1;
                        if vote_count * 2 > peers_len {
                            success = true;
                        }
                    }
                }
            }
        }
        if success {
            self.convert_to_leader();
            info!("{}: became leader on {}", self.me, current_term);
        }
        success
    }

    /// Append logs to peers.
    pub fn append_logs_to_peers(&mut self, action_sender: UnboundedSender<Action>) {
        let me = self.me;
        let mut futures: FuturesUnordered<Receiver<Result<AppendLogsReply>>> =
            FuturesUnordered::new();

        let result_receiver: Vec<Receiver<Result<AppendLogsReply>>> = self
            .peers
            .iter()
            .enumerate()
            .filter(|(peer_id, _)| *peer_id != me)
            .map(|(peer_id, _)| self.append_logs_to_peer(peer_id))
            .collect();
        for receiver in result_receiver {
            futures.push(receiver);
        }
        tokio::spawn(async move {
            for _ in 0..futures.len() {
                if let Some(reply) = futures.next().await {
                    if let Ok(reply) = reply {
                        if let Ok(reply) = reply {
                            if !action_sender.is_closed() {
                                action_sender
                                    .clone()
                                    .unbounded_send(Action::AppendLogsResult(reply))
                                    .map_err(|_| ())
                                    .unwrap_or_else(|_| ());
                            }
                        }
                    }
                }
            }
        });
    }

    fn append_logs_to_peer(&self, peer_id: usize) -> Receiver<Result<AppendLogsReply>> {
        let me = self.me;
        assert_ne!(peer_id, me);
        let current_term = self.current_term.load(Ordering::SeqCst);
        // Peer next need append log index.
        let peer_next_index = self.next_indexes[peer_id];
        // Peer pre next need append log index.
        let peer_prev_next_index = peer_next_index - 1;
        let append_logs_args = AppendLogsArgs {
            term: current_term,
            leader_id: me as u64,
            prev_log_index: peer_prev_next_index as u64,
            prev_log_term: self.logs[peer_prev_next_index].term,
            entries: self.logs.split_at(peer_next_index).1.to_vec(),
            leader_committed_index: self.committed_index as u64,
        };
        self.send_append_log(peer_id, append_logs_args)
    }

    pub fn handle_append_logs_reply(&mut self, reply: AppendLogsReply) {
        let current_term = self.current_term.load(Ordering::SeqCst);
        if self.role != Role::Leader || current_term != reply.append_term {
            return;
        }

        if reply.success {
            // Update matched indexes and next indexes.
            self.matched_indexes[reply.peer_id as usize] =
                (reply.prev_log_index + reply.entries_len) as usize;
            self.next_indexes[reply.peer_id as usize] =
                self.matched_indexes[reply.peer_id as usize] + 1;
            self.update_committed_index();
        } else {
            // If replay term more than current term, we should convert ourselves be a follower.
            if reply.term > current_term {
                self.convert_to_follower(reply.term);
            } else {
                // It means follower's log conflict with leader log.
                // So we need to fast back up.
                let mut prev_index = reply.prev_log_index as i64; // Get the previous log index.

                // We will back up to a index which is first index of previous log term.
                while prev_index >= 0 && self.logs[prev_index as usize].term == reply.prev_log_term
                {
                    prev_index -= 1;
                }
                self.next_indexes[reply.peer_id as usize] = (prev_index + 1) as usize
            }
        }
    }

    /// Apply the msg.
    pub fn apply(&mut self) {
        while self.last_applied_index < self.committed_index {
            self.last_applied_index += 1;
            let msg = ApplyMsg {
                command_valid: true,
                command: self.logs[self.last_applied_index].clone().command_buf,
                command_index: self.last_applied_index as u64,
            };
            self.apply_ch.unbounded_send(msg).unwrap_or_else(|_| ());
        }
    }
}

impl RaftPeer {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        self.persist();
        let _ = &self.persister;
    }
}
