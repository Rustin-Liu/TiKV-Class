use crate::proto::raftpb::*;
use crate::raft::defs::{Role, HEARTBEAT_INTERVAL};
use crate::raft::errors::*;
use crate::raft::{Raft, State};
use futures::future;
use labrpc::RpcFuture;
use rand::Rng;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Clone)]
pub struct Node {
    raft: Arc<Mutex<Raft>>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        let raft = Arc::new(Mutex::new(raft));
        let raft_c = Arc::clone(&raft);
        thread::spawn(|| {
            start_leader_election(raft_c);
        });
        Node { raft }
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        self.raft.lock().unwrap().start(command)
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.get_state().term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.get_state().is_leader()
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        self.raft.lock().unwrap().state.borrow().clone()
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        self.raft.lock().unwrap().kill();
    }
}

impl RaftService for Node {
    fn request_vote(&self, args: RequestVoteArgs) -> RpcFuture<RequestVoteReply> {
        let raft = self.raft.lock().unwrap();
        info!("{}: get vote request from {}", raft.me, args.candidate_id);
        let mut state = raft.state.borrow_mut();
        let mut reply = RequestVoteReply::default();
        reply.term = state.term;
        if args.term < state.term {
            reply.vote_granted = false;
        } else {
            if args.term > state.term {
                raft.convert_to_follower(args.term);
            }
            if state.voted_for.is_none() {
                state.voted_for = Some(args.candidate_id as usize);
                state.last_receive_time = Instant::now();
                reply.vote_granted = true;
                info!("{}: vote for {}", raft.me, args.candidate_id);
            }
        }
        Box::new(future::result(Ok(reply)))
    }

    fn append_log(&self, args: AppendLogArgs) -> RpcFuture<AppendLogReply> {
        let raft = self.raft.lock().unwrap();
        info!("{}: get append log from {}", raft.me, args.leader_id);
        let mut state = raft.state.borrow_mut();
        if args.term < state.term {
            return Box::new(future::result(Ok(AppendLogReply {
                term: state.term,
                success: false,
            })));
        }
        state.leader_id = Some(args.leader_id as usize);
        state.last_receive_time = Instant::now();

        if args.term > state.term {
            raft.convert_to_follower(args.term)
        }
        Box::new(future::result(Ok(AppendLogReply {
            term: state.term,
            success: true,
        })))
    }
}

fn start_leader_election(raft_arc: Arc<Mutex<Raft>>) {
    let mut rng = rand::thread_rng();
    info!("{}: started", raft_arc.lock().unwrap().me);
    loop {
        let start_time = Instant::now();
        info!("{}: start at {:?}", raft_arc.lock().unwrap().me, start_time);
        let election_timeout = rng.gen_range(0, 300);
        thread::sleep(Duration::from_millis(HEARTBEAT_INTERVAL + election_timeout));
        let raft = Arc::clone(&raft_arc);
        if raft.lock().unwrap().dead.load(Ordering::SeqCst) {
            return;
        }
        let last_receive_time = raft_arc.lock().unwrap().state.borrow().last_receive_time;
        info!(
            "{}: last receive time at {:?}",
            raft_arc.lock().unwrap().me,
            last_receive_time
        );
        thread::spawn(move || {
            let timeout = last_receive_time
                .checked_duration_since(start_time)
                .is_none();
            if timeout {
                kick_off_election(raft);
            }
        });
    }
}

fn kick_off_election(raft_arc: Arc<Mutex<Raft>>) {
    let raft = raft_arc.lock().unwrap();
    let state = raft.state.borrow().clone();
    info!("{}: kicks off election on term: {}", raft.me, state.term);
    raft.convert_to_candidate(raft.me);
    let request_vote_args = RequestVoteArgs {
        term: state.term,
        candidate_id: raft.me as u64,
    };
    let mut get_voted_num = 1;
    for (peer_id, _peer) in raft.peers.iter().enumerate() {
        if peer_id != raft.me {
            info!("{}: send vote request to {}", raft.me, peer_id);
            let receiver = raft.send_request_vote(peer_id, &request_vote_args);
            let reply = receiver.recv().unwrap().unwrap();
            if reply.term > state.term {
                raft.convert_to_follower(reply.term);
                return;
            }
            if reply.vote_granted {
                info!("{}: get granted from {}", raft.me, peer_id);
                get_voted_num += 1;
                if get_voted_num > raft.peers.len() / 2 && state.role == Role::Candidate {
                    info!("{}: became leader on term {}", raft.me, state.term);
                    raft.convert_to_leader(raft.me);
                    let raft_arc = Arc::clone(&raft_arc);
                    thread::spawn(|| {
                        replica_log_to_peers(raft_arc);
                    });
                }
            }
        }
    }
}

fn replica_log_to_peers(raft_arc: Arc<Mutex<Raft>>) {
    let raft = raft_arc.lock().unwrap();
    for (peer_id, _peer) in raft.peers.iter().enumerate() {
        if peer_id != raft.me {
            info!("{}: start send log to {}", raft.me, peer_id);
            let raft = Arc::clone(&raft_arc);
            thread::spawn(move || loop {
                let raft = raft.lock().unwrap();
                let state = raft.state.borrow();
                if state.role != Role::Leader {
                    return;
                }
                let append_log_args = AppendLogArgs {
                    term: state.term,
                    leader_id: raft.me as u64,
                };
                let receiver = raft.send_append_log(peer_id, &append_log_args);
                let reply = receiver.recv().unwrap().unwrap();
                info!("{}: get append reply form {}", raft.me, peer_id);
                if reply.term > state.term {
                    raft.convert_to_follower(reply.term);
                }
                thread::sleep(Duration::from_millis(HEARTBEAT_INTERVAL));
            });
        }
    }
}
