use crate::raft::defs::Action;
use crate::raft::raft_peer::RaftPeer;
use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender};
use rand::Rng;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const APPLY_INTERVAL: u64 = 300;

pub struct RaftSever {
    pub raft: RaftPeer,
    pub action_sender: UnboundedSender<Action>,
    pub action_receiver: Arc<Mutex<UnboundedReceiver<Action>>>,
    pub last_receive_time: Arc<Mutex<Instant>>,
}

impl RaftSever {
    pub fn action_handler(&mut self) {
        let mut msg_receiver = self.action_receiver.lock().unwrap();
        loop {
            if self.raft.dead.load(Ordering::SeqCst) {
                return;
            }
            let msg = msg_receiver.try_next();
            if let Ok(msg) = msg {
                match msg {
                    Some(msg) => match msg {
                        Action::RequestVote(args, sender) => {
                            debug!("{}: Got a request vote action", self.raft.me);
                            let reply = self.raft.request_vote_handler(&args);
                            if reply.vote_granted {
                                let mut last_update_time = self.last_receive_time.lock().unwrap();
                                *last_update_time = Instant::now();
                            }
                            sender.send(reply).unwrap_or_else(|_| {
                                debug!("{}: send RequestVoteReply error", self.raft.me);
                            })
                        }
                        Action::AppendLogs(args, sender) => {
                            debug!(
                                "{}: Got a append logs from {}",
                                self.raft.me, args.leader_id
                            );
                            let reply = self.raft.append_logs_handler(&args);
                            if reply.success {
                                let mut last_update_time = self.last_receive_time.lock().unwrap();
                                *last_update_time = Instant::now();
                            }
                            sender.send(reply).unwrap_or_else(|_| {
                                debug!("{}: send AppendLogsReply error", self.raft.me);
                            })
                        }
                        Action::KickOffElection => {
                            debug!("{}: Got a kick off election action", self.raft.me);
                            self.raft.convert_to_candidate();
                            let success = self.raft.kick_off_election();
                            if success {
                                self.raft.append_logs_to_peers();
                            }
                        }
                        Action::Start(command_buf, sender) => {
                            debug!("{}: Got a start action", self.raft.me);
                            let result = self.raft.start(command_buf);
                            sender.send(result).unwrap_or_else(|_| {
                                debug!("{}: send Start result error", self.raft.me);
                            })
                        }
                        Action::Apply => {
                            debug!("{}: Got a apply action", self.raft.me);
                            self.raft.apply()
                        }
                    },
                    None => info!("Got a none msg"),
                }
            }
            if self.raft.is_leader.load(Ordering::SeqCst) {
                self.raft.append_logs_to_peers();
            }
        }
    }

    pub fn election_timer(
        action_sender: UnboundedSender<Action>,
        is_leader: Arc<AtomicBool>,
        dead: Arc<AtomicBool>,
        last_receive_time: Arc<Mutex<Instant>>,
    ) {
        let mut rng = rand::thread_rng();
        loop {
            let start_time = Instant::now();
            let election_timeout = rng.gen_range(50, 300);
            thread::sleep(Duration::from_millis(election_timeout));
            if dead.load(Ordering::SeqCst) {
                return;
            }
            if !is_leader.load(Ordering::SeqCst) {
                let last_receive_time = last_receive_time.lock().unwrap();
                let timeout = last_receive_time
                    .checked_duration_since(start_time)
                    .is_none();
                if timeout && !action_sender.is_closed() {
                    action_sender
                        .clone()
                        .unbounded_send(Action::KickOffElection)
                        .map_err(|_| ())
                        .unwrap_or_else(|_| ());
                }
            }
        }
    }

    pub fn apply_timer(action_sender: UnboundedSender<Action>, dead: Arc<AtomicBool>) {
        loop {
            if dead.load(Ordering::SeqCst) {
                return;
            }
            thread::sleep(Duration::from_millis(APPLY_INTERVAL));
            if !action_sender.is_closed() {
                action_sender
                    .clone()
                    .unbounded_send(Action::Apply)
                    .map_err(|_| ())
                    .unwrap_or_else(|_| ());
            }
        }
    }
}
