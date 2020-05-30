use crate::raft::defs::{Action, Role};
use crate::raft::raft_peer::RaftPeer;
use crate::raft::{APPLY_INTERVAL, HEARTBEAT_INTERVAL};
use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender};
use rand::{thread_rng, Rng};
use std::ops::Sub;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

pub struct RaftSever {
    pub raft: RaftPeer,
    pub action_sender: UnboundedSender<Action>,
    pub action_receiver: Arc<Mutex<UnboundedReceiver<Action>>>,
}

impl RaftSever {
    #[tokio::main]
    pub async fn action_handler(&mut self) {
        let apply_timer_sender = self.action_sender.clone();
        let dead_for_apply_timer = Arc::clone(&self.raft.dead);
        thread::spawn(|| RaftSever::apply_timer(apply_timer_sender, dead_for_apply_timer));
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
                            info!("{}: Got a request vote action", self.raft.me);
                            let reply = self.raft.handle_request_vote(&args);
                            sender.send(reply).unwrap_or_else(|_| {
                                info!("PRC ERROR {}: send RequestVoteReply error", self.raft.me);
                            })
                        }
                        Action::AppendLogs(args, sender) => {
                            let reply = self.raft.handle_append_logs(&args);
                            sender.send(reply).unwrap_or_else(|_| {
                                info!("PRC ERROR {}: send AppendLogsReply error", self.raft.me);
                            })
                        }
                        Action::ElectionSuccess => {
                            info!("{}: Election success", self.raft.me);
                            self.raft.convert_to_leader();
                            let is_leader = Arc::clone(&self.raft.is_leader);
                            let sender = self.action_sender.clone();
                            self.raft.append_logs_to_peers(sender.clone());
                            thread::spawn(|| RaftSever::append_timer(sender, is_leader));
                        }
                        Action::Start(command_buf, sender) => {
                            info!("{}: Got a start action", self.raft.me);
                            let result = self.raft.start(command_buf);
                            sender.send(result).unwrap_or_else(|_| {
                                info!("PRC ERROR {}: send Start result error", self.raft.me);
                            })
                        }
                        Action::Apply => self.raft.apply(),
                        Action::StartAppendLogs => {
                            let sender = self.action_sender.clone();
                            self.raft.append_logs_to_peers(sender);
                        }
                        Action::AppendLogsResult(reply) => {
                            self.raft.handle_append_logs_reply(reply);
                        }
                        Action::ElectionFailed(reply) => {
                            self.raft.last_receive_time = Instant::now();
                            self.raft.convert_to_follower(reply.term)
                        }
                    },
                    None => info!("Got a none msg"),
                }
            }

            let now = Instant::now();
            if self.raft.role == Role::Follower
                && self
                    .raft
                    .last_receive_time
                    .checked_duration_since(now.sub(Duration::from_millis(
                        HEARTBEAT_INTERVAL * 5 + thread_rng().gen_range(0, 300),
                    )))
                    .is_none()
            {
                info!("{}: Got a kick off election action", self.raft.me);
                self.raft.convert_to_candidate();
                let sender = self.action_sender.clone();
                self.raft.kick_off_election(sender.clone());
            }
        }
    }

    fn apply_timer(action_sender: UnboundedSender<Action>, dead: Arc<AtomicBool>) {
        loop {
            if dead.load(Ordering::SeqCst) {
                return;
            }
            if !action_sender.is_closed() {
                action_sender
                    .clone()
                    .unbounded_send(Action::Apply)
                    .map_err(|_| ())
                    .unwrap_or_else(|_| ());
            }
            thread::sleep(Duration::from_millis(APPLY_INTERVAL))
        }
    }

    fn append_timer(action_sender: UnboundedSender<Action>, is_leader: Arc<AtomicBool>) {
        loop {
            if !is_leader.load(Ordering::SeqCst) {
                return;
            }
            if !action_sender.is_closed() {
                action_sender
                    .clone()
                    .unbounded_send(Action::StartAppendLogs)
                    .map_err(|_| ())
                    .unwrap_or_else(|_| ());
            }
            thread::sleep(Duration::from_millis(HEARTBEAT_INTERVAL))
        }
    }
}
