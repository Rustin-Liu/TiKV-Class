use crate::raft::defs::Action;
use crate::raft::raft_peer::RaftPeer;
use crate::raft::{APPLY_INTERVAL, HEARTBEAT_INTERVAL};
use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender};
use rand::{thread_rng, Rng};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tokio::time;

pub struct RaftSever {
    pub raft: RaftPeer,
    pub action_sender: UnboundedSender<Action>,
    pub action_receiver: Arc<Mutex<UnboundedReceiver<Action>>>,
    pub last_receive_time: Arc<Mutex<Instant>>,
}

impl RaftSever {
    #[tokio::main]
    pub async fn action_handler(&mut self) {
        let election_timer_sender = self.action_sender.clone();
        let apply_timer_sender = self.action_sender.clone();
        let is_leader_for_server = Arc::clone(&self.raft.is_leader);
        let dead_for_election_timer = Arc::clone(&self.raft.dead);
        let dead_for_apply_timer = Arc::clone(&self.raft.dead);
        let last_receive_time = Arc::clone(&self.last_receive_time);
        thread::spawn(|| {
            RaftSever::election_timer(
                election_timer_sender,
                is_leader_for_server,
                dead_for_election_timer,
                last_receive_time,
            )
        });
        tokio::spawn(RaftSever::apply_timer(
            apply_timer_sender,
            dead_for_apply_timer,
        ));
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
                            let mut last_update_time = self.last_receive_time.lock().unwrap();
                            *last_update_time = Instant::now();
                            sender.send(reply).unwrap_or_else(|_| {
                                info!("PRC ERROR {}: send RequestVoteReply error", self.raft.me);
                            })
                        }
                        Action::AppendLogs(args, sender) => {
                            let reply = self.raft.handle_append_logs(&args);
                            let mut last_update_time = self.last_receive_time.lock().unwrap();
                            *last_update_time = Instant::now();
                            sender.send(reply).unwrap_or_else(|_| {
                                info!("PRC ERROR {}: send AppendLogsReply error", self.raft.me);
                            })
                        }
                        Action::KickOffElection => {
                            info!("{}: Got a kick off election action", self.raft.me);
                            self.raft.convert_to_candidate();
                            let sender = self.action_sender.clone();
                            self.raft.kick_off_election(sender.clone());
                        }
                        Action::ElectionSuccess => {
                            info!("{}: Election success", self.raft.me);
                            self.raft.convert_to_leader();
                            let is_leader = Arc::clone(&self.raft.is_leader);
                            let sender = self.action_sender.clone();
                            self.raft.append_logs_to_peers(sender.clone());
                            tokio::spawn(RaftSever::append_timer(sender, is_leader));
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
                    },
                    None => info!("Got a none msg"),
                }
            }
        }
    }

    fn election_timer(
        action_sender: UnboundedSender<Action>,
        is_leader: Arc<AtomicBool>,
        dead: Arc<AtomicBool>,
        last_receive_time: Arc<Mutex<Instant>>,
    ) {
        let mut rng = thread_rng();
        loop {
            let start_time = Instant::now();
            thread::sleep(Duration::from_millis(200 + rng.gen_range(0, 300)));
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

    async fn apply_timer(action_sender: UnboundedSender<Action>, dead: Arc<AtomicBool>) {
        let mut interval = time::interval(Duration::from_millis(APPLY_INTERVAL));
        loop {
            interval.tick().await;
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
        }
    }

    async fn append_timer(action_sender: UnboundedSender<Action>, is_leader: Arc<AtomicBool>) {
        let mut interval = time::interval(Duration::from_millis(HEARTBEAT_INTERVAL));
        loop {
            interval.tick().await;
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
        }
    }
}
