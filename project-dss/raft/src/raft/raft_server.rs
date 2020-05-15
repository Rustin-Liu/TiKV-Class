use crate::raft::defs::Action;
use crate::raft::raft_peer::RaftPeer;
use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender};
use rand::Rng;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const HEARTBEAT_INTERVAL: u64 = 50;

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
            let msg = msg_receiver.try_next().unwrap();
            match msg {
                Some(msg) => match msg {
                    Action::RequestVote(args, sender) => {
                        info!("Got a request vote msg");
                        let reply = self.raft.request_vote_handler(args);
                        if reply.vote_granted {
                            let mut last_update_time = self.last_receive_time.lock().unwrap();
                            *last_update_time = Instant::now()
                        }
                        sender.send(reply).unwrap_or_else(|_| {
                            debug!("send RequestVoteReply error");
                        })
                    }
                    Action::AppendLogs(args, sender) => {
                        info!("Got a append logs msg");
                        let reply = self.raft.append_logs_handler(args);
                        if reply.success {
                            let mut last_update_time = self.last_receive_time.lock().unwrap();
                            *last_update_time = Instant::now()
                        }
                        sender.send(reply).unwrap_or_else(|_| {
                            debug!("send RequestVoteReply error");
                        })
                    }
                    Action::KickOffElection => {
                        self.raft.convert_to_candidate();
                        let _success = self.raft.kick_off_election();
                    }
                    _ => {}
                },
                None => info!("Got a none msg"),
            }
        }
    }

    pub fn election_timer(
        action_sender: UnboundedSender<Action>,
        dead: Arc<AtomicBool>,
        last_receive_time: Arc<Mutex<Instant>>,
    ) {
        let mut rng = rand::thread_rng();
        loop {
            let start_time = Instant::now();
            let election_timeout = rng.gen_range(0, 300);
            thread::sleep(Duration::from_millis(HEARTBEAT_INTERVAL + election_timeout));
            if dead.load(Ordering::SeqCst) {
                return;
            }
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
