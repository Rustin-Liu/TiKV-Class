use crate::raft::defs::Action;
use crate::raft::raft_peer::RaftPeer;
use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender};
use std::sync::{Arc, Mutex};

pub struct RaftSever {
    pub raft: RaftPeer,
    pub action_sender: UnboundedSender<Action>,
    pub action_receiver: Arc<Mutex<UnboundedReceiver<Action>>>,
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
                        sender.send(reply).unwrap_or_else(|_| {
                            debug!("send RequestVoteReply error");
                        })
                    }
                    Action::AppendLogs(args, sender) => {
                        info!("Got a append logs msg");
                        let reply = self.raft.append_logs_handler(args);
                        sender.send(reply).unwrap_or_else(|_| {
                            debug!("send RequestVoteReply error");
                        })
                    }
                },
                None => info!("Got a none msg"),
            }
        }
    }
}
