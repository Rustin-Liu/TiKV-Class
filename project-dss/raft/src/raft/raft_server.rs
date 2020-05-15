use crate::raft::defs::ActionMessage;
use crate::raft::raft_peer::RaftPeer;
use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender};
use std::sync::{Arc, Mutex};

pub struct RaftSever {
    pub raft: RaftPeer,
    pub msg_sender: UnboundedSender<ActionMessage>,
    pub msg_receiver: Arc<Mutex<UnboundedReceiver<ActionMessage>>>,
}

impl RaftSever {
    pub fn action_handler(&mut self) {
        let mut msg_receiver = self.msg_receiver.lock().unwrap();
        loop {
            let msg = msg_receiver.try_next().unwrap();
            match msg {
                Some(msg) => match msg {
                    ActionMessage::RequestVote(args, sender) => {
                        info!("Got a request vote msg");
                        let reply = self.raft.request_vote_handler(args);
                        sender.send(reply).unwrap_or_else(|_| {
                            debug!("send RequestVoteReply error");
                        })
                    }
                    ActionMessage::AppendLogs(args, sender) => {
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
