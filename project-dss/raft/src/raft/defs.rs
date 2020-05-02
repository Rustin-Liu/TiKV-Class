pub struct ApplyMsg {
    pub command_valid: bool,
    pub command: Vec<u8>,
    pub command_index: u64,
}

#[derive(Clone, Debug)]
pub enum Role {
    Follower,
    Leader,
    Candidate,
}

impl Default for Role {
    fn default() -> Self {
        Role::Follower
    }
}
