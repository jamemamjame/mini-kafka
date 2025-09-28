use serde::{Deserialize, Serialize};

pub type BrokerId = usize;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    pub command: String,
}

#[derive(Debug, Clone)]
pub struct RaftState {
    pub current_term: u64,
    pub voted_for: Option<BrokerId>,
    pub role: RaftRole,
    pub log: Vec<LogEntry>,
    pub commit_index: u64,
    pub last_applied: u64,
}

impl RaftState {
    pub fn new() -> Self {
        Self {
            current_term: 0,
            voted_for: None,
            role: RaftRole::Follower,
            log: vec![],
            commit_index: 0,
            last_applied: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteArgs {
    pub term: u64,
    pub candidate_id: BrokerId,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteReply {
    pub term: u64,
    pub vote_granted: bool,
}
