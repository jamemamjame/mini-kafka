use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Message {
    pub id: u64,
    pub timestamp: u64,
    pub value: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Request {
    pub cmd: String, // "produce", "consume", "replicate"
    pub topic: Option<String>,
    pub partition: Option<u32>,
    pub msg: Option<String>,
    pub offset: Option<usize>,
    // For broker comunication
    pub message: Option<Message>, // For replication
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Response {
    pub status: String,
    pub msg: Option<Message>,
    pub error: Option<String>,
    pub next_offset: Option<usize>,
}
