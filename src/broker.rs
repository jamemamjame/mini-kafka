use crate::command::{Command, ConsumeCommand, ProduceCommand};
use crate::protocol::{Request, Response};
use crate::storage::Log;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct Broker {
    pub log: Arc<Mutex<Log>>,
    pub id_counter: Arc<Mutex<u64>>,
}

impl Broker {
    pub fn new(log: Log, id_counter: u64) -> Self {
        Self {
            log: Arc::new(Mutex::new(log)),
            id_counter: Arc::new(Mutex::new(id_counter)),
        }
    }

    pub async fn handle_request(&self, req: Request) -> Response {
        match req.cmd.as_str() {
            "produce" => {
                let cmd = ProduceCommand { req };
                cmd.execute(self).await
            }
            "consume" => {
                let cmd = ConsumeCommand { req };
                cmd.execute(self).await
            }
            _ => Response {
                status: "error".to_string(),
                msg: None,
                error: Some(format!("Unknown command: {}", req.cmd)),
                next_offset: None,
            },
        }
    }
}
