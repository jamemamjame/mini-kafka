use crate::command::{Command, ConsumeCommand, ProduceCommand};
use crate::error::BrokerError;
use crate::protocol::{Request, Response};
use crate::storage::Log;
use log::{error, info};
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
        info!("Processing {} command", req.cmd);
        let result = match req.cmd.as_str() {
            "produce" => {
                let cmd = ProduceCommand { req };
                cmd.execute(self).await
            }
            "consume" => {
                let cmd = ConsumeCommand { req };
                cmd.execute(self).await
            }
            _ => Err(BrokerError::InvalidCommand(req.cmd)),
        };

        match result {
            Ok(resp) => {
                info!("Command completed successfully");
                resp
            }
            Err(e) => {
                error!("Command failed: {}", e);
                Response {
                    status: "error".to_string(),
                    msg: None,
                    error: Some(e.to_string()),
                    next_offset: None,
                }
            }
        }
    }
}
