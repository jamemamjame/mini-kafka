use crate::command::{Command, ConsumeCommand, ProduceCommand, ReplicateCommand};
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
    pub cluster_brokers: Vec<String>,
    pub my_id: usize,
    pub data_dir: String,
}

impl Broker {
    pub fn new(
        log: Log,
        id_counter: u64,
        cluster_brokers: Vec<String>,
        my_id: usize,
        data_dir: String,
    ) -> Self {
        Self {
            log: Arc::new(Mutex::new(log)),
            id_counter: Arc::new(Mutex::new(id_counter)),
            cluster_brokers,
            my_id,
            data_dir,
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
            "replicate" => {
                let cmd = ReplicateCommand { req };
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

    pub fn is_leader(&self, partition: u32) -> bool {
        self.get_leader_id(partition) == self.my_id
    }

    pub fn get_leader_id(&self, partition: u32) -> usize {
        partition as usize % self.cluster_brokers.len()
    }

    pub fn get_leader_addr_by_partition(&self, partition: u32) -> &str {
        let leader_id = self.get_leader_id(partition);
        &self.cluster_brokers[leader_id]
    }
}
