use super::Command;
use crate::broker::Broker;
use crate::error::BrokerError;
use crate::protocol::{Message, Request, Response};
use crate::raft::LogEntry;
use crate::storage::append_message;
use async_trait::async_trait;
use log::{error, info};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

pub struct ProduceCommand {
    pub req: Request,
}

#[async_trait]
impl Command for ProduceCommand {
    async fn execute(&self, broker: &Broker) -> Result<Response, BrokerError> {
        let topic = self.req.topic.clone().ok_or(BrokerError::MissingTopic)?;
        let partition = self.req.partition.unwrap_or(0);

        // If not leader, forward to leader
        if !broker.is_leader(partition) {
            let leader_addr = broker.get_leader_addr_by_partition(partition);
            info!(
                "Not leader for partition {}. Forwarding to leader at {}",
                partition, leader_addr
            );

            // Forward the request to the leader
            let mut stream = TcpStream::connect(leader_addr)
                .await
                .map_err(|e| BrokerError::IoError(format!("Failed to connect to leader: {}", e)))?;
            let req_json = serde_json::to_string(&self.req).map_err(|e| {
                BrokerError::SerdeError(format!("Failed to serialize request: {}", e))
            })?;
            stream
                .write_all(req_json.as_bytes())
                .await
                .map_err(|e| BrokerError::IoError(format!("Failed to send request: {}", e)))?;
            stream
                .write_all(b"\n")
                .await
                .map_err(|e| BrokerError::IoError(format!("Failed to send newline: {}", e)))?;

            // Read the response from the leader
            let mut reader = BufReader::new(stream).lines();
            if let Some(line) = reader
                .next_line()
                .await
                .map_err(|e| BrokerError::IoError(format!("Failed to read response: {}", e)))?
            {
                let resp: Response = serde_json::from_str(&line).map_err(|e| {
                    BrokerError::SerdeError(format!("Failed to parse response: {}", e))
                })?;
                return Ok(resp);
            } else {
                return Err(BrokerError::IoError("No response from leader".to_string()));
            }
        }

        let value = self.req.msg.clone().ok_or(BrokerError::MissingMessage)?;

        // Generate message metadata
        let mut id_counter = broker.id_counter.lock().await;
        let id = *id_counter;
        *id_counter += 1;
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let message = Message {
            id,
            timestamp,
            value,
        };

        // Create a Raft log entry
        let log_entry = LogEntry {
            term: broker.raft_state.lock().await.current_term,
            message: message.clone(),
        };
        broker.replicate_log_entry(log_entry).await;

        // Append to in-memory log
        {
            let mut log = broker.log.lock().await;
            let topic_entry = log.entry(topic.clone()).or_insert_with(HashMap::new);
            let part_entry = topic_entry.entry(partition).or_insert_with(Vec::new);
            part_entry.push(message.clone());
        }
        append_message(&broker.data_dir, &topic, partition, &message).await?;

        Ok(Response {
            status: "ok".to_string(),
            msg: Some(message),
            error: None,
            next_offset: None,
        })
    }
}
