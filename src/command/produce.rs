use super::Command;
use crate::broker::Broker;
use crate::error::BrokerError;
use crate::protocol::{Message, Request, Response};
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

        // Append to in-memory log
        {
            let mut log = broker.log.lock().await;
            let topic_entry = log.entry(topic.clone()).or_insert_with(HashMap::new);
            let part_entry = topic_entry.entry(partition).or_insert_with(Vec::new);
            part_entry.push(message.clone());
        }
        append_message(&broker.data_dir, &topic, partition, &message).await?;

        // Replicate to other brokers
        let brokers = broker.cluster_brokers.clone();
        let my_id = broker.my_id;
        let topic_clone = topic.clone();
        let message_clone = message.clone();

        // Spawn replication in background
        tokio::spawn(async move {
            for (i, addr) in brokers.iter().enumerate() {
                if i == my_id {
                    continue; // Don't replicate to self
                }
                if let Err(_e) =
                    replicate_to_peer(addr, &topic_clone, partition, &message_clone).await
                {
                    error!("Replication to {} failed", addr);
                } else {
                    info!("Replicated message to {}", addr);
                }
            }
        });

        Ok(Response {
            status: "ok".to_string(),
            msg: Some(message),
            error: None,
            next_offset: None,
        })
    }
}

// Helper function to replicate to a peer broker
async fn replicate_to_peer(
    addr: &str,
    topic: &str,
    partition: u32,
    message: &Message,
) -> Result<(), BrokerError> {
    let mut stream = TcpStream::connect(addr).await?;
    let req = Request {
        cmd: "replicate".to_string(),
        topic: Some(topic.to_string()),
        partition: Some(partition),
        msg: None,
        offset: None,
        message: Some(message.clone()),
    };
    let line = serde_json::to_string(&req)?;
    stream.write_all(line.as_bytes()).await?;
    stream.write_all(b"\n").await?;

    // Optional, read the response but do nothing
    let mut reader = BufReader::new(stream).lines();
    let _ = reader.next_line().await;

    Ok(())
}
