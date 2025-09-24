use super::Command;
use crate::broker::Broker;
use crate::error::BrokerError;
use crate::protocol::{Message, Request, Response};
use crate::storage::append_message;
use async_trait::async_trait;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct ProduceCommand {
    pub req: Request,
}

#[async_trait]
impl Command for ProduceCommand {
    async fn execute(&self, broker: &Broker) -> Result<Response, BrokerError> {
        let topic = self.req.topic.clone().ok_or(BrokerError::MissingTopic)?;
        let partition = self.req.partition.unwrap_or(0);
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
        let mut log = broker.log.lock().await;
        let topic_entry = log.entry(topic.clone()).or_insert_with(HashMap::new);
        let part_entry = topic_entry.entry(partition).or_insert_with(Vec::new);
        part_entry.push(message.clone());

        append_message(&topic, partition, &message).await?;

        Ok(Response {
            status: "ok".to_string(),
            msg: Some(message),
            error: None,
            next_offset: None,
        })
    }
}
