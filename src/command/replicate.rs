use super::Command;
use crate::broker::Broker;
use crate::error::BrokerError;
use crate::protocol::{Request, Response};
use crate::storage::append_message;
use async_trait::async_trait;
use std::collections::HashMap;

pub struct ReplicateCommand {
    pub req: Request,
}

#[async_trait]
impl Command for ReplicateCommand {
    async fn execute(&self, broker: &Broker) -> Result<Response, BrokerError> {
        let topic = self.req.topic.clone().ok_or(BrokerError::MissingTopic)?;
        let partition = self.req.partition.unwrap_or(0);
        let message = self
            .req
            .message
            .clone()
            .ok_or(BrokerError::MissingMessage)?;

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
