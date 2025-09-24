use super::Command;
use crate::broker::Broker;
use crate::error::BrokerError;
use crate::protocol::{Request, Response};
use async_trait::async_trait;

pub struct ConsumeCommand {
    pub req: Request,
}

#[async_trait]
impl Command for ConsumeCommand {
    async fn execute(&self, broker: &Broker) -> Result<Response, BrokerError> {
        let topic = self.req.topic.clone().ok_or(BrokerError::MissingTopic)?;
        let partition = self.req.partition.unwrap_or(0);
        let offset = self.req.offset.unwrap_or(0);
        let log = broker.log.lock().await;
        if let Some(topic_map) = log.get(&topic) {
            if let Some(messages) = topic_map.get(&partition) {
                if offset < messages.len() {
                    let message = messages[offset].clone();
                    return Ok(Response {
                        status: "ok".to_string(),
                        msg: Some(message),
                        error: None,
                        next_offset: Some(offset + 1),
                    });
                } else {
                    return Err(BrokerError::InvalidOffset {
                        requested: offset,
                        max: messages.len().saturating_sub(1),
                    });
                }
            } else {
                return Err(BrokerError::PartitionNotFound(partition));
            }
        } else {
            return Err(BrokerError::TopicNotFound(topic));
        }
    }
}
