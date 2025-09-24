use super::Command;
use crate::broker::Broker;
use crate::protocol::{Request, Response};
use async_trait::async_trait;

pub struct ConsumeCommand {
    pub req: Request,
}

#[async_trait]
impl Command for ConsumeCommand {
    async fn execute(&self, broker: &Broker) -> Response {
        let topic = match &self.req.topic {
            Some(t) => t.clone(),
            None => {
                return Response {
                    status: "error".to_string(),
                    msg: None,
                    error: Some("Missing topic".to_string()),
                    next_offset: None,
                }
            }
        };
        let partition = self.req.partition.unwrap_or(0);

        let offset = self.req.offset.unwrap_or(0);
        let log = broker.log.lock().await;
        if let Some(topic_map) = log.get(&topic) {
            if let Some(messages) = topic_map.get(&partition) {
                if offset < messages.len() {
                    let message = messages[offset].clone();
                    return Response {
                        status: "ok".to_string(),
                        msg: Some(message),
                        error: None,
                        next_offset: Some(offset + 1),
                    };
                } else {
                    Response {
                        status: "error".to_string(),
                        msg: None,
                        error: Some(format!(
                            "Offset {} out of range. Max offset: {}",
                            offset,
                            messages.len().saturating_sub(1)
                        )),
                        next_offset: Some(messages.len()),
                    }
                }
            } else {
                Response {
                    status: "error".to_string(),
                    msg: None,
                    error: Some(format!("Partition {} not found", partition)),
                    next_offset: None,
                }
            }
        } else {
            Response {
                status: "error".to_string(),
                msg: None,
                error: Some(format!("Topic '{}' not found", topic)),
                next_offset: None,
            }
        }
    }
}
