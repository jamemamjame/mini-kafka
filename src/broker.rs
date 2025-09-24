use crate::protocol::{Message, Request, Response};
use crate::storage::{append_message, Log};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
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
        let topic = match &req.topic {
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
        let partition = req.partition.unwrap_or(0);

        match req.cmd.as_str() {
            "produce" => {
                let value = match &req.msg {
                    Some(m) => m.clone(),
                    None => {
                        return Response {
                            status: "error".to_string(),
                            msg: None,
                            error: Some("Missing message".to_string()),
                            next_offset: None,
                        }
                    }
                };

                // Generate message metadata
                let mut id_counter = self.id_counter.lock().await;
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
                let mut log = self.log.lock().await;
                let topic_entry = log.entry(topic.clone()).or_insert_with(HashMap::new);
                let part_entry = topic_entry.entry(partition).or_insert_with(Vec::new);
                part_entry.push(message.clone());

                if let Err(e) = append_message(&topic, partition, &message).await {
                    return Response {
                        status: "error".to_string(),
                        msg: None,
                        error: Some(format!("File write error: {}", e)),
                        next_offset: None,
                    };
                }

                Response {
                    status: "ok".to_string(),
                    msg: Some(message),
                    error: None,
                    next_offset: None,
                }
            }
            "consume" => {
                let offset = req.offset.unwrap_or(0);
                let log = self.log.lock().await;
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
            _ => Response {
                status: "error".to_string(),
                msg: None,
                error: Some(format!("Unknown command: {}", req.cmd)),
                next_offset: None,
            },
        }
    }
}
