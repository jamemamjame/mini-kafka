use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::TcpListener,
};

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Message {
    id: u64,
    timestamp: u64,
    value: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct Request {
    cmd: String,
    topic: Option<String>,
    partition: Option<u32>,
    msg: Option<String>,
    offset: Option<usize>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Response {
    status: String,
    msg: Option<Message>,
    error: Option<String>,
    next_offset: Option<usize>,
}

// topic -> partition -> messages
type Log = HashMap<String, HashMap<u32, Vec<Message>>>;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load persisted log
    let mut log = Log::new();
    let re = Regex::new(r"^topic_(.+)_part_(\d+)\.txt$").unwrap();
    let mut dir = fs::read_dir(".").await?;
    let mut max_id = 0u64;

    while let Some(entry) = dir.next_entry().await? {
        let path = entry.path();
        let fname = path.file_name().unwrap().to_string_lossy();
        if let Some(caps) = re.captures(&fname) {
            let topic = caps[1].to_string();
            let partition: u32 = caps[2].parse().unwrap();
            let file = fs::File::open(&path).await?;
            let reader = BufReader::new(file);
            let mut lines = reader.lines();
            let mut messages = Vec::new();
            while let Some(line) = lines.next_line().await? {
                // Deserialize each line as Message
                if let Ok(msg) = serde_json::from_str::<Message>(&line) {
                    max_id = max_id.max(msg.id);
                    messages.push(msg);
                }
            }
            log.entry(topic)
                .or_insert_with(HashMap::new)
                .insert(partition, messages);
        }
    }

    let log = Arc::new(Mutex::new(log));
    let id_counter = Arc::new(Mutex::new(max_id + 1));
    let listener = TcpListener::bind("127.0.0.1:9000").await?;
    println!("Broker listening on 127.0.0.1:9000");

    loop {
        let (socket, _) = listener.accept().await?;
        let log = log.clone();
        let id_counter = id_counter.clone();

        tokio::spawn(async move {
            let (reader, mut writer) = socket.into_split();
            let mut reader = BufReader::new(reader).lines();

            while let Ok(Some(line)) = reader.next_line().await {
                let req: Request = match serde_json::from_str(&line) {
                    Ok(r) => r,
                    Err(e) => {
                        let resp = Response {
                            status: "error".into(),
                            msg: None,
                            error: Some(format!("Invalid JSON: {}", e)),
                            next_offset: None,
                        };
                        let resp = serde_json::to_string(&resp).unwrap();
                        writer.write_all(resp.as_bytes()).await.unwrap();
                        writer.write_all(b"\n").await.unwrap();
                        continue;
                    }
                };

                // Check required fields
                let topic = match req.topic {
                    Some(t) => t.clone(),
                    None => {
                        let resp = Response {
                            status: "error".into(),
                            msg: None,
                            error: Some("Missing topic".into()),
                            next_offset: None,
                        };
                        let resp = serde_json::to_string(&resp).unwrap();
                        writer.write_all(resp.as_bytes()).await.unwrap();
                        writer.write_all(b"\n").await.unwrap();
                        continue;
                    }
                };
                let partition = req.partition.unwrap_or(0);

                if req.cmd == "produce" {
                    let value = match req.msg {
                        Some(m) => m.clone(),
                        None => {
                            let resp = Response {
                                status: "error".into(),
                                msg: None,
                                error: Some("Missing message".into()),
                                next_offset: None,
                            };
                            let resp = serde_json::to_string(&resp).unwrap();
                            writer.write_all(resp.as_bytes()).await.ok();
                            writer.write_all(b"\n").await.ok();
                            continue;
                        }
                    };

                    // Generate message metadata
                    let mut id_counter = id_counter.lock().await;
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
                    let mut log = log.lock().await;
                    let topic_entry = log.entry(topic.clone()).or_insert_with(HashMap::new);
                    let part_entry = topic_entry.entry(partition).or_insert_with(Vec::new);
                    part_entry.push(message.clone());

                    // Append to file for this topic/partition
                    let filename = format!("topic_{}_part_{}.txt", topic, partition);
                    match OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(&filename)
                        .await
                    {
                        Ok(mut file) => {
                            let line = serde_json::to_string(&message).unwrap();
                            if let Err(e) = file.write_all(line.as_bytes()).await {
                                let resp = Response {
                                    status: "error".into(),
                                    msg: None,
                                    error: Some(format!("File write error: {}", e)),
                                    next_offset: None,
                                };
                                let resp = serde_json::to_string(&resp).unwrap();
                                writer.write_all(resp.as_bytes()).await.ok();
                                writer.write_all(b"\n").await.ok();
                                continue;
                            }
                            file.write_all(b"\n").await.ok();
                        }
                        Err(e) => {
                            let resp = Response {
                                status: "error".into(),
                                msg: None,
                                error: Some(format!("File open error: {}", e)),
                                next_offset: None,
                            };
                            let resp = serde_json::to_string(&resp).unwrap();
                            writer.write_all(resp.as_bytes()).await.ok();
                            writer.write_all(b"\n").await.ok();
                            continue;
                        }
                    }

                    let resp = Response {
                        status: "ok".into(),
                        msg: None,
                        error: None,
                        next_offset: None,
                    };
                    let resp = serde_json::to_string(&resp).unwrap();
                    writer.write_all(resp.as_bytes()).await.ok();
                    writer.write_all(b"\n").await.ok();
                } else if req.cmd == "consume" {
                    let offset = req.offset.unwrap_or(0);
                    let log = log.lock().await;
                    if let Some(topic_map) = log.get(&topic) {
                        if let Some(messages) = topic_map.get(&partition) {
                            if offset < messages.len() {
                                let message = messages[offset].clone();
                                let resp = Response {
                                    status: "ok".into(),
                                    msg: Some(message),
                                    error: None,
                                    next_offset: Some(offset + 1),
                                };
                                let resp = serde_json::to_string(&resp).unwrap();
                                writer.write_all(resp.as_bytes()).await.unwrap();
                                writer.write_all(b"\n").await.unwrap();
                            } else {
                                let resp = Response {
                                    status: "end".to_string(),
                                    msg: None,
                                    error: Some(format!(
                                        "Offset {} out of range. Max offset: {}",
                                        offset,
                                        messages.len().saturating_sub(1),
                                    )),
                                    next_offset: Some(messages.len()),
                                };
                                let resp = serde_json::to_string(&resp).unwrap();
                                writer.write_all(resp.as_bytes()).await.unwrap();
                                writer.write_all(b"\n").await.unwrap();
                            }
                        } else {
                            let resp = Response {
                                status: "error".to_string(),
                                msg: None,
                                error: Some(format!("Partition {} not found", partition)),
                                next_offset: None,
                            };
                            let resp = serde_json::to_string(&resp).unwrap();
                            writer.write_all(resp.as_bytes()).await.unwrap();
                            writer.write_all(b"\n").await.unwrap();
                        }
                    } else {
                        let resp = Response {
                            status: "error".to_string(),
                            msg: None,
                            error: Some(format!("Topic '{}' not found", topic)),
                            next_offset: None,
                        };
                        let resp = serde_json::to_string(&resp).unwrap();
                        writer.write_all(resp.as_bytes()).await.unwrap();
                        writer.write_all(b"\n").await.unwrap();
                    }
                } else {
                    let resp = Response {
                        status: "error".to_string(),
                        msg: None,
                        error: Some(format!("Unknown command: {}", req.cmd)),
                        next_offset: None,
                    };
                    let resp = serde_json::to_string(&resp).unwrap();
                    writer.write_all(resp.as_bytes()).await.ok();
                    writer.write_all(b"\n").await.ok();
                }
            }
        });
    }
}
