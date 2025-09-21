use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::fs;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::TcpListener,
};

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
    msg: Option<String>,
}

// topic -> partition -> messages
type Log = HashMap<String, HashMap<u32, Vec<String>>>;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut log = Log::new();

    // Regex to match filenames like topic_foo_part_0.txt
    let re = Regex::new(r"^topic_(.+)_part_(\d+)\.txt$").unwrap();

    // List files in the current directory
    let mut dir = fs::read_dir(".").await?;
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
                messages.push(line);
            }
            log.entry(topic)
                .or_insert_with(HashMap::new)
                .insert(partition, messages);
        }
    }

    let log = Arc::new(Mutex::new(log));
    let listener = TcpListener::bind("127.0.0.1:9000").await?;
    println!("Broker listening on 127.0.0.1:9000");

    loop {
        let (socket, _) = listener.accept().await?;
        let log = log.clone();

        tokio::spawn(async move {
            let (reader, mut writer) = socket.into_split();
            let mut reader = BufReader::new(reader).lines();

            while let Ok(Some(line)) = reader.next_line().await {
                let req: Request = match serde_json::from_str(&line) {
                    Ok(r) => r,
                    Err(_) => continue,
                };

                // Require topic for both produce and consume
                let topic = match req.topic {
                    Some(t) => t.clone(),
                    None => {
                        let resp = Response {
                            status: "error".into(),
                            msg: Some("Missing topic".into()),
                        };
                        let resp = serde_json::to_string(&resp).unwrap();
                        writer.write_all(resp.as_bytes()).await.unwrap();
                        writer.write_all(b"\n").await.unwrap();
                        continue;
                    }
                };
                let partition = req.partition.unwrap_or(0);

                if req.cmd == "produce" {
                    if let Some(msg) = req.msg {
                        // Append to in-memory log
                        let mut log = log.lock().await;
                        let topic_entry = log.entry(topic.clone()).or_insert(HashMap::new());
                        let part_entry = topic_entry.entry(partition).or_insert(Vec::new());
                        part_entry.push(msg.clone());

                        // Append to file
                        let filename = format!("topic_{}_part_{}.txt", topic, partition);
                        let mut file = OpenOptions::new()
                            .create(true)
                            .append(true)
                            .open(&filename)
                            .await
                            .unwrap();
                        file.write_all(msg.as_bytes()).await.unwrap();
                        file.write_all(b"\n").await.unwrap();

                        let resp = Response {
                            status: "ok".into(),
                            msg: None,
                        };
                        let resp = serde_json::to_string(&resp).unwrap();
                        writer.write_all(resp.as_bytes()).await.unwrap();
                        writer.write_all(b"\n").await.unwrap();
                    }
                } else if req.cmd == "consume" {
                    let offset = req.offset.unwrap_or(0);
                    let log = log.lock().await;
                    if let Some(topic_map) = log.get(&topic) {
                        if let Some(messages) = topic_map.get(&partition) {
                            if offset < messages.len() {
                                let resp = Response {
                                    status: "ok".into(),
                                    msg: Some(messages[offset].clone()),
                                };
                                let resp = serde_json::to_string(&resp).unwrap();
                                writer.write_all(resp.as_bytes()).await.unwrap();
                                writer.write_all(b"\n").await.unwrap();
                            } else {
                                let resp = Response {
                                    status: "end".to_string(),
                                    msg: None,
                                };
                                let resp = serde_json::to_string(&resp).unwrap();
                                writer.write_all(resp.as_bytes()).await.unwrap();
                                writer.write_all(b"\n").await.unwrap();
                            }
                        } else {
                            let resp = Response {
                                status: "end".to_string(),
                                msg: None,
                            };
                            let resp = serde_json::to_string(&resp).unwrap();
                            writer.write_all(resp.as_bytes()).await.unwrap();
                            writer.write_all(b"\n").await.unwrap();
                        }
                    } else {
                        let resp = Response {
                            status: "end".to_string(),
                            msg: None,
                        };
                        let resp = serde_json::to_string(&resp).unwrap();
                        writer.write_all(resp.as_bytes()).await.unwrap();
                        writer.write_all(b"\n").await.unwrap();
                    }
                }
            }
        });
    }
}
