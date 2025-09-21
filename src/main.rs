use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
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
    msg: Option<String>,
    offset: Option<usize>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Response {
    status: String,
    msg: Option<String>,
}

type Log = HashMap<String, Vec<String>>;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let log = Log::new();
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

                if req.cmd == "produce" {
                    if let Some(msg) = req.msg {
                        // Append to in-memory log
                        let mut log = log.lock().await;
                        let entry = log.entry(topic.clone()).or_insert(Vec::new());
                        entry.push(msg.clone());

                        // Append to file
                        let filename = format!("topic_{}.txt", topic);
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
                    if let Some(messages) = log.get(&topic) {
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
                }
            }
        });
    }
}
