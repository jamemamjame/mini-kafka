use serde::{Deserialize, Serialize};
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
    msg: Option<String>,
    offset: Option<usize>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Response {
    status: String,
    msg: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let log_file = "log.txt";

    // Load messages from file at startup
    let mut messages = Vec::new();
    if let Ok(file) = File::open(log_file).await {
        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        while let Some(line) = lines.next_line().await? {
            messages.push(line);
        }
    }

    let log = Arc::new(Mutex::new(messages));
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

                if req.cmd == "produce" {
                    if let Some(msg) = req.msg {
                        // Append to in-memory log
                        let mut log = log.lock().await;
                        log.push(msg.clone());

                        // Append to file
                        let mut file = OpenOptions::new()
                            .create(true)
                            .append(true)
                            .open(&log_file)
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
                    let offet = req.offset.unwrap_or(0);
                    let log = log.lock().await;
                    if offet < log.len() {
                        let resp = Response {
                            status: "ok".into(),
                            msg: Some(log[offet].clone()),
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
                }
            }
        });
    }
}
