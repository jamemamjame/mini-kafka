use serde::{Deserialize, Serialize};
use std::sync::Arc;
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
    let listener = TcpListener::bind("127.0.0.1:9000").await?;
    println!("Broker listening on 127.0.0.1:9000");

    // In-memory log messages
    let log = Arc::new(Mutex::new(Vec::<String>::new()));

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
                        let mut log = log.lock().await;
                        log.push(msg);
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
