mod broker;
mod command;
mod config;
mod error;
mod protocol;
mod storage;

use crate::broker::Broker;
use crate::error::BrokerError;
use crate::protocol::{Request, Response};
use crate::storage::load_log;
use log::{debug, info, warn};
use std::env;
use tokio::io::AsyncWriteExt;
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::TcpListener,
};

#[tokio::main]
async fn main() -> Result<(), BrokerError> {
    env_logger::init();

    let config = config::BrokerConfig::from_file("broker_config.json")
        .map_err(|e| BrokerError::IoError(format!("Config error: {}", e)))?;
    info!("Loaded config: {:?}", config);

    let broker_id: usize = env::var("BROKER_ID")
        .expect("BROKER_ID env variable not set")
        .parse()
        .expect("BROKER_ID must be a valid integer");

    let my_addr = &config.brokers[broker_id];
    let (log, max_id) = load_log().await?;
    let broker = Broker::new(log, max_id + 1);
    let listener = TcpListener::bind(my_addr)
        .await
        .map_err(|e| BrokerError::IoError(format!("Failed to bind: {}", e)))?;
    info!("Broker listening on {}", my_addr);

    loop {
        let (socket, addr) = listener
            .accept()
            .await
            .map_err(|e| BrokerError::IoError(format!("Failed to accept: {}", e)))?;
        info!("Accepted connection from {}", addr);
        let broker = broker.clone();

        tokio::spawn(async move {
            let (reader, mut writer) = socket.into_split();
            let mut reader = BufReader::new(reader).lines();

            while let Ok(Some(line)) = reader.next_line().await {
                debug!("Received line: {}", line);
                let req: Request = match serde_json::from_str(&line) {
                    Ok(r) => r,
                    Err(e) => {
                        warn!("Invalid JSON received: {}", e);
                        let resp = Response {
                            status: "error".to_string(),
                            msg: None,
                            error: Some(format!("Invalid JSON: {}", e)),
                            next_offset: None,
                        };
                        let resp = serde_json::to_string(&resp).unwrap();
                        writer.write_all(resp.as_bytes()).await.ok();
                        writer.write_all(b"\n").await.ok();
                        continue;
                    }
                };

                let resp = broker.handle_request(req).await;
                let resp = serde_json::to_string(&resp).unwrap();
                writer.write_all(resp.as_bytes()).await.ok();
                writer.write_all(b"\n").await.ok();
            }
        });
    }
}
