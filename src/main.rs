mod broker;
mod command;
mod config;
mod error;
mod protocol;
mod raft;
mod storage;

use crate::broker::Broker;
use crate::error::BrokerError;
use crate::protocol::{Request, Response};
use crate::raft::{RaftRole, RequestVoteArgs, RequestVoteReply};
use crate::storage::load_log;
use log::{debug, info, warn};
use std::env;
use std::sync::Arc;
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

    let data_dir = &config.data_dirs[broker_id];
    std::fs::create_dir_all(data_dir)?; // Ensure all directory exists

    let my_addr = &config.brokers[broker_id];
    let (log, max_id) = load_log(data_dir).await?;
    let broker = Broker::new(
        log,
        max_id + 1,
        config.brokers.clone(),
        broker_id,
        data_dir.to_string(),
    );
    // Start the Raft election timer/task!
    let broker_arc = Arc::new(broker);
    broker_arc.clone().start_raft_election_timer();

    let listener = TcpListener::bind(my_addr)
        .await
        .map_err(|e| BrokerError::IoError(format!("Failed to bind: {}", e)))?;
    info!("Broker listening on {}", my_addr);

    loop {
        let (socket, addr) = listener
            .accept()
            .await
            .map_err(|e| BrokerError::IoError(format!("Failed to accept: {}", e)))?;
        debug!("Accepted connection from {}", addr);
        let broker = broker_arc.clone();

        tokio::spawn(async move {
            let (reader, mut writer) = socket.into_split();
            let mut reader = BufReader::new(reader).lines();

            while let Ok(Some(line)) = reader.next_line().await {
                debug!("Received line: {}", line);

                // Handle Raft RequestVote RPC
                if let Ok(("RequestVote", args)) =
                    serde_json::from_str::<(&str, RequestVoteArgs)>(&line)
                {
                    let mut raft = broker.raft_state.lock().await;
                    let mut vote_granted: bool = false;
                    if args.term > raft.current_term {
                        raft.current_term = args.term;
                        raft.voted_for = None;
                        raft.role = RaftRole::Follower;
                    }
                    if raft.voted_for.is_none() || raft.voted_for == Some(args.candidate_id) {
                        raft.voted_for = Some(args.candidate_id);
                        vote_granted = true;
                    }
                    let reply = RequestVoteReply {
                        term: raft.current_term,
                        vote_granted,
                    };
                    let reply_json = match serde_json::to_string(&reply) {
                        Ok(j) => j,
                        Err(e) => {
                            warn!("Failed to serialize reply: {}", e);
                            continue;
                        }
                    };
                    if let Err(e) = writer.write_all(reply_json.as_bytes()).await {
                        warn!("Failed to write reply: {}", e);
                        continue;
                    }
                    if let Err(e) = writer.write_all(b"\n").await {
                        warn!("Failed to write newline: {}", e);
                        continue;
                    }
                    continue;
                }

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
