use crate::command::{Command, ConsumeCommand, ProduceCommand, ReplicateCommand};
use crate::error::BrokerError;
use crate::protocol::{Request, Response};
use crate::raft::{
    AppendEntriesArgs, AppendEntriesReply, RaftRole, RaftState, RequestVoteArgs, RequestVoteReply,
};
use crate::storage::Log;
use log::{debug, error, info};
use rand::Rng;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

#[derive(Debug, Clone)]
pub struct Broker {
    pub log: Arc<Mutex<Log>>,
    pub id_counter: Arc<Mutex<u64>>,
    pub cluster_brokers: Vec<String>,
    pub my_id: usize,
    pub data_dir: String,
    pub raft_state: Arc<Mutex<RaftState>>,
}

impl Broker {
    pub fn new(
        log: Log,
        id_counter: u64,
        cluster_brokers: Vec<String>,
        my_id: usize,
        data_dir: String,
    ) -> Self {
        Self {
            log: Arc::new(Mutex::new(log)),
            id_counter: Arc::new(Mutex::new(id_counter)),
            cluster_brokers,
            my_id,
            data_dir,
            raft_state: Arc::new(Mutex::new(RaftState::new())),
        }
    }

    pub fn start_raft(self: Arc<Self>) {
        // Election timer
        let broker_clone = self.clone();
        tokio::spawn(async move {
            loop {
                let timeout = rand::rng().random_range(1000..=5000); // NOTE: use high timeout value for easier debugging.
                sleep(Duration::from_millis(timeout)).await;

                let mut raft = broker_clone.raft_state.lock().await;
                let elapsed = raft.last_heartbeat.elapsed();
                if raft.role == RaftRole::Leader {
                    continue; // Leader doesn't start elections
                }
                if elapsed < Duration::from_millis(timeout) {
                    debug!("Heartbeat received, don't start election");
                    continue; // Heartbeat received, don't start election
                }

                // If no heartbeat received, start election
                raft.role = RaftRole::Candidate;
                raft.current_term += 1;
                raft.voted_for = Some(broker_clone.my_id);
                let current_term: u64 = raft.current_term;
                drop(raft); // Release lock before network ops

                // Send RequestVote RPCs to all other brokers
                let mut votes = 1; // Vote for self
                let num_brokers = broker_clone.cluster_brokers.len();
                let last_log_index = 0; // Simplified for now
                let last_log_term = 0; // Simplified for now
                for (i, addr) in broker_clone.cluster_brokers.iter().enumerate() {
                    if i == broker_clone.my_id {
                        continue;
                    }
                    let args = RequestVoteArgs {
                        term: current_term,
                        candidate_id: broker_clone.my_id,
                        last_log_index,
                        last_log_term,
                    };
                    if let Ok(reply) = send_request_vote(addr, &args).await {
                        if reply.vote_granted {
                            votes += 1;
                        }
                    }
                }

                // If majority, become leader
                if votes > num_brokers / 2 {
                    let mut raft = broker_clone.raft_state.lock().await;
                    raft.role = RaftRole::Leader;
                    info!(
                        "Broker {} became leader for term {}",
                        broker_clone.my_id, raft.current_term
                    );
                }
            }
        });

        // Heartbeat sender
        let broker_clone = self.clone();
        tokio::spawn(async move {
            use tokio::time::{sleep, Duration};
            loop {
                sleep(Duration::from_millis(50)).await;
                let raft = broker_clone.raft_state.lock().await;
                if raft.role != RaftRole::Leader {
                    continue; // Only leader sends heartbeats
                }
                let term = raft.current_term;
                drop(raft);

                let brokers = broker_clone.cluster_brokers.clone();
                let my_id = broker_clone.my_id;

                for (i, addr) in brokers.iter().enumerate() {
                    if i == my_id {
                        continue;
                    }
                    let args = AppendEntriesArgs {
                        term,
                        leader_id: my_id,
                        prev_log_index: 0,
                        prev_log_term: 0,
                        entries: vec![],
                        leader_commit: 0,
                    };
                    let addr = addr.clone();
                    // Fire and forget
                    tokio::spawn(async move {
                        let _ = send_append_entries(&addr, &args).await;
                    });
                }
            }
        });
    }

    pub async fn handle_request(&self, req: Request) -> Response {
        info!("Processing {} command", req.cmd);
        let result = match req.cmd.as_str() {
            "produce" => {
                let cmd = ProduceCommand { req };
                cmd.execute(self).await
            }
            "consume" => {
                let cmd = ConsumeCommand { req };
                cmd.execute(self).await
            }
            "replicate" => {
                let cmd = ReplicateCommand { req };
                cmd.execute(self).await
            }
            _ => Err(BrokerError::InvalidCommand(req.cmd)),
        };

        match result {
            Ok(resp) => {
                info!("Command completed successfully");
                resp
            }
            Err(e) => {
                error!("Command failed: {}", e);
                Response {
                    status: "error".to_string(),
                    msg: None,
                    error: Some(e.to_string()),
                    next_offset: None,
                }
            }
        }
    }

    pub fn is_leader(&self, partition: u32) -> bool {
        self.get_leader_id(partition) == self.my_id
    }

    pub fn get_leader_id(&self, partition: u32) -> usize {
        partition as usize % self.cluster_brokers.len()
    }

    pub fn get_leader_addr_by_partition(&self, partition: u32) -> &str {
        let leader_id = self.get_leader_id(partition);
        &self.cluster_brokers[leader_id]
    }
}

// Helper function to send RequestVote RPC
async fn send_request_vote(
    addr: &str,
    args: &RequestVoteArgs,
) -> Result<RequestVoteReply, std::io::Error> {
    let mut stream = TcpStream::connect(addr).await?;
    let msg = serde_json::to_string(&("RequestVote", args))?;
    stream.write_all(msg.as_bytes()).await?;
    stream.write_all(b"\n").await?;

    let mut reader = BufReader::new(stream).lines();
    if let Some(line) = reader.next_line().await? {
        let reply: RequestVoteReply = serde_json::from_str(&line)?;
        Ok(reply)
    } else {
        Err(std::io::Error::new(std::io::ErrorKind::Other, "No reply"))
    }
}

// Helper function to send AppendEntries RPC
async fn send_append_entries(
    addr: &str,
    args: &AppendEntriesArgs,
) -> Result<AppendEntriesReply, std::io::Error> {
    let mut stream = TcpStream::connect(addr).await?;
    let msg = serde_json::to_string(&("AppendEntries", args))?;
    stream.write_all(msg.as_bytes()).await?;
    stream.write_all(b"\n").await?;

    let mut reader = BufReader::new(stream).lines();
    if let Some(line) = reader.next_line().await? {
        let reply: AppendEntriesReply = serde_json::from_str(&line)?;
        Ok(reply)
    } else {
        Err(std::io::Error::new(std::io::ErrorKind::Other, "No reply"))
    }
}
