use crate::error::BrokerError;
use crate::protocol::Message;
use regex::Regex;
use std::collections::HashMap;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

pub type Log = HashMap<String, HashMap<u32, Vec<Message>>>;

/// Loads all messages from disk into memory.
/// Returns the log and the highest message ID found.
pub async fn load_log(data_dir: &str) -> Result<(Log, u64), BrokerError> {
    let mut log: Log = HashMap::new();
    let re = Regex::new(r"^topic_(.+)_part_(\d+)\.txt$")?;
    let mut dir = fs::read_dir(data_dir).await?;
    let mut max_id = 0u64;

    while let Some(entry) = dir
        .next_entry()
        .await
        .map_err(|e| BrokerError::IoError(format!("Dir entry error: {}", e)))?
    {
        let path = entry.path();
        let fname = path.file_name().unwrap().to_string_lossy();
        if let Some(caps) = re.captures(&fname) {
            let topic = caps[1].to_string();
            let partition: u32 = caps[2].parse().unwrap();
            let file = File::open(&path)
                .await
                .map_err(|e| BrokerError::FileOpenError(format!("{}", e)))?;
            let reader = BufReader::new(file);
            let mut lines = reader.lines();
            let mut messages = Vec::new();
            while let Some(line) = lines
                .next_line()
                .await
                .map_err(|e| BrokerError::IoError(format!("Read line error: {}", e)))?
            {
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
    Ok((log, max_id))
}

/// Appends a message to the appropriate file.
pub async fn append_message(
    data_dir: &str,
    topic: &str,
    partition: u32,
    message: &Message,
) -> Result<(), BrokerError> {
    let filename = format!("{}/topic_{}_part_{}.txt", data_dir, topic, partition);
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&filename)
        .await?;
    let line = serde_json::to_string(message)?;
    file.write_all(line.as_bytes())
        .await
        .map_err(|e| BrokerError::FileWriteError(e.to_string()))?;
    file.write_all(b"\n")
        .await
        .map_err(|e| BrokerError::FileWriteError(e.to_string()))?;
    Ok(())
}
