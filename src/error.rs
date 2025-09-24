use std::fmt;

#[derive(Debug)]
pub enum BrokerError {
    MissingTopic,
    MissingMessage,
    InvalidOffset { requested: usize, max: usize },
    PartitionNotFound(u32),
    TopicNotFound(String),
    FileWriteError(String),
    FileOpenError(String),
    InvalidCommand(String),
    SerdeError(String),
    IoError(String),
}

impl fmt::Display for BrokerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BrokerError::MissingTopic => write!(f, "Missing topic"),
            BrokerError::MissingMessage => write!(f, "Missing message"),
            BrokerError::InvalidOffset { requested, max } => {
                write!(f, "Offset {} out of range. Max offset: {}", requested, max)
            }
            BrokerError::PartitionNotFound(p) => write!(f, "Partition {} not found", p),
            BrokerError::TopicNotFound(t) => write!(f, "Topic '{}' not found", t),
            BrokerError::FileWriteError(e) => write!(f, "File write error: {}", e),
            BrokerError::FileOpenError(e) => write!(f, "File open error: {}", e),
            BrokerError::InvalidCommand(cmd) => write!(f, "Unknown command: {}", cmd),
            BrokerError::SerdeError(e) => write!(f, "Serde error: {}", e),
            BrokerError::IoError(e) => write!(f, "IO error: {}", e),
        }
    }
}

impl std::error::Error for BrokerError {}

impl From<std::io::Error> for BrokerError {
    fn from(e: std::io::Error) -> Self {
        BrokerError::IoError(e.to_string())
    }
}

impl From<serde_json::Error> for BrokerError {
    fn from(e: serde_json::Error) -> Self {
        BrokerError::SerdeError(e.to_string())
    }
}

impl From<regex::Error> for BrokerError {
    fn from(e: regex::Error) -> Self {
        BrokerError::SerdeError(format!("Regex error: {}", e))
    }
}
