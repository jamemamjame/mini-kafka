use serde::Deserialize;
use std::fs;

#[derive(Debug, Deserialize, Clone)]
pub struct BrokerConfig {
    pub brokers: Vec<String>,
}

impl BrokerConfig {
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let data = fs::read_to_string(path)?;
        let config: BrokerConfig = serde_json::from_str(&data)?;
        Ok(config)
    }
}
