mod consume;
mod produce;

use crate::error::BrokerError;

pub use super::protocol::Response;
pub use async_trait::async_trait;
pub use consume::ConsumeCommand;
pub use produce::ProduceCommand;

#[async_trait]
pub trait Command: Send + Sync {
    async fn execute(&self, broker: &crate::broker::Broker) -> Result<Response, BrokerError>;
}
