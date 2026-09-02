use tokio::sync::mpsc;

use super::error::LlmClientError;
use super::types::LlmMessage;

#[async_trait::async_trait]
pub trait LlmClient: Send + Sync {
    async fn stream(&self, messages: &[LlmMessage]) -> Result<mpsc::UnboundedReceiver<String>, LlmClientError>;

    async fn cancel_stream(&self) -> Result<(), LlmClientError>;
}
