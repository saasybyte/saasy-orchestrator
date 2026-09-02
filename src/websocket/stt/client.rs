use tokio::sync::mpsc;

use super::error::SttClientError;
use super::types::SttTranscript;

#[async_trait::async_trait]
pub trait SttClient: Send + Sync {
    async fn connect(&mut self) -> Result<(), SttClientError>;

    async fn disconnect(&mut self) -> Result<(), SttClientError>;

    async fn generate_text(&self, data: Vec<u8>) -> Result<(), SttClientError>;

    async fn finalize(&self) -> Result<(), SttClientError>;

    fn take_inbound_transcript_receiver(&mut self) -> Option<mpsc::UnboundedReceiver<SttTranscript>>;
}
