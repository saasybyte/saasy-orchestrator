use tokio::sync::mpsc;

use super::error::TtsClientError;
use super::types::TtsAudioChunk;

#[async_trait::async_trait]
pub trait TtsClient: Send + Sync {
    async fn connect(&mut self) -> Result<(), TtsClientError>;

    async fn disconnect(&mut self) -> Result<(), TtsClientError>;

    async fn generate_speech(&self, text: String, context_id: Option<String>, continue_generation: bool) -> Result<(), TtsClientError>;

    async fn cancel_generation(&self, context_id: String) -> Result<(), TtsClientError>;

    fn take_inbound_audio_receiver(&mut self) -> Option<mpsc::UnboundedReceiver<TtsAudioChunk>>;
}
