use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum TtsProvider {
    #[default]
    Cartesia,
    ElevenLabs,
}

#[derive(Debug, Clone)]
pub enum TtsCredentials {
    ApiKey(String),
    // Future credentials
}

#[derive(Debug, Clone)]
pub struct TtsClientConfig {
    pub provider: TtsProvider,
    pub credentials: TtsCredentials,
    pub model: String,
    pub version: String,
}

#[derive(Debug, Clone)]
pub struct TtsAudioChunk {
    pub data: Vec<u8>,
    pub done: bool,
    pub context_id: Option<String>,
}
