use serde::{Deserialize, Serialize};

/// First message to initialize the connection and first context
#[derive(Debug, Clone, Serialize)]
pub struct ElevenLabsInitConnectionMulti {
    pub text: String,
    pub context_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub voice_settings: Option<ElevenLabsVoiceSettings>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generation_config: Option<ElevenLabsGenerationConfig>,
}

/// Create/initialize a new context (after the first one)
#[derive(Debug, Clone, Serialize)]
pub struct ElevenLabsInitialiseContext {
    pub text: String,
    pub context_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub voice_settings: Option<ElevenLabsVoiceSettings>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generation_config: Option<ElevenLabsGenerationConfig>,
}

/// Send text to a specific context
#[derive(Debug, Clone, Serialize)]
pub struct ElevenLabsSendTextMulti {
    pub text: String,
    pub context_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flush: Option<bool>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ElevenLabsFlushContext {
    pub context_id: String,
    pub flush: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ElevenLabsCloseContext {
    pub context_id: String,
    pub close_context: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ElevenLabsCloseSocket {
    pub close_socket: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ElevenLabsKeepContextAlive {
    pub text: String, // Must be empty string ""
    pub context_id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ElevenLabsVoiceSettings {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stability: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub similarity_boost: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub style: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub use_speaker_boost: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub speed: Option<f32>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ElevenLabsGenerationConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chunk_length_schedule: Option<Vec<u32>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum ElevenLabsResponse {
    Audio(ElevenLabsAudioOutputMulti),
    Final(ElevenLabsFinalOutputMulti),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ElevenLabsAudioOutputMulti {
    pub audio: String, // Base64 encoded audio chunk
    pub context_id: Option<String>,
    #[serde(default)]
    pub alignment: Option<ElevenLabsAlignment>,
    #[serde(default)]
    pub normalized_alignment: Option<ElevenLabsAlignment>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ElevenLabsFinalOutputMulti {
    pub is_final: bool,
    pub context_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ElevenLabsAlignment {
    pub char_start_times_ms: Option<Vec<u32>>,
    pub char_durations_ms: Option<Vec<u32>>,
    pub chars: Option<Vec<String>>,
}
