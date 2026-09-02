use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize)]
pub struct CartesiaGenerationRequest {
    pub model_id: String,
    pub transcript: String,
    pub voice: CartesiaVoice,
    pub output_format: CartesiaOutputFormat,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub language: Option<CartesiaLanguage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_id: Option<String>,
    #[serde(rename = "continue", skip_serializing_if = "Option::is_none")]
    pub continue_generation: Option<bool>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CartesiaCancelContextRequest {
    pub context_id: String,
    pub cancel: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct CartesiaVoice {
    pub mode: String,
    pub id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct CartesiaOutputFormat {
    pub container: CartesiaContainer,
    pub encoding: CartesiaEncoding,
    pub sample_rate: u32,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CartesiaContainer {
    Raw,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CartesiaEncoding {
    PcmS16le,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CartesiaLanguage {
    En,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type")]
pub enum CartesiaResponse {
    #[serde(rename = "chunk")]
    Chunk(CartesiaChunkMessage),

    #[serde(rename = "done")]
    Done(CartesiaDoneMessage),

    #[serde(rename = "error")]
    Error(CartesiaErrorMessage),
}

#[derive(Debug, Clone, Deserialize)]
pub struct CartesiaChunkMessage {
    pub data: String, // Base64-encoded audio
    pub done: bool,
    pub status_code: u16,
    #[serde(default)]
    pub context_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CartesiaDoneMessage {
    pub done: bool,
    pub status_code: u16,
    #[serde(default)]
    pub context_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CartesiaErrorMessage {
    pub error: String,
    pub done: bool,
    pub status_code: u16,
    #[serde(default)]
    pub context_id: Option<String>,
}
