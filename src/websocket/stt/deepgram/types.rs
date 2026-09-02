use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum DeepgramRequest {
    #[serde(rename = "KeepAlive")]
    KeepAlive,

    #[serde(rename = "Finalize")]
    Finalize,

    #[serde(rename = "CloseStream")]
    CloseStream,
}

impl DeepgramRequest {
    pub fn to_json_string(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type")]
pub enum DeepgramResponse {
    #[serde(rename = "Results")]
    Results(DeepgramResultsMessage),

    #[serde(rename = "Metadata")]
    Metadata(DeepgramMetadataMessage),

    #[serde(rename = "UtteranceEnd")]
    UtteranceEnd(DeepgramUtteranceEndMessage),

    #[serde(rename = "SpeechStarted")]
    SpeechStarted(DeepgramSpeechStartedMessage),
}

#[derive(Debug, Clone, Deserialize)]
pub struct DeepgramResultsMessage {
    #[serde(default)]
    pub start: f64,
    #[serde(default)]
    pub speech_final: Option<bool>,
    #[serde(default)]
    pub from_finalize: Option<bool>,
    pub channel: DeepgramChannel,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DeepgramChannel {
    pub alternatives: Vec<DeepgramAlternative>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DeepgramAlternative {
    pub transcript: String,
    pub confidence: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DeepgramMetadataMessage {
    pub request_id: String,
    pub created: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DeepgramUtteranceEndMessage {
    pub last_word_end: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DeepgramSpeechStartedMessage {
    pub timestamp: f64,
}
