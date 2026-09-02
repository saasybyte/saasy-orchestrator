use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize)]
pub struct StartRecognitionMessage {
    pub message: &'static str,
    pub audio_format: AudioFormat,
    pub transcription_config: TranscriptionConfig,
}

impl StartRecognitionMessage {
    pub fn new(sample_rate: u32, language: &str, operating_point: Option<String>) -> Self {
        Self {
            message: "StartRecognition",
            audio_format: AudioFormat {
                r#type: "raw",
                encoding: "pcm_s16le",
                sample_rate,
            },
            transcription_config: TranscriptionConfig {
                language: language.to_string(),
                enable_partials: true,
                operating_point,
                max_delay: 0.7,
            },
        }
    }

    pub fn to_json_string(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct AudioFormat {
    pub r#type: &'static str,
    pub encoding: &'static str,
    pub sample_rate: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct TranscriptionConfig {
    pub language: String,
    pub enable_partials: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operating_point: Option<String>,
    pub max_delay: f32,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "message")]
pub enum SpeechmaticsRequest {
    #[serde(rename = "ForceEndOfUtterance")]
    ForceEndOfUtterance,

    #[serde(rename = "EndOfStream")]
    EndOfStream { last_seq_no: u64 },
}

impl SpeechmaticsRequest {
    pub fn to_json_string(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "message")]
pub enum SpeechmaticsResponse {
    #[serde(rename = "RecognitionStarted")]
    RecognitionStarted(RecognitionStartedMessage),

    #[serde(rename = "AudioAdded")]
    AudioAdded(AudioAddedMessage),

    #[serde(rename = "AddPartialTranscript")]
    AddPartialTranscript(TranscriptMessage),

    #[serde(rename = "AddTranscript")]
    AddTranscript(TranscriptMessage),

    #[serde(rename = "EndOfUtterance")]
    EndOfUtterance(EndOfUtteranceMessage),

    #[serde(rename = "EndOfTranscript")]
    EndOfTranscript,

    #[serde(rename = "Info")]
    Info(InfoMessage),

    #[serde(rename = "Warning")]
    Warning(WarningMessage),

    #[serde(rename = "Error")]
    Error(ErrorMessage),

    /// Catch-all for unknown message types (e.g., ChannelAudioAdded, AudioEventStarted, etc.)
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RecognitionStartedMessage {
    pub id: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct AudioAddedMessage {
    #[serde(default)]
    pub seq_no: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TranscriptMessage {
    pub metadata: TranscriptMetadata,
    #[serde(default)]
    pub results: Vec<TranscriptResult>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TranscriptMetadata {
    pub transcript: String,
    pub start_time: f64,
    pub end_time: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TranscriptResult {
    pub r#type: String,
    pub start_time: f64,
    pub end_time: f64,
    #[serde(default)]
    pub alternatives: Vec<TranscriptAlternative>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TranscriptAlternative {
    pub content: String,
    pub confidence: f64,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct EndOfUtteranceMessage {
    #[serde(default)]
    pub metadata: EndOfUtteranceMetadata,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct EndOfUtteranceMetadata {
    #[serde(default)]
    pub start_time: f64,
    #[serde(default)]
    pub end_time: f64,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct InfoMessage {
    #[serde(default)]
    pub r#type: String,
    #[serde(default)]
    pub code: u32,
    #[serde(default)]
    pub reason: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct WarningMessage {
    #[serde(default)]
    pub r#type: String,
    #[serde(default)]
    pub code: u32,
    #[serde(default)]
    pub reason: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ErrorMessage {
    #[serde(default)]
    pub r#type: String,
    #[serde(default)]
    pub code: u32,
    #[serde(default)]
    pub reason: String,
}
