use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum SttProvider {
    #[default]
    Deepgram,
    Speechmatics,
}

#[derive(Debug, Clone)]
pub enum SttCredentials {
    ApiKey(String),
    // Future credentials
}

#[derive(Debug, Clone)]
pub struct SttClientConfig {
    pub provider: SttProvider,
    pub model: String,
    pub credentials: SttCredentials,
    pub encoding: String,
    pub sample_rate: u32,
    pub language: Option<String>,
    pub channels: Option<u32>,
    pub diarize: Option<bool>,
    pub endpointing: Option<u32>,
    pub filler_words: Option<bool>,
    pub interim_results: Option<bool>,
    pub keywords: Option<Vec<String>>,
    pub numerals: Option<bool>,
    pub profanity_filter: Option<bool>,
    pub punctuate: Option<bool>,
    pub smart_format: Option<bool>,
    pub utterance_end_ms: Option<u32>,
    pub vad_events: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct SttTranscript {
    pub text: String,
    pub speech_final: bool,
    pub confidence: Option<f32>,
    pub timestamp: Option<u64>,
    pub from_finalize: bool,
}
