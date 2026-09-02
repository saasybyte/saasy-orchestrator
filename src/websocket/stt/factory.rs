use crate::stores::{ApiKeyStore, CloudProviderStore};
use super::deepgram::DeepgramClient;
use super::speechmatics::SpeechmaticsClient;
use super::client::SttClient;
use super::error::SttClientError;
use super::types::{SttClientConfig, SttCredentials, SttProvider};

pub fn create_stt_config(
    provider: &str,
    model: &str,
    api_key_store: &ApiKeyStore,
    _cloud_provider_store: &CloudProviderStore,
) -> Option<SttClientConfig> {
    let (stt_provider, credentials) = match provider {
        "deepgram" => {
            let api_key = api_key_store.get_stt_key("deepgram")?;
            (SttProvider::Deepgram, SttCredentials::ApiKey(api_key.to_string()))
        }
        "speechmatics" => {
            let api_key = api_key_store.get_stt_key("speechmatics")?;
            (SttProvider::Speechmatics, SttCredentials::ApiKey(api_key.to_string()))
        }
        _ => return None,
    };

    Some(SttClientConfig {
        provider: stt_provider,
        credentials,
        model: model.to_string(),
        encoding: "linear16".to_string(),
        sample_rate: 48000,
        language: Some("en-US".to_string()),
        interim_results: Some(true),
        punctuate: Some(true),
        endpointing: None,
        channels: None,
        diarize: None,
        filler_words: None,
        keywords: None,
        numerals: None,
        profanity_filter: None,
        smart_format: None,
        utterance_end_ms: None,
        vad_events: None,
    })
}

pub fn create_stt_client(config: &SttClientConfig) -> Result<Box<dyn SttClient>, SttClientError> {
    match config.provider {
        SttProvider::Deepgram => Ok(Box::new(DeepgramClient::new(config)?)),
        SttProvider::Speechmatics => Ok(Box::new(SpeechmaticsClient::new(config)?)),
    }
}
