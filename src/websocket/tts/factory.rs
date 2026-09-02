use crate::stores::{ApiKeyStore, CloudProviderStore};
use super::cartesia::CartesiaClient;
use super::client::TtsClient;
use super::elevenlabs::ElevenLabsClient;
use super::error::TtsClientError;
use super::types::{TtsClientConfig, TtsCredentials, TtsProvider};

pub fn create_tts_config(
    provider: &str,
    model: &str,
    api_key_store: &ApiKeyStore,
    _cloud_provider_store: &CloudProviderStore,
) -> Option<TtsClientConfig> {
    let (tts_provider, credentials) = match provider {
        "cartesia" => {
            let api_key = api_key_store.get_tts_key("cartesia")?;
            (TtsProvider::Cartesia, TtsCredentials::ApiKey(api_key.to_string()))
        }
        "elevenlabs" => {
            let api_key = api_key_store.get_tts_key("elevenlabs")?;
            (TtsProvider::ElevenLabs, TtsCredentials::ApiKey(api_key.to_string()))
        }
        _ => return None,
    };

    Some(TtsClientConfig {
        provider: tts_provider,
        credentials,
        model: model.to_string(),
        version: "2024-11-13".to_string(),
    })
}

pub fn create_tts_client(config: &TtsClientConfig) -> Result<Box<dyn TtsClient>, TtsClientError> {
    match config.provider {
        TtsProvider::Cartesia => Ok(Box::new(CartesiaClient::new(config)?)),
        TtsProvider::ElevenLabs => Ok(Box::new(ElevenLabsClient::new(config)?)),
    }
}
