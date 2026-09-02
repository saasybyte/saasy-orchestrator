use tracing::info;

use super::client::EdgeClient;
use super::error::EdgeClientError;
use super::cache::EdgeCache;

pub struct EdgeService {
    client: EdgeClient,
    provider_cache: EdgeCache,
}

impl EdgeService {
    pub async fn new(url: &str) -> Result<Self, EdgeClientError> {
        let client = EdgeClient::connect(url).await?;
        let provider_data = client.list_provider_models().await?;
        let provider_cache = EdgeCache::new(provider_data);

        Ok(Self { client, provider_cache })
    }

    pub async fn refresh_providers(&self) -> Result<(), EdgeClientError> {
        let data = self.client.list_provider_models().await?;
        self.provider_cache.update(data).await;
        Ok(())
    }

    pub async fn has_llm_provider(&self, provider: &str, model_id: &str) -> bool {
        self.provider_cache.has_llm_provider(provider, model_id).await
    }

    pub async fn has_tts_provider(&self, provider: &str, model_id: &str) -> bool {
        self.provider_cache.has_tts_provider(provider, model_id).await
    }

    pub async fn has_stt_provider(&self, provider: &str, model_id: &str) -> bool {
        self.provider_cache.has_stt_provider(provider, model_id).await
    }
}
