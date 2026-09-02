use saasy_proto_rust::edge::v1::ListProviderModelsResponse;
use tokio::sync::RwLock;

pub struct EdgeCache {
    provider_cache: RwLock<ListProviderModelsResponse>,
}

impl EdgeCache {
    pub fn new(provider_data: ListProviderModelsResponse) -> Self {
        Self {
            provider_cache: RwLock::new(provider_data),
        }
    }

    pub async fn update(&self, provider_data: ListProviderModelsResponse) {
        let mut provider_cache = self.provider_cache.write().await;
        *provider_cache = provider_data;
    }

    pub async fn has_llm_provider(&self, provider: &str, model_id: &str) -> bool {
        let provider_cache = self.provider_cache.read().await;
        provider_cache.llm.iter().any(|m| m.provider == provider && m.model_id == model_id)
    }

    pub async fn has_tts_provider(&self, provider: &str, model_id: &str) -> bool {
        let provider_cache = self.provider_cache.read().await;
        provider_cache.tts.iter().any(|m| m.provider == provider && m.model_id == model_id)
    }

    pub async fn has_stt_provider(&self, provider: &str, model_id: &str) -> bool {
        let provider_cache = self.provider_cache.read().await;
        provider_cache.stt.iter().any(|m| m.provider == provider && m.model_id == model_id)
    }
}
