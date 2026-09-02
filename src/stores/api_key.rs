use std::collections::HashMap;

#[allow(clippy::struct_field_names)]
pub struct ApiKeyStore {
    llm_keys: HashMap<String, String>,
    stt_keys: HashMap<String, String>,
    tts_keys: HashMap<String, String>,
}

impl ApiKeyStore {
    pub fn new(
        llm_keys: HashMap<String, String>,
        stt_keys: HashMap<String, String>,
        tts_keys: HashMap<String, String>,
    ) -> Self {
        Self {
            llm_keys,
            stt_keys,
            tts_keys,
        }
    }

    pub fn get_llm_key(&self, provider: &str) -> Option<&str> {
        self.llm_keys.get(provider).map(String::as_str)
    }

    pub fn get_stt_key(&self, provider: &str) -> Option<&str> {
        self.stt_keys.get(provider).map(String::as_str)
    }

    pub fn get_tts_key(&self, provider: &str) -> Option<&str> {
        self.tts_keys.get(provider).map(String::as_str)
    }
}
