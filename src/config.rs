use std::collections::HashMap;

use config::{Config, ConfigError, File, Environment};
use dotenvy::dotenv;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct RawServerConfig {
    http_host: String,
    http_port: u16,
    signal_http_url: String,
    signal_system_ws_url: String,
    signal_session_ws_url: String,
    edge_grpc_url: String,
    listening_engine_grpc_uds: String,
    speaking_engine_grpc_uds: String,
    // API keys
    openai_api_key: Option<String>,
    groq_api_key: Option<String>,
    anthropic_api_key: Option<String>,
    xai_api_key: Option<String>,
    deepgram_api_key: Option<String>,
    speechmatics_api_key: Option<String>,
    cartesia_api_key: Option<String>,
    elevenlabs_api_key: Option<String>,
    // Cloud providers
    gcp_project_id: Option<String>,
    gcp_region: Option<String>,
    aws_region: Option<String>,
}

#[derive(Debug)]
pub struct ServerConfig {
    pub http_host: String,
    pub http_port: u16,
    pub signal_http_url: String,
    pub signal_system_ws_url: String,
    pub signal_session_ws_url: String,
    pub edge_grpc_url: String,
    pub listening_engine_grpc_uds: String,
    pub speaking_engine_grpc_uds: String,
    // API keys
    pub openai_api_key: String,
    pub groq_api_key: String,
    pub anthropic_api_key: String,
    pub xai_api_key: String,
    pub deepgram_api_key: String,
    pub speechmatics_api_key: String,
    pub cartesia_api_key: String,
    pub elevenlabs_api_key: String,
    // Cloud providers
    pub gcp_project_id: String,
    pub gcp_region: String,
    pub aws_region: String,
}

impl ServerConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        dotenv().ok();

        let raw_server_config: RawServerConfig = Config::builder()
            .add_source(File::with_name("config/default").required(false))
            .add_source(Environment::default())
            .build()?
            .try_deserialize()?;

        Self::validate(raw_server_config)
    }

    fn validate(raw: RawServerConfig) -> Result<Self, ConfigError> {
        let openai_api_key = raw.openai_api_key
            .ok_or_else(|| ConfigError::Message("OPENAI_API_KEY must be set".to_string()))?;
        let groq_api_key = raw.groq_api_key
            .ok_or_else(|| ConfigError::Message("GROQ_API_KEY must be set".to_string()))?;
        let anthropic_api_key = raw.anthropic_api_key
            .ok_or_else(|| ConfigError::Message("ANTHROPIC_API_KEY must be set".to_string()))?;
        let xai_api_key = raw.xai_api_key
            .ok_or_else(|| ConfigError::Message("XAI_API_KEY must be set".to_string()))?;
        let deepgram_api_key = raw.deepgram_api_key
            .ok_or_else(|| ConfigError::Message("DEEPGRAM_API_KEY must be set".to_string()))?;
        let speechmatics_api_key = raw.speechmatics_api_key
            .ok_or_else(|| ConfigError::Message("SPEECHMATICS_API_KEY must be set".to_string()))?;
        let cartesia_api_key = raw.cartesia_api_key
            .ok_or_else(|| ConfigError::Message("CARTESIA_API_KEY must be set".to_string()))?;
        let elevenlabs_api_key = raw.elevenlabs_api_key
            .ok_or_else(|| ConfigError::Message("ELEVENLABS_API_KEY must be set".to_string()))?;
        let gcp_project_id = raw.gcp_project_id
            .ok_or_else(|| ConfigError::Message("GCP_PROJECT_ID must be set".to_string()))?;
        let gcp_region = raw.gcp_region
            .ok_or_else(|| ConfigError::Message("GCP_REGION must be set".to_string()))?;
        let aws_region = raw.aws_region
            .ok_or_else(|| ConfigError::Message("AWS_REGION must be set".to_string()))?;

        // Validate external env var required by google-cloud-auth
        if std::env::var("GOOGLE_APPLICATION_CREDENTIALS").is_err() {
            return Err(ConfigError::Message(
                "GOOGLE_APPLICATION_CREDENTIALS must be set".to_string()
            ));
        }

        // Validate external env vars required by aws-config
        if std::env::var("AWS_ACCESS_KEY_ID").is_err() {
            return Err(ConfigError::Message(
                "AWS_ACCESS_KEY_ID must be set".to_string()
            ));
        }
        if std::env::var("AWS_SECRET_ACCESS_KEY").is_err() {
            return Err(ConfigError::Message(
                "AWS_SECRET_ACCESS_KEY must be set".to_string()
            ));
        }

        Ok(Self {
            http_host: raw.http_host,
            http_port: raw.http_port,
            signal_http_url: raw.signal_http_url,
            signal_system_ws_url: raw.signal_system_ws_url,
            signal_session_ws_url: raw.signal_session_ws_url,
            edge_grpc_url: raw.edge_grpc_url,
            listening_engine_grpc_uds: raw.listening_engine_grpc_uds,
            speaking_engine_grpc_uds: raw.speaking_engine_grpc_uds,
            openai_api_key,
            groq_api_key,
            anthropic_api_key,
            xai_api_key,
            deepgram_api_key,
            speechmatics_api_key,
            cartesia_api_key,
            elevenlabs_api_key,
            gcp_project_id,
            gcp_region,
            aws_region,
        })
    }

    pub fn llm_api_keys(&self) -> HashMap<String, String> {
        let mut keys = HashMap::new();
        keys.insert("openai".to_string(), self.openai_api_key.clone());
        keys.insert("groq".to_string(), self.groq_api_key.clone());
        keys.insert("anthropic".to_string(), self.anthropic_api_key.clone());
        keys.insert("xai".to_string(), self.xai_api_key.clone());
        keys
    }

    pub fn stt_api_keys(&self) -> HashMap<String, String> {
        let mut keys = HashMap::new();
        keys.insert("deepgram".to_string(), self.deepgram_api_key.clone());
        keys.insert("speechmatics".to_string(), self.speechmatics_api_key.clone());
        keys
    }

    pub fn tts_api_keys(&self) -> HashMap<String, String> {
        let mut keys = HashMap::new();
        keys.insert("cartesia".to_string(), self.cartesia_api_key.clone());
        keys.insert("elevenlabs".to_string(), self.elevenlabs_api_key.clone());
        keys
    }

    pub fn cloud_provider_configs(&self) -> HashMap<String, String> {
        let mut configs = HashMap::new();
        configs.insert("gcp_project_id".to_string(), self.gcp_project_id.clone());
        configs.insert("gcp_region".to_string(), self.gcp_region.clone());
        configs.insert("aws_region".to_string(), self.aws_region.clone());
        configs
    }
}
