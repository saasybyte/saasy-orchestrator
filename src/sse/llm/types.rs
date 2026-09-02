use serde::{Deserialize, Serialize};

use crate::stores::{AwsConfig, GcpConfig};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum LlmProvider {
    #[default]
    OpenAi,
    Groq,
    Anthropic,
    XAi,
    Gcp,
    Aws,
    // Azure,
}

#[derive(Debug, Clone)]
pub enum LlmCredentials {
    ApiKey(String),
    Gcp(GcpConfig),
    Aws(AwsConfig),
    // Azure(AzureConfig),
}

#[derive(Debug, Clone)]
pub struct LlmClientConfig {
    pub provider: LlmProvider,
    pub model: String,
    pub credentials: LlmCredentials,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmMessage {
    pub role: LlmRole,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LlmRole {
    #[serde(rename = "system")]
    System,
    #[serde(rename = "user")]
    User,
    #[serde(rename = "assistant")]
    Assistant,
}
