use serde::{Deserialize, Serialize};

use crate::sse::llm::types::LlmMessage;

#[derive(Debug, Serialize)]
pub struct AnthropicRequest<'a> {
    pub model: &'a str,
    pub messages: &'a [AnthropicMessage],
    pub max_tokens: u32,
    pub stream: bool,
}

/// Anthropic uses a different message format - system messages go in a separate field
#[derive(Debug, Clone, Serialize)]
pub struct AnthropicMessage {
    pub role: String,
    pub content: String,
}

impl From<&LlmMessage> for AnthropicMessage {
    fn from(msg: &LlmMessage) -> Self {
        Self {
            role: match msg.role {
                crate::sse::llm::types::LlmRole::System => "user".to_string(), // Will be filtered out
                crate::sse::llm::types::LlmRole::User => "user".to_string(),
                crate::sse::llm::types::LlmRole::Assistant => "assistant".to_string(),
            },
            content: msg.content.clone(),
        }
    }
}

/// Anthropic request with system message as separate field
#[derive(Debug, Serialize)]
pub struct AnthropicRequestWithSystem<'a> {
    pub model: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system: Option<&'a str>,
    pub messages: Vec<AnthropicMessage>,
    pub max_tokens: u32,
    pub stream: bool,
}

/// Stream event wrapper - Anthropic sends typed events
#[derive(Debug, Deserialize)]
pub struct AnthropicStreamEvent {
    #[serde(rename = "type")]
    pub event_type: String,
    #[serde(default)]
    pub delta: Option<AnthropicDelta>,
}

#[derive(Debug, Deserialize)]
pub struct AnthropicDelta {
    #[serde(rename = "type")]
    pub delta_type: String,
    #[serde(default)]
    pub text: Option<String>,
}
