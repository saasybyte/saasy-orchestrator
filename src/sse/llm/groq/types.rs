use serde::{Deserialize, Serialize};

use crate::sse::llm::types::LlmMessage;

#[derive(Debug, Serialize)]
pub struct GroqRequest<'a> {
    pub model: &'a str,
    pub messages: &'a [LlmMessage],
    pub stream: bool,
}

#[derive(Debug, Deserialize)]
pub struct GroqStreamResponse {
    #[serde(default)]
    pub choices: Vec<GroqStreamChoice>,
}

#[derive(Debug, Deserialize)]
pub struct GroqStreamChoice {
    #[serde(default)]
    pub delta: GroqDelta,
}

#[derive(Debug, Deserialize, Default)]
pub struct GroqDelta {
    #[serde(default)]
    pub content: Option<String>,
}
