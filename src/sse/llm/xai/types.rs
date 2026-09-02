use serde::{Deserialize, Serialize};

use crate::sse::llm::types::LlmMessage;

#[derive(Debug, Serialize)]
pub struct XaiRequest<'a> {
    pub model: &'a str,
    pub messages: &'a [LlmMessage],
    pub stream: bool,
}

#[derive(Debug, Deserialize)]
pub struct XaiStreamResponse {
    #[serde(default)]
    pub choices: Vec<XaiStreamChoice>,
}

#[derive(Debug, Deserialize)]
pub struct XaiStreamChoice {
    #[serde(default)]
    pub delta: XaiDelta,
}

#[derive(Debug, Deserialize, Default)]
pub struct XaiDelta {
    #[serde(default)]
    pub content: Option<String>,
}
