use serde::{Deserialize, Serialize};

use crate::sse::llm::types::LlmMessage;

#[derive(Debug, Serialize)]
pub struct OpenAiRequest<'a> {
    pub model: &'a str,
    pub messages: &'a [LlmMessage],
    pub stream: bool,
}

#[derive(Debug, Deserialize)]
pub struct OpenAiStreamResponse {
    #[serde(default)]
    pub choices: Vec<OpenAiStreamChoice>,
}

#[derive(Debug, Deserialize)]
pub struct OpenAiStreamChoice {
    #[serde(default)]
    pub delta: OpenAiDelta,
}

#[derive(Debug, Deserialize, Default)]
pub struct OpenAiDelta {
    #[serde(default)]
    pub content: Option<String>,
}
