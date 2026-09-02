use serde::{Deserialize, Serialize};

use crate::sse::llm::types::LlmMessage;

#[derive(Debug, Serialize)]
pub struct GcpRequest<'a> {
    pub model: &'a str,
    pub messages: &'a [LlmMessage],
    pub stream: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<&'a str>,
}


#[derive(Debug, Deserialize)]
pub struct GcpStreamResponse {
    #[serde(default)]
    pub choices: Vec<GcpStreamChoice>,
}

#[derive(Debug, Deserialize)]
pub struct GcpStreamChoice {
    #[serde(default)]
    pub delta: GcpDelta,
}

#[derive(Debug, Deserialize, Default)]
pub struct GcpDelta {
    #[serde(default)]
    pub content: Option<String>,
}
