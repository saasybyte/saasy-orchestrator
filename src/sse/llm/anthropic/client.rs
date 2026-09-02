use std::time::Instant;

use eventsource_stream::Eventsource;
use reqwest::Client;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tracing::{error, info};

use super::types::{AnthropicMessage, AnthropicRequestWithSystem, AnthropicStreamEvent};
use crate::sse::llm::client::LlmClient;
use crate::sse::llm::error::LlmClientError;
use crate::sse::llm::types::{LlmClientConfig, LlmCredentials, LlmMessage, LlmRole};

const ANTHROPIC_API_URL: &str = "https://api.anthropic.com/v1/messages";
const ANTHROPIC_VERSION: &str = "2023-06-01";
const DEFAULT_MAX_TOKENS: u32 = 4096;

pub struct AnthropicClient {
    model: String,
    api_key: String,
    client: Client,
}

impl AnthropicClient {
    pub fn new(config: &LlmClientConfig) -> Result<Self, LlmClientError> {
        let api_key = match &config.credentials {
            LlmCredentials::ApiKey(key) => key.clone(),
            _ => return Err(LlmClientError::Config(
                "AnthropicClient requires ApiKey credentials".to_string()
            )),
        };
        Ok(Self {
            model: config.model.clone(),
            api_key,
            client: Client::new(),
        })
    }
}

#[async_trait::async_trait]
impl LlmClient for AnthropicClient {
    async fn stream(
        &self,
        messages: &[LlmMessage],
    ) -> Result<mpsc::UnboundedReceiver<String>, LlmClientError> {
        let llm_request_start = Instant::now();

        // Extract system message (Anthropic requires it as separate field)
        let system_message: Option<String> = messages
            .iter()
            .find(|m| matches!(m.role, LlmRole::System))
            .map(|m| m.content.clone());

        // Convert non-system messages to Anthropic format
        let anthropic_messages: Vec<AnthropicMessage> = messages
            .iter()
            .filter(|m| !matches!(m.role, LlmRole::System))
            .map(AnthropicMessage::from)
            .collect();

        let request_body = AnthropicRequestWithSystem {
            model: &self.model,
            system: system_message.as_deref(),
            messages: anthropic_messages,
            max_tokens: DEFAULT_MAX_TOKENS,
            stream: true,
        };

        let response = self
            .client
            .post(ANTHROPIC_API_URL)
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", ANTHROPIC_VERSION)
            .header("content-type", "application/json")
            .json(&request_body)
            .send()
            .await
            .map_err(|e| LlmClientError::Http(format!("Request failed: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            return Err(LlmClientError::Api(format!(
                "API error {status}: {error_text}"
            )));
        }

        let (token_tx, token_rx) = mpsc::unbounded_channel();

        tokio::spawn(async move {
            let mut stream = response.bytes_stream().eventsource();
            let mut first_token_received = true;

            while let Some(event) = stream.next().await {
                match event {
                    Ok(event) => {
                        // Anthropic uses event types, not [DONE]
                        if event.event == "message_stop" {
                            break;
                        }

                        // Only process content_block_delta events
                        if event.event != "content_block_delta" {
                            continue;
                        }

                        match serde_json::from_str::<AnthropicStreamEvent>(&event.data) {
                            Ok(response) => {
                                if let Some(delta) = &response.delta {
                                    // Only extract text from text_delta events
                                    if delta.delta_type == "text_delta" {
                                        if let Some(text) = &delta.text {
                                            if first_token_received {
                                                let ttft = llm_request_start.elapsed();
                                                info!(
                                                    "[LATENCY] First LLM token received | elapsed: {}ms",
                                                    ttft.as_millis()
                                                );
                                                first_token_received = false;
                                            }

                                            if token_tx.send(text.clone()).is_err() {
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Failed to parse SSE event: {e}");
                            }
                        }
                    }
                    Err(e) => {
                        error!("Stream error: {e}");
                        break;
                    }
                }
            }
        });

        Ok(token_rx)
    }

    async fn cancel_stream(&self) -> Result<(), LlmClientError> {
        // Anthropic doesn't support server-side cancellation via HTTP/SSE
        // Cancellation is handled by dropping the receiver in the orchestrator
        Ok(())
    }
}
