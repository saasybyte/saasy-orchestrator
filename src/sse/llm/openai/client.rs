use std::time::Instant;

use eventsource_stream::Eventsource;
use reqwest::Client;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tracing::{error, info};

use super::types::{OpenAiRequest, OpenAiStreamResponse};
use crate::sse::llm::client::LlmClient;
use crate::sse::llm::error::LlmClientError;
use crate::sse::llm::types::{LlmClientConfig, LlmCredentials, LlmMessage};

pub struct OpenAiClient {
    model: String,
    api_key: String,
    client: Client,
}

impl OpenAiClient {
    pub fn new(config: &LlmClientConfig) -> Result<Self, LlmClientError> {
        let api_key = match &config.credentials {
            LlmCredentials::ApiKey(key) => key.clone(),
            _ => return Err(LlmClientError::Config(
                "OpenAiClient requires ApiKey credentials".to_string()
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
impl LlmClient for OpenAiClient {
    async fn stream(
        &self,
        messages: &[LlmMessage],
    ) -> Result<mpsc::UnboundedReceiver<String>, LlmClientError> {
        let llm_request_start = Instant::now();

        let request_body = OpenAiRequest {
            model: &self.model,
            messages,
            stream: true,
        };

        let response = self
            .client
            .post("https://api.openai.com/v1/chat/completions")
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
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
                        if event.data == "[DONE]" {
                            break;
                        }

                        match serde_json::from_str::<OpenAiStreamResponse>(&event.data) {
                            Ok(response) => {
                                if let Some(content) = response.choices.first().and_then(|c| c.delta.content.as_ref()) {
                                    if first_token_received {
                                        let ttft = llm_request_start.elapsed();
                                        info!(
                                            "[LATENCY] First LLM token received | elapsed: {}ms",
                                            ttft.as_millis()
                                        );
                                        first_token_received = false;
                                    }

                                    if token_tx.send(content.clone()).is_err() {
                                        break;
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
        // OpenAI doesn't support server-side cancellation via HTTP/SSE
        // Cancellation is handled by dropping the receiver in the orchestrator
        Ok(())
    }
}
