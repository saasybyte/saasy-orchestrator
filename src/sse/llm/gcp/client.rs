use std::time::Instant;

use eventsource_stream::Eventsource;
use google_cloud_auth::credentials::{Builder, CacheableResource, Credentials};
use http::Extensions;
use reqwest::Client;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tracing::{error, info};

use super::types::{GcpRequest, GcpStreamResponse};
use crate::sse::llm::client::LlmClient;
use crate::sse::llm::error::LlmClientError;
use crate::sse::llm::types::{LlmClientConfig, LlmCredentials, LlmMessage};

const VERTEX_AI_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";

pub struct GcpClient {
    model: String,
    project_id: String,
    region: String,
    credentials: Credentials,
    client: Client,
}

impl GcpClient {
    pub fn new(config: &LlmClientConfig) -> Result<Self, LlmClientError> {
        let gcp_config = match &config.credentials {
            LlmCredentials::Gcp(gcp) => gcp.clone(),
            _ => return Err(LlmClientError::Config(
                "GcpClient requires Gcp credentials".to_string()
            )),
        };

        // Build credentials using ADC
        let credentials = Builder::default()
            .with_scopes([VERTEX_AI_SCOPE])
            .build()
            .map_err(|e| LlmClientError::Config(format!(
                "Failed to build GCP credentials: {e}"
            )))?;

        Ok(Self {
            model: config.model.clone(),
            project_id: gcp_config.project_id,
            region: gcp_config.region,
            credentials,
            client: Client::new(),
        })
    }

    fn endpoint_url(&self) -> String {
        format!(
            "https://aiplatform.googleapis.com/v1/projects/{}/locations/{}/endpoints/openapi/chat/completions",
            self.project_id,
            self.region
        )
    }

    async fn get_auth_headers(&self) -> Result<http::HeaderMap, LlmClientError> {
        let cacheable = self.credentials
            .headers(Extensions::new())
            .await
            .map_err(|e| LlmClientError::Auth(format!("Failed to get GCP auth headers: {e}")))?;

        match cacheable {
            CacheableResource::New { data, .. } => Ok(data),
            CacheableResource::NotModified => {
                Err(LlmClientError::Auth("Unexpected NotModified response for first auth request".into()))
            }
        }
    }
}

#[async_trait::async_trait]
impl LlmClient for GcpClient {
    async fn stream(
        &self,
        messages: &[LlmMessage],
    ) -> Result<mpsc::UnboundedReceiver<String>, LlmClientError> {
        let llm_request_start = Instant::now();

        let request_body = GcpRequest {
            model: &self.model,
            messages,
            stream: true,
            reasoning_effort: Some("minimal"),
        };

        // Get fresh auth headers
        let auth_headers = self.get_auth_headers().await?;

        let mut request_builder = self
            .client
            .post(self.endpoint_url())
            .header("Content-Type", "application/json")
            .json(&request_body);

        // Add all auth headers from google-cloud-auth
        for (key, value) in auth_headers.iter() {
            request_builder = request_builder.header(key, value);
        }

        let response = request_builder
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
                "GCP Vertex AI API error {status}: {error_text}"
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

                        match serde_json::from_str::<GcpStreamResponse>(&event.data) {
                            Ok(response) => {
                                if let Some(choice) = response.choices.first() {
                                    if let Some(content) = &choice.delta.content {
                                        if first_token_received {
                                            let ttft = llm_request_start.elapsed();
                                            info!(
                                                "[LATENCY] First GCP LLM token received | elapsed: {}ms",
                                                ttft.as_millis()
                                            );
                                            first_token_received = false;
                                        }

                                        if token_tx.send(content.clone()).is_err() {
                                            break;
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Failed to parse GCP SSE event: {e}");
                            }
                        }
                    }
                    Err(e) => {
                        error!("GCP stream error: {e}");
                        break;
                    }
                }
            }
        });

        Ok(token_rx)
    }

    async fn cancel_stream(&self) -> Result<(), LlmClientError> {
        // GCP doesn't support server-side cancellation via HTTP/SSE
        // Cancellation is handled by dropping the receiver in the orchestrator
        Ok(())
    }
}
