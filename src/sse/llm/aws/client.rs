use std::time::Instant;

use aws_sdk_bedrockruntime::{
    types::{ContentBlock, ConversationRole, Message, SystemContentBlock},
    Client,
};
use tokio::sync::{mpsc, OnceCell};
use tracing::{error, info};

use crate::sse::llm::client::LlmClient;
use crate::sse::llm::error::LlmClientError;
use crate::sse::llm::types::{LlmClientConfig, LlmCredentials, LlmMessage, LlmRole};

pub struct AwsClient {
    model: String,
    region: String,
    client: OnceCell<Client>,
}

impl AwsClient {
    pub fn new(config: &LlmClientConfig) -> Result<Self, LlmClientError> {
        let aws_config = match &config.credentials {
            LlmCredentials::Aws(aws) => aws.clone(),
            _ => return Err(LlmClientError::Config(
                "AwsClient requires Aws credentials".to_string()
            )),
        };

        Ok(Self {
            model: config.model.clone(),
            region: aws_config.region,
            client: OnceCell::new(),
        })
    }

    async fn get_client(&self) -> Result<&Client, LlmClientError> {
        self.client.get_or_try_init(|| async {
            let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
                .region(aws_config::Region::new(self.region.clone()))
                .load()
                .await;

            Ok(Client::new(&sdk_config))
        }).await
    }

    fn convert_messages(messages: &[LlmMessage]) -> (Option<Vec<SystemContentBlock>>, Vec<Message>) {
        let mut system_blocks: Vec<SystemContentBlock> = Vec::new();
        let mut bedrock_messages: Vec<Message> = Vec::new();

        for msg in messages {
            match msg.role {
                LlmRole::System => {
                    system_blocks.push(SystemContentBlock::Text(msg.content.clone()));
                }
                LlmRole::User => {
                    match Message::builder()
                        .role(ConversationRole::User)
                        .content(ContentBlock::Text(msg.content.clone()))
                        .build()
                    {
                        Ok(message) => bedrock_messages.push(message),
                        Err(e) => error!("Failed to build Bedrock user message: {e}"),
                    }
                }
                LlmRole::Assistant => {
                    match Message::builder()
                        .role(ConversationRole::Assistant)
                        .content(ContentBlock::Text(msg.content.clone()))
                        .build()
                    {
                        Ok(message) => bedrock_messages.push(message),
                        Err(e) => error!("Failed to build Bedrock assistant message: {e}"),
                    }
                }
            }
        }

        let system = if system_blocks.is_empty() {
            None
        } else {
            Some(system_blocks)
        };

        (system, bedrock_messages)
    }

    fn extract_text_delta(event: &aws_sdk_bedrockruntime::types::ConverseStreamOutput) -> Option<String> {
        use aws_sdk_bedrockruntime::types::{ContentBlockDelta, ConverseStreamOutput};

        match event {
            ConverseStreamOutput::ContentBlockDelta(delta) => {
                delta.delta.as_ref().and_then(|d| {
                    if let ContentBlockDelta::Text(text) = d {
                        Some(text.clone())
                    } else {
                        None
                    }
                })
            }
            _ => None,
        }
    }
}

#[async_trait::async_trait]
impl LlmClient for AwsClient {
    async fn stream(
        &self,
        messages: &[LlmMessage],
    ) -> Result<mpsc::UnboundedReceiver<String>, LlmClientError> {
        let llm_request_start = Instant::now();

        let client = self.get_client().await?;
        let (system_blocks, bedrock_messages) = Self::convert_messages(messages);

        let mut request = client
            .converse_stream()
            .model_id(&self.model)
            .set_messages(Some(bedrock_messages));

        if let Some(system) = system_blocks {
            request = request.set_system(Some(system));
        }

        let response = request
            .send()
            .await
            .map_err(|e| LlmClientError::Api(format!("Bedrock API error: {e:?}")))?;

        let (token_tx, token_rx) = mpsc::unbounded_channel();
        let mut stream = response.stream;

        tokio::spawn(async move {
            let mut first_token_received = true;

            loop {
                match stream.recv().await {
                    Ok(Some(event)) => {
                        if let Some(text) = Self::extract_text_delta(&event) {
                            if first_token_received {
                                let ttft = llm_request_start.elapsed();
                                info!(
                                    "[LATENCY] First AWS Bedrock LLM token received | elapsed: {}ms",
                                    ttft.as_millis()
                                );
                                first_token_received = false;
                            }

                            if token_tx.send(text).is_err() {
                                break;
                            }
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        error!("Bedrock stream error: {e}");
                        break;
                    }
                }
            }
        });

        Ok(token_rx)
    }

    async fn cancel_stream(&self) -> Result<(), LlmClientError> {
        // AWS Bedrock doesn't support server-side cancellation
        // Cancellation is handled by dropping the receiver in the orchestrator
        Ok(())
    }
}
