use crate::stores::{ApiKeyStore, CloudProviderStore};
use super::anthropic::AnthropicClient;
use super::aws::AwsClient;
use super::client::LlmClient;
use super::error::LlmClientError;
use super::gcp::GcpClient;
use super::groq::GroqClient;
use super::openai::OpenAiClient;
use super::xai::XaiClient;
use super::types::{LlmClientConfig, LlmCredentials, LlmProvider};

pub fn create_llm_config(
    provider: &str,
    model: &str,
    api_key_store: &ApiKeyStore,
    cloud_provider_store: &CloudProviderStore,
) -> Option<LlmClientConfig> {
    let (llm_provider, credentials) = match provider {
        "openai" => {
            let api_key = api_key_store.get_llm_key("openai")?;
            (LlmProvider::OpenAi, LlmCredentials::ApiKey(api_key.to_string()))
        }
        "groq" => {
            let api_key = api_key_store.get_llm_key("groq")?;
            (LlmProvider::Groq, LlmCredentials::ApiKey(api_key.to_string()))
        }
        "anthropic" => {
            let api_key = api_key_store.get_llm_key("anthropic")?;
            (LlmProvider::Anthropic, LlmCredentials::ApiKey(api_key.to_string()))
        }
        "xai" => {
            let api_key = api_key_store.get_llm_key("xai")?;
            (LlmProvider::XAi, LlmCredentials::ApiKey(api_key.to_string()))
        }
        "gcp" => {
            (LlmProvider::Gcp, LlmCredentials::Gcp(cloud_provider_store.gcp.clone()))
        }
        "aws" => {
            (LlmProvider::Aws, LlmCredentials::Aws(cloud_provider_store.aws.clone()))
        }
        // Future providers:
        // "azure" => (LlmProvider::Azure, LlmCredentials::Azure(cloud_provider_store.azure.clone())),
        _ => return None,
    };

    Some(LlmClientConfig {
        provider: llm_provider,
        model: model.to_string(),
        credentials,
    })
}

pub fn create_llm_client(config: &LlmClientConfig) -> Result<Box<dyn LlmClient>, LlmClientError> {
    match config.provider {
        LlmProvider::OpenAi => Ok(Box::new(OpenAiClient::new(config)?)),
        LlmProvider::Groq => Ok(Box::new(GroqClient::new(config)?)),
        LlmProvider::Anthropic => Ok(Box::new(AnthropicClient::new(config)?)),
        LlmProvider::XAi => Ok(Box::new(XaiClient::new(config)?)),
        LlmProvider::Gcp => Ok(Box::new(GcpClient::new(config)?)),
        LlmProvider::Aws => Ok(Box::new(AwsClient::new(config)?)),
        // Future providers:
        // LlmProvider::Azure => Ok(Box::new(AzureClient::new(config)?)),
    }
}
