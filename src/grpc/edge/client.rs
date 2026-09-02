use std::sync::Arc;

use saasy_proto_rust::edge::v1::{
    edge_service_client::EdgeServiceClient,
    ListProviderModelsRequest,
    ListProviderModelsResponse,
};
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tracing::info;

use super::error::EdgeClientError;

pub struct EdgeClient {
    inner: Arc<Mutex<EdgeServiceClient<Channel>>>,
}

impl EdgeClient {
    pub async fn connect(url: &str) -> Result<Self, EdgeClientError> {
        info!("Connecting to saasy-edge at {url}");

        let channel = Channel::from_shared(url.to_string())?
            .connect()
            .await?;

        info!("Connected to saasy-edge");

        Ok(Self {
            inner: Arc::new(Mutex::new(EdgeServiceClient::new(channel))),
        })
    }

    pub async fn list_provider_models(&self) -> Result<ListProviderModelsResponse, EdgeClientError> {
        let response = self.inner
            .lock()
            .await
            .list_provider_models(ListProviderModelsRequest {})
            .await?;

        Ok(response.into_inner())
    }
}
