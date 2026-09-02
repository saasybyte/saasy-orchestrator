use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::grpc::listening_engine::ListeningEngineClient;
use crate::grpc::speaking_engine::SpeakingEngineClient;

#[derive(Debug, Clone, Default)]
pub struct HealthStatus {
    pub listening_engine: bool,
    pub speaking_engine: bool,
    pub signal: bool,
}

impl HealthStatus {
    pub fn is_ready(&self) -> bool {
        self.listening_engine && self.speaking_engine && self.signal
    }
}

pub struct HealthBackgroundService {
    http_client: reqwest::Client,
    status: Arc<RwLock<HealthStatus>>,
    check_interval: Duration,
    listening_engine_client: Arc<ListeningEngineClient>,
    speaking_engine_client: Arc<SpeakingEngineClient>,
    signal_http_url: String,
}

impl HealthBackgroundService {
    pub fn new(
        listening_engine_client: Arc<ListeningEngineClient>,
        speaking_engine_client: Arc<SpeakingEngineClient>,
        signal_http_url: String,
        check_interval: Duration,
    ) -> (Self, Arc<RwLock<HealthStatus>>) {
        let status = Arc::new(RwLock::new(HealthStatus::default()));
        let service = Self {
            http_client: reqwest::Client::new(),
            status: status.clone(),
            check_interval,
            listening_engine_client,
            speaking_engine_client,
            signal_http_url,
        };
        (service, status)
    }

    pub async fn run(self, shutdown_token: CancellationToken) {
        info!(
            "Starting health background service (interval: {:?})",
            self.check_interval
        );

        let mut ticker = interval(self.check_interval);

        loop {
            tokio::select! {
                biased;
                
                () = shutdown_token.cancelled() => {
                    info!("Health background service received shutdown signal");
                    break;
                }
                _ = ticker.tick() => {
                    self.check_all().await;
                }
            }
        }
        
        info!("Health background service stopped");
    }

    async fn check_all(&self) {
        let listening = self.check_listening_engine().await;
        let speaking = self.check_speaking_engine().await;
        let signaling = self.check_signal_service().await;

        let new_status = HealthStatus {
            listening_engine: listening,
            speaking_engine: speaking,
            signal: signaling,
        };

        debug!(
            "Health check: listening={listening}, speaking={speaking}, signaling={signaling}"
        );

        let mut status = self.status.write().await;
        *status = new_status;
    }

    async fn check_listening_engine(&self) -> bool {
        match self.listening_engine_client.health_check().await {
            Ok(alive) => alive,
            Err(e) => {
                error!("Listening engine health check failed: {e}");
                false
            }
        }
    }

    async fn check_speaking_engine(&self) -> bool {
        match self.speaking_engine_client.health_check().await {
            Ok(alive) => alive,
            Err(e) => {
                error!("Speaking engine health check failed: {e}");
                false
            }
        }
    }

    async fn check_signal_service(&self) -> bool {
        let url = format!("{}/health/live", &self.signal_http_url);

        match self.http_client
            .get(url)
            .timeout(Duration::from_secs(5))
            .send()
            .await
        {
            Ok(response) => response.status().is_success(),
            Err(e) => {
                error!("Signal health check failed: {e}");
                false
            }
        }
    }
}
