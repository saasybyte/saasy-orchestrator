use std::sync::Arc;
use std::time::Duration;

use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::grpc::edge::EdgeService;

pub struct ProviderBackgroundService {
    edge_service: Arc<EdgeService>,
    refresh_interval: Duration,
}

impl ProviderBackgroundService {
    pub fn new(edge_service: Arc<EdgeService>) -> Self {
        Self {
            edge_service,
            refresh_interval: Duration::from_secs(86400), // 24 hours
        }
    }

    pub async fn run(self, shutdown_token: CancellationToken) {
        info!(
            "Starting provider background service (interval: {:?})",
            self.refresh_interval
        );

        let mut ticker = interval(self.refresh_interval);
        ticker.tick().await; // Skip immediate first tick

        loop {
            tokio::select! {
                biased;

                () = shutdown_token.cancelled() => {
                    info!("Provider background service received shutdown signal");
                    break;
                }
                _ = ticker.tick() => {
                    if let Err(e) = self.edge_service.refresh_providers().await {
                        warn!("Daily provider cache refresh failed: {e}");
                    }
                }
            }
        }

        info!("Provider background service stopped");
    }
}
