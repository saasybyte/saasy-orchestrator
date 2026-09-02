mod background;
mod config;
mod stores;
mod grpc;
mod http;
mod orchestrator;
mod sse;
mod websocket;

use std::io;
use std::sync::Arc;
use std::time::Duration;

use actix_web::{web, App, HttpServer};
use tokio::signal;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use background::{HealthBackgroundService, ProviderBackgroundService};
use config::ServerConfig;
use stores::{ApiKeyStore, CloudProviderStore};
use grpc::EdgeService;
use orchestrator::OrchestratorCore;
use websocket::signal::SignalSystemClient;

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::main]
#[allow(clippy::redundant_pub_crate)]
#[allow(clippy::too_many_lines)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Arc::new(ServerConfig::from_env()
        .map_err(|e| io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("Failed to load config: {e}")
        ))?);

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // Shared cancellation token for graceful shutdown
    let shutdown_token = CancellationToken::new();

    // Build API key store
    let api_key_store = ApiKeyStore::new(
        config.llm_api_keys(),
        config.stt_api_keys(),
        config.tts_api_keys(),
    );

    // Build cloud provider store
    let cloud_provider_store = Arc::new(CloudProviderStore::new(config.cloud_provider_configs()));

    // Initialize edge service (fail-fast if edge unreachable)
    let edge_service = Arc::new(
        EdgeService::new(&config.edge_grpc_url)
            .await
            .map_err(|e| io::Error::new(
                io::ErrorKind::ConnectionRefused,
                format!("Failed to connect to saasy-edge at {}: {e}", config.edge_grpc_url)
            ))?
    );

    info!("Starting Orchestrator...");
    let orchestrator_core = Arc::new(
        OrchestratorCore::new(
            api_key_store,
            cloud_provider_store,
            config.signal_session_ws_url.clone(),
            config.listening_engine_grpc_uds.clone(),
            config.speaking_engine_grpc_uds.clone(),
            shutdown_token.clone(),
            edge_service.clone(),
        ).await?
    );

    // Spawn health background service
    let (health_background_service, health_status) = HealthBackgroundService::new(
        orchestrator_core.listening_engine_client(),
        orchestrator_core.speaking_engine_client(),
        config.signal_http_url.clone(),
        Duration::from_secs(5),
    );
    let health_background_service_token = shutdown_token.clone();
    let health_background_service_task_handle = tokio::spawn(async move {
        health_background_service.run(health_background_service_token).await;
    });

    // Spawn background provider refresh service
    let provider_background_service = ProviderBackgroundService::new(edge_service.clone());
    let provider_background_service_token = shutdown_token.clone();
    let provider_background_service_task_handle = tokio::spawn(async move {
        provider_background_service.run(provider_background_service_token).await;
    });

    // Spawn HTTP health server
    let http_host = config.http_host.clone();
    let http_port = config.http_port;
    info!("Starting health server on {}:{}", http_host, http_port);

    let health_server = HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(health_status.clone()))
            .service(http::health::liveness)
            .service(http::health::readiness)
    })
        .bind((http_host, http_port))
        .map_err(|e| io::Error::new(
            io::ErrorKind::AddrInUse,
            format!("Failed to bind health server: {e}")
        ))?
        .disable_signals()
        .run();

    let health_server_control_handle = health_server.handle();
    let health_server_task_handle = tokio::spawn(health_server);

    // Spawn Signal System client
    let signal_system_client = SignalSystemClient::new(
        config.signal_system_ws_url.clone(),
    );
    let (signal_system_event_tx, mut signal_system_event_rx) = mpsc::channel(100);
    let signal_system_client_token = shutdown_token.clone();
    let mut signal_system_client_task_handle = tokio::spawn(async move {
        if let Err(e) = signal_system_client.run(signal_system_event_tx, signal_system_client_token).await {
            error!("Signal System handler error: {e}");
        }
    });

    // Spawn Signal System event processor
    let orchestrator_core_clone = orchestrator_core.clone();
    let signal_system_event_token = shutdown_token.clone();
    let signal_system_event_task_handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;

                () = signal_system_event_token.cancelled() => {
                    info!("Event processor received shutdown signal");
                    break;
                }
                signal_system_event = signal_system_event_rx.recv() => {
                    if let Some(event) = signal_system_event {
                        orchestrator_core_clone.handle_system_event(event).await;
                    } else {
                        info!("Event channel closed");
                        break;
                    }
                }
            }
        }
        info!("System event processing loop ended");
    });

    info!("All services started. Waiting for signal system events...");

    // Wait for shutdown signal or unexpected task termination
    tokio::select! {
        () = shutdown_signal() => {
            info!("Shutdown signal received");
        }
        result = &mut signal_system_client_task_handle => {
            match result {
                Ok(()) => warn!("Signal System client ended unexpectedly"),
                Err(e) => error!("Signal System client panicked: {e}"),
            }
        }
    }

    // Graceful Shutdown Sequence
    info!("Starting graceful shutdown...");
    shutdown_token.cancel(); // Signal all tasks to stop
    health_server_control_handle.stop(true).await; // Stop accepting new HTTP connections
    orchestrator_core.shutdown_all_sessions().await; // Shutdown all active sessions
    let shutdown_result = tokio::time::timeout( // Await all tasks with timeout
        SHUTDOWN_TIMEOUT,
        async {
            let _ = tokio::join!(
                health_background_service_task_handle,
                provider_background_service_task_handle,
                health_server_task_handle,
                signal_system_event_task_handle,
            );

            if !signal_system_client_task_handle.is_finished() {
                let _ = signal_system_client_task_handle.await;
            }
        }
    ).await;

    if shutdown_result.is_ok() {
        info!("All tasks shut down gracefully");
    } else {
        warn!("Shutdown timed out after {:?}, forcing exit", SHUTDOWN_TIMEOUT);
    }

    info!("Orchestrator shutdown complete");
    Ok(())
}

#[allow(clippy::expect_used)]
#[allow(clippy::redundant_pub_crate)]
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install ctrl+c signal handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install terminate signal handler")
            .recv()
            .await;
    };

    tokio::select! {
        () = ctrl_c => {
            info!("Received Ctrl+C");
        },
        () = terminate => {
            info!("Received terminate signal");
        },
    }
}
