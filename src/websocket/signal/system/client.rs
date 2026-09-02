use futures_util::{SinkExt, StreamExt};
use saasy_proto_rust::sfu::SfuEvent;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};
use tokio_tungstenite::{connect_async, tungstenite::Message as WebsocketMessage};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use super::super::SignalInboundHandler;

pub struct SignalSystemClient {
    url: String,
}

impl SignalSystemClient {
    pub fn new(url: String) -> Self {
        Self { url }
    }

    pub async fn run(
        &self,
        signal_system_event_tx: mpsc::Sender<SfuEvent>,
        shutdown_token: CancellationToken,
    ) -> Result<(), Box<dyn std::error::Error>> {
        info!("Connecting to Signal System at: {}", self.url);
        
        let (websocket_stream, _) = connect_async(&self.url).await?;
        info!("Connected to Signal System websocket");
        
        let (mut sink, mut stream) = websocket_stream.split();
        
        // Heartbeat to keep connection alive
        let mut heartbeat_interval = interval(Duration::from_secs(30));
        
        loop {
            tokio::select! {
                biased;
                
                () = shutdown_token.cancelled() => {
                    info!("Signal System client received shutdown signal");
                    break;
                }
                
                _ = heartbeat_interval.tick() => {
                    debug!("Sending ping");
                    if let Err(e) = sink.send(WebsocketMessage::Ping(vec![].into())).await {
                        error!("Failed to send ping: {e}");
                        break;
                    }
                }

                // Handle inbound messages from signal service (system)
                Some(message) = stream.next() => {
                    match message {
                        Ok(WebsocketMessage::Binary(data)) => {
                            if let Err(e) = SignalInboundHandler::handle_system_event(
                                signal_system_event_tx.clone(),
                                data,
                            ).await {
                                error!("Failed to handle signal system event: {e}");
                            }
                        }
                        Ok(WebsocketMessage::Pong(_)) => {
                            debug!("Received pong");
                        }
                        Ok(WebsocketMessage::Ping(data)) => {
                            debug!("Received ping, sending pong");
                            if let Err(e) = sink.send(WebsocketMessage::Pong(data)).await {
                                error!("Failed to send pong: {e}");
                                break;
                            }
                        }
                        Ok(WebsocketMessage::Close(_)) => {
                            info!("Received Close message");
                            break;
                        }
                        Ok(WebsocketMessage::Text(text)) => {
                            warn!("Unexpected Text message: {} bytes", text.len());
                        }
                        Ok(_) => {}
                        Err(e) => {
                            error!("WebSocket error: {e}");
                            break;
                        }
                    }
                }
            }
        }
        
        info!("Signal System websocket connection closed");
        Ok(())
    }
}
