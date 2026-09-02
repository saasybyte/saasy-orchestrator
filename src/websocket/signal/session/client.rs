use std::collections::HashMap;
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use prost::Message;
use saasy_proto_rust::signal::SignalResponseEnvelope;
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio::time::{interval, Duration};
use tokio_tungstenite::{connect_async, tungstenite::Message as WebsocketMessage};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::orchestrator::SessionState;
use super::outbound_commands::SignalSessionPendingRequest;
use super::super::SignalInboundHandler;

#[derive(Clone)]
pub struct SignalSessionClient {
    url: String,
}

impl SignalSessionClient {
    pub fn new(url: String) -> Self {
        Self { url }
    }

    pub async fn run(
        self,
        session_id: String,
        participant_id: String,
        session_state: Arc<RwLock<SessionState>>,
        mut signal_session_control_rx: mpsc::Receiver<SignalSessionPendingRequest>,
        signal_session_client_token: CancellationToken,
    ) -> Result<(), Box<dyn std::error::Error>> {
        info!("Connecting to Signal Session at: {} for session: {}", self.url, session_id);
        
        let (websocket_stream, _) = connect_async(&self.url).await?;
        info!("Connected to Signal Session websocket as AI participant: {}", participant_id);
        
        let (mut sink, mut stream) = websocket_stream.split();
        
        // Heartbeat to keep connection alive
        let mut heartbeat_interval = interval(Duration::from_secs(30));

        let mut pending_responses: HashMap<String, oneshot::Sender<Result<SignalResponseEnvelope, String>>> = HashMap::new();
        
        loop {
            tokio::select! {
                biased;
        
                () = signal_session_client_token.cancelled() => {
                    info!("Signal Session client received cancellation signal");
                    break;
                }

                _ = heartbeat_interval.tick() => {
                    debug!("Sending ping");
                    if let Err(e) = sink.send(WebsocketMessage::Ping(vec![].into())).await {
                        error!("Failed to send ping: {e}");
                        break;
                    }
                }
                
                // Prepare outbound messages for signal service (session)
                Some(command) = signal_session_control_rx.recv() => {
                    let SignalSessionPendingRequest { request, response_tx } = command;
    
                    let request_id = request.request_id.clone();

                    let mut buf = Vec::new();
                    if let Err(e) = request.encode(&mut buf) {
                        let _ = response_tx.send(Err(format!("Failed to encode request: {e}")));
                        continue;
                    }

                    if let Err(e) = sink.send(WebsocketMessage::Binary(buf.into())).await {
                        let _ = response_tx.send(Err(format!("Failed to send request: {e}")));
                        continue;
                    }

                    pending_responses.insert(request_id, response_tx);
                }
                
                // Handle inbound messages from signal service (session)
                Some(message) = stream.next() => {
                    match message {
                        Ok(WebsocketMessage::Binary(data)) => {
                            match SignalResponseEnvelope::decode(&data[..]) {
                                Ok(response) if !response.request_id.is_empty() => {
                                    info!("Received response type: {} for request: {}", response.r#type, response.request_id);

                                    match pending_responses.remove(&response.request_id) {
                                        Some(response_tx) => {
                                            // Send response back to waiting negotiator
                                            let _ = response_tx.send(Ok(response));
                                        }
                                        None => {
                                            error!(
                                                "Received response for unknown request_id: {} (type: {})",
                                                response.request_id,
                                                response.r#type
                                            );
                                        }
                                    }
                                }
                                _ => {
                                    // Either decode failed or request_id is empty - treat as event
                                    if let Err(e) = SignalInboundHandler::handle_session_event(
                                        session_state.clone(),
                                        data,
                                    ).await {
                                        error!("Failed to handle signal session event: {e}");
                                    }
                                }
                            }
                        }
                        Ok(WebsocketMessage::Pong(_)) => {
                            debug!("Received pong");
                        }
                        Ok(WebsocketMessage::Ping(data)) => {
                            debug!("Received ping, sending pong");
                            if let Err(e) = sink.send(WebsocketMessage::Pong(data)).await {
                                error!("Failed to send pong: {}", e);
                                break;
                            }
                        }
                        Ok(WebsocketMessage::Close(_)) => {
                            info!("Received close frame");
                            break;
                        }
                        Ok(WebsocketMessage::Text(text)) => {
                            warn!("Unexpected text message: {} bytes", text.len());
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

        for (_, response_tx) in pending_responses {
            let _ = response_tx.send(Err("Signal Session websocket connection closed".to_string()));
        }
        
        info!("Signal Session websocket connection closed for AI participant {}", participant_id);
        Ok(())
    }
}
