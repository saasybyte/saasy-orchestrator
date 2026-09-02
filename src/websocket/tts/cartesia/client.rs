use std::collections::HashSet;
use std::time::Instant;

use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{connect_async, tungstenite::Message as WebSocketMessage};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use super::inbound_handler::CartesiaInboundHandler;
use super::types::{
    CartesiaCancelContextRequest, CartesiaContainer, CartesiaEncoding, CartesiaGenerationRequest,
    CartesiaLanguage, CartesiaOutputFormat, CartesiaResponse, CartesiaVoice,
};
use crate::websocket::tts::client::TtsClient;
use crate::websocket::tts::error::TtsClientError;
use crate::websocket::tts::types::{TtsAudioChunk, TtsClientConfig, TtsCredentials};

#[derive(Debug, Clone)]
enum CartesiaCommand {
    SendGeneration(CartesiaGenerationRequest),
    SendCancelContext(CartesiaCancelContextRequest),
}

pub struct CartesiaClient {
    api_key: String,
    model: String,
    version: String,
    shutdown_tx: Option<mpsc::Sender<()>>,
    command_tx: Option<mpsc::Sender<CartesiaCommand>>,
    audio_rx: Option<mpsc::UnboundedReceiver<TtsAudioChunk>>,
}

impl CartesiaClient {
    pub fn new(config: &TtsClientConfig) -> Result<Self, TtsClientError> {
        #[allow(unreachable_patterns)]
        let api_key = match &config.credentials {
            TtsCredentials::ApiKey(key) => key.clone(),
            _ => return Err(TtsClientError::Config(
                "CartesiaClient requires ApiKey credentials".to_string()
            )),
        };
        Ok(Self {
            api_key,
            model: config.model.clone(),
            version: config.version.clone(),
            shutdown_tx: None,
            command_tx: None,
            audio_rx: None,
        })
    }

    #[allow(clippy::too_many_lines)]
    async fn run_connection(
        api_key: String,
        version: String,
        mut shutdown_rx: mpsc::Receiver<()>,
        mut command_rx: mpsc::Receiver<CartesiaCommand>,
        audio_tx: mpsc::UnboundedSender<TtsAudioChunk>,
    ) -> Result<(), TtsClientError> {
        info!("Connecting to Cartesia...");

        let url = format!(
            "wss://api.cartesia.ai/tts/websocket?api_key={api_key}&cartesia_version={version}",
        );

        let request = url
            .into_client_request()
            .map_err(|e| TtsClientError::Connection(e.to_string()))?;

        let (websocket_stream, _) = connect_async(request)
            .await
            .map_err(|e| TtsClientError::Connection(format!("Failed to connect: {e}")))?;

        info!("Connected to Cartesia");

        let (mut sink, mut stream) = websocket_stream.split();

        let mut heartbeat_interval = interval(Duration::from_secs(30));

        let mut logged_context_ids: HashSet<String> = HashSet::new();
        let mut tts_request_starts: std::collections::HashMap<String, Instant> =
            std::collections::HashMap::new();

        loop {
            tokio::select! {
                _ = heartbeat_interval.tick() => {
                    debug!("Sending ping to Cartesia");
                    if let Err(e) = sink.send(WebSocketMessage::Ping(vec![].into())).await {
                        error!("Failed to send ping: {e}");
                        break;
                    }
                }

                // Prepare outbound messages
                Some(command) = command_rx.recv() => {
                    match command {
                        CartesiaCommand::SendGeneration(request) => {
                            if let Some(ref context_id) = request.context_id {
                                tts_request_starts.insert(context_id.clone(), Instant::now());
                            }

                            match serde_json::to_string(&request) {
                                Ok(json) => {
                                    if let Err(e) = sink.send(WebSocketMessage::Text(json.into())).await {
                                        error!("Failed to send TTS request: {e}");
                                        break;
                                    }
                                }
                                Err(e) => error!("Failed to serialize TTS request: {e}"),
                            }
                        }
                        CartesiaCommand::SendCancelContext(request) => {
                            match serde_json::to_string(&request) {
                                Ok(json) => {
                                    if let Err(e) = sink.send(WebSocketMessage::Text(json.into())).await {
                                        error!("Failed to send cancel request: {e}");
                                    }
                                }
                                Err(e) => error!("Failed to serialize cancel request: {e}"),
                            }
                        }
                    }
                }

                // Handle inbound messages
                Some(message) = stream.next() => {
                    match message {
                        Ok(WebSocketMessage::Text(text)) => {
                            match serde_json::from_str::<CartesiaResponse>(&text) {
                                Ok(response) => {
                                    if let Some(audio_chunk) = CartesiaInboundHandler::handle_response(response) {
                                        if !audio_chunk.data.is_empty() {
                                            if let Some(ref context_id) = audio_chunk.context_id {
                                                if !logged_context_ids.contains(context_id) {
                                                    if let Some(start) = tts_request_starts.get(context_id) {
                                                        let elapsed = start.elapsed();
                                                        info!(
                                                            "[LATENCY] First TTS audio chunk received | elapsed: {}ms | context_id: {}",
                                                            elapsed.as_millis(),
                                                            context_id
                                                        );
                                                        logged_context_ids.insert(context_id.clone());
                                                    }
                                                }
                                            }
                                        }

                                        if audio_tx.send(audio_chunk).is_err() {
                                            warn!("Audio receiver dropped");
                                            break;
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to parse Cartesia response: {}", e);
                                    debug!("Raw response: {}", text);
                                }
                            }
                        }
                        Ok(WebSocketMessage::Binary(data)) => {
                            warn!("Unexpected binary message: {} bytes", data.len());
                        }
                        Ok(WebSocketMessage::Pong(_)) => {
                            debug!("Received pong from Cartesia");
                        }
                        Ok(WebSocketMessage::Ping(data)) => {
                            debug!("Received ping from Cartesia, sending pong");
                            if let Err(e) = sink.send(WebSocketMessage::Pong(data)).await {
                                error!("Failed to send pong: {e}");
                                break;
                            }
                        }
                        Ok(WebSocketMessage::Close(_)) => {
                            info!("Received Close message from Cartesia");
                            break;
                        }
                        Ok(_) => {}
                        Err(e) => {
                            error!("WebSocket error: {e}");
                            break;
                        }
                    }
                }

                _ = shutdown_rx.recv() => {
                    info!("Shutdown signal received");
                    break;
                }
            }
        }

        info!("Cartesia connection closed");
        Ok(())
    }
}

#[async_trait::async_trait]
impl TtsClient for CartesiaClient {
    async fn connect(&mut self) -> Result<(), TtsClientError> {
        if self.shutdown_tx.is_some() {
            return Ok(());
        }

        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
        let (command_tx, command_rx) = mpsc::channel(32);
        let (audio_tx, audio_rx) = mpsc::unbounded_channel();

        self.shutdown_tx = Some(shutdown_tx);
        self.command_tx = Some(command_tx);
        self.audio_rx = Some(audio_rx);

        let api_key = self.api_key.clone();
        let version = self.version.clone();

        tokio::spawn(async move {
            if let Err(e) =
                Self::run_connection(api_key, version, shutdown_rx, command_rx, audio_tx).await
            {
                error!("Websocket connection error: {e}");
            }
        });

        Ok(())
    }

    async fn disconnect(&mut self) -> Result<(), TtsClientError> {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(()).await;
        }

        self.command_tx = None;
        self.audio_rx = None;

        Ok(())
    }

    async fn generate_speech(
        &self,
        transcript: String,
        context_id: Option<String>,
        continue_generation: bool,
    ) -> Result<(), TtsClientError> {
        if let Some(ref command_tx) = self.command_tx {
            let request = CartesiaGenerationRequest {
                model_id: self.model.clone(),
                transcript,
                voice: CartesiaVoice {
                    mode: "id".to_string(),
                    id: "f9836c6e-a0bd-460e-9d3c-f7299fa60f94".to_string(),
                },
                output_format: CartesiaOutputFormat {
                    container: CartesiaContainer::Raw,
                    encoding: CartesiaEncoding::PcmS16le,
                    sample_rate: 48000,
                },
                language: Some(CartesiaLanguage::En),
                context_id: context_id.or_else(|| Some(Uuid::new_v4().to_string())),
                continue_generation: Some(continue_generation),
            };

            command_tx
                .send(CartesiaCommand::SendGeneration(request))
                .await
                .map_err(|e| TtsClientError::Connection(format!("Failed to send generation: {e}")))?;
            Ok(())
        } else {
            Err(TtsClientError::Connection("Not connected".to_string()))
        }
    }

    async fn cancel_generation(&self, context_id: String) -> Result<(), TtsClientError> {
        if let Some(ref command_tx) = self.command_tx {
            let request = CartesiaCancelContextRequest {
                context_id,
                cancel: true,
            };

            command_tx
                .send(CartesiaCommand::SendCancelContext(request))
                .await
                .map_err(|e| TtsClientError::Connection(format!("Failed to send cancel: {e}")))?;
            Ok(())
        } else {
            Err(TtsClientError::Connection("Not connected".to_string()))
        }
    }

    fn take_inbound_audio_receiver(&mut self) -> Option<mpsc::UnboundedReceiver<TtsAudioChunk>> {
        self.audio_rx.take()
    }
}
