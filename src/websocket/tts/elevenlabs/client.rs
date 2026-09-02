use std::collections::HashSet;
use std::time::Instant;

use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{connect_async, tungstenite::Message as WebSocketMessage};
use tracing::{debug, error, info, warn};

use super::inbound_handler::ElevenLabsInboundHandler;
use super::types::{
    ElevenLabsCloseContext, ElevenLabsCloseSocket, ElevenLabsFlushContext,
    ElevenLabsGenerationConfig, ElevenLabsInitConnectionMulti, ElevenLabsInitialiseContext,
    ElevenLabsKeepContextAlive, ElevenLabsResponse, ElevenLabsSendTextMulti,
    ElevenLabsVoiceSettings,
};
use crate::websocket::tts::client::TtsClient;
use crate::websocket::tts::error::TtsClientError;
use crate::websocket::tts::types::{TtsAudioChunk, TtsClientConfig, TtsCredentials};

#[derive(Debug, Clone)]
enum ElevenLabsCommand {
    SendText { text: String, context_id: String, flush: bool },
    FlushContext { context_id: String },
    CancelContext { context_id: String },
}

pub struct ElevenLabsClient {
    api_key: String,
    model: String,
    voice_id: String,
    shutdown_tx: Option<mpsc::Sender<()>>,
    command_tx: Option<mpsc::Sender<ElevenLabsCommand>>,
    audio_rx: Option<mpsc::UnboundedReceiver<TtsAudioChunk>>,
}

impl ElevenLabsClient {
    pub fn new(config: &TtsClientConfig) -> Result<Self, TtsClientError> {
        #[allow(unreachable_patterns)]
        let api_key = match &config.credentials {
            TtsCredentials::ApiKey(key) => key.clone(),
            _ => {
                return Err(TtsClientError::Config(
                    "ElevenLabsClient requires ApiKey credentials".to_string(),
                ))
            }
        };
        Ok(Self {
            api_key,
            model: config.model.clone(),
            // Default voice - can be made configurable later
            voice_id: "21m00Tcm4TlvDq8ikWAM".to_string(), // "Rachel" voice
            shutdown_tx: None,
            command_tx: None,
            audio_rx: None,
        })
    }

    #[allow(clippy::too_many_lines)]
    async fn run_connection(
        api_key: String,
        model: String,
        voice_id: String,
        mut shutdown_rx: mpsc::Receiver<()>,
        mut command_rx: mpsc::Receiver<ElevenLabsCommand>,
        audio_tx: mpsc::UnboundedSender<TtsAudioChunk>,
    ) -> Result<(), TtsClientError> {
        const INIT_CONTEXT_ID: &str = "__init__";

        info!("Connecting to ElevenLabs Multi-Context...");

        // Multi-Context endpoint
        let url = format!(
            "wss://api.elevenlabs.io/v1/text-to-speech/{voice_id}/multi-stream-input?model_id={model}&output_format=pcm_24000&inactivity_timeout=180",
        );

        let mut request = url
            .into_client_request()
            .map_err(|e| TtsClientError::Connection(e.to_string()))?;

        // Add API key header
        request.headers_mut().insert(
            "xi-api-key",
            api_key
                .parse()
                .map_err(|e| TtsClientError::Connection(format!("Invalid API key header: {e}")))?,
        );

        let (websocket_stream, _) = connect_async(request)
            .await
            .map_err(|e| TtsClientError::Connection(format!("Failed to connect: {e}")))?;

        info!("Connected to ElevenLabs Multi-Context");

        let (mut sink, mut stream) = websocket_stream.split();

        // Eager init: send InitConnectionMulti immediately with __init__ context
        let init_msg = ElevenLabsInitConnectionMulti {
            text: " ".to_string(),
            context_id: INIT_CONTEXT_ID.to_string(),
            voice_settings: Some(ElevenLabsVoiceSettings {
                stability: Some(0.5),
                similarity_boost: Some(0.75),
                style: Some(0.0),
                use_speaker_boost: Some(true),
                speed: Some(1.0),
            }),
            generation_config: Some(ElevenLabsGenerationConfig {
                chunk_length_schedule: Some(vec![50, 120, 200, 260]),
            }),
        };

        match serde_json::to_string(&init_msg) {
            Ok(json) => {
                debug!("Sending InitConnectionMulti with __init__ context");
                if let Err(e) = sink.send(WebSocketMessage::Text(json.into())).await {
                    error!("Failed to send init message: {e}");
                    return Err(TtsClientError::Connection(format!("Failed to init: {e}")));
                }
            }
            Err(e) => {
                error!("Failed to serialize init message: {e}");
                return Err(TtsClientError::Connection(format!("Failed to serialize init: {e}")));
            }
        }

        let mut heartbeat_interval = interval(Duration::from_secs(30));
        let mut keepalive_interval = interval(Duration::from_secs(60));

        // Track which contexts have been initialized (__init__ is always there)
        let mut initialized_contexts: HashSet<String> = HashSet::from([INIT_CONTEXT_ID.to_string()]);
        // Track first audio per context for latency logging
        let mut first_audio_logged: HashSet<String> = HashSet::new();
        let mut context_start_times: std::collections::HashMap<String, Instant> =
            std::collections::HashMap::new();

        loop {
            tokio::select! {
                _ = heartbeat_interval.tick() => {
                    debug!("Sending ping to ElevenLabs");
                    if let Err(e) = sink.send(WebSocketMessage::Ping(vec![].into())).await {
                        error!("Failed to send ping: {e}");
                        break;
                    }
                }

                _ = keepalive_interval.tick() => {
                    // Send keepalive to __init__ context to prevent 180s inactivity timeout
                    let keepalive_msg = ElevenLabsKeepContextAlive {
                        text: String::new(), // Empty string per docs
                        context_id: INIT_CONTEXT_ID.to_string(),
                    };

                    match serde_json::to_string(&keepalive_msg) {
                        Ok(json) => {
                            debug!("Sending keepContextAlive to ElevenLabs");
                            if let Err(e) = sink.send(WebSocketMessage::Text(json.into())).await {
                                error!("Failed to send keepalive: {e}");
                                break;
                            }
                        }
                        Err(e) => error!("Failed to serialize keepalive: {e}"),
                    }
                }

                // Prepare outbound messages
                Some(command) = command_rx.recv() => {
                    match command {
                        ElevenLabsCommand::SendText { text, context_id, flush } => {
                            // Track start time for latency measurement
                            if !context_start_times.contains_key(&context_id) {
                                context_start_times.insert(context_id.clone(), Instant::now());
                            }

                            // If new context, initialize it first
                            if !initialized_contexts.contains(&context_id) {
                                let init_ctx_msg = ElevenLabsInitialiseContext {
                                    text: " ".to_string(),
                                    context_id: context_id.clone(),
                                    voice_settings: None, // Inherit from connection
                                    generation_config: None,
                                };

                                match serde_json::to_string(&init_ctx_msg) {
                                    Ok(json) => {
                                        debug!("Sending InitialiseContext for context {}", context_id);
                                        if let Err(e) = sink.send(WebSocketMessage::Text(json.into())).await {
                                            error!("Failed to send init context message: {e}");
                                            break;
                                        }
                                    }
                                    Err(e) => error!("Failed to serialize init context message: {e}"),
                                }

                                initialized_contexts.insert(context_id.clone());
                            }

                            // Send the text
                            let text_msg = ElevenLabsSendTextMulti {
                                text,
                                context_id,
                                flush: Some(flush),
                            };

                            match serde_json::to_string(&text_msg) {
                                Ok(json) => {
                                    if let Err(e) = sink.send(WebSocketMessage::Text(json.into())).await {
                                        error!("Failed to send text message: {e}");
                                        break;
                                    }
                                }
                                Err(e) => error!("Failed to serialize text message: {e}"),
                            }
                        }

                        ElevenLabsCommand::FlushContext { context_id } => {
                            let flush_msg = ElevenLabsFlushContext {
                                context_id: context_id.clone(),
                                flush: true,
                                text: None,
                            };

                            match serde_json::to_string(&flush_msg) {
                                Ok(json) => {
                                    debug!("Flushing context {}", context_id);
                                    if let Err(e) = sink.send(WebSocketMessage::Text(json.into())).await {
                                        error!("Failed to send flush message: {e}");
                                    }
                                }
                                Err(e) => error!("Failed to serialize flush message: {e}"),
                            }
                        }

                        ElevenLabsCommand::CancelContext { context_id } => {
                            let close_msg = ElevenLabsCloseContext {
                                context_id: context_id.clone(),
                                close_context: true,
                            };

                            match serde_json::to_string(&close_msg) {
                                Ok(json) => {
                                    debug!("Cancelling context {}", context_id);
                                    if let Err(e) = sink.send(WebSocketMessage::Text(json.into())).await {
                                        error!("Failed to send close context message: {e}");
                                    }
                                }
                                Err(e) => error!("Failed to serialize close context message: {e}"),
                            }

                            // Clean up tracking for this context
                            initialized_contexts.remove(&context_id);
                            first_audio_logged.remove(&context_id);
                            context_start_times.remove(&context_id);
                        }
                    }
                }

                // Handle inbound messages
                Some(message) = stream.next() => {
                    match message {
                        Ok(WebSocketMessage::Text(text)) => {
                            match serde_json::from_str::<ElevenLabsResponse>(&text) {
                                Ok(response) => {
                                    if let Some(audio_chunk) = ElevenLabsInboundHandler::handle_response(response) {
                                        if !audio_chunk.data.is_empty() {
                                            if let Some(ref ctx_id) = audio_chunk.context_id {
                                                if !first_audio_logged.contains(ctx_id) {
                                                    if let Some(start) = context_start_times.get(ctx_id) {
                                                        let elapsed = start.elapsed();
                                                        info!(
                                                            "[LATENCY] First TTS audio chunk received | elapsed: {}ms | context_id: {}",
                                                            elapsed.as_millis(),
                                                            ctx_id
                                                        );
                                                        first_audio_logged.insert(ctx_id.clone());
                                                    }
                                                }
                                            }
                                        }

                                        // Clean up tracking when context is done
                                        if audio_chunk.done {
                                            if let Some(ref ctx_id) = audio_chunk.context_id {
                                                initialized_contexts.remove(ctx_id);
                                                first_audio_logged.remove(ctx_id);
                                                context_start_times.remove(ctx_id);
                                            }
                                        }

                                        if audio_tx.send(audio_chunk).is_err() {
                                            warn!("Audio receiver dropped");
                                            break;
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to parse ElevenLabs response: {}", e);
                                    debug!("Raw response: {}", text);
                                }
                            }
                        }
                        Ok(WebSocketMessage::Binary(data)) => {
                            warn!("Unexpected binary message: {} bytes", data.len());
                        }
                        Ok(WebSocketMessage::Pong(_)) => {
                            debug!("Received pong from ElevenLabs");
                        }
                        Ok(WebSocketMessage::Ping(data)) => {
                            debug!("Received ping from ElevenLabs, sending pong");
                            if let Err(e) = sink.send(WebSocketMessage::Pong(data)).await {
                                error!("Failed to send pong: {e}");
                                break;
                            }
                        }
                        Ok(WebSocketMessage::Close(_)) => {
                            info!("Received Close message from ElevenLabs");
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
                    info!("Shutdown signal received, sending closeSocket");

                    // Send closeSocketClient for graceful shutdown
                    let close_msg = ElevenLabsCloseSocket {
                        close_socket: true,
                    };

                    match serde_json::to_string(&close_msg) {
                        Ok(json) => {
                            if let Err(e) = sink.send(WebSocketMessage::Text(json.into())).await {
                                warn!("Failed to send closeSocket: {e}");
                            }
                        }
                        Err(e) => warn!("Failed to serialize closeSocket: {e}"),
                    }

                    break;
                }
            }
        }

        info!("ElevenLabs Multi-Context connection closed");
        Ok(())
    }
}

#[async_trait::async_trait]
impl TtsClient for ElevenLabsClient {
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
        let model = self.model.clone();
        let voice_id = self.voice_id.clone();

        tokio::spawn(async move {
            if let Err(e) =
                Self::run_connection(api_key, model, voice_id, shutdown_rx, command_rx, audio_tx)
                    .await
            {
                error!("WebSocket connection error: {e}");
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
        let Some(ref command_tx) = self.command_tx else {
            return Err(TtsClientError::Connection("Not connected".to_string()));
        };

        let context_id = context_id.ok_or_else(|| {
            TtsClientError::Connection("ElevenLabs requires context_id".to_string())
        })?;

        if transcript.is_empty() && !continue_generation {
            // Empty text with continue=false means flush and close context
            command_tx
                .send(ElevenLabsCommand::FlushContext { context_id })
                .await
                .map_err(|e| TtsClientError::Connection(format!("Failed to send flush: {e}")))?;
        } else if !transcript.is_empty() {
            // Send text to context
            command_tx
                .send(ElevenLabsCommand::SendText {
                    text: transcript,
                    context_id,
                    flush: !continue_generation,
                })
                .await
                .map_err(|e| TtsClientError::Connection(format!("Failed to send text: {e}")))?;
        }

        Ok(())
    }

    async fn cancel_generation(&self, context_id: String) -> Result<(), TtsClientError> {
        let Some(ref command_tx) = self.command_tx else {
            return Err(TtsClientError::Connection("Not connected".to_string()));
        };

        command_tx
            .send(ElevenLabsCommand::CancelContext { context_id })
            .await
            .map_err(|e| TtsClientError::Connection(format!("Failed to send cancel: {e}")))?;

        Ok(())
    }

    fn take_inbound_audio_receiver(&mut self) -> Option<mpsc::UnboundedReceiver<TtsAudioChunk>> {
        self.audio_rx.take()
    }
}
