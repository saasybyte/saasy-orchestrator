use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{interval, Duration};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{connect_async, tungstenite::Message as WebSocketMessage};
use tracing::{debug, error, info, warn};

use super::inbound_handler::SpeechmaticsInboundHandler;
use super::types::{SpeechmaticsRequest, SpeechmaticsResponse, StartRecognitionMessage};
use crate::websocket::stt::client::SttClient;
use crate::websocket::stt::error::SttClientError;
use crate::websocket::stt::types::{SttClientConfig, SttCredentials, SttTranscript};

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone)]
enum SpeechmaticsCommand {
    SendAudio(Vec<u8>),
    SendForceEndOfUtterance,
    SendEndOfStream,
}

pub struct SpeechmaticsClient {
    api_key: String,
    sample_rate: u32,
    language: String,
    operating_point: Option<String>,
    shutdown_tx: Option<mpsc::Sender<()>>,
    command_tx: Option<mpsc::Sender<SpeechmaticsCommand>>,
    transcript_rx: Option<mpsc::UnboundedReceiver<SttTranscript>>,
}

impl SpeechmaticsClient {
    pub fn new(config: &SttClientConfig) -> Result<Self, SttClientError> {
        #[allow(unreachable_patterns)]
        let api_key = match &config.credentials {
            SttCredentials::ApiKey(key) => key.clone(),
            _ => {
                return Err(SttClientError::Config(
                    "SpeechmaticsClient requires ApiKey credentials".to_string(),
                ))
            }
        };

        // Extract language code (e.g., "en-US" -> "en")
        let language = config
            .language
            .as_ref()
            .map_or_else(
                || "en".to_string(),
                |l| l.split('-').next().unwrap_or("en").to_string(),
            );

        // Model maps to operating_point (e.g., "standard", "enhanced")
        let operating_point = if config.model.is_empty() {
            None
        } else {
            Some(config.model.clone())
        };

        Ok(Self {
            api_key,
            sample_rate: config.sample_rate,
            language,
            operating_point,
            shutdown_tx: None,
            command_tx: None,
            transcript_rx: None,
        })
    }

    #[allow(clippy::too_many_lines)]
    #[allow(clippy::too_many_arguments)]
    async fn run_connection(
        api_key: String,
        sample_rate: u32,
        language: String,
        operating_point: Option<String>,
        mut shutdown_rx: mpsc::Receiver<()>,
        mut command_rx: mpsc::Receiver<SpeechmaticsCommand>,
        transcript_tx: mpsc::UnboundedSender<SttTranscript>,
        ready_tx: oneshot::Sender<()>,
    ) -> Result<(), SttClientError> {
        info!("Connecting to Speechmatics...");

        let url = "wss://us.rt.speechmatics.com/v2";

        let mut request = url
            .into_client_request()
            .map_err(|e| SttClientError::Connection(e.to_string()))?;

        // Speechmatics uses Bearer token auth
        request.headers_mut().insert(
            "Authorization",
            format!("Bearer {api_key}")
                .parse()
                .map_err(|e| SttClientError::Connection(format!("Invalid header value: {e}")))?,
        );

        let (websocket_stream, _) = connect_async(request)
            .await
            .map_err(|e| SttClientError::Connection(format!("Failed to connect: {e}")))?;

        info!("Connected to Speechmatics WebSocket");

        let (mut sink, mut stream) = websocket_stream.split();

        // Send StartRecognition message
        let start_recognition = StartRecognitionMessage::new(sample_rate, &language, operating_point);
        let start_json = start_recognition
            .to_json_string()
            .map_err(|e| SttClientError::Connection(format!("Failed to serialize: {e}")))?;

        sink.send(WebSocketMessage::Text(start_json.into()))
            .await
            .map_err(|e| {
                SttClientError::Connection(format!("Failed to send StartRecognition: {e}"))
            })?;

        info!("Sent StartRecognition, waiting for RecognitionStarted...");

        // Wait for RecognitionStarted response
        tokio::time::timeout(Duration::from_secs(10), async {
            while let Some(msg) = stream.next().await {
                match msg {
                    Ok(WebSocketMessage::Text(text)) => {
                        match serde_json::from_str::<SpeechmaticsResponse>(&text) {
                            Ok(SpeechmaticsResponse::RecognitionStarted(started)) => {
                                info!("Speechmatics RecognitionStarted: id={}", started.id);
                                return Ok(());
                            }
                            Ok(SpeechmaticsResponse::Error(err)) => {
                                return Err(SttClientError::Connection(format!(
                                    "Speechmatics error: code={}, reason={}",
                                    err.code, err.reason
                                )));
                            }
                            Ok(other) => {
                                debug!("Received unexpected message while waiting for RecognitionStarted: {other:?}");
                            }
                            Err(e) => {
                                error!("Failed to parse response: {e}");
                                debug!("Raw response: {text}");
                            }
                        }
                    }
                    Ok(WebSocketMessage::Close(frame)) => {
                        return Err(SttClientError::Connection(format!(
                            "Connection closed: {frame:?}",
                        )));
                    }
                    Err(e) => {
                        return Err(SttClientError::Connection(format!("WebSocket error: {e}")));
                    }
                    _ => {}
                }
            }
            Err(SttClientError::Connection(
                "Stream ended before RecognitionStarted".to_string(),
            ))
        })
        .await
        .map_err(|_| {
            SttClientError::Connection("Timeout waiting for RecognitionStarted".to_string())
        })??;

        // Signal that we're ready to receive audio
        let _ = ready_tx.send(());

        info!("Speechmatics ready for audio");

        // Track sequence number for audio chunks
        let seq_no = Arc::new(AtomicU64::new(0));

        // Track if ForceEndOfUtterance was sent (for from_finalize flag)
        let pending_force_end = Arc::new(AtomicBool::new(false));

        // WebSocket ping interval for transport-level keepalive
        let mut heartbeat_interval = interval(Duration::from_secs(30));

        loop {
            tokio::select! {
                _ = heartbeat_interval.tick() => {
                    debug!("Sending ping to Speechmatics");
                    if let Err(e) = sink.send(WebSocketMessage::Ping(vec![].into())).await {
                        error!("Failed to send ping: {e}");
                        break;
                    }
                }

                // Handle outbound commands
                Some(command) = command_rx.recv() => {
                    match command {
                        SpeechmaticsCommand::SendAudio(audio_data) => {
                            seq_no.fetch_add(1, Ordering::SeqCst);
                            if let Err(e) = sink.send(WebSocketMessage::Binary(audio_data.into())).await {
                                error!("Failed to send audio: {e}");
                                break;
                            }
                        }
                        SpeechmaticsCommand::SendForceEndOfUtterance => {
                            pending_force_end.store(true, Ordering::SeqCst);
                            let force_end = SpeechmaticsRequest::ForceEndOfUtterance;
                            match force_end.to_json_string() {
                                Ok(json) => {
                                    debug!("Sending ForceEndOfUtterance to Speechmatics");
                                    if let Err(e) = sink.send(WebSocketMessage::Text(json.into())).await {
                                        error!("Failed to send ForceEndOfUtterance: {e}");
                                        break;
                                    }
                                }
                                Err(e) => error!("Failed to serialize ForceEndOfUtterance: {e}"),
                            }
                        }
                        SpeechmaticsCommand::SendEndOfStream => {
                            let last_seq = seq_no.load(Ordering::SeqCst);
                            let end_of_stream = SpeechmaticsRequest::EndOfStream { last_seq_no: last_seq };
                            match end_of_stream.to_json_string() {
                                Ok(json) => {
                                    debug!("Sending EndOfStream to Speechmatics (last_seq_no={last_seq})");
                                    if let Err(e) = sink.send(WebSocketMessage::Text(json.into())).await {
                                        error!("Failed to send EndOfStream: {e}");
                                    }
                                }
                                Err(e) => error!("Failed to serialize EndOfStream: {e}"),
                            }
                        }
                    }
                }

                // Handle inbound messages
                Some(message) = stream.next() => {
                    match message {
                        Ok(WebSocketMessage::Text(text)) => {
                            match serde_json::from_str::<SpeechmaticsResponse>(&text) {
                                Ok(response) => {
                                    let is_pending_force_end = pending_force_end.load(Ordering::SeqCst);

                                    // Clear pending flag on final transcript
                                    if matches!(response, SpeechmaticsResponse::AddTranscript(_)) && is_pending_force_end {
                                        pending_force_end.store(false, Ordering::SeqCst);
                                    }

                                    if let Some(transcript) = SpeechmaticsInboundHandler::handle_response(response, is_pending_force_end) {
                                        if transcript_tx.send(transcript).is_err() {
                                            warn!("Transcript receiver dropped");
                                            break;
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to parse Speechmatics response: {e}");
                                    debug!("Raw response: {text}");
                                }
                            }
                        }
                        Ok(WebSocketMessage::Binary(data)) => {
                            warn!("Unexpected binary message: {} bytes", data.len());
                        }
                        Ok(WebSocketMessage::Pong(_)) => {
                            debug!("Received pong from Speechmatics");
                        }
                        Ok(WebSocketMessage::Ping(data)) => {
                            debug!("Received ping from Speechmatics, sending pong");
                            if let Err(e) = sink.send(WebSocketMessage::Pong(data)).await {
                                error!("Failed to send pong: {e}");
                                break;
                            }
                        }
                        Ok(WebSocketMessage::Close(_)) => {
                            info!("Received Close message from Speechmatics");
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
                    let last_seq = seq_no.load(Ordering::SeqCst);
                    let end_of_stream = SpeechmaticsRequest::EndOfStream { last_seq_no: last_seq };
                    if let Ok(json) = end_of_stream.to_json_string() {
                        let _ = sink.send(WebSocketMessage::Text(json.into())).await;
                    }
                    info!("Shutdown signal received");
                    break;
                }
            }
        }

        info!("Speechmatics connection closed");
        Ok(())
    }
}

#[async_trait::async_trait]
impl SttClient for SpeechmaticsClient {
    async fn connect(&mut self) -> Result<(), SttClientError> {
        if self.shutdown_tx.is_some() {
            return Ok(());
        }

        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
        let (command_tx, command_rx) = mpsc::channel(32);
        let (transcript_tx, transcript_rx) = mpsc::unbounded_channel();
        let (ready_tx, ready_rx) = oneshot::channel();

        self.shutdown_tx = Some(shutdown_tx);
        self.command_tx = Some(command_tx);
        self.transcript_rx = Some(transcript_rx);

        let api_key = self.api_key.clone();
        let sample_rate = self.sample_rate;
        let language = self.language.clone();
        let operating_point = self.operating_point.clone();

        tokio::spawn(async move {
            if let Err(e) =
                Self::run_connection(api_key, sample_rate, language, operating_point, shutdown_rx, command_rx, transcript_tx, ready_tx)
                    .await
            {
                error!("Websocket connection error: {e}");
            }
        });

        // Wait for RecognitionStarted before returning
        ready_rx.await.map_err(|_| {
            SttClientError::Connection("Connection failed before ready".to_string())
        })?;

        Ok(())
    }

    async fn disconnect(&mut self) -> Result<(), SttClientError> {
        if let Some(ref command_tx) = self.command_tx {
            let _ = command_tx.send(SpeechmaticsCommand::SendEndOfStream).await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(()).await;
        }

        self.command_tx = None;
        self.transcript_rx = None;

        Ok(())
    }

    async fn generate_text(&self, data: Vec<u8>) -> Result<(), SttClientError> {
        if let Some(ref command_tx) = self.command_tx {
            command_tx
                .send(SpeechmaticsCommand::SendAudio(data))
                .await
                .map_err(|e| SttClientError::Connection(format!("Failed to send audio: {e}")))?;
            Ok(())
        } else {
            Err(SttClientError::Connection("Not connected".to_string()))
        }
    }

    async fn finalize(&self) -> Result<(), SttClientError> {
        if let Some(ref command_tx) = self.command_tx {
            command_tx
                .send(SpeechmaticsCommand::SendForceEndOfUtterance)
                .await
                .map_err(|e| {
                    SttClientError::Connection(format!("Failed to send ForceEndOfUtterance: {e}"))
                })?;
            Ok(())
        } else {
            Err(SttClientError::Connection("Not connected".to_string()))
        }
    }

    fn take_inbound_transcript_receiver(&mut self) -> Option<mpsc::UnboundedReceiver<SttTranscript>> {
        self.transcript_rx.take()
    }
}
