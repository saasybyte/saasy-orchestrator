use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{connect_async, tungstenite::Message as WebSocketMessage};
use tracing::{debug, error, info, warn};

use super::inbound_handler::DeepgramInboundHandler;
use super::types::{DeepgramRequest, DeepgramResponse};
use crate::websocket::stt::error::SttClientError;
use crate::websocket::stt::client::SttClient;
use crate::websocket::stt::types::{SttClientConfig, SttCredentials, SttTranscript};

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone)]
enum DeepgramCommand {
    SendAudio(Vec<u8>),
    SendKeepAlive,
    SendFinalize,
    SendCloseStream,
}

pub struct DeepgramClient {
    api_key: String,
    model: String,
    encoding: String,
    sample_rate: u32,
    language: Option<String>,
    shutdown_tx: Option<mpsc::Sender<()>>,
    command_tx: Option<mpsc::Sender<DeepgramCommand>>,
    transcript_rx: Option<mpsc::UnboundedReceiver<SttTranscript>>,
}

impl DeepgramClient {
    pub fn new(config: &SttClientConfig) -> Result<Self, SttClientError> {
        #[allow(unreachable_patterns)]
        let api_key = match &config.credentials {
            SttCredentials::ApiKey(key) => key.clone(),
            _ => return Err(SttClientError::Config(
                "DeepgramClient requires ApiKey credentials".to_string()
            )),
        };
        Ok(Self {
            api_key,
            model: config.model.clone(),
            encoding: config.encoding.clone(),
            sample_rate: config.sample_rate,
            language: config.language.clone(),
            shutdown_tx: None,
            command_tx: None,
            transcript_rx: None,
        })
    }

    #[allow(clippy::too_many_lines)]
    #[allow(clippy::too_many_arguments)]
    async fn run_connection(
        api_key: String,
        model: String,
        encoding: String,
        sample_rate: u32,
        language: Option<String>,
        mut shutdown_rx: mpsc::Receiver<()>,
        mut command_rx: mpsc::Receiver<DeepgramCommand>,
        transcript_tx: mpsc::UnboundedSender<SttTranscript>,
    ) -> Result<(), SttClientError> {
        info!("Connecting to Deepgram...");

        let mut url = format!(
            "wss://api.deepgram.com/v1/listen?model={model}&encoding={encoding}&sample_rate={sample_rate}",
        );

        if let Some(lang) = &language {
            url.push_str("&language=");
            url.push_str(lang);
        }

        let mut request = url
            .into_client_request()
            .map_err(|e| SttClientError::Connection(e.to_string()))?;

        request.headers_mut().insert(
            "Authorization",
            format!("Token {api_key}")
                .parse()
                .map_err(|e| SttClientError::Connection(format!("Invalid header value: {e}")))?,
        );

        let (websocket_stream, _) = connect_async(request)
            .await
            .map_err(|e| SttClientError::Connection(format!("Failed to connect: {e}")))?;

        info!("Connected to Deepgram");

        let (mut sink, mut stream) = websocket_stream.split();

        let mut heartbeat_interval = interval(Duration::from_secs(30));

        let mut keepalive_interval = interval(Duration::from_secs(8));

        loop {
            tokio::select! {
                _ = heartbeat_interval.tick() => {
                    debug!("Sending ping to Deepgram");
                    if let Err(e) = sink.send(WebSocketMessage::Ping(vec![].into())).await {
                        error!("Failed to send ping: {e}");
                        break;
                    }
                }

                _ = keepalive_interval.tick() => {
                    debug!("Sending KeepAlive to Deepgram");
                    match DeepgramRequest::KeepAlive.to_json_string() {
                        Ok(json) => {
                            if let Err(e) = sink.send(WebSocketMessage::Text(json.into())).await {
                                error!("Failed to send KeepAlive: {e}");
                                break;
                            }
                        }
                        Err(e) => error!("Failed to serialize KeepAlive: {e}"),
                    }
                }

                // Handle outbound messages
                Some(command) = command_rx.recv() => {
                    match command {
                        DeepgramCommand::SendAudio(audio_data) => {
                            if let Err(e) = sink.send(WebSocketMessage::Binary(audio_data.into())).await {
                                error!("Failed to send audio: {e}");
                                break;
                            }
                        }
                        DeepgramCommand::SendKeepAlive => {
                            let keep_alive = DeepgramRequest::KeepAlive;
                            match keep_alive.to_json_string() {
                                Ok(json) => {
                                    if let Err(e) = sink.send(WebSocketMessage::Text(json.into())).await {
                                        error!("Failed to send KeepAlive: {e}");
                                        break;
                                    }
                                }
                                Err(e) => error!("Failed to serialize KeepAlive: {e}"),
                            }
                        }
                        DeepgramCommand::SendFinalize => {
                            let finalize = DeepgramRequest::Finalize;
                            match finalize.to_json_string() {
                                Ok(json) => {
                                    debug!("Sending Finalize to Deepgram");
                                    if let Err(e) = sink.send(WebSocketMessage::Text(json.into())).await {
                                        error!("Failed to send Finalize: {e}");
                                        break;
                                    }
                                }
                                Err(e) => error!("Failed to serialize Finalize: {e}"),
                            }
                        }
                        DeepgramCommand::SendCloseStream => {
                            let close_stream = DeepgramRequest::CloseStream;
                            match close_stream.to_json_string() {
                                Ok(json) => {
                                    if let Err(e) = sink.send(WebSocketMessage::Text(json.into())).await {
                                        error!("Failed to send CloseStream: {e}");
                                        // Don't break here, let connection close naturally
                                    }
                                }
                                Err(e) => error!("Failed to serialize CloseStream: {e}"),
                            }
                        }
                    }
                }

                // Handle inbound messages
                Some(message) = stream.next() => {
                    match message {
                        Ok(WebSocketMessage::Text(text)) => {
                            match serde_json::from_str::<DeepgramResponse>(&text) {
                                Ok(response) => {
                                    if let Some(transcript) = DeepgramInboundHandler::handle_response(response) {
                                        if transcript_tx.send(transcript).is_err() {
                                            warn!("Transcript receiver dropped");
                                            break;
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to parse Deepgram response: {}", e);
                                    debug!("Raw response: {}", text);
                                }
                            }
                        }
                        Ok(WebSocketMessage::Binary(data)) => {
                            warn!("Unexpected binary message: {} bytes", data.len());
                        }
                        Ok(WebSocketMessage::Pong(_)) => {
                            debug!("Received pong from Deepgram");
                        }
                        Ok(WebSocketMessage::Ping(data)) => {
                            debug!("Received ping from Deepgram, sending pong");
                            if let Err(e) = sink.send(WebSocketMessage::Pong(data)).await {
                                error!("Failed to send pong: {e}");
                                break;
                            }
                        }
                        Ok(WebSocketMessage::Close(_)) => {
                            info!("Received Close message from Deepgram");
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
                    let close_stream = DeepgramRequest::CloseStream;
                    if let Ok(json) = close_stream.to_json_string() {
                        let _ = sink.send(WebSocketMessage::Text(json.into())).await;
                    }
                    info!("Shutdown signal received");
                    break;
                }
            }
        }

        info!("Deepgram connection closed");
        Ok(())
    }
}

#[async_trait::async_trait]
impl SttClient for DeepgramClient {
    async fn connect(&mut self) -> Result<(), SttClientError> {
        if self.shutdown_tx.is_some() {
            return Ok(());
        }

        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
        let (command_tx, command_rx) = mpsc::channel(32);
        let (transcript_tx, transcript_rx) = mpsc::unbounded_channel();

        self.shutdown_tx = Some(shutdown_tx);
        self.command_tx = Some(command_tx);
        self.transcript_rx = Some(transcript_rx);

        let api_key = self.api_key.clone();
        let model = self.model.clone();
        let encoding = self.encoding.clone();
        let sample_rate = self.sample_rate;
        let language = self.language.clone();

        tokio::spawn(async move {
            if let Err(e) =
                Self::run_connection(api_key, model, encoding, sample_rate, language, shutdown_rx, command_rx, transcript_tx).await
            {
                error!("Websocket connection error: {e}");
            }
        });

        Ok(())
    }

    async fn disconnect(&mut self) -> Result<(), SttClientError> {
        if let Some(ref command_tx) = self.command_tx {
            let _ = command_tx.send(DeepgramCommand::SendCloseStream).await;
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
                .send(DeepgramCommand::SendAudio(data))
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
                .send(DeepgramCommand::SendFinalize)
                .await
                .map_err(|e| SttClientError::Connection(format!("Failed to send finalize: {e}")))?;
            Ok(())
        } else {
            Err(SttClientError::Connection("Not connected".to_string()))
        }
    }

    fn take_inbound_transcript_receiver(&mut self) -> Option<mpsc::UnboundedReceiver<SttTranscript>> {
        self.transcript_rx.take()
    }
}
