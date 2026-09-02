use std::collections::HashMap;
use std::sync::Arc;

use hyper_util::rt::TokioIo;
use saasy_proto_rust::speaking_engine::{
    speaking_engine_media_ack,
    DirectionEnum,
    EngineToOrchestratorEvent,
    OrchestratorToEngineEvent,
    SpeakingEngineControlMessage,
    SpeakingEngineMediaPayload,
    SpeakingEngineServiceClient,
};
use tokio::sync::{mpsc, oneshot, Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Endpoint, Uri};
use tonic::Request;
use tower::service_fn;
use tracing::{error, info, warn};

use crate::grpc::engines::error::EngineClientError;

type SpeakingEnginePendingRequests = Arc<RwLock<HashMap<String, oneshot::Sender<Result<SpeakingEngineControlMessage, String>>>>>;

pub struct SpeakingContext {
    pub outbound_control_tx: mpsc::Sender<SpeakingEngineControlMessage>,
    pub inbound_control_rx: mpsc::Receiver<SpeakingEngineControlMessage>,
    outbound_event_tx: mpsc::Sender<OrchestratorToEngineEvent>,
    inbound_event_rx: mpsc::Receiver<EngineToOrchestratorEvent>,
    pub outbound_media_tx: mpsc::Sender<SpeakingEngineMediaPayload>,
    control_streaming_task: JoinHandle<()>,
    event_streaming_task: JoinHandle<()>,
    media_streaming_task: JoinHandle<()>,
    pub pending_requests: SpeakingEnginePendingRequests,
}

pub struct SpeakingEngineClient{
    inner: Arc<Mutex<SpeakingEngineServiceClient<Channel>>>,
}

impl SpeakingEngineClient {
    pub async fn connect(socket_path: impl AsRef<str>) -> Result<Self, EngineClientError> {
        let socket_path = socket_path.as_ref().to_string();

        let channel = Endpoint::try_from("http://[::]:50051")? // dummy URI
            .connect_with_connector(service_fn(move |_: Uri| {
                let socket_path = socket_path.clone();
                async move {
                    let stream = tokio::net::UnixStream::connect(socket_path).await?;
                    Ok::<_, std::io::Error>(TokioIo::new(stream))
                }
            }))
            .await?;

        Ok(Self {
            inner: Arc::new(Mutex::new(SpeakingEngineServiceClient::new(channel))),
        })
    }

    pub async fn health_check(&self) -> Result<bool, EngineClientError> {
        let request = saasy_proto_rust::speaking_engine::HealthCheckRequest {};
        
        let response = self.inner
            .lock()
            .await
            .health_check(tonic::Request::new(request))
            .await?;
        
        Ok(response.into_inner().alive)
    }

    pub async fn start_streams(
        &self,
        session_id: &str,
        participant_id: &str,
        session_token: CancellationToken,
    ) -> Result<SpeakingContext, EngineClientError> {
        info!("Starting streaming connections for session {session_id}");

        let pending_requests = Arc::new(RwLock::new(HashMap::<String, oneshot::Sender<Result<SpeakingEngineControlMessage, String>>>::new()));

        let (outbound_control_tx, inbound_control_rx, control_streaming_task) =
            self.setup_control_stream(session_id, participant_id, pending_requests.clone(), session_token.clone()).await?;

        let (outbound_event_tx, inbound_event_rx, event_streaming_task) =
            self.setup_events_stream(session_id, participant_id, session_token.clone()).await?;

        let (outbound_media_tx, media_streaming_task) =
            self.setup_media_stream(session_id, participant_id, session_token.clone()).await?;

        Ok(SpeakingContext {
            outbound_control_tx,
            inbound_control_rx,
            outbound_event_tx,
            inbound_event_rx,
            outbound_media_tx,
            control_streaming_task,
            event_streaming_task,
            media_streaming_task,
            pending_requests,
        })
    }

    async fn setup_control_stream(
        &self,
        session_id: &str,
        participant_id: &str,
        pending_requests: SpeakingEnginePendingRequests,
        session_token: CancellationToken,
    ) -> Result<(mpsc::Sender<SpeakingEngineControlMessage>, mpsc::Receiver<SpeakingEngineControlMessage>, JoinHandle<()>), EngineClientError> {
        // outbound_control_tx >> outbound_control_rx >> engine >> inbound_control_tx >> inbound_control_rx
        // engine >> inbound_control_tx >> inbound_control_rx >> outbound_control_tx >> outbound_control_rx >> engine
        let (outbound_control_tx, outbound_control_rx) = mpsc::channel(32);
        let (inbound_control_tx, inbound_control_rx) = mpsc::channel(32);

        let outbound_stream = Self::create_outbound_stream(
            ReceiverStream::new(outbound_control_rx),
            session_id,
            participant_id,
        )?;

        let mut inbound_stream = self.inner
            .lock()
            .await
            .control(outbound_stream)
            .await?
            .into_inner();

        let pending_requests_clone = pending_requests.clone();
        let control_streaming_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;

                    () = session_token.cancelled() => {
                        info!("Control stream received cancellation signal");
                        break;
                    }

                    result = inbound_stream.message() => {
                        let Ok(Some(message)) = result else {
                            if !session_token.is_cancelled() {
                                warn!("Speaking engine control stream broke unexpectedly, cancelling session");
                                session_token.cancel();
                            }
                            break;
                        };

                        if message.r#type == "control_stream_connected" {
                            info!("Control stream successfully connected to speaking engine.");
                            continue;
                        }

                        match DirectionEnum::try_from(message.direction) {
                            Ok(DirectionEnum::Request) => { // This is a request from the engine
                                if inbound_control_tx.send(message).await.is_err() {
                                    error!("Failed to forward engine request");
                                }
                            }
                            Ok(DirectionEnum::Response) => { // This is the engine's response to our request
                                let mut pending_requests_guard = pending_requests_clone.write().await;
                                if let Some(response_tx) = pending_requests_guard.remove(&message.request_id) {
                                    let _ = response_tx.send(Ok(message));
                                }
                            }
                            Err(_) => {
                                error!("Invalid direction enum value: {}", message.direction);
                            }
                        }
                    }
                }
            }
            info!("Control stream ended");
        });

        Ok((outbound_control_tx, inbound_control_rx, control_streaming_task))
    }

    async fn setup_events_stream(
        &self,
        session_id: &str,
        participant_id: &str,
        session_token: CancellationToken,
    ) -> Result<(mpsc::Sender<OrchestratorToEngineEvent>, mpsc::Receiver<EngineToOrchestratorEvent>, JoinHandle<()>), EngineClientError> {
        // outbound_event_tx >> outbound_event_rx >> engine >> inbound_event_tx >> inbound_event_rx
        // engine >> inbound_event_tx >> inbound_event_rx >> outbound_event_tx >> outbound_event_rx >> engine
        let (outbound_event_tx, outbound_event_rx) = mpsc::channel(32);
        let (inbound_event_tx, inbound_event_rx) = mpsc::channel(32);

        let outbound_stream = Self::create_outbound_stream(
            ReceiverStream::new(outbound_event_rx),
            session_id,
            participant_id,
        )?;

        let mut inbound_stream = self.inner
            .lock()
            .await
            .events(outbound_stream)
            .await?
            .into_inner();
        
        let event_streaming_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;

                    () = session_token.cancelled() => {
                        info!("Events stream received cancellation signal");
                        break;
                    }

                    result = inbound_stream.message() => {
                        let Ok(Some(event)) = result else {
                            if !session_token.is_cancelled() {
                                warn!("Speaking engine events stream broke unexpectedly, cancelling session");
                                session_token.cancel();
                            }
                            break;
                        };

                        if event.r#type == "event_stream_connected" {
                            info!("Event stream successfully connected to speaking engine.");
                            continue;
                        }

                        if inbound_event_tx.send(event).await.is_err() {
                            error!("Failed to forward engine event");
                            break;
                        }
                    }
                }
            }
            info!("Events stream ended");
        });

        Ok((outbound_event_tx, inbound_event_rx, event_streaming_task))
    }

    async fn setup_media_stream(
        &self,
        session_id: &str,
        participant_id: &str,
        session_token: CancellationToken,
    ) -> Result<(mpsc::Sender<SpeakingEngineMediaPayload>, JoinHandle<()>), EngineClientError> {
        // outbound_media_tx >> outbound_media_rx >> engine
        let (outbound_media_tx, outbound_media_rx) = mpsc::channel(100);

        let outbound_stream = Self::create_outbound_stream(
            ReceiverStream::new(outbound_media_rx),
            session_id,
            participant_id,
        )?;

        let mut inbound_stream = self.inner
            .lock()
            .await
            .media(outbound_stream)
            .await?
            .into_inner();
        
        let media_streaming_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;

                    () = session_token.cancelled() => {
                        info!("Media stream received cancellation signal");
                        break;
                    }

                    result = inbound_stream.message() => {
                        let Ok(Some(ack)) = result else {
                            if !session_token.is_cancelled() {
                                warn!("Speaking engine media stream broke unexpectedly, cancelling session");
                                session_token.cancel();
                            }
                            break;
                        };

                        // After the initial connection confirmation, we don't expect periodic ACKs.
                        // This loop only catches the initial connection and any error responses.
                        if ack.r#type == "media_stream_connected" {
                            info!("Media stream successfully connected to speaking engine.");
                            continue;
                        }

                        if let Some(speaking_engine_media_ack::Data::ErrorResponse(error)) = ack.data {
                            error!("Media stream error: {} - {}", error.code, error.message);
                        }
                    }
                }
            }
            info!("Media stream ended");
        });

        Ok((outbound_media_tx, media_streaming_task))
    }

    fn create_outbound_stream<T>(
        stream: T,
        session_id: &str,
        participant_id: &str,
    ) -> Result<Request<T>, EngineClientError> {
        let mut outbound_stream = Request::new(stream);
        outbound_stream.metadata_mut().insert(
            "session-id",
            MetadataValue::try_from(session_id)
                .map_err(|e| EngineClientError::InvalidMetadata(e.to_string()))?
        );
        outbound_stream.metadata_mut().insert(
            "participant-id",
            MetadataValue::try_from(participant_id)
                .map_err(|e| EngineClientError::InvalidMetadata(e.to_string()))?
        );
        Ok(outbound_stream)
    }

    pub fn take_inbound_control_receiver(
        context: &mut SpeakingContext
    ) -> mpsc::Receiver<SpeakingEngineControlMessage> {
        std::mem::replace(&mut context.inbound_control_rx, mpsc::channel(1).1)
    }

    pub fn take_inbound_events_receiver(
        context: &mut SpeakingContext
    ) -> mpsc::Receiver<EngineToOrchestratorEvent> {
        std::mem::replace(&mut context.inbound_event_rx, mpsc::channel(1).1)
    }
}

impl Drop for SpeakingContext {
    fn drop(&mut self) {
        self.control_streaming_task.abort();
        self.event_streaming_task.abort();
        self.media_streaming_task.abort();
    }
}
