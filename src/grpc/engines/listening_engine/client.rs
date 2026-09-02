use std::collections::HashMap;
use std::sync::Arc;

use hyper_util::rt::TokioIo;
use saasy_proto_rust::listening_engine::{
    listening_engine_media_payload,
    DirectionEnum,
    EngineToOrchestratorEvent,
    ListeningEngineControlMessage,
    ListeningEngineMediaAck,
    ListeningEngineServiceClient,
    MediaFrame,
    OrchestratorToEngineEvent,
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

type ListeningEnginePendingRequests = Arc<RwLock<HashMap<String, oneshot::Sender<Result<ListeningEngineControlMessage, String>>>>>;

pub struct ListeningContext {
    pub outbound_control_tx: mpsc::Sender<ListeningEngineControlMessage>,
    inbound_control_rx: mpsc::Receiver<ListeningEngineControlMessage>,
    outbound_event_tx: mpsc::Sender<OrchestratorToEngineEvent>,
    inbound_event_rx: mpsc::Receiver<EngineToOrchestratorEvent>,
    outbound_media_tx: mpsc::Sender<ListeningEngineMediaAck>,
    inbound_media_rx: mpsc::Receiver<MediaFrame>,
    control_streaming_task: JoinHandle<()>,
    event_streaming_task: JoinHandle<()>,
    media_streaming_task: JoinHandle<()>,
    pub pending_requests: ListeningEnginePendingRequests,
}

pub struct ListeningEngineClient {
    inner: Arc<Mutex<ListeningEngineServiceClient<Channel>>>,
}

impl ListeningEngineClient {
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
            inner: Arc::new(Mutex::new(ListeningEngineServiceClient::new(channel))),
        })
    }

    pub async fn health_check(&self) -> Result<bool, EngineClientError> {
        let request = saasy_proto_rust::listening_engine::HealthCheckRequest {};
        
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
    ) -> Result<ListeningContext, EngineClientError> {
        info!("Starting streaming connections for session {session_id}");

        let pending_requests = Arc::new(RwLock::new(HashMap::<String, oneshot::Sender<Result<ListeningEngineControlMessage, String>>>::new()));

        let (outbound_control_tx, inbound_control_rx, control_streaming_task) =
            self.setup_control_stream(session_id, participant_id, pending_requests.clone(), session_token.clone()).await?;
        
        let (outbound_event_tx, inbound_event_rx, event_streaming_task) =
            self.setup_events_stream(session_id, participant_id, session_token.clone()).await?;
        
        let (outbound_media_tx, inbound_media_rx, media_streaming_task) =
            self.setup_media_stream(session_id, participant_id, session_token.clone()).await?;

        Ok(ListeningContext {
            outbound_control_tx,
            inbound_control_rx,
            outbound_event_tx,
            inbound_event_rx,
            outbound_media_tx,
            inbound_media_rx,
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
        pending_requests: ListeningEnginePendingRequests,
        session_token: CancellationToken,
    ) -> Result<(mpsc::Sender<ListeningEngineControlMessage>, mpsc::Receiver<ListeningEngineControlMessage>, JoinHandle<()>), EngineClientError> {
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
                                warn!("Listening engine control stream broke unexpectedly, cancelling session");
                                session_token.cancel();
                            }
                            break;
                        };

                        if message.r#type == "control_stream_connected" {
                            info!("Control stream successfully connected to listening engine.");
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
                                warn!("Listening engine events stream broke unexpectedly, cancelling session");
                                session_token.cancel();
                            }
                            break;
                        };

                        if event.r#type == "event_stream_connected" {
                            info!("Event stream successfully connected to listening engine.");
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
    ) -> Result<(mpsc::Sender<ListeningEngineMediaAck>, mpsc::Receiver<MediaFrame>, JoinHandle<()>), EngineClientError> {
        // outbound_media_tx >> outbound_media_rx >> engine >> inbound_media_tx >> inbound_media_rx (INIT only)
        // engine >> inbound_media_tx >> inbound_media_rx
        let (outbound_media_tx, outbound_media_rx) = mpsc::channel(32);
        let (inbound_media_tx, inbound_media_rx) = mpsc::channel(100);
        
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
                        let Ok(Some(payload)) = result else {
                            if !session_token.is_cancelled() {
                                warn!("Listening engine media stream broke unexpectedly, cancelling session");
                                session_token.cancel();
                            }
                            break;
                        };

                        if payload.r#type == "media_stream_connected" {
                            info!("Media stream successfully connected to listening engine.");
                            continue;
                        }

                        match payload.data {
                            Some(listening_engine_media_payload::Data::MediaFrame(frame)) => {
                                if inbound_media_tx.send(frame).await.is_err() {
                                    error!("Failed to forward media frame - ending stream");
                                    break;
                                }
                            }
                            None => {
                                warn!("Received media payload with no data and unknown type: {}", payload.r#type);
                            }
                        }
                    }
                }
            }
            info!("Media stream ended");
        });

        Ok((outbound_media_tx, inbound_media_rx, media_streaming_task))
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

    pub fn take_inbound_events_receiver(
        handles: &mut ListeningContext
    ) -> mpsc::Receiver<EngineToOrchestratorEvent> {
        std::mem::replace(&mut handles.inbound_event_rx, mpsc::channel(1).1)
    }

    pub fn take_inbound_media_receiver(
        handles: &mut ListeningContext
    ) -> mpsc::Receiver<MediaFrame> {
        std::mem::replace(&mut handles.inbound_media_rx, mpsc::channel(1).1)
    }
}

impl Drop for ListeningContext {
    fn drop(&mut self) {
        self.control_streaming_task.abort();
        self.event_streaming_task.abort();
        self.media_streaming_task.abort();
    }
}
