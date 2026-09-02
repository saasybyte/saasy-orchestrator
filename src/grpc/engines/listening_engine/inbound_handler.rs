use std::sync::Arc;

use saasy_proto_rust::listening_engine::{self, engine_to_orchestrator_event, EngineToOrchestratorEvent};
use tokio::sync::{mpsc, Mutex};
use tracing::{info, warn};

use crate::grpc::engines::error::EngineClientError;
use crate::websocket::signal::{SignalOutboundCommands, SignalSessionPendingRequest};
use crate::websocket::stt::SttClient;

type SttClientArc = Arc<Mutex<Box<dyn SttClient>>>;

pub enum VadTurnEvent {
    SpeechStarted { timestamp_ms: u64 },
    UserTurnComplete { confidence: f32, timestamp_ms: u64 },
}

pub struct ListeningEngineInboundHandler {
    signal_session_control_tx: mpsc::Sender<SignalSessionPendingRequest>,
    vad_turn_event_tx: mpsc::Sender<VadTurnEvent>,
    stt_client: Arc<Mutex<Option<SttClientArc>>>,
}

impl ListeningEngineInboundHandler {
    pub fn new(
        signal_session_control_tx: mpsc::Sender<SignalSessionPendingRequest>,
        vad_turn_event_tx: mpsc::Sender<VadTurnEvent>,
    ) -> Self {
        Self {
            signal_session_control_tx,
            vad_turn_event_tx,
            stt_client: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn handle_engine_event(
        &self,
        event: EngineToOrchestratorEvent,
        session_id: &str,
        participant_id: &str,
    ) -> Result<(), EngineClientError> {
        match event.data {
            Some(engine_to_orchestrator_event::Data::OnConnect(data)) => {
                info!("Received OnConnect from listening engine for transport: {}", data.transport_id);
                self.handle_on_connect(
                    session_id,
                    participant_id,
                    data.transport_id,
                    data.device_dtls_parameters,
                ).await
            }
            Some(engine_to_orchestrator_event::Data::OnSpeechStarted(data)) => {
                info!("User speech started at {}ms", data.timestamp_ms);
                self.handle_on_speech_started(session_id, participant_id, data.timestamp_ms).await
            }
            Some(engine_to_orchestrator_event::Data::OnUserTurnComplete(data)) => {
                info!("User turn complete (confidence: {}) at {}ms", data.confidence, data.timestamp_ms);
                self.handle_on_user_turn_complete(session_id, participant_id, data.confidence, data.timestamp_ms).await
            }
            _ => {
                // Safe to ignore unhandled events
                Ok(())
            }
        }
    }

    async fn handle_on_connect(
        &self,
        session_id: &str,
        participant_id: &str,
        transport_id: String,
        device_dtls_parameters: Option<saasy_proto_rust::shared::DtlsParameters>,
    ) -> Result<(), EngineClientError> {
        let dtls_parameters = device_dtls_parameters
            .ok_or_else(|| EngineClientError::InvalidRequest("No DTLS parameters in OnConnect event".to_string()))?;

        info!("Forwarding DTLS parameters to Signal for transport: {transport_id}");

        SignalOutboundCommands::connect_transport(
            &self.signal_session_control_tx,
            session_id,
            participant_id,
            saasy_proto_rust::shared::TransportId { id: transport_id },
            dtls_parameters,
        ).await
        .map_err(|e| EngineClientError::Signal(format!("Failed to connect transport: {e}")))?;

        Ok(())
    }

    async fn handle_on_speech_started(
        &self,
        _session_id: &str,
        _participant_id: &str,
        timestamp_ms: u64,
    ) -> Result<(), EngineClientError> {
        info!("Sending speech started event to main loop");
        
        self.vad_turn_event_tx
            .send(VadTurnEvent::SpeechStarted { timestamp_ms })
            .await
            .map_err(|e| EngineClientError::Internal(format!("Failed to send speech started event: {e}")))?;
        
        Ok(())
    }

    async fn handle_on_user_turn_complete(
        &self,
        _session_id: &str,
        _participant_id: &str,
        confidence: f32,
        timestamp_ms: u64,
    ) -> Result<(), EngineClientError> {
        info!("Sending turn complete event to main loop (confidence: {})", confidence);
        
        self.vad_turn_event_tx
            .send(VadTurnEvent::UserTurnComplete { confidence, timestamp_ms })
            .await
            .map_err(|e| EngineClientError::Internal(format!("Failed to send turn complete event: {e}")))?;
        
        Ok(())
    }

    pub async fn handle_media_frame(
        &self,
        frame: listening_engine::MediaFrame,
    ) -> Result<(), EngineClientError> {
        let stt_client_clone = self.stt_client.lock().await.clone();

        if let Some(stt_client) = stt_client_clone {
            let result = {
                let locked_stt_client = stt_client.lock().await;
                locked_stt_client.generate_text(frame.frame_data).await
            };
            
            if let Err(e) = result {
                warn!("Failed to send audio to STT: {e}");
            }
        }
        
        Ok(())
    }

    pub async fn set_stt_client(&self, stt_client: SttClientArc) {
        *self.stt_client.lock().await = Some(stt_client);
    }
}
