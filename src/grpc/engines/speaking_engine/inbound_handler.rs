use std::sync::Arc;

use saasy_proto_rust::shared::{MediaKind, TransportId};
use saasy_proto_rust::speaking_engine::{
    engine_to_orchestrator_event,
    speaking_engine_control_message,
    EngineToOrchestratorEvent,
    GetRouterProducerIdResponse,
    SpeakingEngineControlMessage,
};
use tokio::sync::{RwLock, mpsc};
use tracing::{error, info};

use crate::grpc::engines::error::EngineClientError;
use crate::orchestrator::{ConversationState, SessionState};
use crate::websocket::signal::{SignalOutboundCommands, SignalSessionPendingRequest};
use super::SpeakingEngineOutboundCommands;

#[derive(Debug)]
pub enum PlaybackCompleteAction {
    TransitionToIdle,
    RetryLlm,
    EndSession,
}

pub struct SpeakingEngineInboundHandler {
    signal_control_tx: mpsc::Sender<SignalSessionPendingRequest>,
    playback_complete_action_tx: mpsc::Sender<PlaybackCompleteAction>,
}

impl SpeakingEngineInboundHandler {
    pub fn new(
        signal_control_tx: mpsc::Sender<SignalSessionPendingRequest>,
        playback_complete_action_tx: mpsc::Sender<PlaybackCompleteAction>,
    ) -> Self {
        Self {
            signal_control_tx,
            playback_complete_action_tx,
        }
    }

    #[allow(clippy::single_match_else)]  // We'll add more match arms later
    pub async fn handle_engine_request(
        &self,
        mut request: SpeakingEngineControlMessage,
        outbound_control_tx: &mpsc::Sender<SpeakingEngineControlMessage>,
    ) -> Result<(), EngineClientError> {
        match request.data.take() {
            Some(speaking_engine_control_message::Data::GetRouterProducerIdRequest(data)) => {
                self.handle_get_router_producer_id(
                    data,
                    &request.request_id,
                    &request.session_id,
                    &request.participant_id,
                    outbound_control_tx,
                ).await
            }
            _ => {
                error!("Unexpected request type from engine: {}", request.r#type);
                SpeakingEngineOutboundCommands::send_control_response(
                    outbound_control_tx,
                    &request.request_id,
                    &request.session_id,
                    &request.participant_id,
                    "error",
                    speaking_engine_control_message::Data::ErrorResponse(
                        saasy_proto_rust::shared::ErrorResponse {
                            code: "NOT_IMPLEMENTED".to_string(),
                            message: format!("Handler not implemented for request type: {}", request.r#type),
                        }
                    ),
                ).await
            }
        }
    }

    pub async fn handle_engine_event(
        &self,
        event: EngineToOrchestratorEvent,
        session_id: &str,
        participant_id: &str,
        session_state: Arc<RwLock<SessionState>>,
    ) -> Result<(), EngineClientError> {
        match event.data {
            Some(engine_to_orchestrator_event::Data::OnConnect(data)) => {
                info!("Received OnConnect from speaking engine for transport: {}", data.transport_id);
                
                self.handle_on_connect(
                    session_id,
                    participant_id,
                    data.transport_id,
                    data.device_dtls_parameters,
                ).await
            }
            Some(engine_to_orchestrator_event::Data::OnPlaybackComplete(_)) => {
                info!("Received OnPlaybackComplete event from speaking engine");
                
                self.handle_on_playback_complete(session_state).await
            }
            _ => {
                // Safe to ignore unhandled events
                Ok(())
            }
        }
    }

    async fn handle_get_router_producer_id(
        &self,
        data: saasy_proto_rust::speaking_engine::GetRouterProducerIdRequest,
        request_id: &str,
        session_id: &str,
        participant_id: &str,
        outbound_control_tx: &mpsc::Sender<SpeakingEngineControlMessage>,
    ) -> Result<(), EngineClientError> {
        info!("Processing GetRouterProducerId request for transport: {}", data.transport_id);

        let transport_id = data.transport_id;
        let kind = MediaKind::try_from(data.kind)
            .map_err(|_| EngineClientError::InvalidRequest("Invalid media kind".to_string()))?;
        let rtp_parameters = data.rtp_parameters
            .ok_or_else(|| EngineClientError::InvalidRequest("Missing RTP parameters".to_string()))?;

        info!("Creating producer on Signal for engine request");
        
        let response = SignalOutboundCommands::create_producer(
            &self.signal_control_tx,
            session_id,
            participant_id,
            TransportId { id: transport_id },
            kind,
            rtp_parameters,
        ).await;

        match response {
            Ok(response) => {
                if let Some(router_producer_id) = response.producer_id {
                    info!("Successfully created producer {} on Signal/SFU", router_producer_id.id);
                    SpeakingEngineOutboundCommands::send_control_response(
                        outbound_control_tx,
                        request_id,
                        session_id,
                        participant_id,
                        "get_router_producer_id",
                        speaking_engine_control_message::Data::GetRouterProducerIdResponse(
                            GetRouterProducerIdResponse { producer_id: router_producer_id.id }
                        ),
                    ).await
                } else {
                    error!("Invalid response from Signal: missing router_producer_id");
                    SpeakingEngineOutboundCommands::send_control_response(
                        outbound_control_tx,
                        request_id,
                        session_id,
                        participant_id,
                        "error",
                        speaking_engine_control_message::Data::ErrorResponse(
                            saasy_proto_rust::shared::ErrorResponse {
                                code: "INTERNAL".to_string(),
                                message: "Invalid response from Signal: missing router_producer_id".to_string(),
                            }
                        ),
                    ).await
                }
            }
            Err(e) => {
                error!("Failed to create producer on Signal/SFU: {e}");
                SpeakingEngineOutboundCommands::send_control_response(
                    outbound_control_tx,
                    request_id,
                    session_id,
                    participant_id,
                    "error",
                    speaking_engine_control_message::Data::ErrorResponse(
                        saasy_proto_rust::shared::ErrorResponse {
                            code: "INTERNAL".to_string(),
                            message: format!("Failed to create producer: {e}"),
                        }
                    ),
                ).await
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
            &self.signal_control_tx,
            session_id,
            participant_id,
            saasy_proto_rust::shared::TransportId { id: transport_id },
            dtls_parameters,
        ).await
        .map_err(|e| EngineClientError::Signal(format!("Failed to connect transport: {e}")))?;

        Ok(())
    }

    async fn handle_on_playback_complete(
        &self,
        session_state: Arc<RwLock<SessionState>>,
    ) -> Result<(), EngineClientError> {
        let action = {
            let mut state = session_state.write().await;
            
            if state.pending_session_end {
                state.pending_session_end = false;
                PlaybackCompleteAction::EndSession
            } else if state.pending_llm_retry {
                state.pending_llm_retry = false;
                // Reset timer so timeout doesn't fire immediately on retry
                state.last_conversation_state_change = std::time::Instant::now();
                PlaybackCompleteAction::RetryLlm
            } else {
                state.transition_to(ConversationState::Idle);
                PlaybackCompleteAction::TransitionToIdle
            }
        };
    
        info!("[PLAYBACK_COMPLETE] Action: {:?}", action);
        
        if let Err(e) = self.playback_complete_action_tx.send(action).await {
            error!("[PLAYBACK_COMPLETE] Failed to send action: {}", e);
        }
        
        Ok(())
    }
}
