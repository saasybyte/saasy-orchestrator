use std::sync::Arc;

use saasy_proto_rust::speaking_engine::SpeakingEngineControlMessage;
use tokio::sync::{RwLock, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::orchestrator::SessionState;
use super::listening_engine::ListeningEngineInboundHandler;
use super::speaking_engine::SpeakingEngineInboundHandler;

pub struct EngineInboundHandler {
    speaking_inbound_handler: Arc<SpeakingEngineInboundHandler>,
    listening_inbound_handler: Arc<ListeningEngineInboundHandler>,
}

impl EngineInboundHandler {
    pub fn new(
        speaking_inbound_handler: Arc<SpeakingEngineInboundHandler>,
        listening_inbound_handler: Arc<ListeningEngineInboundHandler>,
    ) -> Self {
        Self {
            speaking_inbound_handler,
            listening_inbound_handler,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn run(
        self,
        session_id: String,
        participant_id: String,
        session_state: Arc<RwLock<SessionState>>,
        speaking_outbound_control_tx: mpsc::Sender<SpeakingEngineControlMessage>,
        mut speaking_inbound_control_rx: mpsc::Receiver<SpeakingEngineControlMessage>,
        mut speaking_inbound_event_rx: mpsc::Receiver<saasy_proto_rust::speaking_engine::EngineToOrchestratorEvent>,
        mut listening_inbound_event_rx: mpsc::Receiver<saasy_proto_rust::listening_engine::EngineToOrchestratorEvent>,
        mut listening_inbound_media_rx: mpsc::Receiver<saasy_proto_rust::listening_engine::MediaFrame>,
        session_token: CancellationToken,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            info!("Starting inbound messaging loop");
            
            loop {
                tokio::select! {
                    biased;

                    () = session_token.cancelled() => {
                        info!("Engine inbound handler received cancellation signal");
                        break;
                    }

                    Some(request) = speaking_inbound_control_rx.recv() => {
                        if let Err(e) = self.speaking_inbound_handler.handle_engine_request(
                            request, 
                            &speaking_outbound_control_tx
                        ).await {
                            error!("Failed to handle speaking engine request: {e}");
                        }
                    }

                    Some(event) = speaking_inbound_event_rx.recv() => {
                        if let Err(e) = self.speaking_inbound_handler.handle_engine_event(
                            event,
                            &session_id,
                            &participant_id,
                            session_state.clone(),
                        ).await {
                            error!("Failed to handle speaking engine event: {e}");
                        }
                    }

                    Some(event) = listening_inbound_event_rx.recv() => {
                        if let Err(e) = self.listening_inbound_handler.handle_engine_event(
                            event,
                            &session_id,
                            &participant_id,
                        ).await {
                            error!("Failed to handle listening engine event: {e}");
                        }
                    }

                    Some(frame) = listening_inbound_media_rx.recv() => {
                        if let Err(e) = self.listening_inbound_handler.handle_media_frame(
                            frame,
                        ).await {
                            error!("Failed to handle media frame: {e}");
                        }
                    }
                    
                    else => {
                        debug!("All channels closed, ending event loop");
                        break;
                    }
                }
            }
        })
    }
}
