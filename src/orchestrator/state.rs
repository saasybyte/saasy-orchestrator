use std::sync::Arc;
use std::time::Instant;

use saasy_proto_rust::shared::{
    ProducerId,
    ConsumerId,
    TransportId,
    RtpCapabilities,
    RtpCapabilitiesFinalized,
};
use tokio::task::JoinHandle;

use crate::grpc::speaking_engine::SpeakingContext;
use crate::grpc::listening_engine::ListeningContext;
use crate::sse::llm::LlmMessage;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConversationState {
    Idle,
    UserSpeaking,
    AiThinking,
    AiSpeaking,
}

impl Default for ConversationState {
    fn default() -> Self {
        Self::Idle
    }
}

pub struct SessionState {
    pub session_id: String,
    pub participant_id: Option<String>, // AI participant
    pub router_rtp_capabilities: Option<RtpCapabilitiesFinalized>,
    pub device_rtp_capabilities: Option<RtpCapabilities>,
    pub send_transport_id: Option<TransportId>,
    pub recv_transport_id: Option<TransportId>,
    pub router_producer_id: Option<ProducerId>,
    pub router_consumer_id: Option<ConsumerId>,
    pub device_producer_id: Option<String>,
    pub device_consumer_id: Option<String>,
    pub has_remote_producer: bool,
    pub speaking_context: Option<Arc<SpeakingContext>>,
    pub listening_context: Option<Arc<ListeningContext>>,
    pub engine_inbound_loop_task_handle: Option<JoinHandle<()>>,
    pub conversation_history: Vec<LlmMessage>,
    pub llm_response_task_handle: Option<JoinHandle<()>>,
    pub stream_to_tts_task_handle: Option<JoinHandle<()>>,
    pub current_tts_context_id: Option<String>,
    pub cancelled_tts_context_id: Option<String>,
    pub conversation_state: ConversationState,
    pub last_conversation_state_change: Instant,
    pub ai_thinking_retry_count: u8,
    pub pending_llm_retry: bool,
    pub pending_session_end: bool,
    pub farewell_requested: bool,
    pub farewell_mode: bool,
}

impl SessionState {
    pub fn new(session_id: String) -> Self {
        Self {
            session_id,
            participant_id: None,
            router_rtp_capabilities: None,
            device_rtp_capabilities: None,
            send_transport_id: None,
            recv_transport_id: None,
            router_producer_id: None,
            router_consumer_id: None,
            device_producer_id: None,
            device_consumer_id: None,
            has_remote_producer: false,
            speaking_context: None,
            listening_context: None,
            engine_inbound_loop_task_handle: None,
            conversation_history: Vec::new(),
            llm_response_task_handle: None,
            stream_to_tts_task_handle: None,
            current_tts_context_id: None,
            cancelled_tts_context_id: None,
            conversation_state: ConversationState::default(),
            last_conversation_state_change: Instant::now(),
            ai_thinking_retry_count: 0,
            pending_llm_retry: false,
            pending_session_end: false,
            farewell_requested: false,
            farewell_mode: false,
        }
    }

    pub fn participant_id(&self) -> Option<&str> {
        self.participant_id.as_deref()
    }

    pub fn device_producer_id(&self) -> Option<&str> {
        self.device_producer_id.as_deref()
    }

    pub fn transition_to(&mut self, new_state: ConversationState) {
        if self.conversation_state != new_state {
            tracing::info!(
                "State transition: {:?} -> {:?} | session: {}",
                self.conversation_state,
                new_state,
                self.session_id
            );
            self.conversation_state = new_state;
            self.last_conversation_state_change = Instant::now();

            // Reset retry properties on new turn or successful response
            if new_state == ConversationState::UserSpeaking || new_state == ConversationState::AiSpeaking {
                self.ai_thinking_retry_count = 0;
                self.pending_llm_retry = false;
            }
        }
    }
}
