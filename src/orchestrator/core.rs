use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use saasy_proto_rust::listening_engine::{
    CloseSessionRequest as CloseSessionRequestForListeningEngine
};
use saasy_proto_rust::sfu::{sfu_event, SfuEvent};
use saasy_proto_rust::shared::MediaKind;
use saasy_proto_rust::speaking_engine::{
    CloseSessionRequest as CloseSessionRequestForSpeakingEngine,
    SpeakingEngineMediaPayload,
};
use tokio::sync::{mpsc, RwLock};
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::stores::{ApiKeyStore, CloudProviderStore};
use crate::grpc::listening_engine::{
    ListeningEngineClient,
    ListeningEngineInboundHandler,
    ListeningEngineOutboundCommands,
    VadTurnEvent,
};
use crate::grpc::speaking_engine::{
    PlaybackCompleteAction,
    SpeakingContext,
    SpeakingEngineClient,
    SpeakingEngineInboundHandler,
    SpeakingEngineOutboundCommands,
};
use crate::grpc::{EdgeService, EngineInboundHandler};
use crate::sse::llm::{
    create_llm_client,
    create_llm_config,
    LlmClient,
    LlmClientConfig,
    LlmMessage,
    LlmRole,
};
use crate::websocket::signal::SignalSessionClient;
use crate::websocket::stt::{
    create_stt_client,
    create_stt_config,
    SttClient,
    SttClientConfig,
    SttTranscript,
};
use crate::websocket::tts::{
    create_tts_client,
    create_tts_config,
    TtsClient,
    TtsClientConfig,
};
use super::manager::SessionManager;
use super::negotiator::SessionNegotiator;
use super::state::{ConversationState, SessionState};

// Timeout recovery constants
const AI_THINKING_TIMEOUT_SECS: u64 = 5;
const AI_SPEAKING_TIMEOUT_SECS: u64 = 60;
const MAX_AI_THINKING_RETRIES: u8 = 2; // 0, 1, 2 = 3 attempts total

// Hardcoded recovery messages
const RECOVERY_MSG_RETRY_1: &str = "Sorry, let me try that again.";
const RECOVERY_MSG_RETRY_2: &str = "One more moment.";
const RECOVERY_MSG_RETRY_3: &str = "Something's wrong on my end. I'll have to end our session, sorry about that.";
const RECOVERY_MSG_RAMBLING: &str = "Sorry, I got carried away. I'll stop there.";

#[derive(Clone, Default)]
struct TranscriptBuffer {
    text: String,
    from_finalize: bool,
    speech_final: bool,
}

impl TranscriptBuffer {
    fn is_finalized(&self) -> bool {
        self.from_finalize || self.speech_final
    }

    fn clear(&mut self) {
        self.text.clear();
        self.from_finalize = false;
        self.speech_final = false;
    }
}

pub struct OrchestratorCore {
    api_key_store: ApiKeyStore,
    cloud_provider_store: Arc<CloudProviderStore>,
    session_manager: Arc<SessionManager>,
    signal_session_client: SignalSessionClient,
    listening_engine_client: Arc<ListeningEngineClient>,
    speaking_engine_client: Arc<SpeakingEngineClient>,
    shutdown_token: CancellationToken,
    edge_service: Arc<EdgeService>,
}

impl OrchestratorCore {
    pub async fn new(
        api_key_store: ApiKeyStore,
        cloud_provider_store: Arc<CloudProviderStore>,
        signal_session_ws_url: String,
        listening_engine_grpc_uds: String,
        speaking_engine_grpc_uds: String,
        shutdown_token: CancellationToken,
        edge_service: Arc<EdgeService>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let session_manager = Arc::new(SessionManager::new());

        let listening_engine_client = Arc::new(
            ListeningEngineClient::connect(&listening_engine_grpc_uds).await?
        );
        info!("Connected to Listening Engine at {listening_engine_grpc_uds}");

        let speaking_engine_client = Arc::new(
            SpeakingEngineClient::connect(&speaking_engine_grpc_uds).await?
        );
        info!("Connected to Speaking Engine at {speaking_engine_grpc_uds}");

        let signal_session_client = SignalSessionClient::new(
            signal_session_ws_url.clone(),
        );
        info!("Signal Session client initialized with URL: {signal_session_ws_url}");

        Ok(Self {
            api_key_store,
            cloud_provider_store,
            session_manager,
            signal_session_client,
            listening_engine_client,
            speaking_engine_client,
            shutdown_token,
            edge_service,
        })
    }

    #[allow(clippy::too_many_lines)]
    pub async fn handle_system_event(&self, sfu_event: SfuEvent) {
        match sfu_event.event {
            Some(sfu_event::Event::SessionCreated(event)) => {
                if !event.requires_ai {
                    info!("Session {} does not require AI, ignoring", event.session_id);
                    return;
                }

                // Validate provider selections against edge service cache
                let llm_valid = self.edge_service
                    .has_llm_provider(&event.llm_provider, &event.llm_model_id)
                    .await;
                let tts_valid = self.edge_service
                    .has_tts_provider(&event.tts_provider, &event.tts_model_id)
                    .await;
                let stt_valid = self.edge_service
                    .has_stt_provider(&event.stt_provider, &event.stt_model_id)
                    .await;

                if !llm_valid {
                    error!(
                        "Session {} has invalid LLM provider: {} / {}",
                        event.session_id, event.llm_provider, event.llm_model_id
                    );
                    return;
                }
                if !tts_valid {
                    error!(
                        "Session {} has invalid TTS provider: {} / {}",
                        event.session_id, event.tts_provider, event.tts_model_id
                    );
                    return;
                }
                if !stt_valid {
                    error!(
                        "Session {} has invalid STT provider: {} / {}",
                        event.session_id, event.stt_provider, event.stt_model_id
                    );
                    return;
                }

                info!(
                    "Session {} providers validated: LLM={}/{}, TTS={}/{}, STT={}/{}",
                    event.session_id,
                    event.llm_provider, event.llm_model_id,
                    event.tts_provider, event.tts_model_id,
                    event.stt_provider, event.stt_model_id
                );

                // Create configs dynamically based on validated provider selections
                let Some(llm_config) = create_llm_config(
                    &event.llm_provider,
                    &event.llm_model_id,
                    &self.api_key_store,
                    &self.cloud_provider_store,
                ) else {
                    error!(
                        "Session {} failed to create LLM config for: {} / {}",
                        event.session_id, event.llm_provider, event.llm_model_id
                    );
                    return;
                };
                let llm_config = Arc::new(llm_config);

                let Some(stt_config) = create_stt_config(
                    &event.stt_provider,
                    &event.stt_model_id,
                    &self.api_key_store,
                    &self.cloud_provider_store,
                ) else {
                    error!(
                        "Session {} failed to create STT config for: {} / {}",
                        event.session_id, event.stt_provider, event.stt_model_id
                    );
                    return;
                };
                let stt_config = Arc::new(stt_config);

                let Some(tts_config) = create_tts_config(
                    &event.tts_provider,
                    &event.tts_model_id,
                    &self.api_key_store,
                    &self.cloud_provider_store,
                ) else {
                    error!(
                        "Session {} failed to create TTS config for: {} / {}",
                        event.session_id, event.tts_provider, event.tts_model_id
                    );
                    return;
                };
                let tts_config = Arc::new(tts_config);

                self.spawn_session(
                    &event.session_id,
                    stt_config,
                    llm_config,
                    tts_config,
                ).await;
            }
            Some(sfu_event::Event::FarewellRequested(event)) => {
                info!("Farewell requested for session {}", event.session_id);
                if let Some(state) = self.session_manager.get_session_state(&event.session_id).await {
                    let mut s = state.write().await;
                    s.farewell_requested = true;
                }
            }
            Some(sfu_event::Event::SessionEnded(event)) => {
                info!("Session {} ended, cleaning up", event.session_id);
                self.end_session(&event.session_id).await;
            }
            _ => {
                // Ignore any unexpected events
            }
        }
    }

    async fn spawn_session(
        &self,
        session_id: &str,
        stt_client_config: Arc<SttClientConfig>,
        llm_client_config: Arc<LlmClientConfig>,
        tts_client_config: Arc<TtsClientConfig>,
    ) {
        info!("Spawning session {session_id} for AI participant to join");

        let participant_id = format!("ai-{}", Uuid::new_v4());
        info!("Created AI participant {participant_id} for session {session_id}");

        let session_token = self.shutdown_token.child_token();
        let session_token_for_manager = session_token.clone();

        let session_id_owned = session_id.to_string();
        let signal_session_client = self.signal_session_client.clone();
        let listening_engine_client = self.listening_engine_client.clone();
        let speaking_engine_client = self.speaking_engine_client.clone();
        let session_manager = self.session_manager.clone();

        let session_task_handle = tokio::spawn(async move {
            let Some(session_state) = session_manager.get_session_state(&session_id_owned).await else {
                error!("Session {session_id_owned} not found in manager");
                return;
            };

            {
                let mut state = session_state.write().await;
                state.participant_id = Some(participant_id.clone());
            }

            if let Err(e) = Self::run_session(
                session_id_owned,
                participant_id,
                session_state,
                signal_session_client,
                listening_engine_client,
                speaking_engine_client,
                stt_client_config,
                llm_client_config,
                tts_client_config,
                session_token,
            ).await {
                error!("Session spawning error: {e}");
            }
        });

        self.session_manager.add_session(session_id, session_task_handle, session_token_for_manager).await;
    }

    #[allow(clippy::too_many_lines)]
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::useless_let_if_seq)]
    async fn run_session(
        session_id: String,
        participant_id: String,
        session_state: Arc<RwLock<SessionState>>,
        signal_session_client: SignalSessionClient,
        listening_engine_client: Arc<ListeningEngineClient>,
        speaking_engine_client: Arc<SpeakingEngineClient>,
        stt_client_config: Arc<SttClientConfig>,
        llm_client_config: Arc<LlmClientConfig>,
        tts_client_config: Arc<TtsClientConfig>,
        session_token: CancellationToken,
    ) -> Result<(), Box<dyn std::error::Error>> {
        info!("Starting negotiation for session {session_id}");

        let (signal_session_control_tx, signal_session_control_rx) = mpsc::channel(32);

        let (vad_turn_event_tx, mut vad_turn_event_rx) = mpsc::channel(32);
        let (playback_complete_action_tx, mut playback_complete_action_rx) = mpsc::channel::<PlaybackCompleteAction>(32);

        let signal_session_client_clone = signal_session_client.clone();
        let session_id_clone = session_id.clone();
        let participant_id_clone = participant_id.clone();
        let session_state_clone = session_state.clone();

        let signal_session_client_token = session_token.clone();
        let signal_session_client_task_handle = tokio::spawn(async move {
            if let Err(e) = signal_session_client_clone.run(
                session_id_clone,
                participant_id_clone,
                session_state_clone,
                signal_session_control_rx,
                signal_session_client_token.clone(),
            ).await {
                error!("SignalSessionClient error: {e}");
                signal_session_client_token.cancel();
            }
        });

        let speaking_engine_inbound_handler = Arc::new(
            SpeakingEngineInboundHandler::new(
                signal_session_control_tx.clone(),
                playback_complete_action_tx,
            )
        );
        let listening_engine_inbound_handler = Arc::new(
            ListeningEngineInboundHandler::new(
                signal_session_control_tx.clone(),
                vad_turn_event_tx,
            )
        );
        let engine_inbound_handler = EngineInboundHandler::new(
            speaking_engine_inbound_handler,
            listening_engine_inbound_handler.clone(),
        );

        let session_negotiator = SessionNegotiator::new(
            signal_session_control_tx
        );

        match session_negotiator.run(
            session_state.clone(),
            listening_engine_client.clone(),
            speaking_engine_client.clone(),
            engine_inbound_handler,
            session_token.clone(),
        ).await {
            Ok(()) => (),
            Err(e) => {
                error!("Session negotiation failed: {e}");
                session_token.cancel();
                return Err(e);
            }
        };

        info!("Session negotiation complete");

        let (speaking_context, device_producer_id) = {
            let state = session_state.read().await;
            let speaking_ctx = state.speaking_context.as_ref()
                .ok_or("No speaking context")?
                .clone();
            let device_producer_id = state.device_producer_id()
                .ok_or("No device_producer_id")?
                .to_owned();
            drop(state);
            (speaking_ctx, device_producer_id)
        };

        info!("Connecting to STT, LLM, and TTS clients");

        let speaking_outbound_media_tx = speaking_context.outbound_media_tx.clone();

        let mut stt_client = match create_stt_client(&stt_client_config) {
            Ok(client) => client,
            Err(e) => {
                session_token.cancel();
                return Err(e.into());
            }
        };
        if let Err(e) = stt_client.connect().await {
            session_token.cancel();
            return Err(e.into());
        }

        let stt_inbound_transcript_rx_opt = stt_client.take_inbound_transcript_receiver();

        let stt_client_arc = Arc::new(tokio::sync::Mutex::new(stt_client));
        listening_engine_inbound_handler.set_stt_client(stt_client_arc.clone()).await;

        let llm_client = match create_llm_client(&llm_client_config) {
            Ok(client) => Arc::new(client),
            Err(e) => {
                session_token.cancel();
                return Err(e.into());
            }
        };

        let mut tts_client = match create_tts_client(&tts_client_config) {
            Ok(client) => client,
            Err(e) => {
                session_token.cancel();
                return Err(e.into());
            }
        };
        if let Err(e) = tts_client.connect().await {
            session_token.cancel();
            return Err(e.into());
        }

        let tts_inbound_audio_rx_opt = tts_client.take_inbound_audio_receiver();

        let tts_client_arc = Arc::new(tokio::sync::Mutex::new(tts_client));

        let mut tts_to_engine_loop_task_handle: Option<tokio::task::JoinHandle<()>> = None;

        if let Some(mut tts_inbound_audio_rx) = tts_inbound_audio_rx_opt  {
            let session_state_clone = session_state.clone();
            let speaking_context_clone = speaking_context.clone();
            let session_id_clone = session_id.clone();
            let participant_id_clone = participant_id.clone();
            let speaking_outbound_media_tx_clone = speaking_outbound_media_tx.clone();
            let device_producer_id_clone = device_producer_id.clone();

            let tts_to_engine_loop_token = session_token.clone();
            tts_to_engine_loop_task_handle = Some(tokio::spawn(async move {
                let mut first_chunk_per_context: HashSet<String> = HashSet::new();

                loop {
                    tokio::select! {
                        biased;

                        () = tts_to_engine_loop_token.cancelled() => {
                            info!("TTS to engine loop received cancellation");
                            break;
                        }

                        result = tts_inbound_audio_rx.recv() => {
                            let Some(audio_chunk) = result else {
                                error!("TTS inbound audio channel closed unexpectedly");
                                tts_to_engine_loop_token.cancel();
                                break;
                            };

                            // Check if this context was cancelled
                            let was_cancelled = {
                                let state = session_state_clone.read().await;
                                audio_chunk.context_id.is_some() && audio_chunk.context_id == state.cancelled_tts_context_id
                            };

                            // Process audio if present
                            if !audio_chunk.data.is_empty() {
                                // Log first audio chunk per context
                                if let Some(ref context_id) = audio_chunk.context_id {
                                    if first_chunk_per_context.insert(context_id.clone()) {
                                        info!(
                                            "[LATENCY] First audio chunk sent to Speaking Engine | context_id: {} | bytes: {}",
                                            context_id,
                                            audio_chunk.data.len(),
                                        );
                                    }
                                }

                                if was_cancelled {
                                    debug!("Skipping stream_chunks_to_speaking_engine for cancelled context: {:?}", audio_chunk.context_id);
                                } else if let Err(e) = Self::stream_chunks_to_speaking_engine(
                                    &session_id_clone,
                                    &participant_id_clone,
                                    session_state_clone.clone(),
                                    audio_chunk.context_id.clone(),
                                    &speaking_outbound_media_tx_clone,
                                    device_producer_id_clone.clone(),
                                    &audio_chunk.data,
                                ).await {
                                    error!("Failed to send TTS audio to speaking engine: {e}");
                                }
                            }

                            if audio_chunk.done {
                                info!("TTS generation complete for context_id: {:?}", audio_chunk.context_id);

                                // Clear current_tts_context_id when audio is actually done
                                {
                                    let mut state = session_state_clone.write().await;
                                    if audio_chunk.context_id.is_some() && audio_chunk.context_id == state.current_tts_context_id {
                                        state.current_tts_context_id = None;
                                    }
                                }

                                // Clean up first-chunk tracking for this context
                                if let Some(ref context_id) = audio_chunk.context_id {
                                    first_chunk_per_context.remove(context_id);
                                }

                                if was_cancelled {
                                    info!("Skipping speech_generation_complete for cancelled context: {:?}", audio_chunk.context_id);
                                } else {
                                    // Only mark complete for non-cancelled contexts
                                    if let Err(e) = SpeakingEngineOutboundCommands::speech_generation_complete(
                                        &speaking_context_clone,
                                        &session_id_clone,
                                        &participant_id_clone,
                                        saasy_proto_rust::speaking_engine::SpeechGenerationCompleteRequest {},
                                    ).await {
                                        error!("[PLAYBACK] Failed to mark speech generation complete: {}", e);
                                    } else {
                                        info!("[PLAYBACK] Marked speech generation complete");
                                    }
                                }

                                // Do not transition to IDLE - wait for OnPlaybackComplete event
                                continue;
                            }
                        }
                    }
                }
            }));
        }

        {
            let mut state = session_state.write().await;
            state.conversation_history.push(LlmMessage {
                role: LlmRole::System,
                content: include_str!("../prompts/system.md").to_string(),
            });
        }

        // Have AI speak first in order to greet the user
        if let Err(e) = Self::send_hardcoded_response(
            &session_state,
            &tts_client_arc,
            "Hi, I'm Anysia. How can I help?",
        ).await {
            warn!("Failed to send AI greeting: {e}");
        }

        let mut engine_to_stt_loop_task_handle: Option<tokio::task::JoinHandle<()>> = None;

        if let Some(mut stt_inbound_transcript_rx) = stt_inbound_transcript_rx_opt {
            let session_id_clone = session_id.clone();
            let participant_id_clone = participant_id.clone();
            let session_state_clone = session_state.clone();
            let tts_client_clone = tts_client_arc.clone();
            let speaking_context_clone = speaking_context.clone();
            let llm_client_clone = llm_client.clone();
            let stt_client_clone = stt_client_arc.clone();

            let engine_to_stt_loop_token = session_token.clone();
            engine_to_stt_loop_task_handle = Some(tokio::spawn(async move {
                let mut final_transcript_buffer = TranscriptBuffer::default();
                let mut segment_finalized = false;
                let mut timeout_check_interval = tokio::time::interval(Duration::from_secs(1));
                timeout_check_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

                loop {
                    tokio::select! {
                        biased;

                        () = engine_to_stt_loop_token.cancelled() => {
                            info!("Engine to STT loop received cancellation");
                            break;
                        }

                        // VAD/Turn events from Listening Engine
                        Some(event) = vad_turn_event_rx.recv() => {
                            let conversation_state = {
                                let state = session_state_clone.read().await;
                                state.conversation_state
                            };

                            match event {
                                VadTurnEvent::SpeechStarted { timestamp_ms } => {
                                    match conversation_state {
                                        ConversationState::Idle => {
                                            info!("[SPEECH_STARTED] User started speaking at {}ms", timestamp_ms);

                                            final_transcript_buffer.clear();
                                            segment_finalized = false;

                                            let mut state = session_state_clone.write().await;
                                            state.transition_to(ConversationState::UserSpeaking);
                                        }
                                        ConversationState::UserSpeaking => {
                                            info!("[SPEECH_STARTED] User already speaking (self-interruption or resumed)");
                                        }
                                        ConversationState::AiThinking | ConversationState::AiSpeaking => {
                                            let farewell_mode = {
                                                let state = session_state_clone.read().await;
                                                state.farewell_mode
                                            };

                                            if farewell_mode {
                                                info!("[FAREWELL] Ignoring SpeechStarted interruption during farewell");
                                            } else {
                                                info!("[SPEECH_STARTED INTERRUPTION] User interrupted during {:?}", conversation_state);

                                                final_transcript_buffer.clear();
                                                segment_finalized = false;

                                                Self::handle_interruption(
                                                    session_id_clone.clone(),
                                                    participant_id_clone.clone(),
                                                    session_state_clone.clone(),
                                                    speaking_context_clone.clone(),
                                                    tts_client_clone.clone(),
                                                ).await;
                                            }
                                        }
                                    }
                                }
                                VadTurnEvent::UserTurnComplete { confidence, timestamp_ms } => {
                                    match conversation_state {
                                        ConversationState::Idle | ConversationState::UserSpeaking => {
                                            info!("[TURN_COMPLETE] User finished their turn at {}ms with confidence {}",
                                                timestamp_ms,
                                                confidence,
                                            );

                                            // Check if we already have the final transcript
                                            let final_transcript = if final_transcript_buffer.is_finalized() {
                                                info!("[FINALIZE] Final transcript already buffered, skipping drain");
                                                final_transcript_buffer.text.clone()
                                            } else {
                                                Self::drain_final_transcript(
                                                    &stt_client_clone,
                                                    &mut stt_inbound_transcript_rx,
                                                    &final_transcript_buffer,
                                                ).await
                                            };

                                            let final_transcript_trimmed = final_transcript.trim();
                                            if final_transcript_trimmed.is_empty() {
                                                warn!("[TURN_COMPLETE] Empty transcript, ignoring turn");
                                                final_transcript_buffer.clear();
                                                segment_finalized = false;

                                                // Transition back to Idle instead of triggering LLM
                                                let mut state = session_state_clone.write().await;
                                                state.transition_to(ConversationState::Idle);
                                                drop(state);
                                                continue;
                                            }

                                            // Setup state synchronously BEFORE spawning
                                            let context_id = {
                                                let mut state = session_state_clone.write().await;
                                                state.conversation_history.push(LlmMessage {
                                                    role: LlmRole::User,
                                                    content: final_transcript,
                                                });
                                                state.transition_to(ConversationState::AiThinking);

                                                // Generate context_id and store it
                                                let context_id = Uuid::new_v4().to_string();
                                                state.current_tts_context_id = Some(context_id.clone());
                                                context_id
                                            };

                                            // Now spawn the AI response task (non-blocking)
                                            let session_state_for_task = session_state_clone.clone();
                                            let tts_client_for_task = tts_client_clone.clone();
                                            let llm_client_for_task = llm_client_clone.clone();
                                            let transcript_time = Instant::now();
                                            let llm_response_token = engine_to_stt_loop_token.clone();

                                            let llm_response_task_handle = tokio::spawn(async move {
                                                Self::initiate_llm_response(
                                                    session_state_for_task,
                                                    tts_client_for_task,
                                                    llm_client_for_task.as_ref().as_ref(),
                                                    context_id,
                                                    transcript_time,
                                                    llm_response_token,
                                                ).await;
                                            });

                                            // Store handle for cancellation
                                            {
                                                let mut state = session_state_clone.write().await;
                                                state.llm_response_task_handle = Some(llm_response_task_handle);
                                            }

                                            final_transcript_buffer.clear();
                                            segment_finalized = false;
                                        }
                                        ConversationState::AiThinking | ConversationState::AiSpeaking => {
                                            let farewell_mode = {
                                                let state = session_state_clone.read().await;
                                                state.farewell_mode
                                            };

                                            if farewell_mode {
                                                info!("[FAREWELL] Ignoring UserTurnComplete interruption during farewell");
                                            } else {
                                                info!("[TURN_COMPLETE INTERRUPTION] Interruption during {:?}", conversation_state);

                                                final_transcript_buffer.clear();
                                                segment_finalized = false;

                                                Self::handle_interruption(
                                                    session_id_clone.clone(),
                                                    participant_id_clone.clone(),
                                                    session_state_clone.clone(),
                                                    speaking_context_clone.clone(),
                                                    tts_client_clone.clone(),
                                                ).await;
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        // STT interim transcripts for text buffering
                        Some(transcript) = stt_inbound_transcript_rx.recv() => {
                            if transcript.from_finalize {
                                final_transcript_buffer.from_finalize = true;
                            }
                            if transcript.speech_final {
                                final_transcript_buffer.speech_final = true;
                                segment_finalized = true; // Mark that this segment ended
                            }

                            if !Self::should_process_transcript(&transcript) {
                                continue;
                            }

                            if segment_finalized {
                                // New segment (interim or final) after previous one finalized - append
                                final_transcript_buffer.text.push(' ');
                                final_transcript_buffer.text.push_str(&transcript.text);
                                if !transcript.speech_final {
                                    segment_finalized = false;
                                }
                            } else {
                                // Same segment - replace
                                final_transcript_buffer.text.clone_from(&transcript.text);
                            }
                        }

                        _ = timeout_check_interval.tick() => {
                            // Check for farewell request first
                            let farewell_requested = {
                                let state = session_state_clone.read().await;
                                state.farewell_requested && !state.farewell_mode
                            };

                            if farewell_requested {
                                info!("[FAREWELL] Playing farewell message");
                                {
                                    let mut state = session_state_clone.write().await;
                                    state.farewell_mode = true;
                                    state.farewell_requested = false;
                                }

                                let _ = Self::send_hardcoded_response(
                                    &session_state_clone,
                                    &tts_client_clone,
                                    "Time's up! It was great talking with you. Come back any time.",
                                ).await;

                                continue;
                            }

                            let (conversation_state, elapsed, retry_count) = {
                                let state = session_state_clone.read().await;
                                (
                                    state.conversation_state,
                                    state.last_conversation_state_change.elapsed(),
                                    state.ai_thinking_retry_count,
                                )
                            };

                            match conversation_state {
                                ConversationState::AiThinking if elapsed >= Duration::from_secs(AI_THINKING_TIMEOUT_SECS) => {
                                    warn!("[TIMEOUT] AI_THINKING timeout after {}s (retry {})", elapsed.as_secs(), retry_count);

                                    // Abort current LLM and TTS tasks
                                    {
                                        let mut state = session_state_clone.write().await;
                                        if let Some(handle) = state.llm_response_task_handle.take() {
                                            handle.abort();
                                        }
                                        if let Some(handle) = state.stream_to_tts_task_handle.take() {
                                            handle.abort();
                                        }
                                    }

                                    if retry_count >= MAX_AI_THINKING_RETRIES {
                                        // Final failure - send message and end session
                                        info!("[TIMEOUT] Max retries exceeded, ending session");
                                        let _ = Self::send_hardcoded_response(
                                            &session_state_clone,
                                            &tts_client_clone,
                                            RECOVERY_MSG_RETRY_3,
                                        ).await;
                                        // Session will end after playback - set flag
                                        {
                                            let mut state = session_state_clone.write().await;
                                            state.pending_session_end = true;
                                        }
                                    } else {
                                        // Retry - send recovery message, then retry LLM after playback
                                        let msg = if retry_count == 0 { RECOVERY_MSG_RETRY_1 } else { RECOVERY_MSG_RETRY_2 };
                                        info!("[TIMEOUT] Sending retry message: {}", msg);

                                        {
                                            let mut state = session_state_clone.write().await;
                                            state.ai_thinking_retry_count += 1;
                                            state.pending_llm_retry = true;
                                        }

                                        let _ = Self::send_hardcoded_response(
                                            &session_state_clone,
                                            &tts_client_clone,
                                            msg,
                                        ).await;
                                    }
                                }
                                ConversationState::AiSpeaking if elapsed >= Duration::from_secs(AI_SPEAKING_TIMEOUT_SECS) => {
                                    warn!("[TIMEOUT] AI_SPEAKING timeout after {}s", elapsed.as_secs());

                                    // Cancel current TTS/audio
                                    Self::handle_interruption(
                                        session_id_clone.clone(),
                                        participant_id_clone.clone(),
                                        session_state_clone.clone(),
                                        speaking_context_clone.clone(),
                                        tts_client_clone.clone(),
                                    ).await;

                                    // Send rambling apology
                                    let _ = Self::send_hardcoded_response(
                                        &session_state_clone,
                                        &tts_client_clone,
                                        RECOVERY_MSG_RAMBLING,
                                    ).await;
                                }
                                _ => {}
                            }
                        }

                        Some(action) = playback_complete_action_rx.recv() => {
                            match action {
                                PlaybackCompleteAction::TransitionToIdle => {
                                    info!("[PLAYBACK_ACTION] Transition to Idle");
                                    // Already transitioned in handler, nothing to do
                                }
                                PlaybackCompleteAction::RetryLlm => {
                                    info!("[PLAYBACK_ACTION] Retrying LLM");

                                    let context_id = {
                                        let mut state = session_state_clone.write().await;
                                        let context_id = Uuid::new_v4().to_string();
                                        state.current_tts_context_id = Some(context_id.clone());
                                        state.transition_to(ConversationState::AiThinking);
                                        context_id
                                    };

                                    let session_state_for_task = session_state_clone.clone();
                                    let tts_client_for_task = tts_client_clone.clone();
                                    let llm_client_for_task = llm_client_clone.clone();
                                    let transcript_time = Instant::now();
                                    let llm_response_token = engine_to_stt_loop_token.clone();

                                    let llm_response_task_handle = tokio::spawn(async move {
                                        Self::initiate_llm_response(
                                            session_state_for_task,
                                            tts_client_for_task,
                                            llm_client_for_task.as_ref().as_ref(),
                                            context_id,
                                            transcript_time,
                                            llm_response_token,
                                        ).await;
                                    });

                                    {
                                        let mut state = session_state_clone.write().await;
                                        state.llm_response_task_handle = Some(llm_response_task_handle);
                                    }
                                }
                                PlaybackCompleteAction::EndSession => {
                                    info!("[PLAYBACK_ACTION] Ending session due to timeout failure");
                                    engine_to_stt_loop_token.cancel();
                                }
                            }
                        }

                        // TODO: we need to determine if this still needed
                        // because the addition of timeouts above makes this branch dead code
                        else => {
                            error!("Engine to STT loop closed unexpectedly");
                            engine_to_stt_loop_token.cancel();
                            break;
                        }
                    }
                }
            }));
        }

        // Keep session alive until cancelled
        session_token.cancelled().await;
        info!("Session {session_id} received cancellation signal");

        // Best-effort close engine sessions (may fail if streams already dead)
        Self::close_session_in_engines(&session_id, &session_state).await;

        // Await all session tasks
        let _ = signal_session_client_task_handle.await;
        if let Some(handle) = tts_to_engine_loop_task_handle {
            let _ = handle.await;
        }
        if let Some(handle) = engine_to_stt_loop_task_handle {
            let _ = handle.await;
        }

        // Disconnect provider clients
        let stt_disconnect_result = stt_client_arc.lock().await.disconnect().await;
        if let Err(e) = stt_disconnect_result {
            error!("Failed to disconnect STT: {e}");
        }

        let tts_disconnect_result = tts_client_arc.lock().await.disconnect().await;
        if let Err(e) = tts_disconnect_result {
            error!("Failed to disconnect TTS: {e}");
        }

        info!("AI participant {participant_id} has exited session {session_id}");

        Ok(())
    }

    fn should_process_transcript(transcript: &SttTranscript) -> bool {
        const MIN_CONFIDENCE: f32 = 0.7;
        const MIN_WORDS: usize = 1;

        // Always accept finalized transcripts
        if transcript.speech_final || transcript.from_finalize {
            return !transcript.text.trim().is_empty();
        }

        // Check confidence for interim results
        if let Some(confidence) = transcript.confidence {
            if confidence < MIN_CONFIDENCE {
                tracing::debug!(
                    "Ignoring low-confidence transcript: {} (confidence: {:.2})",
                    transcript.text,
                    confidence
                );
                return false;
            }
        }

        let word_count = transcript.text.split_whitespace().count();
        if word_count < MIN_WORDS {
            tracing::debug!(
                "Ignoring empty transcript: '{}' (word count: {})",
                transcript.text,
                word_count
            );
            return false;
        }

        true
    }

    async fn drain_final_transcript(
        stt_client: &Arc<tokio::sync::Mutex<Box<dyn SttClient>>>,
        stt_inbound_transcript_rx: &mut mpsc::UnboundedReceiver<SttTranscript>,
        transcript_buffer: &TranscriptBuffer,
    ) -> String {
        const FINALIZE_TIMEOUT_MS: u64 = 100;
        const FALLBACK_TIMEOUT_MS: u64 = 300; // Longer timeout for when we have nothing

        // Send Finalize command to STT and wait for final transcript
        let stt_client_guard = stt_client.lock().await;
        if let Err(e) = stt_client_guard.finalize().await {
            error!("Failed to send Finalize to STT: {e}");
        }
        drop(stt_client_guard);

        let drain_start = Instant::now();
        let mut text = transcript_buffer.text.clone();
        let has_good_transcript = !text.trim().is_empty();

        let timeout_ms = if has_good_transcript { FINALIZE_TIMEOUT_MS } else { FALLBACK_TIMEOUT_MS };

        while drain_start.elapsed() < Duration::from_millis(timeout_ms) {
            match tokio::time::timeout(
                Duration::from_millis(timeout_ms).saturating_sub(drain_start.elapsed()),
                stt_inbound_transcript_rx.recv()
            ).await {
                Ok(Some(transcript)) => {
                    if Self::should_process_transcript(&transcript) {
                        text.clone_from(&transcript.text);
                    }

                    if transcript.from_finalize || transcript.speech_final {
                        info!("[FINALIZE] Got final transcript: from_finalize={}, speech_final={}",
                            transcript.from_finalize, transcript.speech_final);
                        break;
                    }
                }
                Ok(None) => {
                    warn!("[FINALIZE] STT channel closed unexpectedly");
                    break;
                }
                Err(_) => {
                    info!("[FINALIZE] Timeout after {}ms, using current buffer",
                        drain_start.elapsed().as_millis());
                    break;
                }
            }
        }

        info!("[FINALIZE] Drain complete in {}ms", drain_start.elapsed().as_millis());
        text
    }

    async fn initiate_llm_response(
        session_state: Arc<RwLock<SessionState>>,
        tts_client: Arc<tokio::sync::Mutex<Box<dyn TtsClient>>>,
        llm_client: &dyn LlmClient,
        context_id: String,
        transcript_received_start: Instant,
        session_token: CancellationToken,
    ) {
        let messages = {
            let state = session_state.read().await;
            state.conversation_history.clone()
        };

        let llm_inbound_token_str_rx = match llm_client.stream(&messages).await {
            Ok(token_str_rx) => {
                let transcript_sent_to_llm = transcript_received_start.elapsed();
                info!(
                    "[LATENCY] Transcript sent to LLM | elapsed: {}ms since transcript",
                    transcript_sent_to_llm.as_millis()
                );
                token_str_rx
            }
            Err(e) => {
                error!("LLM streaming error: {e}");
                // Reset to IDLE on error
                {
                    let mut state = session_state.write().await;
                    state.transition_to(ConversationState::Idle);
                    state.current_tts_context_id = None;
                    state.llm_response_task_handle = None;
                }
                return;
            }
        };

        let session_state_for_streaming = session_state.clone();
        let tts_client_for_streaming = tts_client.clone();
        let context_id_clone = context_id.clone();
        let stream_to_tts_token = session_token.clone();

        let stream_to_tts_task_handle = tokio::spawn(async move {
            Self::stream_tokens_to_tts(
                context_id_clone,
                session_state_for_streaming,
                tts_client_for_streaming,
                llm_inbound_token_str_rx,
                stream_to_tts_token,
            ).await;
        });

        // Store handle for cancellation
        {
            let mut state = session_state.write().await;
            state.stream_to_tts_task_handle = Some(stream_to_tts_task_handle);
        }
    }

    async fn handle_interruption(
        session_id: String,
        participant_id: String,
        session_state: Arc<RwLock<SessionState>>,
        speaking_context: Arc<SpeakingContext>,
        tts_client: Arc<tokio::sync::Mutex<Box<dyn TtsClient>>>,
    ) {
        const FADE_DURATION_MS: u32 = 150;

        info!("[INTERRUPTION] Cancelling AI generation");

        // Abort tasks and cancel TTS generation
        {
            let mut state = session_state.write().await;

            // Abort LLM response task
            if let Some(handle) = state.llm_response_task_handle.take() {
                handle.abort();
                info!("[INTERRUPTION] LLM response task aborted");
            }

            // Abort TTS streaming task
            if let Some(handle) = state.stream_to_tts_task_handle.take() {
                handle.abort();
                info!("[INTERRUPTION] TTS streaming task aborted");
            }

            // Cancel TTS generation with provider and track cancelled context
            if let Some(context_id) = state.current_tts_context_id.take() {
                let client_guard = tts_client.lock().await;
                if let Err(e) = client_guard.cancel_generation(context_id.clone()).await {
                    debug!("[INTERRUPTION] TTS cancel skipped: {e}");
                } else {
                    info!("[INTERRUPTION] TTS generation cancelled");
                }
                drop(client_guard);

                state.cancelled_tts_context_id = Some(context_id);
            }
        }

        // Flush Speaking Engine audio buffer
        if let Err(e) = SpeakingEngineOutboundCommands::flush_audio(
            &speaking_context,
            &session_id,
            &participant_id,
            saasy_proto_rust::speaking_engine::FlushAudioRequest {
                fade_duration_ms: FADE_DURATION_MS,
            },
        ).await {
            error!("[INTERRUPTION] Failed to flush audio buffer: {e}");
        } else {
            info!("[INTERRUPTION] Audio buffer flushed");
        }

        // Transition to UserSpeaking
        {
            let mut state = session_state.write().await;
            state.llm_response_task_handle = None;
            state.stream_to_tts_task_handle = None;
            state.transition_to(ConversationState::UserSpeaking);
        }

        info!("[INTERRUPTION] Complete - waiting for user to finish speaking");
    }

    async fn stream_tokens_to_tts(
        context_id: String,
        session_state: Arc<RwLock<SessionState>>,
        tts_client: Arc<tokio::sync::Mutex<Box<dyn TtsClient>>>,
        mut llm_inbound_token_str_rx: mpsc::UnboundedReceiver<String>,
        session_token: CancellationToken,
    ) {
        let mut accumulated_text = String::new();
        let mut full_response = String::new();
        let mut first_audio_sent = false;

        loop {
            tokio::select! {
                biased;

                () = session_token.cancelled() => {
                    info!("Stream to TTS received cancellation");
                    break;
                }

                result = llm_inbound_token_str_rx.recv() => {
                    let Some(token_str) = result else {
                        break;
                    };

                    full_response.push_str(&token_str);
                    accumulated_text.push_str(&token_str);

                    if token_str.contains('.') || token_str.contains('?') || token_str.contains('!') {
                        let text_to_send = accumulated_text.trim().to_string();
                        if !text_to_send.is_empty() {
                            let client_guard = tts_client.lock().await;
                            if let Err(e) = client_guard.generate_speech(text_to_send, Some(context_id.clone()), true).await {
                                error!("Failed to send text to TTS: {e}");
                                break;
                            }
                            drop(client_guard);
                            accumulated_text.clear();

                            // Transition AI_THINKING → AI_SPEAKING on first TTS request
                            if !first_audio_sent {
                                first_audio_sent = true;
                                let mut state = session_state.write().await;
                                state.transition_to(ConversationState::AiSpeaking);
                            }
                        }
                    }
                }
            }
        }

        // Only do cleanup if not cancelled (normal completion)
        if session_token.is_cancelled() {
            info!("Stream to TTS cancelled, skipping cleanup");
            return;
        }

        // Send any remaining text
        if !accumulated_text.trim().is_empty() {
            let text_to_send = accumulated_text.trim().to_string();
            let client_guard = tts_client.lock().await;
            if let Err(e) = client_guard.generate_speech(text_to_send, Some(context_id.clone()), true).await {
                error!("Failed to send final text to TTS: {e}");
            }
            drop(client_guard);

            // Transition if this is the first audio
            if !first_audio_sent {
                let mut state = session_state.write().await;
                state.transition_to(ConversationState::AiSpeaking);
            }
        }

        // Close the context with 'continue_generation' set to false and empty text
        {
            let client_guard = tts_client.lock().await;
            if let Err(e) = client_guard.generate_speech(String::new(), Some(context_id.clone()), false).await {
                error!("Failed to close TTS context: {e}");
            }
        }

        // Add response to conversation history
        {
            let mut state = session_state.write().await;
            state.conversation_history.push(LlmMessage {
                role: LlmRole::Assistant,
                content: full_response,
            });
        }

        // Clear TTS pipeline state (LLM→TTS complete)
        {
            let mut state = session_state.write().await;
            state.stream_to_tts_task_handle = None;
        }
    }

    async fn stream_chunks_to_speaking_engine(
        session_id: &str,
        participant_id: &str,
        session_state: Arc<RwLock<SessionState>>,
        context_id: Option<String>,
        speaking_outbound_media_tx: &mpsc::Sender<SpeakingEngineMediaPayload>,
        device_producer_id: String,
        audio_data: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        const SAMPLE_RATE: usize = 48000;
        const CHANNELS: usize = 1;
        const FRAME_DURATION_MS: usize = 20;
        const SAMPLES_PER_FRAME: usize = SAMPLE_RATE / (1000 / FRAME_DURATION_MS);
        const BYTES_PER_SAMPLE: usize = 2; // 16-bit
        const FRAME_SIZE: usize = SAMPLES_PER_FRAME * CHANNELS * BYTES_PER_SAMPLE; // 1920 bytes

        for (i, chunk) in audio_data.chunks(FRAME_SIZE).enumerate() {
            // Check every 10 frames (200ms) to minimize lock contention
            if i % 10 == 0 {
                let state = session_state.read().await;
                if context_id.is_some() && context_id == state.cancelled_tts_context_id {
                    info!("Stopping mid-stream for cancelled context after {} frames", i);
                    return Ok(());
                }
            }

            let chunk_data = if chunk.len() < FRAME_SIZE {
                let mut padded = chunk.to_vec();
                padded.resize(FRAME_SIZE, 0);
                padded
            } else {
                chunk.to_vec()
            };

            SpeakingEngineOutboundCommands::send_media_frame(
                speaking_outbound_media_tx,
                session_id,
                participant_id,
                device_producer_id.clone(),
                MediaKind::Audio as i32,
                chunk_data,
            ).await?;

            tokio::time::sleep(Duration::from_millis(20)).await; // TODO: 18ms seems a little snappier
        }

        Ok(())
    }

    async fn send_hardcoded_response(
        session_state: &Arc<RwLock<SessionState>>,
        tts_client: &Arc<tokio::sync::Mutex<Box<dyn TtsClient>>>,
        message: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let context_id = Uuid::new_v4().to_string();

        {
            let mut state = session_state.write().await;
            state.current_tts_context_id = Some(context_id.clone());
            state.transition_to(ConversationState::AiSpeaking);
        }

        let tts_client_guard = tts_client.lock().await;
        tts_client_guard.generate_speech(message.to_string(), Some(context_id), false).await?;
        drop(tts_client_guard);

        Ok(())
    }

    async fn close_session_in_engines(
        session_id: &str,
        session_state: &Arc<RwLock<SessionState>>,
    ) {
        let (listening_context, speaking_context, participant_id) = {
            let state = session_state.read().await;
            (
                state.listening_context.clone(),
                state.speaking_context.clone(),
                state.participant_id.clone(),
            )
        };

        let participant_id = participant_id.unwrap_or_default();
        let close_timeout = Duration::from_millis(500);

        if let Some(ctx) = listening_context {
            let _ = tokio::time::timeout(
                close_timeout,
                ListeningEngineOutboundCommands::close_session(
                    &ctx, session_id, &participant_id,
                    CloseSessionRequestForListeningEngine {}
                )
            ).await;
            info!("Listening engine session closed for {}", session_id);
        }

        if let Some(ctx) = speaking_context {
            let _ = tokio::time::timeout(
                close_timeout,
                SpeakingEngineOutboundCommands::close_session(
                    &ctx, session_id, &participant_id,
                    CloseSessionRequestForSpeakingEngine {}
                )
            ).await;
            info!("Speaking engine session closed for {}", session_id);
        }
    }

    async fn end_session(&self, session_id: &str) {
        if let Some(state) = self.session_manager.get_session_state(session_id).await {
            Self::close_session_in_engines(session_id, &state).await;
        }

        if let Some(handle) = self.session_manager.remove_session(session_id).await {
            handle.session_token.cancel();
            let _ = handle.task.await;
            info!("Session {} ended", session_id);
        }
    }

    pub async fn shutdown_all_sessions(&self) {
        info!("Shutting down all sessions...");

        let session_ids: Vec<String> = self.session_manager.get_all_session_ids().await;
        for session_id in &session_ids {
            if let Some(state) = self.session_manager.get_session_state(session_id).await {
                Self::close_session_in_engines(session_id, &state).await;
            }
        }

        let handles = self.session_manager.shutdown_all_sessions().await;
        let count = handles.len();

        if count == 0 {
            info!("No active sessions to shut down");
            return;
        }

        info!("Awaiting {count} session(s) to shut down");

        for handle in handles {
            let _ = handle.await;
        }

        info!("All sessions shut down");
    }

    pub fn listening_engine_client(&self) -> Arc<ListeningEngineClient> {
        self.listening_engine_client.clone()
    }

    pub fn speaking_engine_client(&self) -> Arc<SpeakingEngineClient> {
        self.speaking_engine_client.clone()
    }
}
