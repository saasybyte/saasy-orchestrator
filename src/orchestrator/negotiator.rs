use std::sync::Arc;
use std::time::Duration;

use saasy_proto_rust::listening_engine::{
    LoadDeviceRequest as LoadDeviceRequestForListeningEngine,
    CreateConsumerRequest as CreateConsumerRequestForListeningEngine,
    CreateTransportRequest as CreateTransportRequestForListeningEngine,
    ResumeConsumerRequest as ResumeConsumerRequestForListeningEngine,
};
use saasy_proto_rust::shared::{TransportDirection, RtpCapabilities};
use saasy_proto_rust::speaking_engine::{
    LoadDeviceRequest as LoadDeviceRequestForSpeakingEngine,
    GetDeviceRtpCapabilitiesRequest as GetDeviceRtpCapabilitiesRequestForSpeakingEngine,
    CreateTransportRequest as CreateTransportRequestForSpeakingEngine,
    StartProductionRequest as StartProductionRequestForSpeakingEngine,
};
use tokio::sync::{mpsc, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::grpc::EngineInboundHandler;
use crate::grpc::listening_engine::{
    ListeningContext,
    ListeningEngineClient,
    ListeningEngineOutboundCommands,
};
use crate::grpc::speaking_engine::{
    SpeakingContext,
    SpeakingEngineClient,
    SpeakingEngineOutboundCommands,
};
use crate::websocket::signal::{SignalOutboundCommands, SignalSessionPendingRequest};
use super::SessionState;

pub struct SessionNegotiator {
    signal_session_control_tx: mpsc::Sender<SignalSessionPendingRequest>,
}

impl SessionNegotiator {
    pub fn new(signal_session_control_tx: mpsc::Sender<SignalSessionPendingRequest>) -> Self {
        Self { signal_session_control_tx }
    }

    pub async fn run(
        self,
        state: Arc<RwLock<SessionState>>,
        listening_engine_client: Arc<ListeningEngineClient>,
        speaking_engine_client: Arc<SpeakingEngineClient>,
        engine_inbound_handler: EngineInboundHandler,
        session_token: CancellationToken,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (session_id, participant_id) = {
            let state_guard = state.read().await;
            let participant_id = state_guard
                .participant_id()
                .ok_or("No participant_id available")?
                .to_owned();
            (state_guard.session_id.clone(), participant_id)
        };
        info!("Starting negotiation for AI participant {}", participant_id);

        let mut speaking_context = speaking_engine_client
            .start_streams(&session_id, &participant_id, session_token.clone()).await?;
        let mut listening_context = listening_engine_client
            .start_streams(&session_id, &participant_id, session_token.clone()).await?;

        self.join_session(state.clone()).await?;
        self.get_router_rtp_capabilities(state.clone()).await?;
        self.load_device(state.clone(), &speaking_context, &listening_context).await?;
        let device_rtp_capabilities = self.get_device_rtp_capabilities(state.clone(), &speaking_context).await?;
        self.set_rtp_capabilities(state.clone(), device_rtp_capabilities).await?;
        self.subscribe_to_events(state.clone()).await?;
        self.create_transports(state.clone(), &speaking_context, &listening_context).await?;
        self.wait_for_remote_producer(state.clone()).await?;
        self.create_consumer(state.clone(), &listening_context).await?;
        self.resume_consumer(state.clone(), &listening_context).await?;

        let speaking_inbound_control_rx = SpeakingEngineClient::take_inbound_control_receiver(&mut speaking_context);
        let speaking_inbound_event_rx = SpeakingEngineClient::take_inbound_events_receiver(&mut speaking_context);
        let listening_inbound_event_rx = ListeningEngineClient::take_inbound_events_receiver(&mut listening_context);
        let listening_inbound_media_rx = ListeningEngineClient::take_inbound_media_receiver(&mut listening_context);

        let engine_inbound_loop_task_handle = engine_inbound_handler.run(
            session_id.clone(),
            participant_id.clone(),
            state.clone(),
            speaking_context.outbound_control_tx.clone(),
            speaking_inbound_control_rx,
            speaking_inbound_event_rx,
            listening_inbound_event_rx,
            listening_inbound_media_rx,
            session_token.clone(),
        );

        self.start_production(state.clone(), &speaking_context).await?;

        {
            let mut state_guard = state.write().await;
            state_guard.speaking_context = Some(Arc::new(speaking_context));
            state_guard.listening_context = Some(Arc::new(listening_context));
            state_guard.engine_inbound_loop_task_handle = Some(engine_inbound_loop_task_handle);
        }

        info!("Negotiation complete!");
        
        Ok(())
    }

    async fn join_session(&self, state: Arc<RwLock<SessionState>>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Joining session");

        let (session_id, participant_id) = {
            let state_guard = state.read().await;
            let participant_id = state_guard
                .participant_id()
                .ok_or("No participant_id available")?
                .to_owned();
            (state_guard.session_id.clone(), participant_id)
        };

        SignalOutboundCommands::join_session(
            &self.signal_session_control_tx,
            &session_id,
            &participant_id,
        ).await?;

        Ok(())
    }

    async fn get_router_rtp_capabilities(
        &self,
        state: Arc<RwLock<SessionState>>
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Getting router RTP capabilities from Signal");

        let (session_id, participant_id) = {
            let state_guard = state.read().await;
            let participant_id = state_guard
                .participant_id()
                .ok_or("No participant_id available")?
                .to_owned();
            (state_guard.session_id.clone(), participant_id)
        };

        let response = SignalOutboundCommands::get_router_rtp_capabilities(
            &self.signal_session_control_tx,
            &session_id,
            &participant_id,
        ).await?;
        
        if let Some(router_rtp_capabilities) = response.rtp_capabilities {
            let mut state_guard = state.write().await;
            state_guard.router_rtp_capabilities = Some(router_rtp_capabilities);
            drop(state_guard);
            info!("Received router RTP capabilities");
        }

        Ok(())
    }

    async fn load_device(
        &self,
        state: Arc<RwLock<SessionState>>,
        speaking_context: &SpeakingContext,
        listening_context: &ListeningContext,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Loading device on engines with router capabilities");
        
        let (session_id, participant_id, router_caps) = {
            let state_guard = state.read().await;
            let participant_id = state_guard
                .participant_id()
                .ok_or("No participant_id available")?
                .to_owned();
            let router_caps = state_guard.router_rtp_capabilities.as_ref()
                .ok_or("No router RTP capabilities available")?
                .clone();
            (state_guard.session_id.clone(), participant_id, router_caps)
        };

        let load_device_request_for_speaking_engine = LoadDeviceRequestForSpeakingEngine {
            router_rtp_capabilities: Some(router_caps.clone()),
        };
        
        SpeakingEngineOutboundCommands::load_device(
            speaking_context,
            &session_id,
            &participant_id,
            load_device_request_for_speaking_engine,
        ).await?;

        let load_device_request_for_listening_engine = LoadDeviceRequestForListeningEngine {
            router_rtp_capabilities: Some(router_caps),
        };
        
        ListeningEngineOutboundCommands::load_device(
            listening_context,
            &session_id,
            &participant_id,
            load_device_request_for_listening_engine,
        ).await?;
        
        Ok(())
    }

    async fn get_device_rtp_capabilities(
        &self,
        state: Arc<RwLock<SessionState>>,
        speaking_context: &SpeakingContext,
    ) -> Result<RtpCapabilities, Box<dyn std::error::Error + Send + Sync>> {
        info!("Getting device RTP capabilities from engine");
        
        let (session_id, participant_id) = {
            let state_guard = state.read().await;
            let participant_id = state_guard
                .participant_id()
                .ok_or("No participant_id available")?
                .to_owned();
            (state_guard.session_id.clone(), participant_id)
        };

        let response = SpeakingEngineOutboundCommands::get_device_rtp_capabilities(
            speaking_context,
            &session_id,
            &participant_id,
            GetDeviceRtpCapabilitiesRequestForSpeakingEngine {},
        ).await?;
        
        let device_rtp_capabilities = response.device_rtp_capabilities
            .ok_or("No device RTP capabilities returned")?;

        {
            let mut state_guard = state.write().await;
            state_guard.device_rtp_capabilities = Some(device_rtp_capabilities.clone());
            drop(state_guard);
            info!("Stored device RTP capabilities in state");
        }
        
        Ok(device_rtp_capabilities)
    }

    async fn set_rtp_capabilities(
        &self,
        state: Arc<RwLock<SessionState>>,
        device_rtp_capabilities: RtpCapabilities,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Setting device RTP capabilities on Signal");

        let (session_id, participant_id) = {
            let state_guard = state.read().await;
            let participant_id = state_guard
                .participant_id()
                .ok_or("No participant_id available")?
                .to_owned();
            (state_guard.session_id.clone(), participant_id)
        };

        SignalOutboundCommands::set_rtp_capabilities(
            &self.signal_session_control_tx,
            &session_id,
            &participant_id,
            device_rtp_capabilities,
        ).await?;

        Ok(())
    }

    async fn subscribe_to_events(
        &self,
        state: Arc<RwLock<SessionState>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Subscribing to events");

        let (session_id, participant_id) = {
            let state_guard = state.read().await;
            let participant_id = state_guard
                .participant_id()
                .ok_or("No participant_id available")?
                .to_owned();
            (state_guard.session_id.clone(), participant_id)
        };

        SignalOutboundCommands::subscribe_to_events(
            &self.signal_session_control_tx,
            &session_id,
            &participant_id,
        ).await?;
        
        info!("Ready to handle Signal events");
        Ok(())
    }

    async fn create_transports(
        &self,
        state: Arc<RwLock<SessionState>>,
        speaking_context: &SpeakingContext,
        listening_context: &ListeningContext,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (session_id, participant_id) = {
            let state_guard = state.read().await;
            let participant_id = state_guard
                .participant_id()
                .ok_or("No participant_id available")?
                .to_owned();
            (state_guard.session_id.clone(), participant_id)
        };

        info!("Creating Send transport on Signal");
        let send_response = SignalOutboundCommands::create_transport(
            &self.signal_session_control_tx,
            &session_id,
            &participant_id,
            TransportDirection::Send,
        ).await?;
        
        let transport_id_for_send = send_response.transport_id
            .ok_or("No transport ID in response")?;
        let ice_parameters_for_send = send_response.ice_parameters
            .ok_or("No ICE parameters in response")?;
        let ice_candidates_for_send = send_response.ice_candidates;
        let dtls_parameters_for_send = send_response.dtls_parameters
            .ok_or("No DTLS parameters in response")?;

        {
            let mut state_guard = state.write().await;
            state_guard.send_transport_id = Some(transport_id_for_send.clone());
        }

        info!("Creating Send transport on speaking engine");
        let create_request_for_speaking_engine = CreateTransportRequestForSpeakingEngine {
            transport_id: Some(transport_id_for_send.clone()),
            ice_parameters: Some(ice_parameters_for_send),
            ice_candidates: ice_candidates_for_send,
            dtls_parameters: Some(dtls_parameters_for_send),
        };

        SpeakingEngineOutboundCommands::create_transport(
            speaking_context,
            &session_id,
            &participant_id,
            create_request_for_speaking_engine,
        ).await?;

        info!("Creating Recv transport on Signal");
        let recv_response = SignalOutboundCommands::create_transport(
            &self.signal_session_control_tx,
            &session_id,
            &participant_id,
            TransportDirection::Recv,
        ).await?;
        
        let transport_id_for_recv = recv_response.transport_id
            .ok_or("No transport ID in response")?;
        let ice_parameters_for_recv = recv_response.ice_parameters
            .ok_or("No ICE parameters in response")?;
        let ice_candidates_for_recv = recv_response.ice_candidates;
        let dtls_parameters_for_recv = recv_response.dtls_parameters
            .ok_or("No DTLS parameters in response")?;

        {
            let mut state_guard = state.write().await;
            state_guard.recv_transport_id = Some(transport_id_for_recv.clone());
        }

        info!("Creating Recv transport on listening engine");
        let create_request_for_listening_engine = CreateTransportRequestForListeningEngine {
            transport_id: Some(transport_id_for_recv),
            ice_parameters: Some(ice_parameters_for_recv),
            ice_candidates: ice_candidates_for_recv,
            dtls_parameters: Some(dtls_parameters_for_recv),
        };

        ListeningEngineOutboundCommands::create_transport(
            listening_context,
            &session_id,
            &participant_id,
            create_request_for_listening_engine,
        ).await?;
        
        Ok(())
    }

    async fn wait_for_remote_producer(
        &self,
        state: Arc<RwLock<SessionState>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let timeout = Duration::from_secs(30);
        let start = tokio::time::Instant::now();
        
        loop {
            {
                let state_guard = state.read().await;
                if state_guard.has_remote_producer && state_guard.router_producer_id.is_some() {
                    info!("Remote producer detected");
                    return Ok(());
                }
            }
            
            if start.elapsed() > timeout {
                return Err("Timeout waiting for remote producer".into());
            }
            
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn create_consumer(
        &self,
        state: Arc<RwLock<SessionState>>,
        listening_context: &ListeningContext,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (session_id, participant_id, recv_transport_id, router_producer_id, device_rtp_capabilities) = {
            let state_guard = state.read().await;
            let participant_id = state_guard
                .participant_id()
                .ok_or("No participant_id available")?
                .to_owned();
            let recv_transport_id = state_guard.recv_transport_id.as_ref()
                .ok_or("No recv transport ID available")?
                .clone();
            let router_producer_id = state_guard.router_producer_id.as_ref()
                .ok_or("No remote producer ID available")?
                .clone();
            let device_rtp_capabilities = state_guard.device_rtp_capabilities.as_ref()
                .ok_or("No device RTP capabilities available")?
                .clone();
            (
                state_guard.session_id.clone(), 
                participant_id, 
                recv_transport_id,
                router_producer_id,
                device_rtp_capabilities,
            )
        };

        info!("Creating consumer for router producer: {}", router_producer_id.id);

        let create_response = SignalOutboundCommands::create_consumer(
            &self.signal_session_control_tx,
            &session_id,
            &participant_id,
            recv_transport_id.clone(),
            router_producer_id.clone(),
            device_rtp_capabilities,
        ).await?;

        let consumer_info = create_response.consumer_info
            .ok_or("No consumer info in response")?;
        let router_consumer_id = saasy_proto_rust::shared::ConsumerId {
            id: consumer_info.id.clone()
        };
        let rtp_parameters = consumer_info.rtp_parameters
            .ok_or("No RTP parameters in consumer info")?;

        {
            let mut state_guard = state.write().await;
            state_guard.router_consumer_id = Some(router_consumer_id.clone());
        }

        let create_consumer_request = CreateConsumerRequestForListeningEngine {
            consumer_id: Some(router_consumer_id.clone()),
            producer_id: Some(router_producer_id),
            kind: consumer_info.kind,
            rtp_parameters: Some(rtp_parameters),
        };

        let engine_response = ListeningEngineOutboundCommands::create_consumer(
            listening_context,
            &session_id,
            &participant_id,
            create_consumer_request,
        ).await?;

        {
            let mut state_guard = state.write().await;
            state_guard.device_consumer_id = Some(engine_response.device_consumer_id.clone());
        }

        info!("Consumer created with ID: {}", router_consumer_id.id);

        Ok(())
    }

    async fn resume_consumer(
        &self,
        state: Arc<RwLock<SessionState>>,
        listening_context: &ListeningContext,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (session_id, participant_id, router_consumer_id, device_consumer_id) = {
            let state_guard = state.read().await;
            let participant_id = state_guard
                .participant_id()
                .ok_or("No participant_id available")?
                .to_owned();
            let router_consumer_id = state_guard.router_consumer_id.as_ref()
                .ok_or("No router consumer ID available")?
                .clone();
            let device_consumer_id = state_guard.device_consumer_id.as_ref()
                .ok_or("No device consumer ID available")?
                .clone();
            (
                state_guard.session_id.clone(), 
                participant_id,
                router_consumer_id,
                device_consumer_id
            )
        };

        info!("Resuming consumer on router with router consumer ID: {}", router_consumer_id.id);

        SignalOutboundCommands::resume_consumer(
            &self.signal_session_control_tx,
            &session_id,
            &participant_id,
            router_consumer_id.clone(),
        ).await?;

        info!("Consumer resumed on router");

        info!("Resuming consumer on listening engine with device consumer ID: {}", device_consumer_id);

        let resume_request = ResumeConsumerRequestForListeningEngine {
            device_consumer_id: device_consumer_id.clone(),
        };

        ListeningEngineOutboundCommands::resume_consumer(
            listening_context,
            &session_id,
            &participant_id,
            resume_request,
        ).await?;

        info!("Consumer resumed on listening engine, ready to receive media");

        Ok(())
    }

    async fn start_production(
        &self,
        state: Arc<RwLock<SessionState>>,
        speaking_context: &SpeakingContext,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (session_id, participant_id, transport_id) = {
            let state_guard = state.read().await;
            let participant_id = state_guard
                .participant_id()
                .ok_or("No participant_id available")?
                .to_owned();
            let transport_id = state_guard.send_transport_id.as_ref()
                .ok_or("No send transport ID available")?
                .id.clone();
            (state_guard.session_id.clone(), participant_id, transport_id)
        };

        info!("Starting production on transport: {}", transport_id);

        let start_production_request = StartProductionRequestForSpeakingEngine {
            transport_id,
        };
        
        let response = SpeakingEngineOutboundCommands::start_production(
            speaking_context,
            &session_id,
            &participant_id,
            start_production_request,
        ).await?;

        if response.device_producer_id.is_empty() {
            return Err("No device_producer_id returned from start_production".into());
        }

        let mut state_guard = state.write().await;
        state_guard.device_producer_id = Some(response.device_producer_id.clone());
        drop(state_guard);
        info!("Production started successfully with device_producer_id: {}", response.device_producer_id);

        Ok(())
    }
}
