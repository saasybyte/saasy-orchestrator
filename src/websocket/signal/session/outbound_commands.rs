use saasy_proto_rust::shared::{
    CloseSessionRequest,
    ConnectTransportRequest,
    ConsumerId,
    CreateConsumerRequest,
    CreateConsumerResponse,
    CreateProducerRequest,
    CreateProducerResponse,
    CreateTransportRequest,
    CreateTransportResponse,
    DtlsParameters,
    GetRouterRtpCapabilitiesRequest,
    GetRouterRtpCapabilitiesResponse,
    JoinSessionRequest, 
    MediaKind,
    ParticipantId,
    ParticipantType,
    ProducerId,
    ResumeConsumerRequest,
    RtpCapabilities,
    RtpParameters,
    SessionId,
    SetRtpCapabilitiesRequest,
    SubscribeToEventsRequest,
    TransportDirection,
    TransportId,
};
use saasy_proto_rust::signal::{
    signal_request_envelope,
    signal_response_envelope,
    SignalRequestEnvelope,
    SignalResponseEnvelope,
};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

pub struct SignalSessionPendingRequest {
    pub request: SignalRequestEnvelope,
    pub response_tx: oneshot::Sender<Result<SignalResponseEnvelope, String>>,
}

pub struct SignalOutboundCommands;

impl SignalOutboundCommands {
    async fn send_control_request(
        signal_control_tx: &mpsc::Sender<SignalSessionPendingRequest>,
        envelope_type: &str,
        request_envelope: SignalRequestEnvelope,
    ) -> Result<SignalResponseEnvelope, Box<dyn std::error::Error + Send + Sync>> {
        let (response_tx, response_rx) = oneshot::channel();
        
        signal_control_tx
            .send(SignalSessionPendingRequest { request: request_envelope, response_tx })
            .await
            .map_err(|_| "Failed to send request")?;
        
        let response = response_rx
            .await
            .map_err(|_| "Failed to receive response")??;

        if response.r#type == envelope_type {
            Ok(response)
        } else if response.r#type == "error" {
            if let Some(signal_response_envelope::Data::ErrorResponse(ref err)) = response.data {
                Err(format!("Signal error: {} - {}", err.code, err.message).into())
            } else {
                Err("Unknown error response".into())
            }
        } else {
            Err(format!("Expected {} but got {}", envelope_type, response.r#type).into())
        }
    }

    pub async fn join_session(
        signal_control_tx: &mpsc::Sender<SignalSessionPendingRequest>,
        session_id: &str,
        participant_id: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let envelope = SignalRequestEnvelope {
            r#type: "join_session".to_string(),
            request_id: Uuid::new_v4().to_string(),
            session_id: session_id.to_string(),
            participant_id: participant_id.to_string(),
            data: Some(signal_request_envelope::Data::JoinSessionRequest(
                JoinSessionRequest {
                    session_id: Some(SessionId { id: session_id.to_string() }),
                    participant_id: Some(ParticipantId { id: participant_id.to_string() }),
                    participant_type: ParticipantType::Llm as i32,
                }
            )),
        };

        let _ = Self::send_control_request(
            signal_control_tx,
            "join_session",
            envelope,
        ).await?;

        Ok(())
    }

    pub async fn get_router_rtp_capabilities(
        signal_control_tx: &mpsc::Sender<SignalSessionPendingRequest>,
        session_id: &str,
        participant_id: &str,
    ) -> Result<GetRouterRtpCapabilitiesResponse, Box<dyn std::error::Error + Send + Sync>> {
        let envelope = SignalRequestEnvelope {
            r#type: "get_router_rtp_capabilities".to_string(),
            request_id: Uuid::new_v4().to_string(),
            session_id: session_id.to_string(),
            participant_id: participant_id.to_string(),
            data: Some(signal_request_envelope::Data::GetRouterRtpCapabilitiesRequest(
                GetRouterRtpCapabilitiesRequest {
                    session_id: Some(SessionId { id: session_id.to_string() }),
                }
            )),
        };

        let response = Self::send_control_request(
            signal_control_tx,
            "get_router_rtp_capabilities",
            envelope,
        ).await?;

        match response.data {
            Some(signal_response_envelope::Data::GetRouterRtpCapabilitiesResponse(resp)) => {
                Ok(resp)
            }
            _ => Err("Unexpected response type".into())
        }
    }

    pub async fn set_rtp_capabilities(
        signal_control_tx: &mpsc::Sender<SignalSessionPendingRequest>,
        session_id: &str,
        participant_id: &str,
        device_rtp_capabilities: RtpCapabilities,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let envelope = SignalRequestEnvelope {
            r#type: "set_rtp_capabilities".to_string(),
            request_id: Uuid::new_v4().to_string(),
            session_id: session_id.to_string(),
            participant_id: participant_id.to_string(),
            data: Some(signal_request_envelope::Data::SetRtpCapabilitiesRequest(
                SetRtpCapabilitiesRequest {
                    session_id: Some(SessionId { id: session_id.to_string() }),
                    rtp_capabilities: Some(device_rtp_capabilities),
                }
            )),
        };

        let _ = Self::send_control_request(
            signal_control_tx,
            "set_rtp_capabilities",
            envelope,
        ).await?;

        Ok(())
    }

    pub async fn create_transport(
        signal_control_tx: &mpsc::Sender<SignalSessionPendingRequest>,
        session_id: &str,
        participant_id: &str,
        direction: TransportDirection,
    ) -> Result<CreateTransportResponse, Box<dyn std::error::Error + Send + Sync>> {
        let envelope = SignalRequestEnvelope {
            r#type: "create_transport".to_string(),
            request_id: Uuid::new_v4().to_string(),
            session_id: session_id.to_string(),
            participant_id: participant_id.to_string(),
            data: Some(signal_request_envelope::Data::CreateTransportRequest(
                CreateTransportRequest {
                    session_id: Some(SessionId { id: session_id.to_string() }),
                    direction: direction as i32,
                }
            )),
        };

        let response = Self::send_control_request(
            signal_control_tx,
            "create_transport",
            envelope,
        ).await?;

        match response.data {
            Some(signal_response_envelope::Data::CreateTransportResponse(resp)) => {
                Ok(resp)
            }
            _ => Err("Unexpected response type".into())
        }
    }

    pub async fn connect_transport(
        signal_control_tx: &mpsc::Sender<SignalSessionPendingRequest>,
        session_id: &str,
        participant_id: &str,
        transport_id: TransportId,
        dtls_parameters: DtlsParameters
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let envelope = SignalRequestEnvelope {
            r#type: "connect_transport".to_string(),
            request_id: Uuid::new_v4().to_string(),
            session_id: session_id.to_string(),
            participant_id: participant_id.to_string(),
            data: Some(signal_request_envelope::Data::ConnectTransportRequest(
                ConnectTransportRequest {
                    session_id: Some(SessionId { id: session_id.to_string() }),
                    transport_id: Some(transport_id),
                    dtls_parameters: Some(dtls_parameters),
                }
            )),
        };

        let _ = Self::send_control_request(
            signal_control_tx,
            "connect_transport",
            envelope,
        ).await?;

        Ok(())
    }

    pub async fn create_producer(
        signal_control_tx: &mpsc::Sender<SignalSessionPendingRequest>,
        session_id: &str,
        participant_id: &str,
        transport_id: TransportId,
        kind: MediaKind,
        rtp_parameters: RtpParameters,
    ) -> Result<CreateProducerResponse, Box<dyn std::error::Error + Send + Sync>> {
        let envelope = SignalRequestEnvelope {
            r#type: "create_producer".to_string(),
            request_id: Uuid::new_v4().to_string(),
            session_id: session_id.to_string(),
            participant_id: participant_id.to_string(),
            data: Some(signal_request_envelope::Data::CreateProducerRequest(
                CreateProducerRequest {
                    session_id: Some(SessionId { id: session_id.to_string() }),
                    transport_id: Some(transport_id),
                    kind: kind as i32,
                    rtp_parameters: Some(rtp_parameters),
                }
            )),
        };

        let response = Self::send_control_request(
            signal_control_tx,
            "create_producer",
            envelope,
        ).await?;

        match response.data {
            Some(signal_response_envelope::Data::CreateProducerResponse(resp)) => {
                Ok(resp)
            }
            _ => Err("Unexpected response type".into())
        }
    }

    pub async fn create_consumer(
        signal_control_tx: &mpsc::Sender<SignalSessionPendingRequest>,
        session_id: &str,
        participant_id: &str,
        transport_id: TransportId,
        producer_id: ProducerId,
        rtp_capabilities: RtpCapabilities,
    ) -> Result<CreateConsumerResponse, Box<dyn std::error::Error + Send + Sync>> {
        let envelope = SignalRequestEnvelope {
            r#type: "create_consumer".to_string(),
            request_id: Uuid::new_v4().to_string(),
            session_id: session_id.to_string(),
            participant_id: participant_id.to_string(),
            data: Some(signal_request_envelope::Data::CreateConsumerRequest(
                CreateConsumerRequest {
                    session_id: Some(SessionId { id: session_id.to_string() }),
                    transport_id: Some(transport_id),
                    producer_id: Some(producer_id),
                    rtp_capabilities: Some(rtp_capabilities),
                }
            )),
        };

        let response = Self::send_control_request(
            signal_control_tx,
            "create_consumer",
            envelope,
        ).await?;

        match response.data {
            Some(signal_response_envelope::Data::CreateConsumerResponse(resp)) => {
                Ok(resp)
            }
            _ => Err("Unexpected response type".into())
        }
    }

    pub async fn resume_consumer(
        signal_control_tx: &mpsc::Sender<SignalSessionPendingRequest>,
        session_id: &str,
        participant_id: &str,
        consumer_id: ConsumerId,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let envelope = SignalRequestEnvelope {
            r#type: "resume_consumer".to_string(),
            request_id: Uuid::new_v4().to_string(),
            session_id: session_id.to_string(),
            participant_id: participant_id.to_string(),
            data: Some(signal_request_envelope::Data::ResumeConsumerRequest(
                ResumeConsumerRequest {
                    session_id: Some(SessionId { id: session_id.to_string() }),
                    consumer_id: Some(consumer_id),
                }
            )),
        };

        let _ = Self::send_control_request(
            signal_control_tx,
            "resume_consumer",
            envelope,
        ).await?;

        Ok(())
    }

    pub async fn subscribe_to_events(
        signal_control_tx: &mpsc::Sender<SignalSessionPendingRequest>,
        session_id: &str,
        participant_id: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let envelope = SignalRequestEnvelope {
            r#type: "subscribe_to_events".to_string(),
            request_id: Uuid::new_v4().to_string(),
            session_id: session_id.to_string(),
            participant_id: participant_id.to_string(),
            data: Some(signal_request_envelope::Data::SubscribeToEventsRequest(
                SubscribeToEventsRequest {
                    session_id: Some(SessionId { id: session_id.to_string() }),
                }
            )),
        };

        let _ = Self::send_control_request(
            signal_control_tx,
            "subscribe_to_events",
            envelope,
        ).await?;

        Ok(())
    }

    pub async fn close_session(
        signal_control_tx: &mpsc::Sender<SignalSessionPendingRequest>,
        session_id: &str,
        participant_id: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let envelope = SignalRequestEnvelope {
            r#type: "close_session".to_string(),
            request_id: Uuid::new_v4().to_string(),
            session_id: session_id.to_string(),
            participant_id: participant_id.to_string(),
            data: Some(signal_request_envelope::Data::CloseSessionRequest(
                CloseSessionRequest {
                    session_id: Some(SessionId { id: session_id.to_string() }),
                }
            )),
        };

        let _ = Self::send_control_request(
            signal_control_tx,
            "close_session",
            envelope,
        ).await?;

        Ok(())
    }
}