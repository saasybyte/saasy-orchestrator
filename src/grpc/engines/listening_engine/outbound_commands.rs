use saasy_proto_rust::listening_engine::{
    listening_engine_control_message,
    CloseSessionRequest,
    CloseSessionResponse,
    CreateConsumerRequest,
    CreateConsumerResponse,
    CreateTransportRequest,
    CreateTransportResponse,
    DirectionEnum,
    ListeningEngineControlMessage,
    LoadDeviceRequest,
    LoadDeviceResponse,
    ResumeConsumerRequest,
    ResumeConsumerResponse,
};
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::grpc::engines::error::EngineClientError;
use super::ListeningContext;

pub struct ListeningEngineOutboundCommands;

impl ListeningEngineOutboundCommands {
    async fn send_control_request(
        context: &ListeningContext,
        session_id: &str,
        participant_id: &str,
        request_type: &str,
        data: listening_engine_control_message::Data,
    ) -> Result<ListeningEngineControlMessage, EngineClientError> {
        let request_id = Uuid::new_v4().to_string();
        
        let request = ListeningEngineControlMessage {
            direction: DirectionEnum::Request as i32,
            r#type: request_type.to_string(),
            request_id: request_id.clone(),
            session_id: session_id.to_string(),
            participant_id: participant_id.to_string(),
            data: Some(data),
        };

        let (response_tx, response_rx) = oneshot::channel();
        {
            let mut pending_requests_guard = context.pending_requests.write().await;
            pending_requests_guard.insert(request_id, response_tx);
        }

        context.outbound_control_tx.send(request).await
            .map_err(|_| EngineClientError::EngineError("Control stream closed".to_string()))?;

        // Wait for response
        match response_rx.await {
            Ok(Ok(response)) => {
                if let Some(listening_engine_control_message::Data::ErrorResponse(error)) = response.data {
                    Err(EngineClientError::EngineError(format!(
                        "Engine error: {} - {}", error.code, error.message
                    )))
                } else {
                    Ok(response)
                }
            }
            Ok(Err(e)) => Err(EngineClientError::EngineError(e)),
            Err(_) => Err(EngineClientError::EngineError("Response channel closed".to_string())),
        }
    }

    pub async fn load_device(
        context: &ListeningContext,
        session_id: &str,
        participant_id: &str,
        request: LoadDeviceRequest,
    ) -> Result<LoadDeviceResponse, EngineClientError> {
        let response = Self::send_control_request(
            context,
            session_id,
            participant_id,
            "load_device",
            listening_engine_control_message::Data::LoadDeviceRequest(request),
        ).await?;

        match response.data {
            Some(listening_engine_control_message::Data::LoadDeviceResponse(data)) => Ok(data),
            _ => Err(EngineClientError::UnexpectedResponse("Invalid response type".to_string())),
        }
    }

    pub async fn create_transport(
        context: &ListeningContext,
        session_id: &str,
        participant_id: &str,
        request: CreateTransportRequest,
    ) -> Result<CreateTransportResponse, EngineClientError> {
        let response = Self::send_control_request(
            context,
            session_id,
            participant_id,
            "create_transport",
            listening_engine_control_message::Data::CreateTransportRequest(request),
        ).await?;

        match response.data {
            Some(listening_engine_control_message::Data::CreateTransportResponse(data)) => Ok(data),
            _ => Err(EngineClientError::UnexpectedResponse("Invalid response type".to_string())),
        }
    }

    pub async fn create_consumer(
        context: &ListeningContext,
        session_id: &str,
        participant_id: &str,
        request: CreateConsumerRequest,
    ) -> Result<CreateConsumerResponse, EngineClientError> {
        let response = Self::send_control_request(
            context,
            session_id,
            participant_id,
            "create_consumer",
            listening_engine_control_message::Data::CreateConsumerRequest(request),
        ).await?;

        match response.data {
            Some(listening_engine_control_message::Data::CreateConsumerResponse(data)) => Ok(data),
            _ => Err(EngineClientError::UnexpectedResponse("Invalid response type".to_string())),
        }
    }

    pub async fn resume_consumer(
        context: &ListeningContext,
        session_id: &str,
        participant_id: &str,
        request: ResumeConsumerRequest,
    ) -> Result<ResumeConsumerResponse, EngineClientError> {
        let response = Self::send_control_request(
            context,
            session_id,
            participant_id,
            "resume_consumer",
            listening_engine_control_message::Data::ResumeConsumerRequest(request),
        ).await?;

        match response.data {
            Some(listening_engine_control_message::Data::ResumeConsumerResponse(data)) => Ok(data),
            _ => Err(EngineClientError::UnexpectedResponse("Invalid response type".to_string())),
        }
    }

    pub async fn close_session(
        context: &ListeningContext,
        session_id: &str,
        participant_id: &str,
        request: CloseSessionRequest,
    ) -> Result<CloseSessionResponse, EngineClientError> {
        let response = Self::send_control_request(
            context,
            session_id,
            participant_id,
            "close_session",
            listening_engine_control_message::Data::CloseSessionRequest(request),
        ).await?;

        match response.data {
            Some(listening_engine_control_message::Data::CloseSessionResponse(data)) => Ok(data),
            _ => Err(EngineClientError::UnexpectedResponse("Invalid response type".to_string())),
        }
    }
}
