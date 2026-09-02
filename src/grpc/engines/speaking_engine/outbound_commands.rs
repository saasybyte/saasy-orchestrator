use saasy_proto_rust::speaking_engine::{
    speaking_engine_control_message,
    speaking_engine_media_payload,
    CloseSessionRequest,
    CloseSessionResponse,
    CreateTransportRequest,
    CreateTransportResponse,
    DirectionEnum,
    FlushAudioRequest,
    FlushAudioResponse,
    GetDeviceRtpCapabilitiesRequest,
    GetDeviceRtpCapabilitiesResponse,
    LoadDeviceRequest,
    LoadDeviceResponse,
    MediaFrame,
    SpeakingEngineControlMessage,
    SpeakingEngineMediaPayload,
    SpeechGenerationCompleteRequest,
    StartProductionRequest,
    StartProductionResponse,
};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::grpc::engines::error::EngineClientError;
use super::client::SpeakingContext;

pub struct SpeakingEngineOutboundCommands;

impl SpeakingEngineOutboundCommands {
    async fn send_control_request(
        context: &SpeakingContext,
        session_id: &str,
        participant_id: &str,
        request_type: &str,
        data: speaking_engine_control_message::Data,
    ) -> Result<SpeakingEngineControlMessage, EngineClientError> {
        let request_id = Uuid::new_v4().to_string();
        
        let request = SpeakingEngineControlMessage {
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
                if let Some(speaking_engine_control_message::Data::ErrorResponse(error)) = response.data {
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

    pub async fn send_control_response(
        outbound_control_tx: &mpsc::Sender<SpeakingEngineControlMessage>,
        request_id: &str,
        session_id: &str,
        participant_id: &str,
        response_type: &str,
        data: speaking_engine_control_message::Data,
    ) -> Result<(), EngineClientError> {
        let response = SpeakingEngineControlMessage {
            direction: DirectionEnum::Response as i32,
            r#type: response_type.to_string(),
            request_id: request_id.to_string(),
            session_id: session_id.to_string(),
            participant_id: participant_id.to_string(),
            data: Some(data),
        };

        outbound_control_tx.send(response).await
            .map_err(|_| EngineClientError::EngineError("Control stream closed".to_string()))
    }

    pub async fn load_device(
        context: &SpeakingContext,
        session_id: &str,
        participant_id: &str,
        request: LoadDeviceRequest,
    ) -> Result<LoadDeviceResponse, EngineClientError> {
        let response = Self::send_control_request(
            context,
            session_id,
            participant_id,
            "load_device",
            speaking_engine_control_message::Data::LoadDeviceRequest(request),
        ).await?;

        match response.data {
            Some(speaking_engine_control_message::Data::LoadDeviceResponse(data)) => Ok(data),
            _ => Err(EngineClientError::UnexpectedResponse("Invalid response type".to_string())),
        }
    }

    pub async fn get_device_rtp_capabilities(
        context: &SpeakingContext,
        session_id: &str,
        participant_id: &str,
        request: GetDeviceRtpCapabilitiesRequest,
    ) -> Result<GetDeviceRtpCapabilitiesResponse, EngineClientError> {
        let response = Self::send_control_request(
            context,
            session_id,
            participant_id,
            "get_device_rtp_capabilities",
            speaking_engine_control_message::Data::GetDeviceRtpCapabilitiesRequest(request),
        ).await?;

        match response.data {
            Some(speaking_engine_control_message::Data::GetDeviceRtpCapabilitiesResponse(data)) => Ok(data),
            _ => Err(EngineClientError::UnexpectedResponse("Invalid response type".to_string())),
        }
    }

    pub async fn create_transport(
        context: &SpeakingContext,
        session_id: &str,
        participant_id: &str,
        request: CreateTransportRequest,
    ) -> Result<CreateTransportResponse, EngineClientError> {
        let response = Self::send_control_request(
            context,
            session_id,
            participant_id,
            "create_transport",
            speaking_engine_control_message::Data::CreateTransportRequest(request),
        ).await?;

        match response.data {
            Some(speaking_engine_control_message::Data::CreateTransportResponse(data)) => Ok(data),
            _ => Err(EngineClientError::UnexpectedResponse("Invalid response type".to_string())),
        }
    }

    pub async fn start_production(
        context: &SpeakingContext,
        session_id: &str,
        participant_id: &str,
        request: StartProductionRequest,
    ) -> Result<StartProductionResponse, EngineClientError> {
        let response = Self::send_control_request(
            context,
            session_id,
            participant_id,
            "start_production",
            speaking_engine_control_message::Data::StartProductionRequest(request),
        ).await?;

        match response.data {
            Some(speaking_engine_control_message::Data::StartProductionResponse(data)) => Ok(data),
            _ => Err(EngineClientError::UnexpectedResponse("Invalid response type".to_string())),
        }
    }

    pub async fn flush_audio(
        context: &SpeakingContext,
        session_id: &str,
        participant_id: &str,
        request: FlushAudioRequest,
    ) -> Result<FlushAudioResponse, EngineClientError> {
        let response = Self::send_control_request(
            context,
            session_id,
            participant_id,
            "flush_audio",
            speaking_engine_control_message::Data::FlushAudioRequest(request),
        ).await?;

        match response.data {
            Some(speaking_engine_control_message::Data::FlushAudioResponse(data)) => Ok(data),
            _ => Err(EngineClientError::UnexpectedResponse("Invalid response type".to_string())),
        }
    }

    pub async fn speech_generation_complete(
        context: &SpeakingContext,
        session_id: &str,
        participant_id: &str,
        request: SpeechGenerationCompleteRequest,
    ) -> Result<(), EngineClientError> {
        let response = Self::send_control_request(
            context,
            session_id,
            participant_id,
            "speech_generation_complete",
            speaking_engine_control_message::Data::SpeechGenerationCompleteRequest(request),
        ).await?;

        match response.data {
            Some(speaking_engine_control_message::Data::SpeechGenerationCompleteResponse(_)) => Ok(()),
            _ => Err(EngineClientError::UnexpectedResponse("Invalid response type".to_string())),
        }
    }

    pub async fn close_session(
        context: &SpeakingContext,
        session_id: &str,
        participant_id: &str,
        request: CloseSessionRequest,
    ) -> Result<CloseSessionResponse, EngineClientError> {
        let response = Self::send_control_request(
            context,
            session_id,
            participant_id,
            "close_session",
            speaking_engine_control_message::Data::CloseSessionRequest(request),
        ).await?;

        match response.data {
            Some(speaking_engine_control_message::Data::CloseSessionResponse(data)) => Ok(data),
            _ => Err(EngineClientError::UnexpectedResponse("Invalid response type".to_string())),
        }
    }

    pub async fn send_media_frame(
        outbound_media_tx: &mpsc::Sender<SpeakingEngineMediaPayload>,
        session_id: &str,
        participant_id: &str,
        device_producer_id: String,
        kind: i32,
        frame_data: Vec<u8>,
    ) -> Result<(), EngineClientError> {
        let request = SpeakingEngineMediaPayload {
            r#type: "media_frame".to_string(),
            request_id: Uuid::new_v4().to_string(),
            session_id: session_id.to_string(),
            participant_id: participant_id.to_string(),
            data: Some(speaking_engine_media_payload::Data::MediaFrame(
                MediaFrame {
                    device_producer_id,
                    kind,
                    frame_data,
                }
            )),
        };

        outbound_media_tx.send(request).await
            .map_err(|_| EngineClientError::EngineError("Media stream closed".to_string()))?;

        Ok(())
    }
}
