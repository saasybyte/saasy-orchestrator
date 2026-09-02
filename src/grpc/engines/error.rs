#[derive(Debug, thiserror::Error)]
pub enum EngineClientError {
    #[error("Transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
    
    #[error("gRPC error: {0}")]
    Status(#[from] tonic::Status),

    #[error("Invalid gRPC URI: {0}")]
    Uri(#[from] tonic::codegen::http::uri::InvalidUri),

    #[error("Engine service error: {0}")]
    EngineError(String),

    #[error("Unexpected response: {0}")]
    UnexpectedResponse(String),

    #[error("Invalid metadata: {0}")]
    InvalidMetadata(String),

    #[error("Invalid request: {0}")]
    InvalidRequest(String),

    #[error("Signal error: {0}")]
    Signal(String),

    #[error("Internal error: {0}")]
    Internal(String),
}
