#[derive(Debug, thiserror::Error)]
pub enum EdgeClientError {
    #[error("Transport error: {0}")]
    Transport(#[from] tonic::transport::Error),

    #[error("gRPC error: {0}")]
    Status(#[from] tonic::Status),

    #[error("Invalid endpoint: {0}")]
    InvalidEndpoint(#[from] tonic::codegen::http::uri::InvalidUri),
}
