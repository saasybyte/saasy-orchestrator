#[derive(Debug, thiserror::Error)]
pub enum LlmClientError {
    #[error("Configuration error: {0}")]
    Config(String),
    
    #[error("Authentication error: {0}")]
    Auth(String),

    #[error("Connection failed: {0}")]
    Connection(String),

    #[error("HTTP error: {0}")]
    Http(String),

    #[error("API error: {0}")]
    Api(String),

    #[error("Serialization error: {0}")]
    Serialization(String),
}
