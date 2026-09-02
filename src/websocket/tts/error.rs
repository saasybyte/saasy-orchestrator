#[derive(Debug, thiserror::Error)]
pub enum TtsClientError {
    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Connection failed: {0}")]
    Connection(String),
}
