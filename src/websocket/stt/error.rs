#[derive(Debug, thiserror::Error)]
pub enum SttClientError {
    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Connection failed: {0}")]
    Connection(String),
}
