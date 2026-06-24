use thiserror::Error;

#[derive(Error, Debug)]
pub enum SocketError {
    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Transport error: {0}")]
    Transport(String),

    #[error("Parser error: {0}")]
    Parser(String),

    #[error("URL error: {0}")]
    Url(String),

    #[error("Encoding error: {0}")]
    Encoding(String),

    #[error("Decoding error: {0}")]
    Decoding(String),

    #[error("Timeout error")]
    Timeout,

    #[error("Invalid packet type: {0}")]
    InvalidPacketType(u8),

    #[error("Invalid namespace: {0}")]
    InvalidNamespace(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("WebSocket error: {0}")]
    WebSocket(String),

    #[error("URL parse error: {0}")]
    UrlParse(#[from] url::ParseError),
}

pub type Result<T> = std::result::Result<T, SocketError>;
