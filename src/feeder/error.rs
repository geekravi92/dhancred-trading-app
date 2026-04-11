use std::error::Error;
use std::fmt;

#[derive(Debug, Eq, PartialEq)]
pub enum FeedError {
    NotSubscribed,
    UnsupportedChannel { broker: String, channel: String },
    InvalidInstrument(String),
    Config(String),
    Http(String),
    Io(String),
    Parse(String),
    Disconnected(String),
}

impl fmt::Display for FeedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotSubscribed => f.write_str("feeder is not subscribed"),
            Self::UnsupportedChannel { broker, channel } => {
                write!(f, "{broker} does not support feeder channel {channel}")
            }
            Self::InvalidInstrument(value) => write!(f, "invalid instrument: {value}"),
            Self::Config(value) => write!(f, "config error: {value}"),
            Self::Http(value) => write!(f, "http error: {value}"),
            Self::Io(value) => write!(f, "io error: {value}"),
            Self::Parse(value) => write!(f, "parse error: {value}"),
            Self::Disconnected(value) => write!(f, "feed disconnected: {value}"),
        }
    }
}

impl Error for FeedError {}

impl From<std::io::Error> for FeedError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value.to_string())
    }
}

impl From<reqwest::Error> for FeedError {
    fn from(value: reqwest::Error) -> Self {
        Self::Http(value.to_string())
    }
}

impl From<serde_json::Error> for FeedError {
    fn from(value: serde_json::Error) -> Self {
        Self::Parse(value.to_string())
    }
}

impl From<tungstenite::Error> for FeedError {
    fn from(value: tungstenite::Error) -> Self {
        Self::Disconnected(value.to_string())
    }
}
