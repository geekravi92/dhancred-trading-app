use std::error::Error;
use std::fmt;

#[derive(Debug, Eq, PartialEq)]
pub enum StrategyError {
    Config(String),
    Io(String),
    Parse(String),
    Rule(String),
    NotFound(String),
    Unsupported(String),
}

impl fmt::Display for StrategyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(value) => write!(f, "strategy config error: {value}"),
            Self::Io(value) => write!(f, "strategy io error: {value}"),
            Self::Parse(value) => write!(f, "strategy parse error: {value}"),
            Self::Rule(value) => write!(f, "strategy rule rejected: {value}"),
            Self::NotFound(value) => write!(f, "strategy not found: {value}"),
            Self::Unsupported(value) => write!(f, "strategy unsupported: {value}"),
        }
    }
}

impl Error for StrategyError {}

impl From<std::io::Error> for StrategyError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value.to_string())
    }
}

impl From<rusqlite::Error> for StrategyError {
    fn from(value: rusqlite::Error) -> Self {
        Self::Io(value.to_string())
    }
}

impl From<serde_json::Error> for StrategyError {
    fn from(value: serde_json::Error) -> Self {
        Self::Parse(value.to_string())
    }
}

impl From<StrategyError> for crate::feeder::FeedError {
    fn from(value: StrategyError) -> Self {
        match value {
            StrategyError::Config(message)
            | StrategyError::Rule(message)
            | StrategyError::Unsupported(message) => Self::Config(message),
            StrategyError::Io(message) => Self::Io(message),
            StrategyError::Parse(message) => Self::Parse(message),
            StrategyError::NotFound(message) => Self::InvalidInstrument(message),
        }
    }
}
