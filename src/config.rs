use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::Path;

use serde::Deserialize;

use crate::feeder::FeedError;

#[derive(Clone, Debug, Deserialize)]
pub struct AppConfig {
    pub feeder: FeederSection,
    pub brokers: BrokersSection,
    pub admin: Option<AdminSection>,
    pub master_scheduler: Option<MasterSchedulerSection>,
    pub channels: ChannelsSection,
    pub runtime: Option<RuntimeSection>,
}

impl AppConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, FeedError> {
        let content = fs::read_to_string(path)?;
        toml::from_str(&content).map_err(|error| FeedError::Config(error.to_string()))
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct FeederSection {
    pub mode: String,
    pub feed_brokers: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct BrokersSection {
    pub delta: Option<DeltaBrokerSection>,
    pub fyers: Option<FyersBrokerSection>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct DeltaBrokerSection {
    pub enabled: bool,
    pub base_instruments_csv: String,
    pub derivatives_csv: String,
    pub master_csv: String,
    pub latest_prices_file: Option<String>,
    pub latest_prices_underlying: Option<String>,
    pub console_logging: Option<bool>,
    pub instrument_types: Vec<String>,
    pub strike_distance_pct: f64,
    pub refresh_trigger_pct: f64,
    pub rest_url_env: String,
    pub ws_public_url_env: String,
    pub ws_private_url_env: String,
    pub api_key_env: String,
    pub api_secret_env: String,
}

impl DeltaBrokerSection {
    pub fn rest_url(&self) -> Result<String, FeedError> {
        env_from_name(&self.rest_url_env)
    }

    pub fn public_ws_url(&self) -> Result<String, FeedError> {
        env_from_name(&self.ws_public_url_env)
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct FyersBrokerSection {
    pub enabled: bool,
    pub base_instruments_csv: String,
    pub derivatives_csv: String,
    pub master_data_dir: String,
    pub master_urls: BTreeMap<String, String>,
    pub instrument_types: Vec<String>,
    pub strike_distance_pct: f64,
    pub refresh_trigger_pct: f64,
    pub data_ws_url: String,
    pub app_id_env: String,
    pub access_token_file: String,
    pub market_sessions: Option<Vec<FyersMarketSessionSection>>,
}

impl FyersBrokerSection {
    pub fn app_id(&self) -> Result<String, FeedError> {
        env_from_name(&self.app_id_env)
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct FyersMarketSessionSection {
    pub enabled: bool,
    pub name: String,
    pub timezone: String,
    pub open_ist: String,
    pub close_ist: String,
    pub connect_before_open_secs: u64,
    pub weekdays_only: bool,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ChannelsSection {
    pub price_tick: bool,
    pub price_candles: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct AdminSection {
    pub enabled: bool,
    pub bind_addr: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct MasterSchedulerSection {
    pub enabled: bool,
    pub time_ist: String,
    pub weekdays_only: bool,
    pub brokers: Option<Vec<String>>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct RuntimeSection {
    pub max_events: Option<usize>,
}

#[derive(Clone, Debug)]
pub struct InstrumentSelection {
    pub instrument_types: Vec<String>,
    pub strike_distance_pct: f64,
    pub refresh_trigger_pct: f64,
}

impl From<&DeltaBrokerSection> for InstrumentSelection {
    fn from(value: &DeltaBrokerSection) -> Self {
        Self {
            instrument_types: value.instrument_types.clone(),
            strike_distance_pct: value.strike_distance_pct,
            refresh_trigger_pct: value.refresh_trigger_pct,
        }
    }
}

impl From<&FyersBrokerSection> for InstrumentSelection {
    fn from(value: &FyersBrokerSection) -> Self {
        Self {
            instrument_types: value.instrument_types.clone(),
            strike_distance_pct: value.strike_distance_pct,
            refresh_trigger_pct: value.refresh_trigger_pct,
        }
    }
}

fn env_from_name(name: &str) -> Result<String, FeedError> {
    env::var(name).map_err(|_| FeedError::Config(format!("missing environment variable {name}")))
}
