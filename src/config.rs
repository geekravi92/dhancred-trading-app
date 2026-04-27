use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::feeder::FeedError;

#[derive(Clone, Debug, Deserialize)]
pub struct AppConfig {
    pub feeder: FeederSection,
    pub brokers: BrokersSection,
    pub historical_candles: Option<HistoricalCandlesSection>,
    pub strategy: Option<StrategySection>,
    pub backtest: Option<BacktestSection>,
    pub admin: Option<AdminSection>,
    pub master_scheduler: Option<MasterSchedulerSection>,
    pub channels: ChannelsSection,
    pub runtime: Option<RuntimeSection>,
    #[serde(skip)]
    pub strategy_runtime: StrategyRuntimeConfig,
}

impl AppConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, FeedError> {
        let path = path.as_ref();
        let content = fs::read_to_string(path)?;
        let mut config: Self =
            toml::from_str(&content).map_err(|error| FeedError::Config(error.to_string()))?;
        config.strategy_runtime = load_strategy_runtime_config(path)?;
        Ok(config)
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
    pub ticker_channel: Option<String>,
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
    pub access_token_file: String,
    pub app_id_env: Option<String>,
    pub latest_prices_file: Option<String>,
    pub console_logging: Option<bool>,
    pub market_sessions: Option<Vec<FyersMarketSessionSection>>,
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
pub struct HistoricalCandlesSection {
    pub enabled: bool,
    pub sqlite_path: String,
    #[serde(default = "default_historical_one_minute_days")]
    pub one_minute_days: u32,
    #[serde(default = "default_historical_one_day_days")]
    pub one_day_days: u32,
    #[serde(default = "default_historical_maintenance_time_ist")]
    pub maintenance_time_ist: String,
    #[serde(default = "default_historical_reconcile_one_minute_days")]
    pub reconcile_one_minute_days: u32,
    #[serde(default = "default_historical_reconcile_one_day_days")]
    pub reconcile_one_day_days: u32,
}

#[derive(Clone, Debug, Deserialize)]
pub struct StrategySection {
    pub enabled: bool,
    pub sqlite_path: String,
    #[serde(default = "default_strategy_warmup_bars")]
    pub warmup_bars: usize,
    #[serde(default = "default_strategy_recent_bars")]
    pub recent_bars: usize,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct BacktestSection {
    pub output_dir: Option<String>,
    pub historical_candles_sqlite_path: Option<String>,
    pub execution: Option<BacktestExecutionSection>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct BacktestExecutionSection {
    #[serde(default)]
    pub slippage_pct: f64,
    #[serde(default)]
    pub brokerage_pct: f64,
    #[serde(default)]
    pub entry_fee_pct: Option<f64>,
    #[serde(default)]
    pub exit_fee_pct: Option<f64>,
    #[serde(default)]
    pub fee_tax_pct: f64,
    #[serde(default)]
    pub fixed_fee_per_order: f64,
    #[serde(default)]
    pub funding_rate_pct: f64,
    #[serde(default = "default_backtest_funding_interval_hours")]
    pub funding_interval_hours: u64,
    #[serde(default)]
    pub funding_charge_mode: String,
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

#[derive(Clone, Debug, Default, Deserialize)]
pub struct StrategyRuntimeConfig {
    #[serde(default)]
    pub diagnostics: StrategyDiagnosticsSection,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct StrategyDiagnosticsSection {
    #[serde(default)]
    pub closed_candle_decisions: bool,
    #[serde(default)]
    pub warmup_replay: bool,
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

fn load_strategy_runtime_config(
    feeder_config_path: &Path,
) -> Result<StrategyRuntimeConfig, FeedError> {
    let path = env::var("STRATEGY_CONFIG_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            feeder_config_path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join("strategy.toml")
        });
    let mut config = match fs::read_to_string(&path) {
        Ok(content) => toml::from_str::<StrategyRuntimeConfig>(&content).map_err(|error| {
            FeedError::Config(format!("failed to parse {}: {error}", path.display()))
        })?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            StrategyRuntimeConfig::default()
        }
        Err(error) => return Err(error.into()),
    };
    apply_strategy_env_overrides(&mut config)?;
    Ok(config)
}

fn apply_strategy_env_overrides(config: &mut StrategyRuntimeConfig) -> Result<(), FeedError> {
    if let Some(value) = optional_bool_env("STRATEGY_DIAGNOSTICS_CLOSED_CANDLE_DECISIONS")? {
        config.diagnostics.closed_candle_decisions = value;
    }
    if let Some(value) = optional_bool_env("STRATEGY_DIAGNOSTICS_WARMUP_REPLAY")? {
        config.diagnostics.warmup_replay = value;
    }
    Ok(())
}

fn optional_bool_env(name: &str) -> Result<Option<bool>, FeedError> {
    let Ok(value) = env::var(name) else {
        return Ok(None);
    };
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(Some(true)),
        "0" | "false" | "no" | "off" => Ok(Some(false)),
        other => Err(FeedError::Config(format!(
            "invalid boolean env {name}={other}; expected true/false"
        ))),
    }
}

fn default_historical_one_minute_days() -> u32 {
    90
}

fn default_historical_one_day_days() -> u32 {
    300
}

fn default_historical_maintenance_time_ist() -> String {
    "00:10".to_string()
}

fn default_historical_reconcile_one_minute_days() -> u32 {
    2
}

fn default_historical_reconcile_one_day_days() -> u32 {
    5
}

fn default_strategy_warmup_bars() -> usize {
    512
}

fn default_strategy_recent_bars() -> usize {
    512
}

fn default_backtest_funding_interval_hours() -> u64 {
    8
}
