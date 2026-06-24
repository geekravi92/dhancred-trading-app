use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use serde::Deserialize;

use crate::feeder::FeedError;

#[derive(Clone, Debug, Deserialize)]
pub struct AppConfig {
    pub feeder: FeederSection,
    #[serde(default)]
    pub brokers: BrokersSection,
    pub historical_candles: Option<HistoricalCandlesSection>,
    pub strategy: Option<StrategySection>,
    pub backtest: Option<BacktestSection>,
    pub admin: Option<AdminSection>,
    pub master_scheduler: Option<MasterSchedulerSection>,
    pub login_scheduler: Option<LoginSchedulerSection>,
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
        config.load_external_broker_configs(path)?;
        config.strategy_runtime = load_strategy_runtime_config(path)?;
        Ok(config)
    }

    pub fn feed_broker_enabled(&self, broker: &str) -> bool {
        self.feeder
            .feed_brokers
            .iter()
            .any(|configured| broker_name_matches(configured, broker))
    }

    fn load_external_broker_configs(&mut self, feeder_config_path: &Path) -> Result<(), FeedError> {
        let Some(config_dir) = self.brokers.config_dir.as_deref() else {
            return Ok(());
        };
        let config_dir = resolve_relative_to_config(feeder_config_path, config_dir);

        self.brokers.delta =
            load_broker_file(config_dir.join("delta.toml"), self.brokers.delta.take())?;
        self.brokers.fyers =
            load_broker_file(config_dir.join("fyers.toml"), self.brokers.fyers.take())?;
        self.brokers.dbinternational = load_broker_file(
            config_dir.join("dbinternational.toml"),
            self.brokers.dbinternational.take(),
        )?;

        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct FeederSection {
    pub mode: String,
    pub feed_brokers: Vec<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct BrokersSection {
    pub config_dir: Option<String>,
    pub delta: Option<DeltaBrokerSection>,
    pub fyers: Option<FyersBrokerSection>,
    pub dbinternational: Option<DbinternationalBrokerSection>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct DeltaBrokerSection {
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
    pub name: String,
    pub timezone: String,
    pub open_ist: String,
    pub close_ist: String,
    pub connect_before_open_secs: u64,
    pub weekdays_only: bool,
}

#[derive(Clone, Debug, Deserialize)]
pub struct DbinternationalBrokerSection {
    pub market_data_base_url: String,
    pub market_data_app_key_env: String,
    pub market_data_secret_key_env: String,
    pub market_data_token_file: String,
    pub market_data_session_file: Option<String>,
    pub market_data_master_file: String,
    pub market_data_exchange_segments: Vec<String>,
    #[serde(default = "default_dbinternational_socket_path")]
    pub market_data_socket_path: String,
    #[serde(default = "default_dbinternational_publish_format")]
    pub market_data_publish_format: String,
    #[serde(default = "default_dbinternational_broadcast_mode")]
    pub market_data_broadcast_mode: String,
    #[serde(default)]
    pub market_data_subscribe_symbols: Vec<String>,
    pub latest_prices_file: Option<String>,
    pub interactive_base_url: String,
    pub interactive_app_key_env: String,
    pub interactive_secret_key_env: String,
    pub interactive_unique_key_env: Option<String>,
    pub interactive_unique_key_file: Option<String>,
    pub interactive_access_token_env: Option<String>,
    pub interactive_access_token_file: Option<String>,
    pub interactive_token_file: String,
    pub interactive_session_file: Option<String>,
    pub console_logging: Option<bool>,
}

impl DbinternationalBrokerSection {
    pub fn market_data_login_url(&self) -> String {
        format!(
            "{}/auth/login",
            self.market_data_base_url.trim_end_matches('/')
        )
    }

    pub fn market_data_master_url(&self) -> String {
        format!(
            "{}/instruments/master",
            self.market_data_base_url.trim_end_matches('/')
        )
    }

    pub fn interactive_login_url(&self) -> String {
        format!(
            "{}/user/session",
            self.interactive_base_url.trim_end_matches('/')
        )
    }
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
}

#[derive(Clone, Debug, Deserialize)]
pub struct LoginSchedulerSection {
    pub enabled: bool,
    pub time_ist: String,
    pub weekdays_only: bool,
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

fn broker_name_matches(configured: &str, wanted: &str) -> bool {
    normalize_broker_name(configured) == normalize_broker_name(wanted)
}

fn normalize_broker_name(value: &str) -> String {
    match value.trim().to_ascii_uppercase().as_str() {
        "DB" | "DBINTERNATIONAL" => "DBINTERNATIONAL".to_string(),
        other => other.to_string(),
    }
}

fn load_broker_file<T>(path: PathBuf, fallback: Option<T>) -> Result<Option<T>, FeedError>
where
    T: DeserializeOwned,
{
    match fs::read_to_string(&path) {
        Ok(content) => toml::from_str(&content).map(Some).map_err(|error| {
            FeedError::Config(format!("failed to parse {}: {error}", path.display()))
        }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(fallback),
        Err(error) => Err(error.into()),
    }
}

fn resolve_relative_to_config(config_path: &Path, value: &str) -> PathBuf {
    let path = PathBuf::from(value);
    if path.is_absolute() {
        return path;
    }

    config_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(path)
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

fn default_dbinternational_socket_path() -> String {
    "/apibinarymarketdata/socket.io".to_string()
}

fn default_dbinternational_publish_format() -> String {
    "JSON".to_string()
}

fn default_dbinternational_broadcast_mode() -> String {
    "Full".to_string()
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

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    #[test]
    fn loads_broker_config_from_external_dir() {
        let dir = temp_config_dir("external-broker-config");
        fs::create_dir_all(dir.join("brokers")).expect("create brokers dir");
        fs::write(
            dir.join("feeder.toml"),
            r#"
[feeder]
mode = "live"
feed_brokers = ["DBINTERNATIONAL"]

[brokers]
config_dir = "brokers"

[channels]
price_tick = true
price_candles = []
"#,
        )
        .expect("write feeder config");
        fs::write(
            dir.join("brokers").join("dbinternational.toml"),
            dbinternational_config("DB_MARKET_APP"),
        )
        .expect("write db broker config");

        let config = AppConfig::load(dir.join("feeder.toml")).expect("load config");

        let db = config
            .brokers
            .dbinternational
            .expect("dbinternational config");
        assert_eq!(db.market_data_app_key_env, "DB_MARKET_APP");
        assert_eq!(db.market_data_login_url(), "md/auth/login");
    }

    #[test]
    fn keeps_inline_broker_config_as_fallback() {
        let dir = temp_config_dir("inline-broker-config");
        fs::create_dir_all(&dir).expect("create config dir");
        fs::write(
            dir.join("feeder.toml"),
            format!(
                r#"
[feeder]
mode = "live"
feed_brokers = ["DBINTERNATIONAL"]

[brokers.dbinternational]
{}

[channels]
price_tick = true
price_candles = []
"#,
                dbinternational_config("INLINE_MARKET_APP")
            ),
        )
        .expect("write feeder config");

        let config = AppConfig::load(dir.join("feeder.toml")).expect("load config");

        let db = config
            .brokers
            .dbinternational
            .expect("dbinternational config");
        assert_eq!(db.market_data_app_key_env, "INLINE_MARKET_APP");
    }

    fn temp_config_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        std::env::temp_dir().join(format!("dhancred-{label}-{nanos}"))
    }

    fn dbinternational_config(market_data_app_key_env: &str) -> String {
        format!(
            r#"
market_data_base_url = "md"
market_data_app_key_env = "{market_data_app_key_env}"
market_data_secret_key_env = "DB_MARKET_SECRET"
market_data_token_file = "runtime/secrets/db_market_token"
market_data_session_file = "runtime/secrets/db_market_session.json"
market_data_master_file = "data/instruments/dbinternational/master/instruments.txt"
market_data_exchange_segments = ["NSECM", "NSEFO"]
interactive_base_url = "interactive"
interactive_app_key_env = "DB_INTERACTIVE_APP"
interactive_secret_key_env = "DB_INTERACTIVE_SECRET"
interactive_unique_key_env = "DB_INTERACTIVE_UNIQUE"
interactive_unique_key_file = "runtime/secrets/db_interactive_unique"
interactive_access_token_env = "DB_INTERACTIVE_ACCESS"
interactive_access_token_file = "runtime/secrets/db_interactive_access"
interactive_token_file = "runtime/secrets/db_interactive_token"
interactive_session_file = "runtime/secrets/db_interactive_session.json"
console_logging = false
"#
        )
    }
}
