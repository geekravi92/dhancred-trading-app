use std::env;
use std::io::ErrorKind;

use dhancred_trading_app::adapters::run_feed_brokers;
use dhancred_trading_app::admin::start_admin_server;
use dhancred_trading_app::config::AppConfig;
use dhancred_trading_app::feeder::FeedError;
use dhancred_trading_app::master_scheduler::start_master_scheduler;
use dhancred_trading_app::notification::{
    AlertSeverity, init_notification_service, notify_failure,
};
use dhancred_trading_app::strategy::start_strategy_runtime;

fn main() -> Result<(), FeedError> {
    load_dotenv()?;
    init_notification_service();

    let result = run();
    if let Err(error) = &result {
        notify_failure(
            "app.main",
            "APP",
            AlertSeverity::Critical,
            format!("fatal application error: {error}"),
        );
    }

    result
}

fn load_dotenv() -> Result<(), FeedError> {
    match dotenvy::from_path(".env") {
        Ok(_) => Ok(()),
        Err(dotenvy::Error::Io(error)) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(FeedError::Config(format!("failed to load .env: {error}"))),
    }
}

fn run() -> Result<(), FeedError> {
    let config_path =
        env::var("FEEDER_CONFIG_PATH").unwrap_or_else(|_| "config/feeder.toml".to_string());
    let config = AppConfig::load(&config_path)?;

    if !config.feeder.mode.eq_ignore_ascii_case("LIVE") {
        return Err(FeedError::Config(format!(
            "unsupported feeder mode {}",
            config.feeder.mode
        )));
    }

    let strategy_runtime = start_strategy_runtime(&config)?;
    let _admin_server = start_admin_server(&config, strategy_runtime.clone())?;
    let _master_scheduler = start_master_scheduler(&config)?;

    run_feed_brokers(&config, strategy_runtime, configured_max_events(&config)?)
}

fn configured_max_events(config: &AppConfig) -> Result<usize, FeedError> {
    if let Ok(value) = env::var("FEEDER_MAX_EVENTS") {
        return value
            .parse()
            .map_err(|error| FeedError::Config(format!("invalid FEEDER_MAX_EVENTS: {error}")));
    }

    Ok(config
        .runtime
        .as_ref()
        .and_then(|runtime| runtime.max_events)
        .unwrap_or(25))
}
