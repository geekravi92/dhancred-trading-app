use std::env;
use std::fs;
use std::io::ErrorKind;

use dhancred_trading_app::adapters::delta::historical::DeltaHistoricalClient;
use dhancred_trading_app::adapters::historical::recover_spot_history;
use dhancred_trading_app::adapters::run_feed_brokers;
use dhancred_trading_app::admin::start_admin_server;
use dhancred_trading_app::config::AppConfig;
use dhancred_trading_app::feeder::{FeedError, InstrumentCatalog};
use dhancred_trading_app::login_scheduler::start_login_scheduler;
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
    let content = match fs::read_to_string(".env") {
        Ok(content) => content,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(FeedError::Config(format!("failed to read .env: {error}"))),
    };

    for (key, value) in parse_dotenv_literal(&content)? {
        // Loaded before worker threads are spawned; this intentionally matches
        // Node dotenv behavior and keeps `$` inside broker secrets literal.
        unsafe {
            env::set_var(key, value);
        }
    }

    Ok(())
}

fn parse_dotenv_literal(content: &str) -> Result<Vec<(String, String)>, FeedError> {
    let mut values = Vec::new();

    for (index, line) in content.lines().enumerate() {
        let line = line.trim_start();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let line = line.strip_prefix("export ").unwrap_or(line);
        let Some((key, raw_value)) = line.split_once('=') else {
            return Err(FeedError::Config(format!(
                "invalid .env line {}: missing '='",
                index + 1
            )));
        };
        let key = key.trim();
        if key.is_empty() {
            return Err(FeedError::Config(format!(
                "invalid .env line {}: empty key",
                index + 1
            )));
        }

        values.push((key.to_string(), parse_dotenv_value(raw_value.trim_start())));
    }

    Ok(values)
}

fn parse_dotenv_value(raw_value: &str) -> String {
    let Some(quote) = raw_value
        .chars()
        .next()
        .filter(|quote| *quote == '\'' || *quote == '"')
    else {
        return strip_unquoted_comment(raw_value).trim_end().to_string();
    };

    let mut value = String::new();
    let mut escaped = false;
    for ch in raw_value[quote.len_utf8()..].chars() {
        if quote == '"' && escaped {
            value.push(match ch {
                'n' => '\n',
                'r' => '\r',
                't' => '\t',
                other => other,
            });
            escaped = false;
            continue;
        }
        if quote == '"' && ch == '\\' {
            escaped = true;
            continue;
        }
        if ch == quote {
            break;
        }
        value.push(ch);
    }

    value
}

fn strip_unquoted_comment(value: &str) -> &str {
    let mut previous_was_whitespace = false;
    for (index, ch) in value.char_indices() {
        if ch == '#' && (index == 0 || previous_was_whitespace) {
            return &value[..index];
        }
        previous_was_whitespace = ch.is_whitespace();
    }
    value
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

    recover_live_history_before_strategy_warmup(&config)?;

    let strategy_runtime = start_strategy_runtime(&config)?;
    let _admin_server = start_admin_server(&config, strategy_runtime.clone())?;
    let _login_scheduler = start_login_scheduler(&config)?;
    let _master_scheduler = start_master_scheduler(&config)?;

    run_feed_brokers(&config, strategy_runtime, configured_max_events(&config)?)
}

fn recover_live_history_before_strategy_warmup(config: &AppConfig) -> Result<(), FeedError> {
    let Some(historical_config) = config
        .historical_candles
        .as_ref()
        .filter(|historical| historical.enabled)
    else {
        return Ok(());
    };

    for broker in &config.feeder.feed_brokers {
        if !broker.eq_ignore_ascii_case("DELTA") {
            continue;
        }

        let Some(delta_config) = config.brokers.delta.as_ref() else {
            continue;
        };
        let base_catalog = InstrumentCatalog::load_csv(&delta_config.base_instruments_csv)?;
        let delta_historical = DeltaHistoricalClient::new(delta_config.rest_url()?);
        recover_spot_history(
            &delta_historical,
            Some(historical_config),
            &base_catalog,
            delta_config.console_logging.unwrap_or(true),
        )?;
    }

    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dotenv_literal_keeps_dollar_in_secret() {
        let values = parse_dotenv_literal("DB_SECRET=Abc123$Xy\n").expect("parse .env");

        assert_eq!(
            values,
            vec![("DB_SECRET".to_string(), "Abc123$Xy".to_string())]
        );
    }

    #[test]
    fn dotenv_literal_strips_simple_quotes() {
        let values = parse_dotenv_literal("DB_SECRET='Abc123$Xy'\n").expect("parse .env");

        assert_eq!(
            values,
            vec![("DB_SECRET".to_string(), "Abc123$Xy".to_string())]
        );
    }
}
