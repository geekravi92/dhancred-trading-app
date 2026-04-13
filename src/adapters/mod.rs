pub mod delta;
pub mod fyers;
pub mod historical;

use std::panic::{self, AssertUnwindSafe};
use std::thread;
use std::time::Duration;

use crate::config::AppConfig;
use crate::feeder::FeedError;
use crate::notification::{AlertSeverity, notify_failure};

const CONFIG_RESTART_DELAY_SECS: u64 = 60;
const TRANSIENT_RESTART_DELAY_SECS: u64 = 10;

pub fn run_feed_brokers(config: &AppConfig, max_events: usize) -> Result<(), FeedError> {
    let mut handles = Vec::new();

    for broker in &config.feeder.feed_brokers {
        match broker.trim().to_ascii_uppercase().as_str() {
            "DELTA" => {
                let delta_config =
                    config.brokers.delta.clone().ok_or_else(|| {
                        FeedError::Config("missing brokers.delta config".to_string())
                    })?;

                if delta_config.enabled {
                    spawn_broker(&mut handles, "DELTA", move || {
                        delta::runtime::run_live(&delta_config, max_events)
                    });
                }
            }
            "FYERS" => {
                let fyers_config =
                    config.brokers.fyers.clone().ok_or_else(|| {
                        FeedError::Config("missing brokers.fyers config".to_string())
                    })?;

                if fyers_config.enabled {
                    spawn_broker(&mut handles, "FYERS", move || {
                        fyers::run_live(&fyers_config, max_events)
                    });
                }
            }
            value => {
                return Err(FeedError::Config(format!(
                    "unsupported feed broker {value}"
                )));
            }
        }
    }

    if handles.is_empty() {
        return Err(FeedError::Config(
            "no enabled feed brokers configured".to_string(),
        ));
    }

    for handle in handles {
        let result = handle
            .join()
            .map_err(|_| FeedError::Config("feed broker thread panicked".to_string()))?;
        result?;
    }

    Ok(())
}

fn spawn_broker<F>(
    handles: &mut Vec<thread::JoinHandle<Result<(), FeedError>>>,
    broker: &'static str,
    run: F,
) where
    F: Fn() -> Result<(), FeedError> + Send + 'static,
{
    handles.push(thread::spawn(move || supervise_broker(broker, run)));
}

fn supervise_broker<F>(broker: &'static str, run: F) -> Result<(), FeedError>
where
    F: Fn() -> Result<(), FeedError>,
{
    loop {
        let result = panic::catch_unwind(AssertUnwindSafe(&run))
            .map_err(|_| FeedError::Config(format!("{broker} feed thread panicked")))
            .and_then(|result| result.map_err(|error| annotate_broker_error(broker, error)));

        match result {
            Ok(()) => return Ok(()),
            Err(error) => {
                let delay_secs = broker_restart_delay_secs(&error);
                eprintln!("{error}");
                eprintln!("{broker} supervisor sleeping {delay_secs}s before retry");
                notify_failure(
                    format!("broker:{broker}"),
                    broker,
                    AlertSeverity::Error,
                    format!("{error}; retry in {delay_secs}s"),
                );
                thread::sleep(Duration::from_secs(delay_secs));
            }
        }
    }
}

fn broker_restart_delay_secs(error: &FeedError) -> u64 {
    match error {
        FeedError::Config(_) => CONFIG_RESTART_DELAY_SECS,
        FeedError::Http(_) | FeedError::Disconnected(_) => TRANSIENT_RESTART_DELAY_SECS,
        FeedError::Io(_) | FeedError::Parse(_) | FeedError::InvalidInstrument(_) => {
            TRANSIENT_RESTART_DELAY_SECS
        }
        FeedError::NotSubscribed | FeedError::UnsupportedChannel { .. } => {
            CONFIG_RESTART_DELAY_SECS
        }
    }
}

fn annotate_broker_error(broker: &str, error: FeedError) -> FeedError {
    match error {
        FeedError::NotSubscribed => FeedError::NotSubscribed,
        FeedError::UnsupportedChannel {
            broker: error_broker,
            channel,
        } => FeedError::UnsupportedChannel {
            broker: format!("{broker}: {error_broker}"),
            channel,
        },
        FeedError::InvalidInstrument(value) => {
            FeedError::InvalidInstrument(format!("{broker} feed failed: {value}"))
        }
        FeedError::Config(value) => FeedError::Config(format!("{broker} feed failed: {value}")),
        FeedError::Http(value) => FeedError::Http(format!("{broker} feed failed: {value}")),
        FeedError::Io(value) => FeedError::Io(format!("{broker} feed failed: {value}")),
        FeedError::Parse(value) => FeedError::Parse(format!("{broker} feed failed: {value}")),
        FeedError::Disconnected(value) => {
            FeedError::Disconnected(format!("{broker} feed failed: {value}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_errors_back_off_longer_than_transient_errors() {
        assert_eq!(
            broker_restart_delay_secs(&FeedError::Config("missing token".to_string())),
            CONFIG_RESTART_DELAY_SECS
        );
        assert_eq!(
            broker_restart_delay_secs(&FeedError::Disconnected("socket closed".to_string())),
            TRANSIENT_RESTART_DELAY_SECS
        );
    }

    #[test]
    fn preserves_transient_error_kind_when_annotating_broker_failures() {
        let error = annotate_broker_error("DELTA", FeedError::Disconnected("socket closed".into()));
        assert!(matches!(error, FeedError::Disconnected(_)));
    }
}
