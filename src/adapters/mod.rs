pub mod delta;
pub mod fyers;
pub mod historical;

use std::thread;

use crate::config::AppConfig;
use crate::feeder::FeedError;

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
                    handles.push((
                        "DELTA".to_string(),
                        thread::spawn(move || delta::runtime::run_live(&delta_config, max_events)),
                    ));
                }
            }
            "FYERS" => {
                let fyers_config =
                    config.brokers.fyers.clone().ok_or_else(|| {
                        FeedError::Config("missing brokers.fyers config".to_string())
                    })?;

                if fyers_config.enabled {
                    handles.push((
                        "FYERS".to_string(),
                        thread::spawn(move || fyers::run_live(&fyers_config, max_events)),
                    ));
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

    for (broker, handle) in handles {
        let result = handle
            .join()
            .map_err(|_| FeedError::Config(format!("{broker} feed thread panicked")))?;
        result.map_err(|error| FeedError::Config(format!("{broker} feed failed: {error}")))?;
    }

    Ok(())
}
