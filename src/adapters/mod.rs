pub mod delta;
pub mod fyers;
pub mod historical;

use std::panic::{self, AssertUnwindSafe};
use std::sync::mpsc;
use std::thread;

use crate::config::AppConfig;
use crate::feeder::FeedError;

pub fn run_feed_brokers(config: &AppConfig, max_events: usize) -> Result<(), FeedError> {
    let mut handles = Vec::new();
    let (tx, rx) = mpsc::channel();

    for broker in &config.feeder.feed_brokers {
        match broker.trim().to_ascii_uppercase().as_str() {
            "DELTA" => {
                let delta_config =
                    config.brokers.delta.clone().ok_or_else(|| {
                        FeedError::Config("missing brokers.delta config".to_string())
                    })?;

                if delta_config.enabled {
                    spawn_broker(&mut handles, &tx, "DELTA", move || {
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
                    spawn_broker(&mut handles, &tx, "FYERS", move || {
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

    drop(tx);

    for _ in 0..handles.len() {
        let result = rx
            .recv()
            .map_err(|error| FeedError::Config(format!("feed broker thread failed: {error}")))?;
        result?;
    }

    for handle in handles {
        handle
            .join()
            .map_err(|_| FeedError::Config("feed broker thread panicked".to_string()))?;
    }

    Ok(())
}

fn spawn_broker<F>(
    handles: &mut Vec<thread::JoinHandle<()>>,
    tx: &mpsc::Sender<Result<(), FeedError>>,
    broker: &'static str,
    run: F,
) where
    F: FnOnce() -> Result<(), FeedError> + Send + 'static,
{
    let tx = tx.clone();
    handles.push(thread::spawn(move || {
        let result = panic::catch_unwind(AssertUnwindSafe(run))
            .map_err(|_| FeedError::Config(format!("{broker} feed thread panicked")))
            .and_then(|result| {
                result.map_err(|error| FeedError::Config(format!("{broker} feed failed: {error}")))
            });
        let _ = tx.send(result);
    }));
}
