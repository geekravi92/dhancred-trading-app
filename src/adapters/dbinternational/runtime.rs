use std::sync::Arc;

use crate::adapters::dbinternational::auth::login_market_data;
use crate::adapters::dbinternational::latest_price_file::DbinternationalLatestPriceFile;
use crate::adapters::dbinternational::live::DbinternationalLiveFeeder;
use crate::config::{DbinternationalBrokerSection, HistoricalCandlesSection};
use crate::feeder::{FeedError, PriceEvent};
use crate::notification::notify_recovery;
use crate::strategy::StrategyRuntimeHandle;

pub fn run_live(
    config: &DbinternationalBrokerSection,
    _historical_candles_config: Option<&HistoricalCandlesSection>,
    strategy_runtime: Option<Arc<StrategyRuntimeHandle>>,
    max_events: usize,
) -> Result<(), FeedError> {
    let mut refreshed_market_data_session = false;

    loop {
        match run_live_once(config, strategy_runtime.clone(), max_events) {
            Err(error)
                if !refreshed_market_data_session && is_market_data_session_rejected(&error) =>
            {
                eprintln!(
                    "DBInternational market-data session rejected by socket; refreshing login"
                );
                let summary = login_market_data(config)?;
                println!(
                    "DBInternational {} login ok user_id={} token_file={} session_file={}",
                    summary.kind.as_str(),
                    summary.user_id.as_deref().unwrap_or("-"),
                    summary.token_file,
                    summary.session_file.as_deref().unwrap_or("-")
                );
                refreshed_market_data_session = true;
            }
            result => return result,
        }
    }
}

fn run_live_once(
    config: &DbinternationalBrokerSection,
    strategy_runtime: Option<Arc<StrategyRuntimeHandle>>,
    max_events: usize,
) -> Result<(), FeedError> {
    let log_to_console = config.console_logging.unwrap_or(true);
    let mut latest_prices = build_latest_price_file(config)?;
    let feeder = DbinternationalLiveFeeder::connect(config, log_to_console)?;
    feeder.subscribe_symbols(&config.market_data_subscribe_symbols)?;

    notify_recovery(
        "broker:DBINTERNATIONAL",
        "DBINTERNATIONAL",
        "market-data socket connected and subscriptions restored",
    );
    if log_to_console {
        println!(
            "Connected DBInternational Socket.IO market-data feed; subscribed symbols: {}",
            config.market_data_subscribe_symbols.join(", ")
        );
        if max_events == 0 {
            println!("Streaming DBInternational live ticks until stopped");
        } else {
            println!("Streaming next {max_events} DBInternational live ticks");
        }
    }

    let mut printed_events = 0usize;
    while max_events == 0 || printed_events < max_events {
        let Some(event) = feeder.next_price_event()? else {
            continue;
        };

        let PriceEvent::Tick(tick) = event else {
            continue;
        };

        if let Some(latest_prices) = latest_prices.as_mut() {
            latest_prices.update_tick(&tick.instrument_name, tick.price.as_f64())?;
        }

        if let Some(strategy_runtime) = strategy_runtime.as_ref() {
            strategy_runtime.on_tick(
                tick.instrument_name.as_str(),
                tick.price.as_f64(),
                tick.time.as_u64(),
                true,
            )?;
        }

        if log_to_console {
            println!(
                "PriceTick {} price={:.4} time={}",
                tick.instrument_name,
                tick.price.as_f64(),
                tick.time.as_u64()
            );
        }
        printed_events += 1;
    }

    Ok(())
}

fn build_latest_price_file(
    config: &DbinternationalBrokerSection,
) -> Result<Option<DbinternationalLatestPriceFile>, FeedError> {
    let Some(path) = &config.latest_prices_file else {
        return Ok(None);
    };

    DbinternationalLatestPriceFile::new(path.clone(), &config.market_data_subscribe_symbols)
        .map(Some)
}

fn is_market_data_session_rejected(error: &FeedError) -> bool {
    let message = match error {
        FeedError::Disconnected(value) | FeedError::Http(value) => value.to_ascii_lowercase(),
        _ => return false,
    };

    message.contains("session has been expired")
        || message.contains("session expired")
        || message.contains("invalid user id")
}
