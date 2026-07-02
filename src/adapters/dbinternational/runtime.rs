use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::adapters::dbinternational::auth::login_market_data;
use crate::adapters::dbinternational::latest_price_file::DbinternationalLatestPriceFile;
use crate::adapters::dbinternational::live::DbinternationalLiveFeeder;
use crate::adapters::dbinternational::universe::{
    DbinternationalSpotReference, DbinternationalUniverseCatalog, DbinternationalUniverseSummary,
    selected_trading_symbols, write_dbinternational_derivatives_csv,
};
use crate::config::{DbinternationalBrokerSection, HistoricalCandlesSection, InstrumentSelection};
use crate::feeder::{
    FeedError, InstrumentDefinition, PriceEvent, RefreshDecision, SubscriptionDiff,
    UniverseRefreshState,
};
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
    let selection = InstrumentSelection::from(config);
    let universe_catalog = DbinternationalUniverseCatalog::load(config)?;
    let spot_references = universe_catalog.spot_references().to_vec();
    if spot_references.is_empty() {
        return Err(FeedError::Config(
            "DBInternational dynamic universe produced no spot anchors".to_string(),
        ));
    }
    let mut runtimes = build_underlying_runtimes(config, &spot_references);
    let spot_to_underlying = spot_to_underlying(&spot_references);
    let mut active_summaries = BTreeMap::new();
    let mut latest_prices = build_latest_price_file(config, &spot_references)?;
    let feeder = DbinternationalLiveFeeder::connect(config, log_to_console)?;
    let spot_symbols = spot_references
        .iter()
        .map(|reference| reference.spot_symbol.clone())
        .collect::<Vec<_>>();
    feeder.subscribe_symbols(&spot_symbols)?;

    notify_recovery(
        "broker:DBINTERNATIONAL",
        "DBINTERNATIONAL",
        "market-data socket connected and spot subscriptions restored",
    );
    if log_to_console {
        println!(
            "Connected DBInternational Socket.IO market-data feed; subscribed {} spot/index anchors",
            spot_symbols.len()
        );
        println!("Waiting for DBInternational anchor ticks to build derivative subscriptions");
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

        let Some(underlying) = spot_to_underlying.get(tick.instrument_name.as_str()) else {
            continue;
        };
        handle_spot_tick(
            &feeder,
            &universe_catalog,
            &mut runtimes,
            &spot_references,
            &mut active_summaries,
            config,
            &selection,
            latest_prices.as_mut(),
            underlying,
            tick.price.as_f64(),
            log_to_console,
        )?;
    }

    Ok(())
}

#[derive(Debug)]
struct UnderlyingRuntime {
    spot_symbol: String,
    refresh_state: UniverseRefreshState,
}

fn build_underlying_runtimes(
    config: &DbinternationalBrokerSection,
    spot_references: &[DbinternationalSpotReference],
) -> BTreeMap<String, UnderlyingRuntime> {
    spot_references
        .iter()
        .map(|reference| {
            (
                reference.underlying.clone(),
                UnderlyingRuntime {
                    spot_symbol: reference.spot_symbol.clone(),
                    refresh_state: UniverseRefreshState::new(config.refresh_trigger_pct),
                },
            )
        })
        .collect()
}

fn spot_to_underlying(
    spot_references: &[DbinternationalSpotReference],
) -> BTreeMap<String, String> {
    spot_references
        .iter()
        .map(|reference| (reference.spot_symbol.clone(), reference.underlying.clone()))
        .collect()
}

fn build_latest_price_file(
    config: &DbinternationalBrokerSection,
    spot_references: &[DbinternationalSpotReference],
) -> Result<Option<DbinternationalLatestPriceFile>, FeedError> {
    let Some(path) = &config.latest_prices_file else {
        return Ok(None);
    };

    let symbols = spot_references
        .iter()
        .map(|reference| reference.spot_symbol.clone())
        .collect::<Vec<_>>();
    DbinternationalLatestPriceFile::new(path.clone(), &symbols).map(Some)
}

#[allow(clippy::too_many_arguments)]
fn handle_spot_tick(
    feeder: &DbinternationalLiveFeeder,
    universe_catalog: &DbinternationalUniverseCatalog,
    runtimes: &mut BTreeMap<String, UnderlyingRuntime>,
    spot_references: &[DbinternationalSpotReference],
    active_summaries: &mut BTreeMap<String, DbinternationalUniverseSummary>,
    config: &DbinternationalBrokerSection,
    selection: &InstrumentSelection,
    latest_prices: Option<&mut DbinternationalLatestPriceFile>,
    underlying: &str,
    price: f64,
    log_to_console: bool,
) -> Result<(), FeedError> {
    let runtime = runtimes.get_mut(underlying).ok_or_else(|| {
        FeedError::Config(format!(
            "missing DBInternational runtime for underlying {underlying}"
        ))
    })?;

    match runtime.refresh_state.on_underlying_price(price) {
        RefreshDecision::Initialized { anchor_price } => {
            if log_to_console {
                println!(
                    "DBInternational anchor initialized: underlying={underlying} spot={} price={anchor_price:.4}",
                    runtime.spot_symbol
                );
            }
            refresh_dbinternational_universe(
                feeder,
                universe_catalog,
                runtime,
                spot_references,
                active_summaries,
                config,
                selection,
                latest_prices,
                underlying,
                anchor_price,
                log_to_console,
            )
        }
        RefreshDecision::Refresh {
            previous_anchor_price,
            new_anchor_price,
            movement_pct,
        } => {
            if log_to_console {
                println!(
                    "DBInternational universe refresh triggered: underlying={underlying} previous_anchor={previous_anchor_price:.4} new_anchor={new_anchor_price:.4} movement={movement_pct:.4}%"
                );
            }
            refresh_dbinternational_universe(
                feeder,
                universe_catalog,
                runtime,
                spot_references,
                active_summaries,
                config,
                selection,
                latest_prices,
                underlying,
                new_anchor_price,
                log_to_console,
            )
        }
        RefreshDecision::Hold { .. } => Ok(()),
    }
}

#[allow(clippy::too_many_arguments)]
fn refresh_dbinternational_universe(
    feeder: &DbinternationalLiveFeeder,
    universe_catalog: &DbinternationalUniverseCatalog,
    runtime: &mut UnderlyingRuntime,
    spot_references: &[DbinternationalSpotReference],
    active_summaries: &mut BTreeMap<String, DbinternationalUniverseSummary>,
    config: &DbinternationalBrokerSection,
    selection: &InstrumentSelection,
    latest_prices: Option<&mut DbinternationalLatestPriceFile>,
    underlying: &str,
    reference_price: f64,
    log_to_console: bool,
) -> Result<(), FeedError> {
    let summary = universe_catalog.build_summary(
        selection,
        underlying,
        &runtime.spot_symbol,
        reference_price,
    )?;

    let diff = runtime
        .refresh_state
        .apply_symbols(selected_trading_symbols(&summary));
    let previous_summary = active_summaries.get(underlying).cloned();
    let subscribe_instruments = selected_instruments_for_symbols(&summary, &diff.subscribe);
    let unsubscribe_instruments = previous_summary
        .as_ref()
        .map(|summary| selected_instruments_for_symbols(summary, &diff.unsubscribe))
        .unwrap_or_default();
    apply_subscription_diff(
        feeder,
        &diff,
        &subscribe_instruments,
        &unsubscribe_instruments,
    )?;

    active_summaries.insert(underlying.to_string(), summary);
    write_active_derivatives_csv(active_summaries, &config.derivatives_csv)?;
    if let Some(latest_prices) = latest_prices {
        latest_prices.set_symbols(&active_price_symbols(spot_references, active_summaries))?;
    }

    if log_to_console {
        let summary = active_summaries
            .get(underlying)
            .expect("summary inserted before logging");
        println!(
            "DBInternational subscription diff: underlying={} futures={} options={} subscribe={} unsubscribe={}",
            underlying,
            summary.futures.len(),
            summary.atm_options.len(),
            diff.subscribe.len(),
            diff.unsubscribe.len()
        );
        println!(
            "Wrote generated DBInternational derivative instruments CSV: {}",
            config.derivatives_csv
        );
    }

    Ok(())
}

fn apply_subscription_diff(
    feeder: &DbinternationalLiveFeeder,
    diff: &SubscriptionDiff,
    subscribe_instruments: &[InstrumentDefinition],
    unsubscribe_instruments: &[InstrumentDefinition],
) -> Result<(), FeedError> {
    if !diff.unsubscribe.is_empty() {
        feeder.unsubscribe_instruments(unsubscribe_instruments)?;
    }
    if !diff.subscribe.is_empty() {
        feeder.subscribe_instruments(subscribe_instruments)?;
    }
    Ok(())
}

fn selected_instruments_for_symbols(
    summary: &DbinternationalUniverseSummary,
    symbols: &[String],
) -> Vec<InstrumentDefinition> {
    let symbols = symbols.iter().map(String::as_str).collect::<BTreeSet<_>>();
    summary
        .futures
        .iter()
        .chain(summary.atm_options.iter())
        .filter(|instrument| symbols.contains(instrument.trading_symbol.as_str()))
        .cloned()
        .collect()
}

fn write_active_derivatives_csv(
    active_summaries: &BTreeMap<String, DbinternationalUniverseSummary>,
    path: &str,
) -> Result<(), FeedError> {
    let summaries = active_summaries.values().cloned().collect::<Vec<_>>();
    write_dbinternational_derivatives_csv(&summaries, path)
}

fn active_price_symbols(
    spot_references: &[DbinternationalSpotReference],
    active_summaries: &BTreeMap<String, DbinternationalUniverseSummary>,
) -> Vec<String> {
    let mut symbols = spot_references
        .iter()
        .map(|reference| reference.spot_symbol.clone())
        .collect::<BTreeSet<_>>();
    for summary in active_summaries.values() {
        symbols.extend(selected_trading_symbols(summary));
    }
    symbols.into_iter().collect()
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
