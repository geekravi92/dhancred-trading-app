use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::adapters::angelone::auth::{current_session, login};
use crate::adapters::angelone::latest_price_file::AngeloneLatestPriceFile;
use crate::adapters::angelone::live::AngeloneLiveFeeder;
use crate::adapters::angelone::master;
use crate::adapters::angelone::universe::{
    AngeloneSpotReference, AngeloneUniverseCatalog, AngeloneUniverseSummary,
    selected_instruments_by_symbol, selected_trading_symbols, spot_references_from_base_catalog,
    write_angelone_derivatives_csv,
};
use crate::config::{
    AngeloneBrokerSection, HistoricalCandlesSection, InstrumentSelection, MarketSessionSection,
};
use crate::feeder::{
    FeedError, InstrumentCatalog, InstrumentDefinition, MarketSessionSchedule, PriceEvent,
    RefreshDecision, SubscriptionDiff, UniverseRefreshState, exchange_key,
};
use crate::notification::notify_recovery;
use crate::strategy::StrategyRuntimeHandle;

pub fn run_live(
    config: &AngeloneBrokerSection,
    market_sessions: &[MarketSessionSection],
    _historical_candles_config: Option<&HistoricalCandlesSection>,
    strategy_runtime: Option<Arc<StrategyRuntimeHandle>>,
    max_events: usize,
) -> Result<(), FeedError> {
    let log_to_console = config.console_logging.unwrap_or(true);
    ensure_session_current(config, log_to_console)?;
    let master_summary = master::ensure_master_current(config)?;
    if log_to_console {
        println!(
            "AngelOne master {}: {} instruments | {}",
            if master_summary.refreshed {
                "refreshed"
            } else {
                "cached"
            },
            master_summary.instrument_count,
            master_summary.output_path
        );
    }

    let market_schedule = MarketSessionSchedule::from_configs(market_sessions)?;
    let base_catalog = InstrumentCatalog::load_csv(&config.base_instruments_csv)?;
    let spot_references = spot_references_from_base_catalog(&base_catalog)?;
    let spot_to_runtime_key = spot_to_runtime_key(&spot_references);
    let strategy_instrument_names = strategy_instrument_names(&base_catalog);
    let selection = InstrumentSelection::from(config);
    let universe_catalog = AngeloneUniverseCatalog::load(config)?;
    let mut runtimes = build_reference_runtimes(config, &spot_references, &market_schedule);
    let mut active_reference_keys = BTreeSet::new();
    let mut active_instrument_names = BTreeSet::new();
    let mut active_summaries = BTreeMap::new();
    let mut latest_prices = build_latest_price_file(config)?;
    write_active_derivatives_csv(&active_summaries, &config.derivatives_csv)?;
    let mut live_feeder = AngeloneLiveFeeder::connect(config, log_to_console)?;

    notify_recovery(
        "broker:ANGELONE",
        "ANGELONE",
        "market-data websocket connected; exchange-session subscriptions are managed",
    );
    if log_to_console {
        println!("Connected AngelOne Smart Stream websocket");
        if max_events == 0 {
            println!("Streaming AngelOne live ticks until stopped");
        } else {
            println!("Streaming next {max_events} AngelOne live ticks");
        }
    }

    apply_session_state(
        &mut live_feeder,
        &mut runtimes,
        &spot_references,
        &mut active_reference_keys,
        &mut active_summaries,
        &mut active_instrument_names,
        config,
        latest_prices.as_mut(),
        &market_schedule,
        now_unix_seconds(),
        log_to_console,
    )?;

    let mut printed_events = 0usize;
    while max_events == 0 || printed_events < max_events {
        apply_session_state(
            &mut live_feeder,
            &mut runtimes,
            &spot_references,
            &mut active_reference_keys,
            &mut active_summaries,
            &mut active_instrument_names,
            config,
            latest_prices.as_mut(),
            &market_schedule,
            now_unix_seconds(),
            log_to_console,
        )?;

        let Some(event) = live_feeder.next_price_event()? else {
            continue;
        };
        let PriceEvent::Tick(tick) = event else {
            continue;
        };

        if !active_instrument_names.contains(tick.instrument_name.as_str()) {
            continue;
        }

        if let Some(latest_prices) = latest_prices.as_mut() {
            latest_prices.update_tick(&tick.instrument_name, tick.price.as_f64())?;
        }

        if let Some(strategy_runtime) = strategy_runtime.as_ref() {
            let strategy_instrument = strategy_instrument_names
                .get(tick.instrument_name.as_str())
                .map(String::as_str)
                .unwrap_or_else(|| tick.instrument_name.as_str());
            strategy_runtime.on_tick(
                strategy_instrument,
                tick.price.as_f64(),
                tick.time.as_u64(),
                spot_to_runtime_key.contains_key(tick.instrument_name.as_str()),
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

        let Some(runtime_key) = spot_to_runtime_key.get(tick.instrument_name.as_str()) else {
            continue;
        };
        if !active_reference_keys.contains(runtime_key) {
            continue;
        }

        handle_spot_tick(
            &mut live_feeder,
            &universe_catalog,
            &mut runtimes,
            &spot_references,
            &active_reference_keys,
            &mut active_summaries,
            &mut active_instrument_names,
            config,
            &selection,
            latest_prices.as_mut(),
            runtime_key,
            tick.price.as_f64(),
            log_to_console,
        )?;
    }

    Ok(())
}

#[derive(Debug)]
struct ReferenceRuntime {
    runtime_key: String,
    underlying: String,
    spot_symbol: String,
    reference_exchange: String,
    allowed_derivative_exchanges: Option<BTreeSet<String>>,
    refresh_state: UniverseRefreshState,
}

#[allow(clippy::too_many_arguments)]
fn apply_session_state(
    live_feeder: &mut AngeloneLiveFeeder,
    runtimes: &mut BTreeMap<String, ReferenceRuntime>,
    spot_references: &[AngeloneSpotReference],
    active_reference_keys: &mut BTreeSet<String>,
    active_summaries: &mut BTreeMap<String, AngeloneUniverseSummary>,
    active_instrument_names: &mut BTreeSet<String>,
    config: &AngeloneBrokerSection,
    latest_prices: Option<&mut AngeloneLatestPriceFile>,
    market_schedule: &MarketSessionSchedule,
    now_utc: u64,
    log_to_console: bool,
) -> Result<(), FeedError> {
    let next_reference_keys = active_reference_keys_for(spot_references, market_schedule, now_utc);
    if next_reference_keys == *active_reference_keys {
        return Ok(());
    }

    let closing_keys = active_reference_keys
        .difference(&next_reference_keys)
        .cloned()
        .collect::<Vec<_>>();
    let opening_keys = next_reference_keys
        .difference(active_reference_keys)
        .cloned()
        .collect::<Vec<_>>();

    for runtime_key in &closing_keys {
        if let Some(runtime) = runtimes.get_mut(runtime_key) {
            let diff = runtime.refresh_state.reset();
            apply_subscription_diff(live_feeder, &diff, &BTreeMap::new())?;
            active_summaries.remove(runtime_key);
        }
        if let Some(reference) = spot_reference_by_key(spot_references, runtime_key) {
            live_feeder.unsubscribe_symbols(&[reference.spot_symbol.clone()])?;
        }
    }

    let opening_instruments = opening_keys
        .iter()
        .filter_map(|runtime_key| spot_reference_by_key(spot_references, runtime_key))
        .map(|reference| reference.instrument.clone())
        .collect::<Vec<_>>();
    live_feeder.subscribe_instruments(&opening_instruments)?;

    *active_reference_keys = next_reference_keys;
    write_active_derivatives_csv(active_summaries, &config.derivatives_csv)?;
    sync_active_tracking(
        spot_references,
        active_reference_keys,
        active_summaries,
        active_instrument_names,
        latest_prices,
    )?;

    if log_to_console {
        println!(
            "AngelOne market-session subscriptions changed: open_base={} close_base={} active_base={}",
            opening_keys.len(),
            closing_keys.len(),
            active_reference_keys.len()
        );
        if !opening_keys.is_empty() {
            println!("  opened: {}", opening_keys.join(", "));
        }
        if !closing_keys.is_empty() {
            println!("  closed: {}", closing_keys.join(", "));
        }
    }

    Ok(())
}

fn ensure_session_current(
    config: &AngeloneBrokerSection,
    log_to_console: bool,
) -> Result<(), FeedError> {
    if current_session(config, now_unix_seconds())?.is_some() {
        if log_to_console {
            println!("AngelOne login skipped: current session file is fresh");
        }
        return Ok(());
    }

    let summary = login(config, None)?;
    if log_to_console {
        println!(
            "AngelOne login ok client_code={} session_file={}",
            summary.client_code, summary.session_file
        );
    }
    Ok(())
}

fn build_reference_runtimes(
    config: &AngeloneBrokerSection,
    spot_references: &[AngeloneSpotReference],
    market_schedule: &MarketSessionSchedule,
) -> BTreeMap<String, ReferenceRuntime> {
    spot_references
        .iter()
        .map(|reference| {
            let runtime_key = reference_key(reference);
            (
                runtime_key.clone(),
                ReferenceRuntime {
                    runtime_key,
                    underlying: reference.underlying.clone(),
                    spot_symbol: reference.spot_symbol.clone(),
                    reference_exchange: exchange_key(&reference.instrument.exchange),
                    allowed_derivative_exchanges: market_schedule
                        .session_exchanges_for_exchange(&reference.instrument.exchange),
                    refresh_state: UniverseRefreshState::new(config.refresh_trigger_pct),
                },
            )
        })
        .collect()
}

fn spot_to_runtime_key(spot_references: &[AngeloneSpotReference]) -> BTreeMap<String, String> {
    spot_references
        .iter()
        .map(|reference| {
            (
                reference.instrument.instrument_name.to_string(),
                reference_key(reference),
            )
        })
        .collect()
}

fn active_reference_keys_for(
    spot_references: &[AngeloneSpotReference],
    market_schedule: &MarketSessionSchedule,
    now_utc: u64,
) -> BTreeSet<String> {
    spot_references
        .iter()
        .filter(|reference| {
            market_schedule.is_exchange_active(&reference.instrument.exchange, now_utc)
        })
        .map(reference_key)
        .collect()
}

fn reference_key(reference: &AngeloneSpotReference) -> String {
    reference.instrument.instrument_name.to_string()
}

fn spot_reference_by_key<'a>(
    spot_references: &'a [AngeloneSpotReference],
    runtime_key: &str,
) -> Option<&'a AngeloneSpotReference> {
    spot_references
        .iter()
        .find(|reference| reference_key(reference) == runtime_key)
}

fn strategy_instrument_names(catalog: &InstrumentCatalog) -> BTreeMap<String, String> {
    catalog
        .instruments()
        .map(|instrument| {
            (
                instrument.trading_symbol.clone(),
                instrument.instrument_name.to_string(),
            )
        })
        .collect()
}

fn build_latest_price_file(
    config: &AngeloneBrokerSection,
) -> Result<Option<AngeloneLatestPriceFile>, FeedError> {
    let Some(path) = &config.latest_prices_file else {
        return Ok(None);
    };
    AngeloneLatestPriceFile::new(path.clone(), &[]).map(Some)
}

#[allow(clippy::too_many_arguments)]
fn handle_spot_tick(
    live_feeder: &mut AngeloneLiveFeeder,
    universe_catalog: &AngeloneUniverseCatalog,
    runtimes: &mut BTreeMap<String, ReferenceRuntime>,
    spot_references: &[AngeloneSpotReference],
    active_reference_keys: &BTreeSet<String>,
    active_summaries: &mut BTreeMap<String, AngeloneUniverseSummary>,
    active_instrument_names: &mut BTreeSet<String>,
    config: &AngeloneBrokerSection,
    selection: &InstrumentSelection,
    latest_prices: Option<&mut AngeloneLatestPriceFile>,
    runtime_key: &str,
    price: f64,
    log_to_console: bool,
) -> Result<(), FeedError> {
    let runtime = runtimes.get_mut(runtime_key).ok_or_else(|| {
        FeedError::Config(format!(
            "missing AngelOne runtime for reference {runtime_key}"
        ))
    })?;

    match runtime.refresh_state.on_underlying_price(price) {
        RefreshDecision::Initialized { anchor_price } => {
            if log_to_console {
                println!(
                    "AngelOne anchor initialized: reference={} exchange={} underlying={} price={anchor_price:.4}",
                    runtime.spot_symbol, runtime.reference_exchange, runtime.underlying
                );
            }
            refresh_angelone_universe(
                live_feeder,
                universe_catalog,
                runtime,
                spot_references,
                active_reference_keys,
                active_summaries,
                active_instrument_names,
                config,
                selection,
                latest_prices,
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
                    "AngelOne universe refresh triggered: reference={} underlying={} previous_anchor={previous_anchor_price:.4} new_anchor={new_anchor_price:.4} movement={movement_pct:.4}%",
                    runtime.spot_symbol, runtime.underlying
                );
            }
            refresh_angelone_universe(
                live_feeder,
                universe_catalog,
                runtime,
                spot_references,
                active_reference_keys,
                active_summaries,
                active_instrument_names,
                config,
                selection,
                latest_prices,
                new_anchor_price,
                log_to_console,
            )
        }
        RefreshDecision::Hold { .. } => Ok(()),
    }
}

#[allow(clippy::too_many_arguments)]
fn refresh_angelone_universe(
    live_feeder: &mut AngeloneLiveFeeder,
    universe_catalog: &AngeloneUniverseCatalog,
    runtime: &mut ReferenceRuntime,
    spot_references: &[AngeloneSpotReference],
    active_reference_keys: &BTreeSet<String>,
    active_summaries: &mut BTreeMap<String, AngeloneUniverseSummary>,
    active_instrument_names: &mut BTreeSet<String>,
    config: &AngeloneBrokerSection,
    selection: &InstrumentSelection,
    latest_prices: Option<&mut AngeloneLatestPriceFile>,
    reference_price: f64,
    log_to_console: bool,
) -> Result<(), FeedError> {
    let summary = universe_catalog.build_summary_for_exchanges(
        selection,
        &runtime.underlying,
        &runtime.spot_symbol,
        reference_price,
        runtime.allowed_derivative_exchanges.as_ref(),
    )?;
    let next_symbols = selected_trading_symbols(&summary);
    let selected_by_symbol = selected_instruments_by_symbol(&summary);
    let diff = runtime.refresh_state.apply_symbols(next_symbols);
    apply_subscription_diff(live_feeder, &diff, &selected_by_symbol)?;
    active_summaries.insert(runtime.runtime_key.clone(), summary);
    write_active_derivatives_csv(active_summaries, &config.derivatives_csv)?;

    sync_active_tracking(
        spot_references,
        active_reference_keys,
        active_summaries,
        active_instrument_names,
        latest_prices,
    )?;

    if log_to_console {
        let active = active_summaries
            .get(&runtime.runtime_key)
            .expect("inserted summary");
        println!(
            "AngelOne universe refreshed: reference={} exchange={} underlying={} futures={} options={} subscribe={} unsubscribe={} derivatives_csv={}",
            runtime.spot_symbol,
            runtime.reference_exchange,
            runtime.underlying,
            active.futures.len(),
            active.atm_options.len(),
            diff.subscribe.len(),
            diff.unsubscribe.len(),
            config.derivatives_csv
        );
    }

    Ok(())
}

fn apply_subscription_diff(
    live_feeder: &mut AngeloneLiveFeeder,
    diff: &SubscriptionDiff,
    selected_by_symbol: &BTreeMap<String, InstrumentDefinition>,
) -> Result<(), FeedError> {
    live_feeder.unsubscribe_symbols(&diff.unsubscribe)?;
    let subscribe = diff
        .subscribe
        .iter()
        .filter_map(|symbol| selected_by_symbol.get(symbol).cloned())
        .collect::<Vec<_>>();
    live_feeder.subscribe_instruments(&subscribe)
}

fn sync_active_tracking(
    spot_references: &[AngeloneSpotReference],
    active_reference_keys: &BTreeSet<String>,
    active_summaries: &BTreeMap<String, AngeloneUniverseSummary>,
    active_instrument_names: &mut BTreeSet<String>,
    latest_prices: Option<&mut AngeloneLatestPriceFile>,
) -> Result<(), FeedError> {
    let symbols = tracked_symbols(spot_references, active_reference_keys, active_summaries);
    *active_instrument_names = symbols.iter().cloned().collect();
    if let Some(latest_prices) = latest_prices {
        latest_prices.set_symbols(&symbols)?;
    }

    Ok(())
}

fn write_active_derivatives_csv(
    active_summaries: &BTreeMap<String, AngeloneUniverseSummary>,
    path: &str,
) -> Result<(), FeedError> {
    let summaries = active_summaries.values().cloned().collect::<Vec<_>>();
    write_angelone_derivatives_csv(&summaries, path)
}

fn tracked_symbols(
    spot_references: &[AngeloneSpotReference],
    active_reference_keys: &BTreeSet<String>,
    active_summaries: &BTreeMap<String, AngeloneUniverseSummary>,
) -> Vec<String> {
    let mut symbols = spot_references
        .iter()
        .filter(|reference| active_reference_keys.contains(&reference_key(reference)))
        .map(|reference| reference.instrument.instrument_name.to_string())
        .collect::<Vec<_>>();
    symbols.extend(active_summaries.values().flat_map(|summary| {
        summary
            .futures
            .iter()
            .chain(summary.atm_options.iter())
            .map(|instrument| instrument.instrument_name.to_string())
    }));
    symbols
}

fn now_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feeder::{InstrumentName, InstrumentType};

    fn reference(symbol: &str, exchange: &str, underlying: &str) -> AngeloneSpotReference {
        AngeloneSpotReference {
            underlying: underlying.to_string(),
            spot_symbol: symbol.to_string(),
            instrument: InstrumentDefinition {
                instrument_name: InstrumentName::new(symbol),
                instrument_type: InstrumentType::Fut,
                strike: None,
                expiry: Some("2026-08-05".to_string()),
                broker: "ANGELONE".to_string(),
                instrument_token: format!("{exchange}:1"),
                trading_symbol: symbol.to_string(),
                exchange: exchange.to_string(),
                segment: "FUTCOM".to_string(),
                underlying: underlying.to_string(),
                lot_size: 1.0,
                tick_size: 1.0,
                tradable: true,
            },
        }
    }

    #[test]
    fn active_reference_keys_follow_exchange_sessions() {
        let sessions = vec![MarketSessionSection {
            id: "MCX".to_string(),
            exchanges: vec!["MCX".to_string()],
            timezone: "Asia/Kolkata".to_string(),
            open: Some("09:00".to_string()),
            close: Some("23:30".to_string()),
            always_open: false,
            connect_before_open_secs: 300,
            weekdays_only: true,
        }];
        let schedule = MarketSessionSchedule::from_configs(&sessions).expect("schedule");
        let references = vec![reference("GOLD05AUG26FUT", "MCX", "GOLD")];

        // 2026-07-02 23:00 IST.
        assert_eq!(
            active_reference_keys_for(&references, &schedule, 1_783_013_400),
            BTreeSet::from(["GOLD05AUG26FUT".to_string()])
        );
        // 2026-07-02 23:31 IST.
        assert!(active_reference_keys_for(&references, &schedule, 1_783_015_260).is_empty());
    }
}
