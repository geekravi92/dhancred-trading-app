use std::collections::BTreeMap;

use crate::adapters::delta::latest_price_file::DeltaLatestPriceFile;
use crate::adapters::delta::live::DeltaLiveFeeder;
use crate::adapters::delta::product_master::{
    DeltaProductClient, DeltaSpotReference, DeltaUniverseSummary,
    build_delta_universe_summary_from_master_csv, ensure_delta_master_csv_with_logging,
    print_delta_universe_summary, selected_trading_symbols, write_delta_derivatives_csv,
};
use crate::config::{DeltaBrokerSection, InstrumentSelection};
use crate::feeder::{
    FeedError, InstrumentCatalog, InstrumentType, PriceEvent, RefreshDecision, SubscriptionDiff,
    UniverseRefreshState,
};

pub fn run_live(config: &DeltaBrokerSection, max_events: usize) -> Result<(), FeedError> {
    let log_to_console = config.console_logging.unwrap_or(true);
    let base_catalog = InstrumentCatalog::load_csv(&config.base_instruments_csv)?;
    let spot_references = spot_references_from_base_catalog(&base_catalog)?;
    let selection = InstrumentSelection::from(config);
    let delta = DeltaProductClient::new(config.rest_url()?);
    ensure_delta_master_csv_with_logging(&delta, &config.master_csv, log_to_console)?;
    let mut live_feeder = DeltaLiveFeeder::connect(&config.public_ws_url()?)?;

    let mut runtimes = build_underlying_runtimes(config, &spot_references);
    let spot_to_underlying = spot_to_underlying(&spot_references);
    let mut active_summaries = BTreeMap::new();
    let mut latest_prices = build_latest_price_file(config, &spot_references)?;
    let spot_symbols = spot_references
        .iter()
        .map(|reference| reference.spot_symbol.clone())
        .collect::<Vec<_>>();

    if log_to_console {
        println!("Connected Delta public WebSocket");
        println!(
            "Subscribing spot/index anchors: {}",
            spot_symbols.join(", ")
        );
    }
    live_feeder.subscribe_spot_symbols(&spot_symbols)?;
    if log_to_console {
        println!("Waiting for first live spot ticks from Delta spot_price");
        println!();
    }

    wait_for_all_spot_anchors(
        &mut live_feeder,
        &mut runtimes,
        &spot_to_underlying,
        &mut active_summaries,
        &mut latest_prices,
        config,
        &selection,
        log_to_console,
    )?;

    print_event_limit(max_events, log_to_console);
    stream_live_ticks(
        &mut live_feeder,
        &mut runtimes,
        &spot_to_underlying,
        &mut active_summaries,
        &mut latest_prices,
        config,
        &selection,
        max_events,
        log_to_console,
    )
}

#[derive(Debug)]
struct UnderlyingRuntime {
    spot_symbol: String,
    refresh_state: UniverseRefreshState,
}

fn spot_references_from_base_catalog(
    catalog: &InstrumentCatalog,
) -> Result<Vec<DeltaSpotReference>, FeedError> {
    let references = catalog
        .instruments()
        .filter(|instrument| instrument.instrument_type == InstrumentType::Spot)
        .filter(|instrument| instrument.tradable)
        .map(|instrument| DeltaSpotReference {
            underlying: instrument.underlying.clone(),
            spot_symbol: instrument.trading_symbol.clone(),
        })
        .collect::<Vec<_>>();

    if references.is_empty() {
        return Err(FeedError::Config(
            "Delta base_instruments_csv must contain at least one tradable SPOT row".to_string(),
        ));
    }

    Ok(references)
}

fn build_underlying_runtimes(
    config: &DeltaBrokerSection,
    spot_references: &[DeltaSpotReference],
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

fn spot_to_underlying(spot_references: &[DeltaSpotReference]) -> BTreeMap<String, String> {
    spot_references
        .iter()
        .map(|reference| (reference.spot_symbol.clone(), reference.underlying.clone()))
        .collect()
}

fn build_latest_price_file(
    config: &DeltaBrokerSection,
    spot_references: &[DeltaSpotReference],
) -> Result<Option<DeltaLatestPriceFile>, FeedError> {
    let Some(path) = &config.latest_prices_file else {
        return Ok(None);
    };

    let underlying = config.latest_prices_underlying.as_deref().unwrap_or("BTC");
    let spot_reference = spot_references
        .iter()
        .find(|reference| reference.underlying == underlying)
        .ok_or_else(|| {
            FeedError::Config(format!(
                "Delta latest_prices_underlying {underlying} is missing from base_instruments_csv"
            ))
        })?;
    let mut latest_prices = DeltaLatestPriceFile::new(path.clone(), underlying);
    latest_prices.set_spot_symbol(&spot_reference.spot_symbol)?;

    Ok(Some(latest_prices))
}

fn wait_for_all_spot_anchors(
    live_feeder: &mut DeltaLiveFeeder,
    runtimes: &mut BTreeMap<String, UnderlyingRuntime>,
    spot_to_underlying: &BTreeMap<String, String>,
    active_summaries: &mut BTreeMap<String, DeltaUniverseSummary>,
    latest_prices: &mut Option<DeltaLatestPriceFile>,
    config: &DeltaBrokerSection,
    selection: &InstrumentSelection,
    log_to_console: bool,
) -> Result<(), FeedError> {
    while active_summaries.len() < runtimes.len() {
        let Some(event) = live_feeder.next_price_event()? else {
            continue;
        };

        let PriceEvent::Tick(tick) = event else {
            continue;
        };

        if let Some(latest_prices) = latest_prices.as_mut() {
            latest_prices.update_tick(&tick.instrument_name, tick.price.as_f64())?;
        }

        if log_to_console {
            println!(
                "PriceTick {} price={:.4} time={}",
                tick.instrument_name,
                tick.price.as_f64(),
                tick.time.as_u64()
            );
        }

        let Some(underlying) = spot_to_underlying.get(tick.instrument_name.as_str()) else {
            continue;
        };

        handle_spot_tick(
            live_feeder,
            runtimes,
            active_summaries,
            config,
            selection,
            latest_prices,
            underlying,
            tick.price.as_f64(),
            log_to_console,
        )?;
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn stream_live_ticks(
    live_feeder: &mut DeltaLiveFeeder,
    runtimes: &mut BTreeMap<String, UnderlyingRuntime>,
    spot_to_underlying: &BTreeMap<String, String>,
    active_summaries: &mut BTreeMap<String, DeltaUniverseSummary>,
    latest_prices: &mut Option<DeltaLatestPriceFile>,
    config: &DeltaBrokerSection,
    selection: &InstrumentSelection,
    max_events: usize,
    log_to_console: bool,
) -> Result<(), FeedError> {
    let mut printed_events = 0usize;
    while max_events == 0 || printed_events < max_events {
        let Some(event) = live_feeder.next_price_event()? else {
            continue;
        };

        let PriceEvent::Tick(tick) = event else {
            continue;
        };

        if let Some(latest_prices) = latest_prices.as_mut() {
            latest_prices.update_tick(&tick.instrument_name, tick.price.as_f64())?;
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
            live_feeder,
            runtimes,
            active_summaries,
            config,
            selection,
            latest_prices,
            underlying,
            tick.price.as_f64(),
            log_to_console,
        )?;
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn handle_spot_tick(
    live_feeder: &mut DeltaLiveFeeder,
    runtimes: &mut BTreeMap<String, UnderlyingRuntime>,
    active_summaries: &mut BTreeMap<String, DeltaUniverseSummary>,
    config: &DeltaBrokerSection,
    selection: &InstrumentSelection,
    latest_prices: &mut Option<DeltaLatestPriceFile>,
    underlying: &str,
    price: f64,
    log_to_console: bool,
) -> Result<(), FeedError> {
    let runtime = runtimes
        .get_mut(underlying)
        .ok_or_else(|| FeedError::Config(format!("missing runtime for underlying {underlying}")))?;

    match runtime.refresh_state.on_underlying_price(price) {
        RefreshDecision::Initialized { anchor_price } => {
            if log_to_console {
                println!(
                    "Spot anchor initialized: underlying={underlying} spot={} price={anchor_price:.4}",
                    runtime.spot_symbol
                );
            }
            refresh_delta_universe(
                live_feeder,
                runtime,
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
                    "Universe refresh triggered: underlying={underlying} previous_anchor={previous_anchor_price:.4} new_anchor={new_anchor_price:.4} movement={movement_pct:.4}%"
                );
            }
            refresh_delta_universe(
                live_feeder,
                runtime,
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
fn refresh_delta_universe(
    live_feeder: &mut DeltaLiveFeeder,
    runtime: &mut UnderlyingRuntime,
    active_summaries: &mut BTreeMap<String, DeltaUniverseSummary>,
    config: &DeltaBrokerSection,
    selection: &InstrumentSelection,
    latest_prices: &mut Option<DeltaLatestPriceFile>,
    underlying: &str,
    reference_price: f64,
    log_to_console: bool,
) -> Result<(), FeedError> {
    let summary = build_delta_universe_summary_from_master_csv(
        &config.master_csv,
        selection,
        underlying,
        &runtime.spot_symbol,
        reference_price,
    )?;

    let diff = runtime
        .refresh_state
        .apply_symbols(selected_trading_symbols(&summary));
    apply_subscription_diff(live_feeder, &diff)?;

    if let Some(latest_prices) = latest_prices.as_mut() {
        latest_prices.update_targets_from_summary(&summary)?;
    }
    active_summaries.insert(underlying.to_string(), summary);
    write_active_derivatives_csv(active_summaries, &config.derivatives_csv)?;

    if log_to_console {
        println!(
            "Wrote generated derivative instruments CSV: {}",
            config.derivatives_csv
        );
        println!(
            "Subscription diff: underlying={} subscribe={} unsubscribe={}",
            underlying,
            diff.subscribe.len(),
            diff.unsubscribe.len()
        );
        println!();

        if let Some(summary) = active_summaries.get(underlying) {
            print_delta_universe_summary(summary);
            println!();
        }
    }

    Ok(())
}

fn write_active_derivatives_csv(
    active_summaries: &BTreeMap<String, DeltaUniverseSummary>,
    path: &str,
) -> Result<(), FeedError> {
    let summaries = active_summaries.values().cloned().collect::<Vec<_>>();
    write_delta_derivatives_csv(&summaries, path)
}

fn apply_subscription_diff(
    live_feeder: &mut DeltaLiveFeeder,
    diff: &SubscriptionDiff,
) -> Result<(), FeedError> {
    live_feeder.subscribe_symbols(&diff.subscribe)?;
    live_feeder.unsubscribe_symbols(&diff.unsubscribe)
}

fn print_event_limit(max_events: usize, log_to_console: bool) {
    if log_to_console {
        if max_events == 0 {
            println!("Streaming live ticks until stopped");
        } else {
            println!("Streaming next {max_events} live ticks");
        }
        println!();
    }
}
