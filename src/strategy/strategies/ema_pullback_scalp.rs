use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use serde::Deserialize;

use crate::strategy::{
    Bar, PositionStatus, PriceUpdated, SignalSide, SsuConfig, Strategy, StrategyContext,
    StrategyError, StrategySignal, Timeframe, TimeframeUpdate,
};

#[derive(Debug, Default)]
pub(crate) struct EmaPullbackScalpStrategy {
    settings: Mutex<BTreeMap<i64, EmaPullbackSettings>>,
    states: Mutex<BTreeMap<StateKey, SetupState>>,
}

impl Strategy for EmaPullbackScalpStrategy {
    fn strategy_key(&self) -> &'static str {
        "ema_pullback_scalp"
    }

    fn on_price_updated(
        &self,
        ctx: &StrategyContext,
        ssu: &SsuConfig,
        event: &PriceUpdated,
        tf_update: &TimeframeUpdate,
    ) -> Result<Vec<StrategySignal>, StrategyError> {
        // Step 1: Load SSU-owned settings. Strategy behavior must not come from code defaults.
        let settings = self.settings_for(ssu)?;

        // Step 2: This strategy is closed-candle driven. Ignore forming-candle ticks.
        if !tf_update.closed_timeframes.contains(&settings.timeframe) {
            return Ok(Vec::new());
        }

        let Some(closed_bar) = ctx
            .timeframes
            .last_closed_bar(&event.trigger_instrument, settings.timeframe)
        else {
            return Ok(Vec::new());
        };

        let state_key = StateKey::new(ssu.ssu_id, &event.trigger_instrument);
        {
            let mut states = self
                .states
                .lock()
                .expect("ema pullback state lock poisoned");
            let state = states.entry(state_key.clone()).or_default();
            if state
                .last_processed_closed_end
                .is_some_and(|end_at| end_at >= closed_bar.end_at)
            {
                return Ok(Vec::new());
            }
            state.last_processed_closed_end = Some(closed_bar.end_at);
        }

        let bars = ctx.timeframes.recent_bars(
            &event.trigger_instrument,
            settings.timeframe,
            settings.lookback_bars,
        );
        if bars.len() < settings.min_required_bars() {
            return Ok(Vec::new());
        }

        // Step 3: Build price-only indicators from the bounded rolling lookback.
        let series = IndicatorSeries::from_bars(&bars, &settings)?;

        // Step 4: Existing legs are managed before any new setup is considered.
        let mut exits = self.manage_open_positions(ctx, ssu, event, &settings, &bars, &series)?;
        if !exits.is_empty() {
            return Ok(exits);
        }

        // Step 5: Search the latest closed candle as a continuation trigger.
        for side in &settings.enabled_sides {
            let Some(setup) = detect_setup(&bars, &series, &settings, *side) else {
                continue;
            };
            // Step 6: Entry policy decides whether a valid setup can become a signal.
            if !self.entry_policy_allows(ctx, ssu, event, &settings, &setup)? {
                continue;
            }

            let entry_price =
                current_ltp(ctx, &event.trigger_instrument).unwrap_or(setup.trigger_close);
            let stop_price = setup.stop_price(settings.stop_buffer_atr);
            let risk = match setup.side {
                SignalSide::Long => entry_price - stop_price,
                SignalSide::Short => stop_price - entry_price,
            };
            if !risk.is_finite() || risk <= 0.0 {
                continue;
            }
            let target_price = settings.target_r_multiple.map(|multiple| match setup.side {
                SignalSide::Long => entry_price + multiple * risk,
                SignalSide::Short => entry_price - multiple * risk,
            });

            if settings.entry_policy == EntryPolicy::Pyramid {
                // Step 7: Pyramid protection movement is internal strategy state only.
                self.adjust_existing_pyramid_stops(ctx, ssu, event, &settings, &setup, stop_price)?;
            }

            // Step 8: Emit immutable signal envelope with one trade instruction for spot v1.
            let mut entry_signal = StrategySignal::single_leg_entry(
                ssu.ssu_id,
                self.strategy_key(),
                &event.trigger_instrument,
                setup.side,
                entry_price,
                setup.entry_reason(settings.timeframe),
                event.at,
            );
            entry_signal.metadata = setup.entry_metadata(entry_price, stop_price, target_price);
            entry_signal.instructions[0].metadata = serde_json::json!({
                "stop_price": stop_price,
                "target_price": target_price,
                "setup_id": setup.setup_id,
            });

            let position = match ctx.strategy_positions.open_position(&entry_signal, ssu) {
                Ok(position) => position,
                Err(StrategyError::Rule(_)) => continue,
                Err(error) => return Err(error),
            };

            // Step 9: Persist trade context so stops/targets survive process restart.
            let metadata = setup.trade_context_metadata(
                &entry_signal,
                &position.position_id,
                entry_price,
                stop_price,
                target_price,
                settings.target_r_multiple.is_some(),
                closed_bar.end_at,
            );
            ctx.trade_contexts.save_context(
                &position.position_id,
                ssu.ssu_id,
                self.strategy_key(),
                &event.trigger_instrument,
                &metadata,
                event.at,
            )?;

            let mut states = self
                .states
                .lock()
                .expect("ema pullback state lock poisoned");
            let state = states.entry(state_key.clone()).or_default();
            state.entered_setup_ids.insert(setup.setup_id.clone());
            state.last_breakout_level = Some(setup.breakout_level);
            state.last_entry_bar_end_at = Some(closed_bar.end_at);
            drop(states);

            exits.push(entry_signal);
            return Ok(exits);
        }

        Ok(Vec::new())
    }
}

impl EmaPullbackScalpStrategy {
    fn settings_for(&self, ssu: &SsuConfig) -> Result<EmaPullbackSettings, StrategyError> {
        if let Some(settings) = self
            .settings
            .lock()
            .expect("ema pullback settings lock poisoned")
            .get(&ssu.ssu_id)
            .cloned()
        {
            return Ok(settings);
        }

        let settings = EmaPullbackSettings::from_ssu(ssu)?;
        self.settings
            .lock()
            .expect("ema pullback settings lock poisoned")
            .insert(ssu.ssu_id, settings.clone());
        Ok(settings)
    }

    fn manage_open_positions(
        &self,
        ctx: &StrategyContext,
        ssu: &SsuConfig,
        event: &PriceUpdated,
        settings: &EmaPullbackSettings,
        bars: &[Bar],
        series: &IndicatorSeries,
    ) -> Result<Vec<StrategySignal>, StrategyError> {
        let Some(closed_bar) = bars.last() else {
            return Ok(Vec::new());
        };
        let Some(ema_slow) = series.ema_slow.last().copied().flatten() else {
            return Ok(Vec::new());
        };

        let mut exits = Vec::new();
        let open_positions = ctx.strategy_positions.list_open_by_ssu(ssu.ssu_id)?;
        for position in open_positions
            .into_iter()
            .filter(|position| position.trade_instrument == event.trigger_instrument)
            .filter(|position| position.status == PositionStatus::Open)
        {
            let Some(mut metadata) = ctx.trade_contexts.load_context(&position.position_id)? else {
                continue;
            };
            let stop_price = required_f64(&metadata, "stop_price")?;
            let target_enabled = required_bool(&metadata, "target_enabled")?;
            let target_price = optional_f64(&metadata, "target_price");
            let bars_since_entry = required_u64(&metadata, "bars_since_entry")?.saturating_add(1);
            let mut ema_fail_bars = required_u64(&metadata, "ema_fail_bars")?;

            let exit_reason = match position.side {
                SignalSide::Long => {
                    if closed_bar.low <= stop_price {
                        Some("stop")
                    } else if target_enabled
                        && target_price.is_some_and(|target| closed_bar.high >= target)
                    {
                        Some("target")
                    } else if bars_since_entry >= settings.time_stop_bars as u64 {
                        Some("time_stop")
                    } else {
                        if closed_bar.close < ema_slow {
                            ema_fail_bars = ema_fail_bars.saturating_add(1);
                        } else {
                            ema_fail_bars = 0;
                        }
                        if ema_fail_bars >= settings.exit_on_ema_fail_bars as u64 {
                            Some("ema_fail")
                        } else {
                            None
                        }
                    }
                }
                SignalSide::Short => {
                    if closed_bar.high >= stop_price {
                        Some("stop")
                    } else if target_enabled
                        && target_price.is_some_and(|target| closed_bar.low <= target)
                    {
                        Some("target")
                    } else if bars_since_entry >= settings.time_stop_bars as u64 {
                        Some("time_stop")
                    } else {
                        if closed_bar.close > ema_slow {
                            ema_fail_bars = ema_fail_bars.saturating_add(1);
                        } else {
                            ema_fail_bars = 0;
                        }
                        if ema_fail_bars >= settings.exit_on_ema_fail_bars as u64 {
                            Some("ema_fail")
                        } else {
                            None
                        }
                    }
                }
            };

            metadata["bars_since_entry"] = serde_json::json!(bars_since_entry);
            metadata["ema_fail_bars"] = serde_json::json!(ema_fail_bars);

            if let Some(reason) = exit_reason {
                let price = current_ltp(ctx, &event.trigger_instrument).unwrap_or(closed_bar.close);
                let mut signal = StrategySignal::single_leg_exit(
                    ssu.ssu_id,
                    self.strategy_key(),
                    &event.trigger_instrument,
                    position.side,
                    price,
                    format!(
                        "ema_pullback_exit|reason={reason}|tf={}|closed_bar_end={}",
                        timeframe_label(settings.timeframe),
                        closed_bar.end_at
                    ),
                    event.at,
                );
                signal.campaign_id = required_string(&metadata, "campaign_id")?;
                signal.signal_id = format!(
                    "SIG-{}-{}-{}-{}",
                    ssu.ssu_id,
                    event.at,
                    signal_type_label_for_side(position.side, false),
                    position.position_id
                );
                signal.instructions[0].instruction_id = format!("{}-I1", signal.signal_id);
                signal.instructions[0].leg_id = position.position_id.clone();
                signal.metadata = serde_json::json!({
                    "exit_reason": reason,
                    "position_id": position.position_id,
                    "closed_bar_end": closed_bar.end_at,
                });
                match ctx.strategy_positions.close_position(&signal) {
                    Ok(_) => {
                        ctx.trade_contexts.delete_context(&position.position_id)?;
                        exits.push(signal);
                    }
                    Err(StrategyError::Rule(_)) | Err(StrategyError::NotFound(_)) => {}
                    Err(error) => return Err(error),
                }
            } else {
                ctx.trade_contexts
                    .update_context(&position.position_id, &metadata, event.at)?;
            }
        }

        Ok(exits)
    }

    fn entry_policy_allows(
        &self,
        ctx: &StrategyContext,
        ssu: &SsuConfig,
        event: &PriceUpdated,
        settings: &EmaPullbackSettings,
        setup: &DetectedSetup,
    ) -> Result<bool, StrategyError> {
        let open_positions = ctx
            .strategy_positions
            .list_open_by_ssu(ssu.ssu_id)?
            .into_iter()
            .filter(|position| position.trade_instrument == event.trigger_instrument)
            .filter(|position| position.side == setup.side)
            .filter(|position| position.status == PositionStatus::Open)
            .collect::<Vec<_>>();

        let state_key = StateKey::new(ssu.ssu_id, &event.trigger_instrument);
        let states = self
            .states
            .lock()
            .expect("ema pullback state lock poisoned");
        if states
            .get(&state_key)
            .is_some_and(|state| state.entered_setup_ids.contains(&setup.setup_id))
        {
            return Ok(false);
        }
        drop(states);

        match settings.entry_policy {
            EntryPolicy::SinglePosition => Ok(open_positions.is_empty()),
            EntryPolicy::Independent => Ok(true),
            EntryPolicy::Pyramid => {
                if open_positions.is_empty() {
                    return Ok(true);
                }
                if settings.pyramid_max_active_legs > 0
                    && open_positions.len() as u32 >= settings.pyramid_max_active_legs
                {
                    return Ok(false);
                }

                let contexts = ctx
                    .trade_contexts
                    .load_open_contexts(ssu.ssu_id, &event.trigger_instrument)?;
                let mut context_by_position = BTreeMap::new();
                for (position_id, metadata) in contexts {
                    context_by_position.insert(position_id, metadata);
                }

                let current_price =
                    current_ltp(ctx, &event.trigger_instrument).unwrap_or(setup.trigger_close);
                let mut latest_entry_at = 0_u64;
                let mut latest_unrealized_r = None;
                let mut latest_context = None;
                for position in &open_positions {
                    let Some(metadata) = context_by_position.get(&position.position_id) else {
                        return Ok(false);
                    };
                    let original_stop = required_f64(metadata, "original_stop_price")?;
                    let initial_risk = match position.side {
                        SignalSide::Long => position.entry_price - original_stop,
                        SignalSide::Short => original_stop - position.entry_price,
                    };
                    if initial_risk <= 0.0 {
                        return Ok(false);
                    }
                    let unrealized_r = match position.side {
                        SignalSide::Long => (current_price - position.entry_price) / initial_risk,
                        SignalSide::Short => (position.entry_price - current_price) / initial_risk,
                    };
                    if unrealized_r < 0.0 {
                        return Ok(false);
                    }
                    if position.entry_at >= latest_entry_at {
                        latest_entry_at = position.entry_at;
                        latest_unrealized_r = Some(unrealized_r);
                        latest_context = Some(metadata.clone());
                    }
                }

                if latest_unrealized_r.unwrap_or(0.0) < settings.pyramid_min_profit_r_before_add {
                    return Ok(false);
                }
                let Some(latest_context) = latest_context else {
                    return Ok(false);
                };
                if settings.pyramid_require_fresh_base_after_last_entry {
                    let latest_entry_bar_end_at =
                        required_u64(&latest_context, "entry_bar_end_at")?;
                    if setup.base_start_at <= latest_entry_bar_end_at {
                        return Ok(false);
                    }
                }
                let last_breakout_level = required_f64(&latest_context, "breakout_level")?;
                let breakout_distance_atr =
                    (setup.breakout_level - last_breakout_level).abs() / setup.atr;
                if breakout_distance_atr < settings.pyramid_min_breakout_level_distance_atr {
                    return Ok(false);
                }
                match setup.side {
                    SignalSide::Long if setup.breakout_level <= last_breakout_level => Ok(false),
                    SignalSide::Short if setup.breakout_level >= last_breakout_level => Ok(false),
                    _ => Ok(true),
                }
            }
        }
    }

    fn adjust_existing_pyramid_stops(
        &self,
        ctx: &StrategyContext,
        ssu: &SsuConfig,
        event: &PriceUpdated,
        settings: &EmaPullbackSettings,
        setup: &DetectedSetup,
        latest_entry_stop: f64,
    ) -> Result<(), StrategyError> {
        if settings.pyramid_stop_adjustment == PyramidStopAdjustment::None {
            return Ok(());
        }
        let open_positions = ctx
            .strategy_positions
            .list_open_by_ssu(ssu.ssu_id)?
            .into_iter()
            .filter(|position| position.trade_instrument == event.trigger_instrument)
            .filter(|position| position.side == setup.side)
            .filter(|position| position.status == PositionStatus::Open)
            .collect::<Vec<_>>();
        if open_positions.is_empty() {
            return Ok(());
        }

        for position in open_positions {
            let Some(mut metadata) = ctx.trade_contexts.load_context(&position.position_id)? else {
                continue;
            };
            let old_stop = required_f64(&metadata, "stop_price")?;
            let new_stop = match (setup.side, settings.pyramid_stop_adjustment) {
                (_, PyramidStopAdjustment::None) => old_stop,
                (SignalSide::Long, PyramidStopAdjustment::Breakeven) => {
                    old_stop.max(position.entry_price)
                }
                (SignalSide::Short, PyramidStopAdjustment::Breakeven) => {
                    old_stop.min(position.entry_price)
                }
                (SignalSide::Long, PyramidStopAdjustment::LatestEntrySl) => {
                    old_stop.max(latest_entry_stop)
                }
                (SignalSide::Short, PyramidStopAdjustment::LatestEntrySl) => {
                    old_stop.min(latest_entry_stop)
                }
                (SignalSide::Long, PyramidStopAdjustment::BetterOfBreakevenOrLatestEntrySl) => {
                    old_stop.max(position.entry_price).max(latest_entry_stop)
                }
                (SignalSide::Short, PyramidStopAdjustment::BetterOfBreakevenOrLatestEntrySl) => {
                    old_stop.min(position.entry_price).min(latest_entry_stop)
                }
            };
            if (new_stop - old_stop).abs() <= f64::EPSILON {
                continue;
            }
            metadata["stop_price"] = serde_json::json!(new_stop);
            ctx.trade_contexts
                .update_context(&position.position_id, &metadata, event.at)?;
        }

        Ok(())
    }
}

#[derive(Clone, Debug)]
struct EmaPullbackSettings {
    timeframe: Timeframe,
    enabled_sides: Vec<SignalSide>,
    entry_policy: EntryPolicy,
    lookback_bars: usize,
    ema_fast_period: usize,
    ema_slow_period: usize,
    atr_period: usize,
    regime_ema_slope_lookback_bars: usize,
    regime_min_fast_slope_atr: f64,
    regime_min_ema_separation_atr: f64,
    base_window_bars: usize,
    base_max_range_atr: f64,
    base_max_close_spread_atr: f64,
    base_max_single_bar_range_atr: f64,
    base_max_directional_efficiency: f64,
    breakout_buffer_atr: f64,
    breakout_min_bar_range_atr: f64,
    breakout_min_close_location: f64,
    impulse_min_height_atr: f64,
    impulse_max_bars: usize,
    impulse_min_efficiency: f64,
    pullback_min_ratio: f64,
    pullback_max_ratio: f64,
    pullback_min_bars: usize,
    pullback_max_bars: usize,
    pullback_max_counter_efficiency: f64,
    ema_zone_buffer_atr: f64,
    max_breakout_level_penetration_atr: f64,
    trigger_break_lookback_bars: usize,
    trigger_buffer_atr: f64,
    trigger_min_close_location: f64,
    max_entry_extension_atr: f64,
    stop_buffer_atr: f64,
    target_r_multiple: Option<f64>,
    time_stop_bars: usize,
    exit_on_ema_fail_bars: usize,
    pyramid_min_profit_r_before_add: f64,
    pyramid_stop_adjustment: PyramidStopAdjustment,
    pyramid_require_fresh_base_after_last_entry: bool,
    pyramid_min_breakout_level_distance_atr: f64,
    pyramid_max_active_legs: u32,
}

impl EmaPullbackSettings {
    fn from_ssu(ssu: &SsuConfig) -> Result<Self, StrategyError> {
        #[derive(Deserialize)]
        struct Raw {
            timeframe: Option<String>,
            enabled_sides: Option<Vec<String>>,
            entry_policy: Option<String>,
            lookback_bars: Option<usize>,
            ema_fast_period: Option<usize>,
            ema_slow_period: Option<usize>,
            atr_period: Option<usize>,
            regime_ema_slope_lookback_bars: Option<usize>,
            regime_min_fast_slope_atr: Option<f64>,
            regime_min_ema_separation_atr: Option<f64>,
            base_window_bars: Option<usize>,
            base_max_range_atr: Option<f64>,
            base_max_close_spread_atr: Option<f64>,
            base_max_single_bar_range_atr: Option<f64>,
            base_max_directional_efficiency: Option<f64>,
            breakout_buffer_atr: Option<f64>,
            breakout_min_bar_range_atr: Option<f64>,
            breakout_min_close_location: Option<f64>,
            impulse_min_height_atr: Option<f64>,
            impulse_max_bars: Option<usize>,
            impulse_min_efficiency: Option<f64>,
            pullback_min_ratio: Option<f64>,
            pullback_max_ratio: Option<f64>,
            pullback_min_bars: Option<usize>,
            pullback_max_bars: Option<usize>,
            pullback_max_counter_efficiency: Option<f64>,
            ema_zone_buffer_atr: Option<f64>,
            max_breakout_level_penetration_atr: Option<f64>,
            trigger_break_lookback_bars: Option<usize>,
            trigger_buffer_atr: Option<f64>,
            trigger_min_close_location: Option<f64>,
            max_entry_extension_atr: Option<f64>,
            stop_buffer_atr: Option<f64>,
            target_enabled: Option<bool>,
            target_r_multiple: Option<f64>,
            time_stop_bars: Option<usize>,
            exit_on_ema_fail_bars: Option<usize>,
            pyramid_min_profit_r_before_add: Option<f64>,
            pyramid_stop_adjustment: Option<String>,
            pyramid_require_fresh_base_after_last_entry: Option<bool>,
            pyramid_min_breakout_level_distance_atr: Option<f64>,
            pyramid_max_active_legs: Option<u32>,
        }

        let raw: Raw = serde_json::from_str(&ssu.params_json).map_err(|error| {
            StrategyError::Parse(format!(
                "invalid ema_pullback_scalp params_json for SSU {}: {error}",
                ssu.ssu_id
            ))
        })?;
        let timeframe = parse_timeframe(&require(raw.timeframe, "timeframe", ssu.ssu_id)?)?;
        if !ssu.required_timeframes.contains(&timeframe) {
            return Err(StrategyError::Config(format!(
                "SSU {} ema_pullback_scalp timeframe {} is not registered",
                ssu.ssu_id,
                timeframe_label(timeframe)
            )));
        }
        let enabled_sides = require(raw.enabled_sides, "enabled_sides", ssu.ssu_id)?
            .iter()
            .map(|side| parse_side(side))
            .collect::<Result<Vec<_>, _>>()?;
        if enabled_sides.is_empty() {
            return Err(StrategyError::Config(format!(
                "SSU {} ema_pullback_scalp enabled_sides must not be empty",
                ssu.ssu_id
            )));
        }
        let target_enabled = require(raw.target_enabled, "target_enabled", ssu.ssu_id)?;
        let entry_policy =
            parse_entry_policy(&require(raw.entry_policy, "entry_policy", ssu.ssu_id)?)?;
        let (
            pyramid_min_profit_r_before_add,
            pyramid_stop_adjustment,
            pyramid_require_fresh_base_after_last_entry,
            pyramid_min_breakout_level_distance_atr,
            pyramid_max_active_legs,
        ) = if entry_policy == EntryPolicy::Pyramid {
            (
                require_positive(
                    raw.pyramid_min_profit_r_before_add,
                    "pyramid_min_profit_r_before_add",
                    ssu.ssu_id,
                )?,
                parse_pyramid_stop_adjustment(&require(
                    raw.pyramid_stop_adjustment,
                    "pyramid_stop_adjustment",
                    ssu.ssu_id,
                )?)?,
                require(
                    raw.pyramid_require_fresh_base_after_last_entry,
                    "pyramid_require_fresh_base_after_last_entry",
                    ssu.ssu_id,
                )?,
                require_non_negative(
                    raw.pyramid_min_breakout_level_distance_atr,
                    "pyramid_min_breakout_level_distance_atr",
                    ssu.ssu_id,
                )?,
                require(
                    raw.pyramid_max_active_legs,
                    "pyramid_max_active_legs",
                    ssu.ssu_id,
                )?,
            )
        } else {
            (0.0, PyramidStopAdjustment::None, false, 0.0, 0)
        };
        let settings = Self {
            timeframe,
            enabled_sides,
            entry_policy,
            lookback_bars: require(raw.lookback_bars, "lookback_bars", ssu.ssu_id)?,
            ema_fast_period: require(raw.ema_fast_period, "ema_fast_period", ssu.ssu_id)?,
            ema_slow_period: require(raw.ema_slow_period, "ema_slow_period", ssu.ssu_id)?,
            atr_period: require(raw.atr_period, "atr_period", ssu.ssu_id)?,
            regime_ema_slope_lookback_bars: require(
                raw.regime_ema_slope_lookback_bars,
                "regime_ema_slope_lookback_bars",
                ssu.ssu_id,
            )?,
            regime_min_fast_slope_atr: require_non_negative(
                raw.regime_min_fast_slope_atr,
                "regime_min_fast_slope_atr",
                ssu.ssu_id,
            )?,
            regime_min_ema_separation_atr: require_non_negative(
                raw.regime_min_ema_separation_atr,
                "regime_min_ema_separation_atr",
                ssu.ssu_id,
            )?,
            base_window_bars: require(raw.base_window_bars, "base_window_bars", ssu.ssu_id)?,
            base_max_range_atr: require_non_negative(
                raw.base_max_range_atr,
                "base_max_range_atr",
                ssu.ssu_id,
            )?,
            base_max_close_spread_atr: require_non_negative(
                raw.base_max_close_spread_atr,
                "base_max_close_spread_atr",
                ssu.ssu_id,
            )?,
            base_max_single_bar_range_atr: require_non_negative(
                raw.base_max_single_bar_range_atr,
                "base_max_single_bar_range_atr",
                ssu.ssu_id,
            )?,
            base_max_directional_efficiency: require_non_negative(
                raw.base_max_directional_efficiency,
                "base_max_directional_efficiency",
                ssu.ssu_id,
            )?,
            breakout_buffer_atr: require_non_negative(
                raw.breakout_buffer_atr,
                "breakout_buffer_atr",
                ssu.ssu_id,
            )?,
            breakout_min_bar_range_atr: require_non_negative(
                raw.breakout_min_bar_range_atr,
                "breakout_min_bar_range_atr",
                ssu.ssu_id,
            )?,
            breakout_min_close_location: require_ratio(
                raw.breakout_min_close_location,
                "breakout_min_close_location",
                ssu.ssu_id,
            )?,
            impulse_min_height_atr: require_non_negative(
                raw.impulse_min_height_atr,
                "impulse_min_height_atr",
                ssu.ssu_id,
            )?,
            impulse_max_bars: require(raw.impulse_max_bars, "impulse_max_bars", ssu.ssu_id)?,
            impulse_min_efficiency: require_non_negative(
                raw.impulse_min_efficiency,
                "impulse_min_efficiency",
                ssu.ssu_id,
            )?,
            pullback_min_ratio: require_non_negative(
                raw.pullback_min_ratio,
                "pullback_min_ratio",
                ssu.ssu_id,
            )?,
            pullback_max_ratio: require_non_negative(
                raw.pullback_max_ratio,
                "pullback_max_ratio",
                ssu.ssu_id,
            )?,
            pullback_min_bars: require(raw.pullback_min_bars, "pullback_min_bars", ssu.ssu_id)?,
            pullback_max_bars: require(raw.pullback_max_bars, "pullback_max_bars", ssu.ssu_id)?,
            pullback_max_counter_efficiency: require_non_negative(
                raw.pullback_max_counter_efficiency,
                "pullback_max_counter_efficiency",
                ssu.ssu_id,
            )?,
            ema_zone_buffer_atr: require_non_negative(
                raw.ema_zone_buffer_atr,
                "ema_zone_buffer_atr",
                ssu.ssu_id,
            )?,
            max_breakout_level_penetration_atr: require_non_negative(
                raw.max_breakout_level_penetration_atr,
                "max_breakout_level_penetration_atr",
                ssu.ssu_id,
            )?,
            trigger_break_lookback_bars: require(
                raw.trigger_break_lookback_bars,
                "trigger_break_lookback_bars",
                ssu.ssu_id,
            )?,
            trigger_buffer_atr: require_non_negative(
                raw.trigger_buffer_atr,
                "trigger_buffer_atr",
                ssu.ssu_id,
            )?,
            trigger_min_close_location: require_ratio(
                raw.trigger_min_close_location,
                "trigger_min_close_location",
                ssu.ssu_id,
            )?,
            max_entry_extension_atr: require_non_negative(
                raw.max_entry_extension_atr,
                "max_entry_extension_atr",
                ssu.ssu_id,
            )?,
            stop_buffer_atr: require_non_negative(
                raw.stop_buffer_atr,
                "stop_buffer_atr",
                ssu.ssu_id,
            )?,
            target_r_multiple: if target_enabled {
                Some(require_positive(
                    raw.target_r_multiple,
                    "target_r_multiple",
                    ssu.ssu_id,
                )?)
            } else {
                None
            },
            time_stop_bars: require(raw.time_stop_bars, "time_stop_bars", ssu.ssu_id)?,
            exit_on_ema_fail_bars: require(
                raw.exit_on_ema_fail_bars,
                "exit_on_ema_fail_bars",
                ssu.ssu_id,
            )?,
            pyramid_min_profit_r_before_add,
            pyramid_stop_adjustment,
            pyramid_require_fresh_base_after_last_entry,
            pyramid_min_breakout_level_distance_atr,
            pyramid_max_active_legs,
        };
        settings.validate(ssu.ssu_id)?;
        Ok(settings)
    }

    fn validate(&self, ssu_id: i64) -> Result<(), StrategyError> {
        if self.ema_fast_period == 0 || self.ema_slow_period <= self.ema_fast_period {
            return Err(StrategyError::Config(format!(
                "SSU {ssu_id} ema periods must satisfy 0 < fast < slow"
            )));
        }
        if self.atr_period == 0
            || self.regime_ema_slope_lookback_bars == 0
            || self.base_window_bars <= 1
            || self.impulse_max_bars == 0
            || self.pullback_min_bars == 0
            || self.trigger_break_lookback_bars == 0
            || self.time_stop_bars == 0
            || self.exit_on_ema_fail_bars == 0
        {
            return Err(StrategyError::Config(format!(
                "SSU {ssu_id} ema_pullback_scalp bar counts must be positive"
            )));
        }
        if self.pullback_max_bars < self.pullback_min_bars
            || self.pullback_max_ratio <= self.pullback_min_ratio
        {
            return Err(StrategyError::Config(format!(
                "SSU {ssu_id} ema_pullback_scalp pullback bounds are invalid"
            )));
        }
        if self.lookback_bars < self.min_required_bars() {
            return Err(StrategyError::Config(format!(
                "SSU {ssu_id} ema_pullback_scalp lookback_bars {} is below min_required_bars {}",
                self.lookback_bars,
                self.min_required_bars()
            )));
        }
        Ok(())
    }

    fn min_required_bars(&self) -> usize {
        self.ema_slow_period.max(self.atr_period)
            + self.regime_ema_slope_lookback_bars
            + self.base_window_bars
            + self.impulse_max_bars
            + self.pullback_max_bars
            + self.trigger_break_lookback_bars
            + 5
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EntryPolicy {
    SinglePosition,
    Independent,
    Pyramid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PyramidStopAdjustment {
    None,
    Breakeven,
    LatestEntrySl,
    BetterOfBreakevenOrLatestEntrySl,
}

#[derive(Clone, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
struct StateKey {
    ssu_id: i64,
    instrument: String,
}

impl StateKey {
    fn new(ssu_id: i64, instrument: &str) -> Self {
        Self {
            ssu_id,
            instrument: instrument.to_string(),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct SetupState {
    last_processed_closed_end: Option<u64>,
    entered_setup_ids: BTreeSet<String>,
    last_breakout_level: Option<f64>,
    last_entry_bar_end_at: Option<u64>,
}

#[derive(Clone, Debug)]
struct IndicatorSeries {
    ema_fast: Vec<Option<f64>>,
    ema_slow: Vec<Option<f64>>,
    atr: Vec<Option<f64>>,
}

impl IndicatorSeries {
    fn from_bars(bars: &[Bar], settings: &EmaPullbackSettings) -> Result<Self, StrategyError> {
        Ok(Self {
            ema_fast: ema_series(bars, settings.ema_fast_period)?,
            ema_slow: ema_series(bars, settings.ema_slow_period)?,
            atr: atr_series(bars, settings.atr_period)?,
        })
    }
}

#[derive(Clone, Debug)]
struct DetectedSetup {
    side: SignalSide,
    setup_id: String,
    base_start_at: u64,
    base_end_at: u64,
    breakout_bar_end_at: u64,
    breakout_level: f64,
    swing_extreme_bar_end_at: u64,
    pullback_extreme_bar_end_at: u64,
    swing_start_price: f64,
    swing_extreme_price: f64,
    pullback_extreme_price: f64,
    impulse_height: f64,
    pullback_ratio: f64,
    impulse_atr: f64,
    atr: f64,
    trigger_close: f64,
}

impl DetectedSetup {
    fn stop_price(&self, stop_buffer_atr: f64) -> f64 {
        match self.side {
            SignalSide::Long => self.pullback_extreme_price - stop_buffer_atr * self.atr,
            SignalSide::Short => self.pullback_extreme_price + stop_buffer_atr * self.atr,
        }
    }

    fn entry_reason(&self, timeframe: Timeframe) -> String {
        format!(
            "ema_pullback_entry|tf={}|side={}|impulse_atr={:.4}|pullback_ratio={:.4}|breakout_end={}",
            timeframe_label(timeframe),
            side_label(self.side),
            self.impulse_atr,
            self.pullback_ratio,
            self.breakout_bar_end_at
        )
    }

    fn entry_metadata(
        &self,
        entry_price: f64,
        stop_price: f64,
        target_price: Option<f64>,
    ) -> serde_json::Value {
        serde_json::json!({
            "setup_id": self.setup_id,
            "side": side_label(self.side),
            "entry_price": entry_price,
            "stop_price": stop_price,
            "target_price": target_price,
            "base_start_at": self.base_start_at,
            "base_end_at": self.base_end_at,
            "breakout_level": self.breakout_level,
            "swing_extreme_bar_end_at": self.swing_extreme_bar_end_at,
            "pullback_extreme_bar_end_at": self.pullback_extreme_bar_end_at,
            "impulse_height": self.impulse_height,
            "pullback_ratio": self.pullback_ratio,
            "impulse_atr": self.impulse_atr,
        })
    }

    fn trade_context_metadata(
        &self,
        signal: &StrategySignal,
        position_id: &str,
        entry_price: f64,
        stop_price: f64,
        target_price: Option<f64>,
        target_enabled: bool,
        entry_bar_end_at: u64,
    ) -> serde_json::Value {
        serde_json::json!({
            "strategy_key": signal.strategy_key,
            "campaign_id": signal.campaign_id,
            "position_id": position_id,
            "side": side_label(self.side),
            "entry_price": entry_price,
            "original_stop_price": stop_price,
            "stop_price": stop_price,
            "target_enabled": target_enabled,
            "target_price": target_price,
            "entry_bar_end_at": entry_bar_end_at,
            "setup_id": self.setup_id,
            "base_start_at": self.base_start_at,
            "base_end_at": self.base_end_at,
            "breakout_level": self.breakout_level,
            "swing_extreme_bar_end_at": self.swing_extreme_bar_end_at,
            "pullback_extreme_bar_end_at": self.pullback_extreme_bar_end_at,
            "swing_start_price": self.swing_start_price,
            "swing_extreme": self.swing_extreme_price,
            "pullback_extreme": self.pullback_extreme_price,
            "ema_fail_bars": 0_u64,
            "bars_since_entry": 0_u64
        })
    }
}

fn detect_setup(
    bars: &[Bar],
    series: &IndicatorSeries,
    settings: &EmaPullbackSettings,
    side: SignalSide,
) -> Option<DetectedSetup> {
    // State 1: Regime must already favor the requested side.
    let trigger_idx = bars.len().checked_sub(1)?;
    let atr_last = series.atr.get(trigger_idx).copied().flatten()?;
    if !regime_valid(bars, series, settings, side, trigger_idx, atr_last) {
        return None;
    }

    let min_breakout_idx = settings.base_window_bars;
    let max_breakout_idx = trigger_idx
        .checked_sub(settings.pullback_min_bars)?
        .checked_sub(1)?;

    for breakout_idx in (min_breakout_idx..=max_breakout_idx).rev() {
        // State 2: A tight base must exist immediately before breakout.
        let atr_breakout = series.atr.get(breakout_idx).copied().flatten()?;
        let base_start = breakout_idx.checked_sub(settings.base_window_bars)?;
        let base = &bars[base_start..breakout_idx];
        let (base_high, base_low) = valid_base(base, settings, atr_breakout)?;
        let breakout_bar = &bars[breakout_idx];
        let breakout_level = match side {
            SignalSide::Long => base_high,
            SignalSide::Short => base_low,
        };
        if !breakout_valid(breakout_bar, settings, side, breakout_level, atr_breakout) {
            continue;
        }

        // State 3: Breakout must become an efficient impulse within the configured window.
        let max_impulse_end = (breakout_idx + settings.impulse_max_bars).min(trigger_idx - 1);
        let Some((swing_idx, swing_extreme)) =
            swing_extreme(&bars[breakout_idx..=max_impulse_end], side)
        else {
            continue;
        };
        let swing_idx = breakout_idx + swing_idx;
        if swing_idx >= trigger_idx {
            continue;
        }
        let swing_start_price = match side {
            SignalSide::Long => breakout_bar.low,
            SignalSide::Short => breakout_bar.high,
        };
        let impulse_height = match side {
            SignalSide::Long => swing_extreme - swing_start_price,
            SignalSide::Short => swing_start_price - swing_extreme,
        };
        if impulse_height <= 0.0 {
            continue;
        }
        let impulse_atr = impulse_height / atr_breakout;
        if impulse_atr < settings.impulse_min_height_atr {
            continue;
        }
        let impulse_efficiency = efficiency(&bars[breakout_idx..=swing_idx])?;
        if impulse_efficiency < settings.impulse_min_efficiency {
            continue;
        }

        // State 4: Impulse must retrace into a healthy EMA-zone pullback.
        let pullback_start = swing_idx + 1;
        let pullback_end = trigger_idx - 1;
        if pullback_end < pullback_start {
            continue;
        }
        let pullback_bars = pullback_end - pullback_start + 1;
        if pullback_bars < settings.pullback_min_bars || pullback_bars > settings.pullback_max_bars
        {
            continue;
        }
        let (pullback_idx, pullback_extreme) =
            pullback_extreme(&bars[pullback_start..=pullback_end], side)?;
        let pullback_idx = pullback_start + pullback_idx;
        let pullback_depth = match side {
            SignalSide::Long => swing_extreme - pullback_extreme,
            SignalSide::Short => pullback_extreme - swing_extreme,
        };
        let pullback_ratio = pullback_depth / impulse_height;
        if pullback_ratio < settings.pullback_min_ratio
            || pullback_ratio > settings.pullback_max_ratio
        {
            continue;
        }
        let pullback_counter_efficiency = efficiency(&bars[pullback_start..=pullback_end])?;
        if pullback_counter_efficiency > settings.pullback_max_counter_efficiency {
            continue;
        }
        let atr_pullback = series.atr.get(pullback_idx).copied().flatten()?;
        let ema_fast = series.ema_fast.get(pullback_idx).copied().flatten()?;
        let ema_slow = series.ema_slow.get(pullback_idx).copied().flatten()?;
        if !pullback_touches_ema_zone(
            pullback_extreme,
            ema_fast,
            ema_slow,
            atr_pullback,
            settings.ema_zone_buffer_atr,
        ) {
            continue;
        }
        if !breakout_level_respected(
            pullback_extreme,
            breakout_level,
            side,
            atr_pullback,
            settings.max_breakout_level_penetration_atr,
        ) {
            continue;
        }
        let trigger_bar = &bars[trigger_idx];

        // State 5: Latest candle must prove renewed continuation before entry.
        if !trigger_valid(
            &bars[pullback_start..=pullback_end],
            trigger_bar,
            side,
            settings,
            atr_last,
            series.ema_fast[trigger_idx]?,
            series.ema_slow[trigger_idx]?,
        ) {
            continue;
        }

        return Some(DetectedSetup {
            side,
            setup_id: format!(
                "{}|{}|{}|{}|{}|{}|{}|{}",
                bars[trigger_idx].instrument,
                timeframe_label(bars[trigger_idx].timeframe),
                side_label(side),
                bars[base_start].start_at,
                bars[breakout_idx - 1].end_at,
                breakout_bar.end_at,
                bars[swing_idx].end_at,
                bars[pullback_idx].end_at
            ),
            base_start_at: bars[base_start].start_at,
            base_end_at: bars[breakout_idx - 1].end_at,
            breakout_bar_end_at: breakout_bar.end_at,
            breakout_level,
            swing_extreme_bar_end_at: bars[swing_idx].end_at,
            pullback_extreme_bar_end_at: bars[pullback_idx].end_at,
            swing_start_price,
            swing_extreme_price: swing_extreme,
            pullback_extreme_price: pullback_extreme,
            impulse_height,
            pullback_ratio,
            impulse_atr,
            atr: atr_last,
            trigger_close: trigger_bar.close,
        });
    }

    None
}

fn regime_valid(
    _bars: &[Bar],
    series: &IndicatorSeries,
    settings: &EmaPullbackSettings,
    side: SignalSide,
    index: usize,
    atr: f64,
) -> bool {
    let Some(past_index) = index.checked_sub(settings.regime_ema_slope_lookback_bars) else {
        return false;
    };
    let Some(ema_fast) = series.ema_fast[index] else {
        return false;
    };
    let Some(ema_slow) = series.ema_slow[index] else {
        return false;
    };
    let Some(ema_fast_past) = series.ema_fast[past_index] else {
        return false;
    };
    let slope_atr = (ema_fast - ema_fast_past) / atr;
    let separation_atr = (ema_fast - ema_slow).abs() / atr;
    match side {
        SignalSide::Long => {
            ema_fast > ema_slow
                && slope_atr >= settings.regime_min_fast_slope_atr
                && separation_atr >= settings.regime_min_ema_separation_atr
        }
        SignalSide::Short => {
            ema_fast < ema_slow
                && slope_atr <= -settings.regime_min_fast_slope_atr
                && separation_atr >= settings.regime_min_ema_separation_atr
        }
    }
}

fn valid_base(bars: &[Bar], settings: &EmaPullbackSettings, atr: f64) -> Option<(f64, f64)> {
    let base_high = bars.iter().map(|bar| bar.high).fold(f64::MIN, f64::max);
    let base_low = bars.iter().map(|bar| bar.low).fold(f64::MAX, f64::min);
    let close_high = bars.iter().map(|bar| bar.close).fold(f64::MIN, f64::max);
    let close_low = bars.iter().map(|bar| bar.close).fold(f64::MAX, f64::min);
    let max_single_bar_range = bars
        .iter()
        .map(|bar| bar.high - bar.low)
        .fold(0.0, f64::max);
    let base_efficiency = efficiency(bars)?;
    if (base_high - base_low) / atr <= settings.base_max_range_atr
        && (close_high - close_low) / atr <= settings.base_max_close_spread_atr
        && max_single_bar_range / atr <= settings.base_max_single_bar_range_atr
        && base_efficiency <= settings.base_max_directional_efficiency
    {
        Some((base_high, base_low))
    } else {
        None
    }
}

fn breakout_valid(
    bar: &Bar,
    settings: &EmaPullbackSettings,
    side: SignalSide,
    breakout_level: f64,
    atr: f64,
) -> bool {
    let bar_range_atr = (bar.high - bar.low) / atr;
    if bar_range_atr < settings.breakout_min_bar_range_atr {
        return false;
    }
    let Some(close_location) = close_location(bar, side) else {
        return false;
    };
    if close_location < settings.breakout_min_close_location {
        return false;
    }
    match side {
        SignalSide::Long => bar.close > breakout_level + settings.breakout_buffer_atr * atr,
        SignalSide::Short => bar.close < breakout_level - settings.breakout_buffer_atr * atr,
    }
}

fn trigger_valid(
    pullback_bars: &[Bar],
    trigger_bar: &Bar,
    side: SignalSide,
    settings: &EmaPullbackSettings,
    atr: f64,
    ema_fast: f64,
    ema_slow: f64,
) -> bool {
    if pullback_bars.len() < settings.trigger_break_lookback_bars {
        return false;
    }
    let recent = &pullback_bars[pullback_bars.len() - settings.trigger_break_lookback_bars..];
    let Some(close_location) = close_location(trigger_bar, side) else {
        return false;
    };
    if close_location < settings.trigger_min_close_location {
        return false;
    }
    match side {
        SignalSide::Long => {
            let reference = recent.iter().map(|bar| bar.high).fold(f64::MIN, f64::max);
            let extension = trigger_bar.close - ema_fast.max(ema_slow);
            trigger_bar.close > reference + settings.trigger_buffer_atr * atr
                && extension / atr <= settings.max_entry_extension_atr
        }
        SignalSide::Short => {
            let reference = recent.iter().map(|bar| bar.low).fold(f64::MAX, f64::min);
            let extension = ema_fast.min(ema_slow) - trigger_bar.close;
            trigger_bar.close < reference - settings.trigger_buffer_atr * atr
                && extension / atr <= settings.max_entry_extension_atr
        }
    }
}

fn ema_series(bars: &[Bar], period: usize) -> Result<Vec<Option<f64>>, StrategyError> {
    if period == 0 {
        return Err(StrategyError::Config(
            "EMA period must be positive".to_string(),
        ));
    }
    let mut values = vec![None; bars.len()];
    if bars.len() < period {
        return Ok(values);
    }
    let alpha = 2.0 / (period as f64 + 1.0);
    let mut ema = bars[..period].iter().map(|bar| bar.close).sum::<f64>() / period as f64;
    values[period - 1] = Some(ema);
    for index in period..bars.len() {
        ema = alpha * bars[index].close + (1.0 - alpha) * ema;
        values[index] = Some(ema);
    }
    Ok(values)
}

fn atr_series(bars: &[Bar], period: usize) -> Result<Vec<Option<f64>>, StrategyError> {
    if period == 0 {
        return Err(StrategyError::Config(
            "ATR period must be positive".to_string(),
        ));
    }
    let mut values = vec![None; bars.len()];
    if bars.len() <= period {
        return Ok(values);
    }
    let mut true_ranges = vec![0.0; bars.len()];
    for index in 1..bars.len() {
        true_ranges[index] = (bars[index].high - bars[index].low)
            .max((bars[index].high - bars[index - 1].close).abs())
            .max((bars[index].low - bars[index - 1].close).abs());
    }
    for index in period..bars.len() {
        let start = index + 1 - period;
        values[index] = Some(true_ranges[start..=index].iter().sum::<f64>() / period as f64);
    }
    Ok(values)
}

fn efficiency(bars: &[Bar]) -> Option<f64> {
    if bars.len() < 2 {
        return None;
    }
    let direct = (bars.last()?.close - bars.first()?.close).abs();
    let travel = bars
        .windows(2)
        .map(|window| (window[1].close - window[0].close).abs())
        .sum::<f64>();
    if travel <= 0.0 {
        None
    } else {
        Some(direct / travel)
    }
}

fn swing_extreme(bars: &[Bar], side: SignalSide) -> Option<(usize, f64)> {
    match side {
        SignalSide::Long => bars
            .iter()
            .enumerate()
            .max_by(|(_, left), (_, right)| left.high.total_cmp(&right.high))
            .map(|(index, bar)| (index, bar.high)),
        SignalSide::Short => bars
            .iter()
            .enumerate()
            .min_by(|(_, left), (_, right)| left.low.total_cmp(&right.low))
            .map(|(index, bar)| (index, bar.low)),
    }
}

fn pullback_extreme(bars: &[Bar], side: SignalSide) -> Option<(usize, f64)> {
    match side {
        SignalSide::Long => bars
            .iter()
            .enumerate()
            .min_by(|(_, left), (_, right)| left.low.total_cmp(&right.low))
            .map(|(index, bar)| (index, bar.low)),
        SignalSide::Short => bars
            .iter()
            .enumerate()
            .max_by(|(_, left), (_, right)| left.high.total_cmp(&right.high))
            .map(|(index, bar)| (index, bar.high)),
    }
}

fn pullback_touches_ema_zone(
    price: f64,
    ema_fast: f64,
    ema_slow: f64,
    atr: f64,
    buffer_atr: f64,
) -> bool {
    price >= ema_fast.min(ema_slow) - buffer_atr * atr
        && price <= ema_fast.max(ema_slow) + buffer_atr * atr
}

fn breakout_level_respected(
    pullback_extreme: f64,
    breakout_level: f64,
    side: SignalSide,
    atr: f64,
    penetration_atr: f64,
) -> bool {
    match side {
        SignalSide::Long => pullback_extreme >= breakout_level - penetration_atr * atr,
        SignalSide::Short => pullback_extreme <= breakout_level + penetration_atr * atr,
    }
}

fn close_location(bar: &Bar, side: SignalSide) -> Option<f64> {
    let range = bar.high - bar.low;
    if range <= 0.0 {
        return None;
    }
    Some(match side {
        SignalSide::Long => (bar.close - bar.low) / range,
        SignalSide::Short => (bar.high - bar.close) / range,
    })
}

fn current_ltp(ctx: &StrategyContext, instrument: &str) -> Option<f64> {
    ctx.prices
        .get_price(instrument)
        .map(|snapshot| snapshot.ltp)
}

fn require<T>(value: Option<T>, field: &str, ssu_id: i64) -> Result<T, StrategyError> {
    value.ok_or_else(|| {
        StrategyError::Config(format!(
            "SSU {ssu_id} ema_pullback_scalp missing required field {field}"
        ))
    })
}

fn require_non_negative(
    value: Option<f64>,
    field: &str,
    ssu_id: i64,
) -> Result<f64, StrategyError> {
    let value = require(value, field, ssu_id)?;
    if value.is_finite() && value >= 0.0 {
        Ok(value)
    } else {
        Err(StrategyError::Config(format!(
            "SSU {ssu_id} ema_pullback_scalp {field} must be finite and non-negative"
        )))
    }
}

fn require_positive(value: Option<f64>, field: &str, ssu_id: i64) -> Result<f64, StrategyError> {
    let value = require(value, field, ssu_id)?;
    if value.is_finite() && value > 0.0 {
        Ok(value)
    } else {
        Err(StrategyError::Config(format!(
            "SSU {ssu_id} ema_pullback_scalp {field} must be finite and positive"
        )))
    }
}

fn require_ratio(value: Option<f64>, field: &str, ssu_id: i64) -> Result<f64, StrategyError> {
    let value = require_non_negative(value, field, ssu_id)?;
    if value <= 1.0 {
        Ok(value)
    } else {
        Err(StrategyError::Config(format!(
            "SSU {ssu_id} ema_pullback_scalp {field} must be between 0 and 1"
        )))
    }
}

fn required_f64(metadata: &serde_json::Value, field: &str) -> Result<f64, StrategyError> {
    metadata
        .get(field)
        .and_then(serde_json::Value::as_f64)
        .ok_or_else(|| StrategyError::Parse(format!("metadata missing numeric field {field}")))
}

fn optional_f64(metadata: &serde_json::Value, field: &str) -> Option<f64> {
    metadata.get(field).and_then(serde_json::Value::as_f64)
}

fn required_u64(metadata: &serde_json::Value, field: &str) -> Result<u64, StrategyError> {
    metadata
        .get(field)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| StrategyError::Parse(format!("metadata missing integer field {field}")))
}

fn required_bool(metadata: &serde_json::Value, field: &str) -> Result<bool, StrategyError> {
    metadata
        .get(field)
        .and_then(serde_json::Value::as_bool)
        .ok_or_else(|| StrategyError::Parse(format!("metadata missing bool field {field}")))
}

fn required_string(metadata: &serde_json::Value, field: &str) -> Result<String, StrategyError> {
    metadata
        .get(field)
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| StrategyError::Parse(format!("metadata missing string field {field}")))
}

fn parse_entry_policy(value: &str) -> Result<EntryPolicy, StrategyError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "single_position" => Ok(EntryPolicy::SinglePosition),
        "independent" => Ok(EntryPolicy::Independent),
        "pyramid" => Ok(EntryPolicy::Pyramid),
        other => Err(StrategyError::Parse(format!(
            "unsupported ema_pullback_scalp entry_policy {other}"
        ))),
    }
}

fn parse_pyramid_stop_adjustment(value: &str) -> Result<PyramidStopAdjustment, StrategyError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "none" => Ok(PyramidStopAdjustment::None),
        "breakeven" => Ok(PyramidStopAdjustment::Breakeven),
        "latest_entry_sl" => Ok(PyramidStopAdjustment::LatestEntrySl),
        "better_of_breakeven_or_latest_entry_sl" => {
            Ok(PyramidStopAdjustment::BetterOfBreakevenOrLatestEntrySl)
        }
        other => Err(StrategyError::Parse(format!(
            "unsupported ema_pullback_scalp pyramid_stop_adjustment {other}"
        ))),
    }
}

fn parse_side(value: &str) -> Result<SignalSide, StrategyError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "long" => Ok(SignalSide::Long),
        "short" => Ok(SignalSide::Short),
        other => Err(StrategyError::Parse(format!(
            "unsupported ema_pullback_scalp side {other}; expected long or short"
        ))),
    }
}

fn parse_timeframe(value: &str) -> Result<Timeframe, StrategyError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1m" | "one_minute" | "oneminute" => Ok(Timeframe::OneMinute),
        "3m" | "three_minute" | "threeminute" => Ok(Timeframe::ThreeMinute),
        "5m" | "five_minute" | "fiveminute" => Ok(Timeframe::FiveMinute),
        "15m" | "fifteen_minute" | "fifteenminute" => Ok(Timeframe::FifteenMinute),
        "1h" | "one_hour" | "onehour" => Ok(Timeframe::OneHour),
        "1d" | "one_day" | "oneday" => Ok(Timeframe::OneDay),
        other => Err(StrategyError::Parse(format!(
            "unsupported ema_pullback_scalp timeframe {other}"
        ))),
    }
}

fn timeframe_label(timeframe: Timeframe) -> &'static str {
    match timeframe {
        Timeframe::OneMinute => "1m",
        Timeframe::ThreeMinute => "3m",
        Timeframe::FiveMinute => "5m",
        Timeframe::FifteenMinute => "15m",
        Timeframe::OneHour => "1h",
        Timeframe::OneDay => "1d",
    }
}

fn side_label(side: SignalSide) -> &'static str {
    match side {
        SignalSide::Long => "long",
        SignalSide::Short => "short",
    }
}

fn signal_type_label_for_side(side: SignalSide, entry: bool) -> &'static str {
    match (side, entry) {
        (SignalSide::Long, true) => "ENTRY_LONG",
        (SignalSide::Long, false) => "EXIT_LONG",
        (SignalSide::Short, true) => "ENTRY_SHORT",
        (SignalSide::Short, false) => "EXIT_SHORT",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn complete_params() -> String {
        serde_json::json!({
            "timeframe": "1m",
            "enabled_sides": ["long"],
            "entry_policy": "pyramid",
            "lookback_bars": 80,
            "ema_fast_period": 3,
            "ema_slow_period": 5,
            "atr_period": 3,
            "regime_ema_slope_lookback_bars": 2,
            "regime_min_fast_slope_atr": 0.01,
            "regime_min_ema_separation_atr": 0.01,
            "base_window_bars": 5,
            "base_max_range_atr": 2.0,
            "base_max_close_spread_atr": 2.0,
            "base_max_single_bar_range_atr": 2.0,
            "base_max_directional_efficiency": 0.8,
            "breakout_buffer_atr": 0.01,
            "breakout_min_bar_range_atr": 0.1,
            "breakout_min_close_location": 0.5,
            "impulse_min_height_atr": 0.5,
            "impulse_max_bars": 6,
            "impulse_min_efficiency": 0.4,
            "pullback_min_ratio": 0.1,
            "pullback_max_ratio": 0.7,
            "pullback_min_bars": 1,
            "pullback_max_bars": 5,
            "pullback_max_counter_efficiency": 1.0,
            "ema_zone_buffer_atr": 2.0,
            "max_breakout_level_penetration_atr": 2.0,
            "trigger_break_lookback_bars": 1,
            "trigger_buffer_atr": 0.01,
            "trigger_min_close_location": 0.5,
            "max_entry_extension_atr": 5.0,
            "stop_buffer_atr": 0.1,
            "target_enabled": false,
            "time_stop_bars": 6,
            "exit_on_ema_fail_bars": 2,
            "pyramid_min_profit_r_before_add": 0.5,
            "pyramid_stop_adjustment": "better_of_breakeven_or_latest_entry_sl",
            "pyramid_require_fresh_base_after_last_entry": true,
            "pyramid_min_breakout_level_distance_atr": 0.1,
            "pyramid_max_active_legs": 0
        })
        .to_string()
    }

    #[test]
    fn settings_parser_requires_entry_policy() {
        let mut value: serde_json::Value = serde_json::from_str(&complete_params()).expect("json");
        value
            .as_object_mut()
            .expect("object")
            .remove("entry_policy");
        let ssu = SsuConfig {
            ssu_id: 1,
            strategy_key: "ema_pullback_scalp".to_string(),
            enabled: true,
            trade_gap_secs: 0,
            max_overlap: 0,
            max_positions_per_day: 0,
            required_timeframes: vec![Timeframe::OneMinute],
            indicator_specs: Vec::new(),
            params_json: value.to_string(),
        };
        assert!(matches!(
            EmaPullbackSettings::from_ssu(&ssu),
            Err(StrategyError::Config(_))
        ));
    }

    #[test]
    fn settings_parser_accepts_complete_pyramid_config() {
        let ssu = SsuConfig {
            ssu_id: 1,
            strategy_key: "ema_pullback_scalp".to_string(),
            enabled: true,
            trade_gap_secs: 0,
            max_overlap: 0,
            max_positions_per_day: 0,
            required_timeframes: vec![Timeframe::OneMinute],
            indicator_specs: Vec::new(),
            params_json: complete_params(),
        };
        let settings = EmaPullbackSettings::from_ssu(&ssu).expect("settings");
        assert_eq!(settings.entry_policy, EntryPolicy::Pyramid);
        assert_eq!(
            settings.pyramid_stop_adjustment,
            PyramidStopAdjustment::BetterOfBreakevenOrLatestEntrySl
        );
        assert!(settings.target_r_multiple.is_none());
    }

    #[test]
    fn ema_series_seeds_with_sma() {
        let bars = (1..=5)
            .map(|close| Bar {
                instrument: "BTCUSD".to_string(),
                timeframe: Timeframe::OneMinute,
                start_at: close * 60_000,
                end_at: (close + 1) * 60_000,
                open: close as f64,
                high: close as f64,
                low: close as f64,
                close: close as f64,
                is_closed: true,
            })
            .collect::<Vec<_>>();
        let ema = ema_series(&bars, 3).expect("ema");
        assert_eq!(ema[2], Some(2.0));
        assert_eq!(ema[3], Some(3.0));
    }
}
