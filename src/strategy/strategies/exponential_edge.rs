use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Mutex;

use serde::Deserialize;

use crate::strategy::{
    Bar, PositionStatus, PriceUpdated, SignalSide, SsuConfig, Strategy, StrategyContext,
    StrategyError, StrategySignal, Timeframe, TimeframeUpdate,
};

const SETUP_ID_CAPACITY: usize = 128;
const STATE_SMALL_BUFFER: usize = 2;
const DEFAULT_PROFIT_TRAIL_ACTIVATION_R: f64 = 3.0;
const DEFAULT_PROFIT_TRAIL_GIVEBACK_PCT: f64 = 0.35;
const DEFAULT_PROFIT_TRAIL_MIN_LOCK_R: f64 = 1.0;

#[derive(Debug, Default)]
pub(crate) struct ExponentialEdgeStrategy {
    settings: Mutex<BTreeMap<i64, ExponentialEdgeSettings>>,
    states: Mutex<BTreeMap<StateKey, RuntimeState>>,
}

impl Strategy for ExponentialEdgeStrategy {
    fn strategy_key(&self) -> &'static str {
        "exponential_edge"
    }

    fn on_price_updated(
        &self,
        ctx: &StrategyContext,
        ssu: &SsuConfig,
        event: &PriceUpdated,
        tf_update: &TimeframeUpdate,
    ) -> Result<Vec<StrategySignal>, StrategyError> {
        let settings = self.settings_for(ssu)?;
        if !tf_update.closed_timeframes.contains(&settings.timeframe) {
            return Ok(Vec::new());
        }

        let Some(closed_bar) = ctx
            .timeframes
            .last_closed_bar(&event.trigger_instrument, settings.timeframe)
        else {
            return Ok(Vec::new());
        };

        let state_key = StateKey::new(ssu.ssu_id, &event.trigger_instrument, settings.timeframe);
        let update = self.bootstrap_and_update_current(
            ctx,
            &state_key,
            &settings,
            &closed_bar,
            ssu.ssu_id,
            &event.trigger_instrument,
        )?;
        let CurrentBarUpdate::Prepared { warmup_bars } = update else {
            return Ok(Vec::new());
        };
        for bar in &warmup_bars {
            self.inspect_recovery_breach(ctx, ssu, &event.trigger_instrument, bar)?;
        }
        self.mark_warmup_recovery_inspected(&state_key)?;

        let indicator_point = {
            let states = self
                .states
                .lock()
                .expect("exponential edge state lock poisoned");
            let Some(state) = states.get(&state_key) else {
                return Ok(Vec::new());
            };
            state.indicators.latest_point()
        };

        let mut exits =
            self.manage_open_positions(ctx, ssu, event, &settings, &closed_bar, indicator_point)?;
        if !exits.is_empty() {
            self.finalize_current_bar(&state_key, &closed_bar)?;
            return Ok(exits);
        }

        let entry_candidates =
            self.detect_entry_candidates(&state_key, ssu, &settings, &closed_bar)?;

        for setup in entry_candidates {
            if !self.entry_policy_allows(ctx, ssu, &event.trigger_instrument, &setup)? {
                continue;
            }

            let mut entry_signal = StrategySignal::single_leg_entry(
                ssu.ssu_id,
                self.strategy_key(),
                &event.trigger_instrument,
                setup.side,
                setup.entry_price,
                setup.entry_reason(),
                closed_bar.end_at,
            );
            apply_entry_identity(&mut entry_signal, &event.trigger_instrument, &setup);
            entry_signal.metadata = setup.entry_metadata();
            entry_signal.instructions[0].metadata = serde_json::json!({
                "setup_id": setup.setup_id,
                "stop_price": setup.stop_price,
                "target_enabled": setup.target_enabled,
                "target_price": setup.target_price,
                "profit_trail_enabled": setup.profit_trail_enabled,
                "profit_trail_activation_r": setup.profit_trail_activation_r,
                "profit_trail_giveback_pct": setup.profit_trail_giveback_pct,
                "profit_trail_min_lock_r": setup.profit_trail_min_lock_r,
            });

            let position_id = entry_signal.instructions[0].leg_id.clone();
            let metadata = setup.trade_context_metadata(&position_id);
            ctx.trade_contexts.save_context(
                &position_id,
                ssu.ssu_id,
                self.strategy_key(),
                &event.trigger_instrument,
                &metadata,
                closed_bar.end_at,
            )?;

            let position = match ctx.strategy_positions.open_position(&entry_signal, ssu) {
                Ok(position) => position,
                Err(StrategyError::Rule(_)) => {
                    let _ = ctx.trade_contexts.delete_context(&position_id);
                    continue;
                }
                Err(error) => {
                    let _ = ctx.trade_contexts.delete_context(&position_id);
                    return Err(error);
                }
            };
            if position.position_id != position_id {
                return Err(StrategyError::Rule(format!(
                    "exponential_edge position id mismatch: expected {position_id}, got {}",
                    position.position_id
                )));
            }

            self.mark_setup_emitted(&state_key, setup.side, &setup.setup_id)?;
            self.finalize_current_bar(&state_key, &closed_bar)?;
            exits.push(entry_signal);
            return Ok(exits);
        }

        self.finalize_current_bar(&state_key, &closed_bar)?;
        Ok(Vec::new())
    }
}

impl ExponentialEdgeStrategy {
    fn settings_for(&self, ssu: &SsuConfig) -> Result<ExponentialEdgeSettings, StrategyError> {
        if let Some(settings) = self
            .settings
            .lock()
            .expect("exponential edge settings lock poisoned")
            .get(&ssu.ssu_id)
            .cloned()
        {
            return Ok(settings);
        }

        let settings = ExponentialEdgeSettings::from_ssu(ssu)?;
        self.settings
            .lock()
            .expect("exponential edge settings lock poisoned")
            .insert(ssu.ssu_id, settings.clone());
        Ok(settings)
    }

    fn bootstrap_and_update_current(
        &self,
        ctx: &StrategyContext,
        state_key: &StateKey,
        settings: &ExponentialEdgeSettings,
        closed_bar: &Bar,
        ssu_id: i64,
        instrument: &str,
    ) -> Result<CurrentBarUpdate, StrategyError> {
        let mut states = self
            .states
            .lock()
            .expect("exponential edge state lock poisoned");
        let state = states
            .entry(state_key.clone())
            .or_insert_with(|| RuntimeState::new(settings));

        if state
            .last_processed_closed_end
            .is_some_and(|end_at| closed_bar.end_at <= end_at)
        {
            return Ok(CurrentBarUpdate::AlreadyProcessed);
        }

        let mut warmup_bars = state.pending_recovery_warmup_bars.clone();
        if state.is_empty() {
            warmup_bars.clear();
            let bars = ctx.timeframes.recent_bars(
                instrument,
                settings.timeframe,
                settings.state_capacity() + 1,
            );
            for bar in bars
                .into_iter()
                .filter(|bar| bar.end_at < closed_bar.end_at)
            {
                if state
                    .last_processed_closed_end
                    .is_some_and(|end_at| bar.end_at <= end_at)
                {
                    continue;
                }
                state.process_warmup_bar(&bar)?;
                warmup_bars.push(bar);
            }
            state.pending_recovery_warmup_bars = warmup_bars.clone();
        }

        if closed_bar.instrument != instrument || closed_bar.timeframe != settings.timeframe {
            return Err(StrategyError::Rule(format!(
                "SSU {ssu_id} exponential_edge got unexpected closed bar {} {:?}",
                closed_bar.instrument, closed_bar.timeframe
            )));
        }

        state.prepare_current_bar(closed_bar)?;
        Ok(CurrentBarUpdate::Prepared { warmup_bars })
    }

    fn detect_entry_candidates(
        &self,
        state_key: &StateKey,
        ssu: &SsuConfig,
        settings: &ExponentialEdgeSettings,
        closed_bar: &Bar,
    ) -> Result<Vec<DetectedSetup>, StrategyError> {
        let states = self
            .states
            .lock()
            .expect("exponential edge state lock poisoned");
        let Some(state) = states.get(state_key) else {
            return Ok(Vec::new());
        };
        if !state.entry_ready(settings) {
            return Ok(Vec::new());
        }
        let Some(point) = state.indicators.latest_point() else {
            return Ok(Vec::new());
        };
        let Some(summary) = state.trend.query_active() else {
            return Ok(Vec::new());
        };

        let mut candidates = Vec::new();
        for side in &settings.enabled_sides {
            let side_state = state.side_state(*side);
            if let Some(setup) = detect_setup(
                ssu,
                settings,
                closed_bar,
                &state.trend,
                &summary,
                point,
                *side,
            )? {
                if side_state.contains(&setup.setup_id) {
                    continue;
                }
                candidates.push(setup);
            }
        }
        Ok(candidates)
    }

    fn finalize_current_bar(
        &self,
        state_key: &StateKey,
        closed_bar: &Bar,
    ) -> Result<(), StrategyError> {
        let mut states = self
            .states
            .lock()
            .expect("exponential edge state lock poisoned");
        let Some(state) = states.get_mut(state_key) else {
            return Ok(());
        };
        state.finalize_current_bar(closed_bar)
    }

    fn mark_warmup_recovery_inspected(&self, state_key: &StateKey) -> Result<(), StrategyError> {
        let mut states = self
            .states
            .lock()
            .expect("exponential edge state lock poisoned");
        let Some(state) = states.get_mut(state_key) else {
            return Ok(());
        };
        state.pending_recovery_warmup_bars.clear();
        Ok(())
    }

    fn mark_setup_emitted(
        &self,
        state_key: &StateKey,
        side: SignalSide,
        setup_id: &str,
    ) -> Result<(), StrategyError> {
        let mut states = self
            .states
            .lock()
            .expect("exponential edge state lock poisoned");
        let Some(state) = states.get_mut(state_key) else {
            return Ok(());
        };
        state.side_state_mut(side).remember(setup_id.to_string());
        Ok(())
    }

    fn entry_policy_allows(
        &self,
        ctx: &StrategyContext,
        ssu: &SsuConfig,
        instrument: &str,
        _setup: &DetectedSetup,
    ) -> Result<bool, StrategyError> {
        let open_positions = ctx.strategy_positions.list_open_by_ssu(ssu.ssu_id)?;
        Ok(!open_positions.into_iter().any(|position| {
            position.trigger_instrument == instrument
                && position.status == PositionStatus::Open
                && position.trade_instrument == instrument
        }))
    }

    fn inspect_recovery_breach(
        &self,
        ctx: &StrategyContext,
        ssu: &SsuConfig,
        instrument: &str,
        warmup_bar: &Bar,
    ) -> Result<(), StrategyError> {
        let open_positions = ctx.strategy_positions.list_open_by_ssu(ssu.ssu_id)?;
        for position in open_positions
            .into_iter()
            .filter(|position| position.trade_instrument == instrument)
            .filter(|position| position.status == PositionStatus::Open)
        {
            let Some(metadata) = ctx.trade_contexts.load_context(&position.position_id)? else {
                return Err(StrategyError::Parse(format!(
                    "exponential_edge missing trade context for open position {} SSU {} instrument {}",
                    position.position_id, ssu.ssu_id, instrument
                )));
            };
            let mut context = TradeContext::from_metadata(&metadata, &position.position_id)?;
            if warmup_bar.end_at <= context.last_exit_check_bar_end_at
                || context.recovery_breach_detected
            {
                continue;
            }

            if let Some(reason) = stop_or_target_breach(position.side, &context, warmup_bar) {
                context.recovery_breach_detected = true;
                context.recovery_breach_reason = Some(reason.to_string());
                context.recovery_breach_bar_end_at = Some(warmup_bar.end_at);
                context.last_exit_check_bar_end_at = warmup_bar.end_at;
                let value = context.to_metadata();
                ctx.trade_contexts.update_context(
                    &position.position_id,
                    &value,
                    warmup_bar.end_at,
                )?;
            } else if profit_trail_breach(position.side, &context, warmup_bar).is_some() {
                context.recovery_breach_detected = true;
                context.recovery_breach_reason = Some("profit_trail".to_string());
                context.recovery_breach_bar_end_at = Some(warmup_bar.end_at);
                context.last_exit_check_bar_end_at = warmup_bar.end_at;
                let value = context.to_metadata();
                ctx.trade_contexts.update_context(
                    &position.position_id,
                    &value,
                    warmup_bar.end_at,
                )?;
            } else {
                update_profit_trail(position.side, &mut context, warmup_bar)?;
                context.last_exit_check_bar_end_at = warmup_bar.end_at;
                let value = context.to_metadata();
                ctx.trade_contexts.update_context(
                    &position.position_id,
                    &value,
                    warmup_bar.end_at,
                )?;
            }
        }
        Ok(())
    }

    fn manage_open_positions(
        &self,
        ctx: &StrategyContext,
        ssu: &SsuConfig,
        event: &PriceUpdated,
        settings: &ExponentialEdgeSettings,
        closed_bar: &Bar,
        point: Option<IndicatorPoint>,
    ) -> Result<Vec<StrategySignal>, StrategyError> {
        let mut exits = Vec::new();
        let open_positions = ctx.strategy_positions.list_open_by_ssu(ssu.ssu_id)?;
        for position in open_positions
            .into_iter()
            .filter(|position| position.trade_instrument == event.trigger_instrument)
            .filter(|position| position.status == PositionStatus::Open)
        {
            let Some(metadata) = ctx.trade_contexts.load_context(&position.position_id)? else {
                return Err(StrategyError::Parse(format!(
                    "exponential_edge missing trade context for open position {} SSU {} instrument {}",
                    position.position_id, ssu.ssu_id, event.trigger_instrument
                )));
            };
            let mut context = TradeContext::from_metadata(&metadata, &position.position_id)?;

            let mut exit = None;
            if context.recovery_breach_detected {
                exit = Some(ExitDecision {
                    reason: "recovery_breach".to_string(),
                    reference_price: current_ltp(ctx, &event.trigger_instrument)
                        .unwrap_or(closed_bar.close),
                });
            } else if let Some(reason) = stop_or_target_breach(position.side, &context, closed_bar)
            {
                let reference_price = match reason {
                    "stop" => context.stop_price,
                    "target" => context.target_price.unwrap_or(closed_bar.close),
                    _ => closed_bar.close,
                };
                exit = Some(ExitDecision {
                    reason: reason.to_string(),
                    reference_price,
                });
            } else if let Some(reference_price) =
                profit_trail_breach(position.side, &context, closed_bar)
            {
                exit = Some(ExitDecision {
                    reason: "profit_trail".to_string(),
                    reference_price,
                });
            } else if let Some(point) = point {
                match position.side {
                    SignalSide::Long => {
                        if closed_bar.close < point.ema {
                            context.ema_fail_bars = context.ema_fail_bars.saturating_add(1);
                        } else {
                            context.ema_fail_bars = 0;
                        }
                    }
                    SignalSide::Short => {
                        if closed_bar.close > point.ema {
                            context.ema_fail_bars = context.ema_fail_bars.saturating_add(1);
                        } else {
                            context.ema_fail_bars = 0;
                        }
                    }
                }
                if context.ema_fail_bars >= settings.exit_on_ema_fail_bars as u64 {
                    exit = Some(ExitDecision {
                        reason: "ema_fail".to_string(),
                        reference_price: closed_bar.close,
                    });
                }
            }

            context.last_exit_check_bar_end_at = closed_bar.end_at;

            if let Some(exit) = exit {
                let mut signal = StrategySignal::single_leg_exit(
                    ssu.ssu_id,
                    self.strategy_key(),
                    &event.trigger_instrument,
                    position.side,
                    exit.reference_price,
                    format!(
                        "exponential_edge_exit|reason={}|tf={}|closed_bar_end={}",
                        exit.reason,
                        timeframe_label(settings.timeframe),
                        closed_bar.end_at
                    ),
                    closed_bar.end_at,
                );
                apply_exit_identity(&mut signal, &event.trigger_instrument, &position, &exit);
                signal.metadata = serde_json::json!({
                    "exit_reason": exit.reason,
                    "position_id": position.position_id,
                    "closed_bar_end": closed_bar.end_at,
                    "recovery_breach_detected": context.recovery_breach_detected,
                    "recovery_breach_reason": context.recovery_breach_reason,
                    "recovery_breach_bar_end_at": context.recovery_breach_bar_end_at,
                    "profit_trail_enabled": context.profit_trail_enabled,
                    "profit_trail_activated": context.profit_trail_activated,
                    "profit_trail_best_price": context.profit_trail_best_price,
                    "profit_trail_stop_price": context.profit_trail_stop_price,
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
                update_profit_trail(position.side, &mut context, closed_bar)?;
                let value = context.to_metadata();
                ctx.trade_contexts.update_context(
                    &position.position_id,
                    &value,
                    closed_bar.end_at,
                )?;
            }
        }

        Ok(exits)
    }
}

#[derive(Clone, Debug)]
enum CurrentBarUpdate {
    AlreadyProcessed,
    Prepared { warmup_bars: Vec<Bar> },
}

#[derive(Clone, Copy, Debug)]
struct NumericRange {
    min: f64,
    max: f64,
}

impl NumericRange {
    fn new(min: f64, max: f64, field: &str, ssu_id: i64) -> Result<Self, StrategyError> {
        if min.is_finite() && max.is_finite() && min >= 0.0 && max >= min {
            Ok(Self { min, max })
        } else {
            Err(StrategyError::Config(format!(
                "SSU {ssu_id} exponential_edge {field} range must satisfy 0 <= min <= max"
            )))
        }
    }

    fn contains(self, value: f64) -> bool {
        value.is_finite() && value >= self.min && value <= self.max
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
struct RawFilters {
    trend_height_atr: Option<RawRange>,
    retracement_ratio: Option<RawRange>,
    ema_touch_tolerance_atr: Option<RawRange>,
    ker: Option<RawRange>,
    adx: Option<RawRange>,
}

#[derive(Clone, Copy, Debug, Deserialize)]
struct RawRange {
    min: Option<f64>,
    max: Option<f64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EntryPolicy {
    SinglePosition,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StopMode {
    PullbackExtreme,
}

#[derive(Clone, Debug)]
struct ExponentialEdgeSettings {
    timeframe: Timeframe,
    enabled_sides: Vec<SignalSide>,
    ema_period: usize,
    atr_period: usize,
    trend_lookback_bars: usize,
    min_retracement_bars: usize,
    max_retracement_bars: usize,
    ker_period: usize,
    adx_period: usize,
    trend_height_atr: NumericRange,
    retracement_ratio: NumericRange,
    ema_touch_tolerance_atr: NumericRange,
    ker: NumericRange,
    adx: NumericRange,
    stop_mode: StopMode,
    stop_buffer_atr: f64,
    target_enabled: bool,
    target_r_multiple: f64,
    profit_trail_enabled: bool,
    profit_trail_activation_r: f64,
    profit_trail_giveback_pct: f64,
    profit_trail_min_lock_r: f64,
    exit_on_ema_fail_bars: usize,
    entry_policy: EntryPolicy,
}

impl ExponentialEdgeSettings {
    fn from_ssu(ssu: &SsuConfig) -> Result<Self, StrategyError> {
        #[derive(Deserialize)]
        struct Raw {
            timeframe: Option<String>,
            enabled_sides: Option<Vec<String>>,
            ema_period: Option<usize>,
            atr_period: Option<usize>,
            trend_lookback_bars: Option<usize>,
            min_retracement_bars: Option<usize>,
            max_retracement_bars: Option<usize>,
            ker_period: Option<usize>,
            adx_period: Option<usize>,
            filters: Option<RawFilters>,
            stop_mode: Option<String>,
            stop_buffer_atr: Option<f64>,
            target_enabled: Option<bool>,
            target_r_multiple: Option<f64>,
            profit_trail_enabled: Option<bool>,
            profit_trail_activation_r: Option<f64>,
            profit_trail_giveback_pct: Option<f64>,
            profit_trail_min_lock_r: Option<f64>,
            exit_on_ema_fail_bars: Option<usize>,
            entry_policy: Option<String>,
        }

        let raw: Raw = serde_json::from_str(&ssu.params_json).map_err(|error| {
            StrategyError::Parse(format!(
                "invalid exponential_edge params_json for SSU {}: {error}",
                ssu.ssu_id
            ))
        })?;
        let timeframe = parse_timeframe(&require(raw.timeframe, "timeframe", ssu.ssu_id)?)?;
        if !ssu.required_timeframes.contains(&timeframe) {
            return Err(StrategyError::Config(format!(
                "SSU {} exponential_edge timeframe {} is not registered",
                ssu.ssu_id,
                timeframe_label(timeframe)
            )));
        }

        let enabled_sides = require(raw.enabled_sides, "enabled_sides", ssu.ssu_id)?
            .iter()
            .map(|side| parse_side(side))
            .collect::<Result<Vec<_>, _>>()?;
        let filters = require(raw.filters, "filters", ssu.ssu_id)?;
        let settings = Self {
            timeframe,
            enabled_sides,
            ema_period: require(raw.ema_period, "ema_period", ssu.ssu_id)?,
            atr_period: require(raw.atr_period, "atr_period", ssu.ssu_id)?,
            trend_lookback_bars: require(
                raw.trend_lookback_bars,
                "trend_lookback_bars",
                ssu.ssu_id,
            )?,
            min_retracement_bars: require(
                raw.min_retracement_bars,
                "min_retracement_bars",
                ssu.ssu_id,
            )?,
            max_retracement_bars: require(
                raw.max_retracement_bars,
                "max_retracement_bars",
                ssu.ssu_id,
            )?,
            ker_period: require(raw.ker_period, "ker_period", ssu.ssu_id)?,
            adx_period: require(raw.adx_period, "adx_period", ssu.ssu_id)?,
            trend_height_atr: range_from_filter(
                filters.trend_height_atr,
                "filters.trend_height_atr",
                ssu.ssu_id,
            )?,
            retracement_ratio: ratio_range_from_filter(
                filters.retracement_ratio,
                "filters.retracement_ratio",
                ssu.ssu_id,
            )?,
            ema_touch_tolerance_atr: range_from_filter(
                filters.ema_touch_tolerance_atr,
                "filters.ema_touch_tolerance_atr",
                ssu.ssu_id,
            )?,
            ker: ratio_range_from_filter(filters.ker, "filters.ker", ssu.ssu_id)?,
            adx: range_with_max_from_filter(filters.adx, "filters.adx", 100.0, ssu.ssu_id)?,
            stop_mode: parse_stop_mode(&require(raw.stop_mode, "stop_mode", ssu.ssu_id)?)?,
            stop_buffer_atr: require_non_negative(
                raw.stop_buffer_atr,
                "stop_buffer_atr",
                ssu.ssu_id,
            )?,
            target_enabled: require(raw.target_enabled, "target_enabled", ssu.ssu_id)?,
            target_r_multiple: require(raw.target_r_multiple, "target_r_multiple", ssu.ssu_id)?,
            profit_trail_enabled: raw.profit_trail_enabled.unwrap_or(false),
            profit_trail_activation_r: raw
                .profit_trail_activation_r
                .unwrap_or(DEFAULT_PROFIT_TRAIL_ACTIVATION_R),
            profit_trail_giveback_pct: raw
                .profit_trail_giveback_pct
                .unwrap_or(DEFAULT_PROFIT_TRAIL_GIVEBACK_PCT),
            profit_trail_min_lock_r: raw
                .profit_trail_min_lock_r
                .unwrap_or(DEFAULT_PROFIT_TRAIL_MIN_LOCK_R),
            exit_on_ema_fail_bars: require(
                raw.exit_on_ema_fail_bars,
                "exit_on_ema_fail_bars",
                ssu.ssu_id,
            )?,
            entry_policy: parse_entry_policy(&require(
                raw.entry_policy,
                "entry_policy",
                ssu.ssu_id,
            )?)?,
        };
        settings.validate(ssu.ssu_id)?;
        Ok(settings)
    }

    fn validate(&self, ssu_id: i64) -> Result<(), StrategyError> {
        if self.enabled_sides.is_empty() {
            return Err(StrategyError::Config(format!(
                "SSU {ssu_id} exponential_edge enabled_sides must not be empty"
            )));
        }
        if self.ema_period == 0 || self.atr_period == 0 {
            return Err(StrategyError::Config(format!(
                "SSU {ssu_id} exponential_edge EMA/ATR periods must be positive"
            )));
        }
        if self.trend_lookback_bars < 3
            || self.min_retracement_bars == 0
            || self.max_retracement_bars < self.min_retracement_bars
            || self.min_retracement_bars >= self.trend_lookback_bars
            || self.max_retracement_bars >= self.trend_lookback_bars
        {
            return Err(StrategyError::Config(format!(
                "SSU {ssu_id} exponential_edge invalid retracement/trend lookback bars"
            )));
        }
        if self.ker_period == 0 || self.adx_period == 0 || self.exit_on_ema_fail_bars == 0 {
            return Err(StrategyError::Config(format!(
                "SSU {ssu_id} exponential_edge KER/ADX/exit counts must be positive"
            )));
        }
        if self.target_enabled && self.target_r_multiple <= 0.0 {
            return Err(StrategyError::Config(format!(
                "SSU {ssu_id} exponential_edge target_r_multiple must be positive when target_enabled"
            )));
        }
        if self.profit_trail_enabled {
            validate_profit_trail_config(
                self.profit_trail_activation_r,
                self.profit_trail_giveback_pct,
                self.profit_trail_min_lock_r,
                ssu_id,
            )?;
        }
        if self.entry_policy != EntryPolicy::SinglePosition {
            return Err(StrategyError::Config(format!(
                "SSU {ssu_id} exponential_edge only supports entry_policy single_position"
            )));
        }
        if self.stop_mode != StopMode::PullbackExtreme {
            return Err(StrategyError::Config(format!(
                "SSU {ssu_id} exponential_edge only supports stop_mode pullback_extreme"
            )));
        }
        Ok(())
    }

    fn state_capacity(&self) -> usize {
        self.trend_lookback_bars
            .max(self.ema_period)
            .max(self.atr_period + 1)
            .max(self.ker_period + 1)
            .max((2 * self.adx_period) + 1)
            + STATE_SMALL_BUFFER
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct StateKey {
    ssu_id: i64,
    instrument: String,
    timeframe: Timeframe,
}

impl StateKey {
    fn new(ssu_id: i64, instrument: &str, timeframe: Timeframe) -> Self {
        Self {
            ssu_id,
            instrument: instrument.to_string(),
            timeframe,
        }
    }
}

#[derive(Clone, Debug)]
struct RuntimeState {
    last_processed_closed_end: Option<u64>,
    prepared_bar_end_at: Option<u64>,
    pending_recovery_warmup_bars: Vec<Bar>,
    indicators: IndicatorBundle,
    trend: RingTrendTree,
    long_state: SideRuntimeState,
    short_state: SideRuntimeState,
}

impl RuntimeState {
    fn new(settings: &ExponentialEdgeSettings) -> Self {
        Self {
            last_processed_closed_end: None,
            prepared_bar_end_at: None,
            pending_recovery_warmup_bars: Vec::new(),
            indicators: IndicatorBundle::new(settings),
            trend: RingTrendTree::new(settings.trend_lookback_bars),
            long_state: SideRuntimeState::new(),
            short_state: SideRuntimeState::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.last_processed_closed_end.is_none() && self.trend.is_empty()
    }

    fn process_warmup_bar(&mut self, bar: &Bar) -> Result<(), StrategyError> {
        self.indicators.on_bar(bar)?;
        self.trend.insert(bar);
        self.last_processed_closed_end = Some(bar.end_at);
        Ok(())
    }

    fn prepare_current_bar(&mut self, bar: &Bar) -> Result<(), StrategyError> {
        if self
            .last_processed_closed_end
            .is_some_and(|end_at| bar.end_at <= end_at)
        {
            return Ok(());
        }
        if let Some(prepared_bar_end_at) = self.prepared_bar_end_at {
            if prepared_bar_end_at == bar.end_at {
                return Ok(());
            }
            return Err(StrategyError::Rule(format!(
                "exponential_edge cannot prepare bar {} while unfinalized bar {} is pending",
                bar.end_at, prepared_bar_end_at
            )));
        }
        self.indicators.on_bar(bar)?;
        self.prepared_bar_end_at = Some(bar.end_at);
        Ok(())
    }

    fn finalize_current_bar(&mut self, bar: &Bar) -> Result<(), StrategyError> {
        if self
            .last_processed_closed_end
            .is_some_and(|end_at| end_at >= bar.end_at)
        {
            return Ok(());
        }
        match self.prepared_bar_end_at {
            Some(prepared_bar_end_at) if prepared_bar_end_at == bar.end_at => {
                self.trend.insert(bar);
                self.last_processed_closed_end = Some(bar.end_at);
                self.prepared_bar_end_at = None;
                Ok(())
            }
            Some(prepared_bar_end_at) => Err(StrategyError::Rule(format!(
                "exponential_edge cannot finalize bar {} while prepared bar {} is pending",
                bar.end_at, prepared_bar_end_at
            ))),
            None => Ok(()),
        }
    }

    fn entry_ready(&self, settings: &ExponentialEdgeSettings) -> bool {
        self.indicators.latest_point().is_some() && self.trend.len() >= settings.trend_lookback_bars
    }

    fn side_state(&self, side: SignalSide) -> &SideRuntimeState {
        match side {
            SignalSide::Long => &self.long_state,
            SignalSide::Short => &self.short_state,
        }
    }

    fn side_state_mut(&mut self, side: SignalSide) -> &mut SideRuntimeState {
        match side {
            SignalSide::Long => &mut self.long_state,
            SignalSide::Short => &mut self.short_state,
        }
    }
}

#[derive(Clone, Debug)]
struct SideRuntimeState {
    recent_setup_ids: VecDeque<String>,
    recent_setup_set: BTreeSet<String>,
}

impl SideRuntimeState {
    fn new() -> Self {
        Self {
            recent_setup_ids: VecDeque::new(),
            recent_setup_set: BTreeSet::new(),
        }
    }

    fn contains(&self, setup_id: &str) -> bool {
        self.recent_setup_set.contains(setup_id)
    }

    fn remember(&mut self, setup_id: String) {
        if !self.recent_setup_set.insert(setup_id.clone()) {
            return;
        }
        self.recent_setup_ids.push_back(setup_id);
        while self.recent_setup_ids.len() > SETUP_ID_CAPACITY {
            if let Some(old) = self.recent_setup_ids.pop_front() {
                self.recent_setup_set.remove(&old);
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct IndicatorPoint {
    ema: f64,
    atr: f64,
    ker: f64,
    adx: f64,
    plus_di: f64,
    minus_di: f64,
}

#[derive(Clone, Debug)]
struct IndicatorBundle {
    ema: EmaIndicator,
    atr: AtrIndicator,
    ker: KerIndicator,
    adx: AdxIndicator,
}

impl IndicatorBundle {
    fn new(settings: &ExponentialEdgeSettings) -> Self {
        Self {
            ema: EmaIndicator::new(settings.ema_period),
            atr: AtrIndicator::new(settings.atr_period),
            ker: KerIndicator::new(settings.ker_period),
            adx: AdxIndicator::new(settings.adx_period),
        }
    }

    fn on_bar(&mut self, bar: &Bar) -> Result<(), StrategyError> {
        self.ema.on_close(bar.close);
        self.atr.on_bar(bar);
        self.ker.on_close(bar.close);
        self.adx.on_bar(bar);
        Ok(())
    }

    fn latest_point(&self) -> Option<IndicatorPoint> {
        let (adx, plus_di, minus_di) = self.adx.value()?;
        Some(IndicatorPoint {
            ema: self.ema.value()?,
            atr: self.atr.value()?,
            ker: self.ker.value()?,
            adx,
            plus_di,
            minus_di,
        })
    }
}

#[derive(Clone, Debug)]
struct EmaIndicator {
    period: usize,
    multiplier: f64,
    seed: Vec<f64>,
    value: Option<f64>,
}

impl EmaIndicator {
    fn new(period: usize) -> Self {
        Self {
            period,
            multiplier: 2.0 / (period as f64 + 1.0),
            seed: Vec::with_capacity(period),
            value: None,
        }
    }

    fn on_close(&mut self, close: f64) {
        if let Some(previous) = self.value {
            self.value = Some(previous + self.multiplier * (close - previous));
            return;
        }
        self.seed.push(close);
        if self.seed.len() == self.period {
            self.value = Some(self.seed.iter().sum::<f64>() / self.period as f64);
        }
    }

    fn value(&self) -> Option<f64> {
        self.value
    }
}

#[derive(Clone, Debug)]
struct AtrIndicator {
    period: usize,
    previous_close: Option<f64>,
    seed_tr: Vec<f64>,
    value: Option<f64>,
}

impl AtrIndicator {
    fn new(period: usize) -> Self {
        Self {
            period,
            previous_close: None,
            seed_tr: Vec::with_capacity(period),
            value: None,
        }
    }

    fn on_bar(&mut self, bar: &Bar) {
        let Some(previous_close) = self.previous_close else {
            self.previous_close = Some(bar.close);
            return;
        };
        let tr = (bar.high - bar.low)
            .max((bar.high - previous_close).abs())
            .max((bar.low - previous_close).abs());
        if let Some(previous) = self.value {
            self.value = Some(((previous * (self.period as f64 - 1.0)) + tr) / self.period as f64);
        } else {
            self.seed_tr.push(tr);
            if self.seed_tr.len() == self.period {
                self.value = Some(self.seed_tr.iter().sum::<f64>() / self.period as f64);
            }
        }
        self.previous_close = Some(bar.close);
    }

    fn value(&self) -> Option<f64> {
        self.value
    }
}

#[derive(Clone, Debug)]
struct KerIndicator {
    period: usize,
    closes: VecDeque<f64>,
    diff_sum: f64,
}

impl KerIndicator {
    fn new(period: usize) -> Self {
        Self {
            period,
            closes: VecDeque::with_capacity(period + 1),
            diff_sum: 0.0,
        }
    }

    fn on_close(&mut self, close: f64) {
        if let Some(previous) = self.closes.back().copied() {
            self.diff_sum += (close - previous).abs();
        }
        self.closes.push_back(close);
        while self.closes.len() > self.period + 1 {
            let old = self.closes.pop_front().expect("close exists");
            if let Some(new_first) = self.closes.front().copied() {
                self.diff_sum -= (new_first - old).abs();
                if self.diff_sum < 0.0 && self.diff_sum > -1e-10 {
                    self.diff_sum = 0.0;
                }
            }
        }
    }

    fn value(&self) -> Option<f64> {
        if self.closes.len() < self.period + 1 {
            return None;
        }
        if self.diff_sum <= 0.0 {
            return Some(0.0);
        }
        let first = self.closes.front()?;
        let last = self.closes.back()?;
        Some((last - first).abs() / self.diff_sum)
    }
}

#[derive(Clone, Debug)]
struct AdxIndicator {
    period: usize,
    previous_high: Option<f64>,
    previous_low: Option<f64>,
    previous_close: Option<f64>,
    seed_count: usize,
    tr_sum: f64,
    plus_dm_sum: f64,
    minus_dm_sum: f64,
    smooth_tr: Option<f64>,
    smooth_plus_dm: Option<f64>,
    smooth_minus_dm: Option<f64>,
    dx_seed: Vec<f64>,
    adx: Option<f64>,
    plus_di: f64,
    minus_di: f64,
}

impl AdxIndicator {
    fn new(period: usize) -> Self {
        Self {
            period,
            previous_high: None,
            previous_low: None,
            previous_close: None,
            seed_count: 0,
            tr_sum: 0.0,
            plus_dm_sum: 0.0,
            minus_dm_sum: 0.0,
            smooth_tr: None,
            smooth_plus_dm: None,
            smooth_minus_dm: None,
            dx_seed: Vec::with_capacity(period),
            adx: None,
            plus_di: 0.0,
            minus_di: 0.0,
        }
    }

    fn on_bar(&mut self, bar: &Bar) {
        let (Some(previous_high), Some(previous_low), Some(previous_close)) =
            (self.previous_high, self.previous_low, self.previous_close)
        else {
            self.previous_high = Some(bar.high);
            self.previous_low = Some(bar.low);
            self.previous_close = Some(bar.close);
            return;
        };

        let up_move = bar.high - previous_high;
        let down_move = previous_low - bar.low;
        let plus_dm = if up_move > down_move && up_move > 0.0 {
            up_move
        } else {
            0.0
        };
        let minus_dm = if down_move > up_move && down_move > 0.0 {
            down_move
        } else {
            0.0
        };
        let tr = (bar.high - bar.low)
            .max((bar.high - previous_close).abs())
            .max((bar.low - previous_close).abs());

        if self.smooth_tr.is_none() {
            self.seed_count += 1;
            self.tr_sum += tr;
            self.plus_dm_sum += plus_dm;
            self.minus_dm_sum += minus_dm;
            if self.seed_count == self.period {
                self.smooth_tr = Some(self.tr_sum);
                self.smooth_plus_dm = Some(self.plus_dm_sum);
                self.smooth_minus_dm = Some(self.minus_dm_sum);
            }
        } else {
            let smooth_tr = smooth(self.smooth_tr.expect("smooth tr"), tr, self.period);
            let smooth_plus = smooth(
                self.smooth_plus_dm.expect("smooth plus dm"),
                plus_dm,
                self.period,
            );
            let smooth_minus = smooth(
                self.smooth_minus_dm.expect("smooth minus dm"),
                minus_dm,
                self.period,
            );
            self.smooth_tr = Some(smooth_tr);
            self.smooth_plus_dm = Some(smooth_plus);
            self.smooth_minus_dm = Some(smooth_minus);
            let dx = self.update_di_and_dx(smooth_tr, smooth_plus, smooth_minus);
            if let Some(previous_adx) = self.adx {
                self.adx =
                    Some(((previous_adx * (self.period as f64 - 1.0)) + dx) / self.period as f64);
            } else {
                self.dx_seed.push(dx);
                if self.dx_seed.len() == self.period {
                    self.adx = Some(self.dx_seed.iter().sum::<f64>() / self.dx_seed.len() as f64);
                }
            }
        }

        self.previous_high = Some(bar.high);
        self.previous_low = Some(bar.low);
        self.previous_close = Some(bar.close);
    }

    fn update_di_and_dx(&mut self, smooth_tr: f64, smooth_plus: f64, smooth_minus: f64) -> f64 {
        if smooth_tr <= 0.0 {
            self.plus_di = 0.0;
            self.minus_di = 0.0;
            return 0.0;
        }
        self.plus_di = 100.0 * smooth_plus / smooth_tr;
        self.minus_di = 100.0 * smooth_minus / smooth_tr;
        let denom = self.plus_di + self.minus_di;
        if denom <= 0.0 {
            0.0
        } else {
            100.0 * (self.plus_di - self.minus_di).abs() / denom
        }
    }

    fn value(&self) -> Option<(f64, f64, f64)> {
        Some((self.adx?, self.plus_di, self.minus_di))
    }
}

fn smooth(previous: f64, current: f64, period: usize) -> f64 {
    ((previous * (period as f64 - 1.0)) + current) / period as f64
}

#[derive(Clone, Debug)]
struct RingTrendTree {
    capacity: usize,
    tree_base: usize,
    leaves: Vec<Option<TrendNode>>,
    tree: Vec<Option<TrendNode>>,
    next_logical_index: u64,
    count: usize,
}

impl RingTrendTree {
    fn new(capacity: usize) -> Self {
        let tree_base = capacity.next_power_of_two();
        Self {
            capacity,
            tree_base,
            leaves: vec![None; capacity],
            tree: vec![None; tree_base * 2],
            next_logical_index: 0,
            count: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }

    fn len(&self) -> usize {
        self.count
    }

    fn latest_logical_index(&self) -> Option<u64> {
        (self.count > 0).then_some(self.next_logical_index - 1)
    }

    fn oldest_logical_index(&self) -> Option<u64> {
        (self.count > 0).then_some(self.next_logical_index - self.count as u64)
    }

    fn insert(&mut self, bar: &Bar) {
        let logical_index = self.next_logical_index;
        let slot = self.slot(logical_index);
        let leaf = TrendNode::leaf(logical_index, bar);
        self.leaves[slot] = Some(leaf.clone());
        let mut tree_index = self.tree_base + slot;
        self.tree[tree_index] = Some(leaf);
        while tree_index > 1 {
            tree_index /= 2;
            self.tree[tree_index] = merge_optional(
                self.tree[tree_index * 2].clone(),
                self.tree[tree_index * 2 + 1].clone(),
            );
        }
        self.next_logical_index += 1;
        self.count = (self.count + 1).min(self.capacity);
    }

    fn query_active(&self) -> Option<TrendNode> {
        let oldest = self.oldest_logical_index()?;
        let latest = self.latest_logical_index()?;
        self.query_logical(oldest, latest)
    }

    fn query_logical(&self, start: u64, end: u64) -> Option<TrendNode> {
        if start > end || self.count == 0 {
            return None;
        }
        let oldest = self.oldest_logical_index()?;
        let latest = self.latest_logical_index()?;
        let start = start.max(oldest);
        let end = end.min(latest);
        if start > end {
            return None;
        }

        let start_slot = self.slot(start);
        let end_slot = self.slot(end);
        if start_slot <= end_slot {
            self.query_physical(start_slot, end_slot)
        } else {
            merge_optional(
                self.query_physical(start_slot, self.capacity - 1),
                self.query_physical(0, end_slot),
            )
        }
    }

    fn query_physical(&self, left: usize, right: usize) -> Option<TrendNode> {
        if left > right || right >= self.capacity {
            return None;
        }
        let mut left_index = self.tree_base + left;
        let mut right_index = self.tree_base + right;
        let mut left_result: Option<TrendNode> = None;
        let mut right_result: Option<TrendNode> = None;
        while left_index <= right_index {
            if left_index % 2 == 1 {
                left_result = merge_optional(left_result, self.tree[left_index].clone());
                left_index += 1;
            }
            if right_index % 2 == 0 {
                right_result = merge_optional(self.tree[right_index].clone(), right_result);
                right_index = right_index.saturating_sub(1);
            }
            left_index /= 2;
            right_index /= 2;
        }
        merge_optional(left_result, right_result)
    }

    fn slot(&self, logical_index: u64) -> usize {
        logical_index as usize % self.capacity
    }
}

#[derive(Clone, Debug)]
struct TrendNode {
    first_index: u64,
    last_index: u64,
    min_low: f64,
    min_low_index: u64,
    min_low_at: u64,
    max_high: f64,
    max_high_index: u64,
    max_high_at: u64,
    best_long: Option<TrendLeg>,
    best_short: Option<TrendLeg>,
}

impl TrendNode {
    fn leaf(logical_index: u64, bar: &Bar) -> Self {
        Self {
            first_index: logical_index,
            last_index: logical_index,
            min_low: bar.low,
            min_low_index: logical_index,
            min_low_at: bar.end_at,
            max_high: bar.high,
            max_high_index: logical_index,
            max_high_at: bar.end_at,
            best_long: None,
            best_short: None,
        }
    }

    fn merge(left: Self, right: Self) -> Self {
        let (min_low, min_low_index, min_low_at) = choose_recent_low(
            (left.min_low, left.min_low_index, left.min_low_at),
            (right.min_low, right.min_low_index, right.min_low_at),
        );
        let (max_high, max_high_index, max_high_at) = choose_recent_high(
            (left.max_high, left.max_high_index, left.max_high_at),
            (right.max_high, right.max_high_index, right.max_high_at),
        );

        let cross_long = TrendLeg {
            side: SignalSide::Long,
            height: right.max_high - left.min_low,
            start_index: left.min_low_index,
            extreme_index: right.max_high_index,
            start_at: left.min_low_at,
            extreme_at: right.max_high_at,
            start_price: left.min_low,
            extreme_price: right.max_high,
        };
        let cross_short = TrendLeg {
            side: SignalSide::Short,
            height: left.max_high - right.min_low,
            start_index: left.max_high_index,
            extreme_index: right.min_low_index,
            start_at: left.max_high_at,
            extreme_at: right.min_low_at,
            start_price: left.max_high,
            extreme_price: right.min_low,
        };

        Self {
            first_index: left.first_index,
            last_index: right.last_index,
            min_low,
            min_low_index,
            min_low_at,
            max_high,
            max_high_index,
            max_high_at,
            best_long: best_leg(best_leg(left.best_long, right.best_long), Some(cross_long)),
            best_short: best_leg(
                best_leg(left.best_short, right.best_short),
                Some(cross_short),
            ),
        }
    }
}

#[derive(Clone, Debug)]
struct TrendLeg {
    side: SignalSide,
    height: f64,
    start_index: u64,
    extreme_index: u64,
    start_at: u64,
    extreme_at: u64,
    start_price: f64,
    extreme_price: f64,
}

fn merge_optional(left: Option<TrendNode>, right: Option<TrendNode>) -> Option<TrendNode> {
    match (left, right) {
        (Some(left), Some(right)) => Some(TrendNode::merge(left, right)),
        (Some(node), None) | (None, Some(node)) => Some(node),
        (None, None) => None,
    }
}

fn choose_recent_low(left: (f64, u64, u64), right: (f64, u64, u64)) -> (f64, u64, u64) {
    if right.0 < left.0 || ((right.0 - left.0).abs() <= f64::EPSILON && right.1 > left.1) {
        right
    } else {
        left
    }
}

fn choose_recent_high(left: (f64, u64, u64), right: (f64, u64, u64)) -> (f64, u64, u64) {
    if right.0 > left.0 || ((right.0 - left.0).abs() <= f64::EPSILON && right.1 > left.1) {
        right
    } else {
        left
    }
}

fn best_leg(left: Option<TrendLeg>, right: Option<TrendLeg>) -> Option<TrendLeg> {
    match (left, right) {
        (Some(left), Some(right)) => {
            if leg_better(&right, &left) {
                Some(right)
            } else {
                Some(left)
            }
        }
        (Some(leg), None) | (None, Some(leg)) => Some(leg),
        (None, None) => None,
    }
}

fn leg_better(candidate: &TrendLeg, incumbent: &TrendLeg) -> bool {
    candidate.height > incumbent.height
        || ((candidate.height - incumbent.height).abs() <= f64::EPSILON
            && (candidate.extreme_index > incumbent.extreme_index
                || (candidate.extreme_index == incumbent.extreme_index
                    && candidate.start_index > incumbent.start_index)))
}

#[derive(Clone, Debug)]
struct DetectedSetup {
    ssu_id: i64,
    setup_id: String,
    timeframe: Timeframe,
    side: SignalSide,
    entry_bar_end_at: u64,
    entry_price: f64,
    stop_price: f64,
    target_enabled: bool,
    target_price: Option<f64>,
    profit_trail_enabled: bool,
    profit_trail_activation_r: f64,
    profit_trail_giveback_pct: f64,
    profit_trail_min_lock_r: f64,
    ema: f64,
    atr: f64,
    trend_start_at: u64,
    trend_extreme_at: u64,
    trend_height: f64,
    trend_height_atr: f64,
    retracement_height: f64,
    retracement_ratio: f64,
    retracement_bars: usize,
    ema_touch_distance_atr: f64,
    ker: f64,
    adx: f64,
    plus_di: f64,
    minus_di: f64,
}

impl DetectedSetup {
    fn entry_reason(&self) -> String {
        format!(
            "exponential_edge_entry|side={}|tf={}|setup_id={}|closed_bar_end={}",
            side_label(self.side),
            timeframe_label(self.timeframe),
            self.setup_id,
            self.entry_bar_end_at
        )
    }

    fn entry_metadata(&self) -> serde_json::Value {
        serde_json::json!({
            "setup_id": self.setup_id,
            "timeframe": timeframe_label(self.timeframe),
            "side": side_label(self.side),
            "entry_bar_end_at": self.entry_bar_end_at,
            "entry_price": self.entry_price,
            "stop_price": self.stop_price,
            "target_enabled": self.target_enabled,
            "target_price": self.target_price,
            "profit_trail_enabled": self.profit_trail_enabled,
            "profit_trail_activation_r": self.profit_trail_activation_r,
            "profit_trail_giveback_pct": self.profit_trail_giveback_pct,
            "profit_trail_min_lock_r": self.profit_trail_min_lock_r,
            "ema": self.ema,
            "atr": self.atr,
            "trend_start_at": self.trend_start_at,
            "trend_extreme_at": self.trend_extreme_at,
            "trend_height": self.trend_height,
            "trend_height_atr": self.trend_height_atr,
            "retracement_height": self.retracement_height,
            "retracement_ratio": self.retracement_ratio,
            "retracement_bars": self.retracement_bars,
            "ema_touch_distance_atr": self.ema_touch_distance_atr,
            "ker": self.ker,
            "adx": self.adx,
            "plus_di": self.plus_di,
            "minus_di": self.minus_di,
        })
    }

    fn trade_context_metadata(&self, position_id: &str) -> serde_json::Value {
        serde_json::json!({
            "strategy_key": "exponential_edge",
            "position_id": position_id,
            "setup_id": self.setup_id,
            "side": side_label(self.side),
            "entry_bar_end_at": self.entry_bar_end_at,
            "entry_price": self.entry_price,
            "stop_price": self.stop_price,
            "target_enabled": self.target_enabled,
            "target_price": self.target_price,
            "profit_trail_enabled": self.profit_trail_enabled,
            "profit_trail_activation_r": self.profit_trail_activation_r,
            "profit_trail_giveback_pct": self.profit_trail_giveback_pct,
            "profit_trail_min_lock_r": self.profit_trail_min_lock_r,
            "profit_trail_activated": false,
            "profit_trail_best_price": self.entry_price,
            "profit_trail_stop_price": serde_json::Value::Null,
            "ema_fail_bars": 0_u64,
            "last_exit_check_bar_end_at": self.entry_bar_end_at,
            "recovery_breach_detected": false,
            "recovery_breach_reason": serde_json::Value::Null,
            "recovery_breach_bar_end_at": serde_json::Value::Null,
        })
    }
}

fn detect_setup(
    ssu: &SsuConfig,
    settings: &ExponentialEdgeSettings,
    trigger_bar: &Bar,
    tree: &RingTrendTree,
    summary: &TrendNode,
    point: IndicatorPoint,
    side: SignalSide,
) -> Result<Option<DetectedSetup>, StrategyError> {
    let leg = match side {
        SignalSide::Long => summary.best_long.clone(),
        SignalSide::Short => summary.best_short.clone(),
    };
    let Some(leg) = leg else {
        return Ok(None);
    };
    if leg.side != side || leg.height <= 0.0 || point.atr <= 0.0 {
        return Ok(None);
    }

    let Some(latest_index) = tree.latest_logical_index() else {
        return Ok(None);
    };
    let pre_range = tree.query_logical(leg.extreme_index + 1, latest_index);
    let pre_retracement_bars = latest_index.saturating_sub(leg.extreme_index) as usize;
    let retracement_bars = pre_retracement_bars + 1;
    if retracement_bars < settings.min_retracement_bars
        || retracement_bars > settings.max_retracement_bars
    {
        return Ok(None);
    }

    let trend_height_atr = leg.height / point.atr;
    if !settings.trend_height_atr.contains(trend_height_atr) {
        return Ok(None);
    }

    let (retracement_height, invalidated, ema_touch_ok, pullback_extreme) = match side {
        SignalSide::Long => {
            let pullback_low = pre_range
                .as_ref()
                .map(|node| node.min_low.min(trigger_bar.low))
                .unwrap_or(trigger_bar.low);
            (
                leg.extreme_price - trigger_bar.close,
                trigger_bar.low < leg.start_price,
                trigger_bar.close >= point.ema
                    && trigger_bar.close
                        <= point.ema + settings.ema_touch_tolerance_atr.max * point.atr,
                pullback_low,
            )
        }
        SignalSide::Short => {
            let pullback_high = pre_range
                .as_ref()
                .map(|node| node.max_high.max(trigger_bar.high))
                .unwrap_or(trigger_bar.high);
            (
                trigger_bar.close - leg.extreme_price,
                trigger_bar.high > leg.start_price,
                trigger_bar.close <= point.ema
                    && trigger_bar.close
                        >= point.ema - settings.ema_touch_tolerance_atr.max * point.atr,
                pullback_high,
            )
        }
    };
    if invalidated {
        return Ok(None);
    }
    let retracement_ratio = retracement_height / leg.height;
    if !settings.retracement_ratio.contains(retracement_ratio) || !ema_touch_ok {
        return Ok(None);
    }
    let ema_touch_distance_atr = (trigger_bar.close - point.ema).abs() / point.atr;
    if !settings
        .ema_touch_tolerance_atr
        .contains(ema_touch_distance_atr)
    {
        return Ok(None);
    }
    if !settings.ker.contains(point.ker) || !settings.adx.contains(point.adx) {
        return Ok(None);
    }

    let stop_price = match side {
        SignalSide::Long => pullback_extreme - settings.stop_buffer_atr * point.atr,
        SignalSide::Short => pullback_extreme + settings.stop_buffer_atr * point.atr,
    };
    let entry_price = trigger_bar.close;
    let risk = (entry_price - stop_price).abs();
    if !risk.is_finite() || risk <= 0.0 {
        return Ok(None);
    }
    let target_price = if settings.target_enabled {
        Some(match side {
            SignalSide::Long => entry_price + settings.target_r_multiple * risk,
            SignalSide::Short => entry_price - settings.target_r_multiple * risk,
        })
    } else {
        None
    };
    let setup_id = format!(
        "EXEDGE-{}-{}-{}-{}-{}-{}",
        ssu.ssu_id,
        trigger_bar.instrument,
        timeframe_label(settings.timeframe),
        side_label(side),
        leg.start_at,
        leg.extreme_at
    ) + &format!("-{}", trigger_bar.end_at);

    Ok(Some(DetectedSetup {
        ssu_id: ssu.ssu_id,
        setup_id,
        timeframe: settings.timeframe,
        side,
        entry_bar_end_at: trigger_bar.end_at,
        entry_price,
        stop_price,
        target_enabled: settings.target_enabled,
        target_price,
        profit_trail_enabled: settings.profit_trail_enabled,
        profit_trail_activation_r: settings.profit_trail_activation_r,
        profit_trail_giveback_pct: settings.profit_trail_giveback_pct,
        profit_trail_min_lock_r: settings.profit_trail_min_lock_r,
        ema: point.ema,
        atr: point.atr,
        trend_start_at: leg.start_at,
        trend_extreme_at: leg.extreme_at,
        trend_height: leg.height,
        trend_height_atr,
        retracement_height,
        retracement_ratio,
        retracement_bars,
        ema_touch_distance_atr,
        ker: point.ker,
        adx: point.adx,
        plus_di: point.plus_di,
        minus_di: point.minus_di,
    }))
}

#[derive(Clone, Debug)]
struct TradeContext {
    strategy_key: String,
    position_id: String,
    setup_id: String,
    side: SignalSide,
    entry_bar_end_at: u64,
    entry_price: f64,
    stop_price: f64,
    target_enabled: bool,
    target_price: Option<f64>,
    profit_trail_enabled: bool,
    profit_trail_activation_r: f64,
    profit_trail_giveback_pct: f64,
    profit_trail_min_lock_r: f64,
    profit_trail_activated: bool,
    profit_trail_best_price: f64,
    profit_trail_stop_price: Option<f64>,
    ema_fail_bars: u64,
    last_exit_check_bar_end_at: u64,
    recovery_breach_detected: bool,
    recovery_breach_reason: Option<String>,
    recovery_breach_bar_end_at: Option<u64>,
}

impl TradeContext {
    fn from_metadata(
        metadata: &serde_json::Value,
        position_id: &str,
    ) -> Result<Self, StrategyError> {
        let entry_price = required_f64(metadata, "entry_price")?;
        let profit_trail_enabled = metadata
            .get("profit_trail_enabled")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        let profit_trail_activation_r = optional_f64(metadata, "profit_trail_activation_r")
            .unwrap_or(DEFAULT_PROFIT_TRAIL_ACTIVATION_R);
        let profit_trail_giveback_pct = optional_f64(metadata, "profit_trail_giveback_pct")
            .unwrap_or(DEFAULT_PROFIT_TRAIL_GIVEBACK_PCT);
        let profit_trail_min_lock_r = optional_f64(metadata, "profit_trail_min_lock_r")
            .unwrap_or(DEFAULT_PROFIT_TRAIL_MIN_LOCK_R);
        if profit_trail_enabled {
            validate_profit_trail_config(
                profit_trail_activation_r,
                profit_trail_giveback_pct,
                profit_trail_min_lock_r,
                0,
            )?;
        }
        let context = Self {
            strategy_key: required_string(metadata, "strategy_key")?,
            position_id: required_string(metadata, "position_id")?,
            setup_id: required_string(metadata, "setup_id")?,
            side: parse_side(&required_string(metadata, "side")?)?,
            entry_bar_end_at: required_u64(metadata, "entry_bar_end_at")?,
            entry_price,
            stop_price: required_f64(metadata, "stop_price")?,
            target_enabled: required_bool(metadata, "target_enabled")?,
            target_price: optional_f64(metadata, "target_price"),
            profit_trail_enabled,
            profit_trail_activation_r,
            profit_trail_giveback_pct,
            profit_trail_min_lock_r,
            profit_trail_activated: metadata
                .get("profit_trail_activated")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false),
            profit_trail_best_price: optional_f64(metadata, "profit_trail_best_price")
                .unwrap_or(entry_price),
            profit_trail_stop_price: optional_f64(metadata, "profit_trail_stop_price"),
            ema_fail_bars: required_u64(metadata, "ema_fail_bars")?,
            last_exit_check_bar_end_at: required_u64(metadata, "last_exit_check_bar_end_at")?,
            recovery_breach_detected: metadata
                .get("recovery_breach_detected")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false),
            recovery_breach_reason: metadata
                .get("recovery_breach_reason")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string),
            recovery_breach_bar_end_at: metadata
                .get("recovery_breach_bar_end_at")
                .and_then(serde_json::Value::as_u64),
        };
        if context.strategy_key != "exponential_edge" || context.position_id != position_id {
            return Err(StrategyError::Parse(format!(
                "malformed exponential_edge trade context for position {position_id}"
            )));
        }
        Ok(context)
    }

    fn to_metadata(&self) -> serde_json::Value {
        serde_json::json!({
            "strategy_key": self.strategy_key,
            "position_id": self.position_id,
            "setup_id": self.setup_id,
            "side": side_label(self.side),
            "entry_bar_end_at": self.entry_bar_end_at,
            "entry_price": self.entry_price,
            "stop_price": self.stop_price,
            "target_enabled": self.target_enabled,
            "target_price": self.target_price,
            "profit_trail_enabled": self.profit_trail_enabled,
            "profit_trail_activation_r": self.profit_trail_activation_r,
            "profit_trail_giveback_pct": self.profit_trail_giveback_pct,
            "profit_trail_min_lock_r": self.profit_trail_min_lock_r,
            "profit_trail_activated": self.profit_trail_activated,
            "profit_trail_best_price": self.profit_trail_best_price,
            "profit_trail_stop_price": self.profit_trail_stop_price,
            "ema_fail_bars": self.ema_fail_bars,
            "last_exit_check_bar_end_at": self.last_exit_check_bar_end_at,
            "recovery_breach_detected": self.recovery_breach_detected,
            "recovery_breach_reason": self.recovery_breach_reason,
            "recovery_breach_bar_end_at": self.recovery_breach_bar_end_at,
        })
    }
}

#[derive(Clone, Debug)]
struct ExitDecision {
    reason: String,
    reference_price: f64,
}

fn stop_or_target_breach(
    side: SignalSide,
    context: &TradeContext,
    bar: &Bar,
) -> Option<&'static str> {
    match side {
        SignalSide::Long => {
            if bar.low <= context.stop_price {
                Some("stop")
            } else if context.target_enabled
                && context
                    .target_price
                    .is_some_and(|target| bar.high >= target)
            {
                Some("target")
            } else {
                None
            }
        }
        SignalSide::Short => {
            if bar.high >= context.stop_price {
                Some("stop")
            } else if context.target_enabled
                && context.target_price.is_some_and(|target| bar.low <= target)
            {
                Some("target")
            } else {
                None
            }
        }
    }
}

fn profit_trail_breach(side: SignalSide, context: &TradeContext, bar: &Bar) -> Option<f64> {
    if !context.profit_trail_enabled || !context.profit_trail_activated {
        return None;
    }
    let trail = context.profit_trail_stop_price?;
    match side {
        SignalSide::Long if bar.low <= trail => Some(trail),
        SignalSide::Short if bar.high >= trail => Some(trail),
        _ => None,
    }
}

fn update_profit_trail(
    side: SignalSide,
    context: &mut TradeContext,
    bar: &Bar,
) -> Result<(), StrategyError> {
    if !context.profit_trail_enabled {
        return Ok(());
    }
    let risk = (context.entry_price - context.stop_price).abs();
    if !risk.is_finite() || risk <= 0.0 {
        return Err(StrategyError::Parse(format!(
            "malformed exponential_edge trade context {}: invalid initial risk",
            context.position_id
        )));
    }

    match side {
        SignalSide::Long => {
            let best_price = context.profit_trail_best_price.max(context.entry_price);
            context.profit_trail_best_price = best_price.max(bar.high);
            let peak_profit = context.profit_trail_best_price - context.entry_price;
            tighten_profit_trail(side, context, peak_profit, risk, |entry, lock_points| {
                entry + lock_points
            });
        }
        SignalSide::Short => {
            let best_price = context.profit_trail_best_price.min(context.entry_price);
            context.profit_trail_best_price = best_price.min(bar.low);
            let peak_profit = context.entry_price - context.profit_trail_best_price;
            tighten_profit_trail(side, context, peak_profit, risk, |entry, lock_points| {
                entry - lock_points
            });
        }
    }
    Ok(())
}

fn tighten_profit_trail(
    side: SignalSide,
    context: &mut TradeContext,
    peak_profit: f64,
    risk: f64,
    price_from_lock: impl FnOnce(f64, f64) -> f64,
) {
    let activation_profit = context.profit_trail_activation_r * risk;
    if peak_profit < activation_profit {
        return;
    }

    context.profit_trail_activated = true;
    let locked_profit = (peak_profit * (1.0 - context.profit_trail_giveback_pct))
        .max(context.profit_trail_min_lock_r * risk);
    let next_trail = price_from_lock(context.entry_price, locked_profit);
    context.profit_trail_stop_price = Some(match (side, context.profit_trail_stop_price) {
        (SignalSide::Long, Some(previous)) => previous.max(next_trail),
        (SignalSide::Short, Some(previous)) => previous.min(next_trail),
        (_, None) => next_trail,
    });
}

fn apply_entry_identity(signal: &mut StrategySignal, instrument: &str, setup: &DetectedSetup) {
    let side = side_label(setup.side);
    signal.campaign_id = format!(
        "EXEDGE-CMP-{}-{}-{}-{}",
        setup.ssu_id, instrument, side, setup.entry_bar_end_at
    );
    signal.signal_id = format!(
        "EXEDGE-SIG-{}-{}-{}-{}-{}",
        setup.ssu_id, instrument, side, setup.entry_bar_end_at, setup.setup_id
    );
    signal.instructions[0].instruction_id = format!("{}-I1", signal.signal_id);
    signal.instructions[0].leg_id = format!(
        "EXEDGE-POS-{}-{}-{}-{}",
        setup.ssu_id, instrument, side, setup.setup_id
    );
}

fn apply_exit_identity(
    signal: &mut StrategySignal,
    instrument: &str,
    position: &crate::strategy::StrategyPosition,
    exit: &ExitDecision,
) {
    let side = side_label(position.side);
    signal.campaign_id = format!(
        "EXEDGE-CMP-{}-{}-{}",
        signal.ssu_id, instrument, position.position_id
    );
    signal.signal_id = format!(
        "EXEDGE-SIG-{}-{}-{}-{}-{}-{}",
        signal.ssu_id, instrument, side, signal.generated_at, position.position_id, exit.reason
    );
    signal.instructions[0].instruction_id = format!("{}-I1", signal.signal_id);
    signal.instructions[0].leg_id = position.position_id.clone();
}

fn current_ltp(ctx: &StrategyContext, instrument: &str) -> Option<f64> {
    ctx.prices
        .get_price(instrument)
        .map(|snapshot| snapshot.ltp)
}

fn range_from_filter(
    range: Option<RawRange>,
    field: &str,
    ssu_id: i64,
) -> Result<NumericRange, StrategyError> {
    let range = require(range, field, ssu_id)?;
    NumericRange::new(
        require(range.min, &format!("{field}.min"), ssu_id)?,
        require(range.max, &format!("{field}.max"), ssu_id)?,
        field,
        ssu_id,
    )
}

fn ratio_range_from_filter(
    range: Option<RawRange>,
    field: &str,
    ssu_id: i64,
) -> Result<NumericRange, StrategyError> {
    range_with_max_from_filter(range, field, 1.0, ssu_id)
}

fn range_with_max_from_filter(
    range: Option<RawRange>,
    field: &str,
    max_allowed: f64,
    ssu_id: i64,
) -> Result<NumericRange, StrategyError> {
    let range = range_from_filter(range, field, ssu_id)?;
    if range.max <= max_allowed {
        Ok(range)
    } else {
        Err(StrategyError::Config(format!(
            "SSU {ssu_id} exponential_edge {field}.max must be <= {max_allowed}"
        )))
    }
}

fn require<T>(value: Option<T>, field: &str, ssu_id: i64) -> Result<T, StrategyError> {
    value.ok_or_else(|| {
        StrategyError::Config(format!(
            "SSU {ssu_id} exponential_edge missing required field {field}"
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
            "SSU {ssu_id} exponential_edge {field} must be finite and non-negative"
        )))
    }
}

fn validate_profit_trail_config(
    activation_r: f64,
    giveback_pct: f64,
    min_lock_r: f64,
    ssu_id: i64,
) -> Result<(), StrategyError> {
    if !activation_r.is_finite() || activation_r <= 0.0 {
        return Err(StrategyError::Config(format!(
            "SSU {ssu_id} exponential_edge profit_trail_activation_r must be finite and positive"
        )));
    }
    if !giveback_pct.is_finite() || giveback_pct <= 0.0 || giveback_pct >= 1.0 {
        return Err(StrategyError::Config(format!(
            "SSU {ssu_id} exponential_edge profit_trail_giveback_pct must satisfy 0 < value < 1"
        )));
    }
    if !min_lock_r.is_finite() || min_lock_r < 0.0 || min_lock_r > activation_r {
        return Err(StrategyError::Config(format!(
            "SSU {ssu_id} exponential_edge profit_trail_min_lock_r must satisfy 0 <= value <= profit_trail_activation_r"
        )));
    }
    Ok(())
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
        other => Err(StrategyError::Parse(format!(
            "unsupported exponential_edge entry_policy {other}"
        ))),
    }
}

fn parse_stop_mode(value: &str) -> Result<StopMode, StrategyError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "pullback_extreme" => Ok(StopMode::PullbackExtreme),
        other => Err(StrategyError::Parse(format!(
            "unsupported exponential_edge stop_mode {other}"
        ))),
    }
}

fn parse_side(value: &str) -> Result<SignalSide, StrategyError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "long" => Ok(SignalSide::Long),
        "short" => Ok(SignalSide::Short),
        other => Err(StrategyError::Parse(format!(
            "unsupported exponential_edge side {other}; expected long or short"
        ))),
    }
}

fn parse_timeframe(value: &str) -> Result<Timeframe, StrategyError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1m" | "one_minute" | "oneminute" => Ok(Timeframe::OneMinute),
        "3m" | "three_minute" | "threeminute" => Ok(Timeframe::ThreeMinute),
        "5m" | "five_minute" | "fiveminute" => Ok(Timeframe::FiveMinute),
        "15m" | "fifteen_minute" | "fifteenminute" => Ok(Timeframe::FifteenMinute),
        "30m" | "thirty_minute" | "thirtyminute" => Ok(Timeframe::ThirtyMinute),
        "75m" | "seventy_five_minute" | "seventyfiveminute" => Ok(Timeframe::SeventyFiveMinute),
        "1h" | "one_hour" | "onehour" | "60m" => Ok(Timeframe::OneHour),
        "4h" | "four_hour" | "fourhour" | "240m" => Ok(Timeframe::FourHour),
        "1d" | "one_day" | "oneday" => Ok(Timeframe::OneDay),
        other => Err(StrategyError::Parse(format!(
            "unsupported exponential_edge timeframe {other}"
        ))),
    }
}

fn timeframe_label(timeframe: Timeframe) -> &'static str {
    match timeframe {
        Timeframe::OneMinute => "1m",
        Timeframe::ThreeMinute => "3m",
        Timeframe::FiveMinute => "5m",
        Timeframe::FifteenMinute => "15m",
        Timeframe::ThirtyMinute => "30m",
        Timeframe::SeventyFiveMinute => "75m",
        Timeframe::OneHour => "1h",
        Timeframe::FourHour => "4h",
        Timeframe::OneDay => "1d",
    }
}

fn side_label(side: SignalSide) -> &'static str {
    match side {
        SignalSide::Long => "long",
        SignalSide::Short => "short",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    use crate::strategy::{
        InMemoryPriceStore, PriceStore, SharedTimeframeEngine, StrategyPosition,
        StrategyPositionBook, StrategyTradeContextStore, TimeframeEngine,
    };

    fn test_bar(index: u64, high: f64, low: f64, close: f64) -> Bar {
        Bar {
            instrument: "BTCUSD".to_string(),
            timeframe: Timeframe::FiveMinute,
            start_at: index * 300_000,
            end_at: (index + 1) * 300_000,
            open: close,
            high,
            low,
            close,
            volume: 0.0,
            is_closed: true,
        }
    }

    fn complete_ssu(params_json: String) -> SsuConfig {
        SsuConfig {
            ssu_id: 7,
            strategy_key: "exponential_edge".to_string(),
            enabled: true,
            trade_gap_secs: 0,
            max_overlap: 1,
            max_positions_per_day: 10,
            required_timeframes: vec![Timeframe::FiveMinute],
            indicator_specs: Vec::new(),
            params_json,
        }
    }

    fn complete_params() -> String {
        serde_json::json!({
            "timeframe": "5m",
            "enabled_sides": ["long", "short"],
            "ema_period": 3,
            "atr_period": 3,
            "trend_lookback_bars": 6,
            "min_retracement_bars": 1,
            "max_retracement_bars": 4,
            "ker_period": 2,
            "adx_period": 2,
            "filters": {
                "trend_height_atr": { "min": 0.1, "max": 1000000.0 },
                "retracement_ratio": { "min": 0.0, "max": 1.0 },
                "ema_touch_tolerance_atr": { "min": 0.0, "max": 1000000.0 },
                "ker": { "min": 0.0, "max": 1.0 },
                "adx": { "min": 0.0, "max": 100.0 }
            },
            "stop_mode": "pullback_extreme",
            "stop_buffer_atr": 0.2,
            "target_enabled": false,
            "target_r_multiple": 2.0,
            "exit_on_ema_fail_bars": 2,
            "entry_policy": "single_position"
        })
        .to_string()
    }

    fn entry_bars() -> Vec<Bar> {
        vec![
            test_bar(0, 91.0, 90.0, 91.0),
            test_bar(1, 95.0, 93.0, 94.0),
            test_bar(2, 112.0, 96.0, 111.0),
            test_bar(3, 110.0, 105.0, 108.0),
            test_bar(4, 109.0, 104.0, 106.0),
            test_bar(5, 108.0, 103.0, 105.0),
            test_bar(6, 108.0, 103.0, 107.0),
        ]
    }

    fn context_metadata(
        position_id: &str,
        side: SignalSide,
        entry_price: f64,
        stop_price: f64,
        target_price: Option<f64>,
    ) -> serde_json::Value {
        serde_json::json!({
            "strategy_key": "exponential_edge",
            "position_id": position_id,
            "setup_id": "SETUP-1",
            "side": side_label(side),
            "entry_bar_end_at": 300_000_u64,
            "entry_price": entry_price,
            "stop_price": stop_price,
            "target_enabled": target_price.is_some(),
            "target_price": target_price,
            "ema_fail_bars": 0_u64,
            "last_exit_check_bar_end_at": 0_u64,
            "recovery_breach_detected": false,
            "recovery_breach_reason": serde_json::Value::Null,
            "recovery_breach_bar_end_at": serde_json::Value::Null,
        })
    }

    fn open_position(position_id: &str, side: SignalSide, entry_price: f64) -> StrategyPosition {
        StrategyPosition {
            position_id: position_id.to_string(),
            ssu_id: 7,
            trigger_instrument: "BTCUSD".to_string(),
            trade_instrument: "BTCUSD".to_string(),
            side,
            entry_price,
            entry_at: 300_000,
            exit_price: None,
            exit_at: None,
            exit_reason: None,
            pnl: None,
            status: PositionStatus::Open,
        }
    }

    fn build_context(
        ssu: &SsuConfig,
        bars: &[Bar],
        position_book: Arc<TestPositionBook>,
        trade_contexts: Arc<TestTradeContextStore>,
    ) -> (StrategyContext, TimeframeUpdate, PriceUpdated) {
        let prices = Arc::new(InMemoryPriceStore::new());
        let timeframes = Arc::new(SharedTimeframeEngine::new(64));
        timeframes.register_ssu(ssu).expect("register");
        let mut update = TimeframeUpdate {
            instrument: "BTCUSD".to_string(),
            tick_at: 0,
            closed_timeframes: Vec::new(),
        };
        for bar in bars {
            prices.put_price(&bar.instrument, bar.close, bar.end_at);
            update = timeframes.on_closed_bar(bar).expect("closed bar");
        }
        let event = PriceUpdated {
            trigger_instrument: "BTCUSD".to_string(),
            at: bars.last().expect("bars").end_at,
        };
        (
            StrategyContext {
                prices: prices as Arc<dyn PriceStore>,
                timeframes: timeframes as Arc<dyn TimeframeEngine>,
                strategy_positions: position_book as Arc<dyn StrategyPositionBook>,
                trade_contexts: trade_contexts as Arc<dyn StrategyTradeContextStore>,
            },
            update,
            event,
        )
    }

    #[derive(Default)]
    struct TestPositionBook {
        positions: Mutex<BTreeMap<String, StrategyPosition>>,
        closed_signals: Mutex<Vec<StrategySignal>>,
    }

    impl TestPositionBook {
        fn insert(&self, position: StrategyPosition) {
            self.positions
                .lock()
                .expect("positions")
                .insert(position.position_id.clone(), position);
        }

        fn position(&self, position_id: &str) -> Option<StrategyPosition> {
            self.positions
                .lock()
                .expect("positions")
                .get(position_id)
                .cloned()
        }

        fn open_count(&self) -> usize {
            self.positions
                .lock()
                .expect("positions")
                .values()
                .filter(|position| position.status == PositionStatus::Open)
                .count()
        }
    }

    impl StrategyPositionBook for TestPositionBook {
        fn list_open_by_ssu(&self, ssu_id: i64) -> Result<Vec<StrategyPosition>, StrategyError> {
            Ok(self
                .positions
                .lock()
                .expect("positions")
                .values()
                .filter(|position| {
                    position.ssu_id == ssu_id && position.status == PositionStatus::Open
                })
                .cloned()
                .collect())
        }

        fn last_entry_time_by_ssu(&self, ssu_id: i64) -> Result<Option<u64>, StrategyError> {
            Ok(self
                .positions
                .lock()
                .expect("positions")
                .values()
                .filter(|position| position.ssu_id == ssu_id)
                .map(|position| position.entry_at)
                .max())
        }

        fn entries_today_by_ssu(&self, ssu_id: i64, _ist_day: &str) -> Result<u32, StrategyError> {
            Ok(self
                .positions
                .lock()
                .expect("positions")
                .values()
                .filter(|position| position.ssu_id == ssu_id)
                .count() as u32)
        }

        fn open_position(
            &self,
            signal: &StrategySignal,
            _ssu: &SsuConfig,
        ) -> Result<StrategyPosition, StrategyError> {
            let side = signal
                .side()
                .ok_or_else(|| StrategyError::Rule("entry signal has no side".to_string()))?;
            let instruction = signal.primary_instruction().ok_or_else(|| {
                StrategyError::Rule("entry signal has no instruction".to_string())
            })?;
            let entry_price = instruction
                .price_policy
                .reference_price
                .ok_or_else(|| StrategyError::Rule("entry signal has no price".to_string()))?;
            let position = StrategyPosition {
                position_id: instruction.leg_id.clone(),
                ssu_id: signal.ssu_id,
                trigger_instrument: signal.trigger_instrument.clone(),
                trade_instrument: instruction.instrument_name.clone(),
                side,
                entry_price,
                entry_at: signal.generated_at,
                exit_price: None,
                exit_at: None,
                exit_reason: None,
                pnl: None,
                status: PositionStatus::Open,
            };
            self.insert(position.clone());
            Ok(position)
        }

        fn close_position(
            &self,
            signal: &StrategySignal,
        ) -> Result<StrategyPosition, StrategyError> {
            let instruction = signal
                .primary_instruction()
                .ok_or_else(|| StrategyError::Rule("exit signal has no instruction".to_string()))?;
            let exit_price = instruction
                .price_policy
                .reference_price
                .ok_or_else(|| StrategyError::Rule("exit signal has no price".to_string()))?;
            let mut positions = self.positions.lock().expect("positions");
            let position = positions
                .get_mut(&instruction.leg_id)
                .ok_or_else(|| StrategyError::NotFound("missing position".to_string()))?;
            position.status = PositionStatus::Closed;
            position.exit_price = Some(exit_price);
            position.exit_at = Some(signal.generated_at);
            position.exit_reason = Some(signal.reason.clone());
            let closed = position.clone();
            self.closed_signals
                .lock()
                .expect("closed signals")
                .push(signal.clone());
            Ok(closed)
        }

        fn partial_close_position(
            &self,
            signal: &StrategySignal,
        ) -> Result<StrategyPosition, StrategyError> {
            let instruction = signal.primary_instruction().ok_or_else(|| {
                StrategyError::Rule("partial exit signal has no instruction".to_string())
            })?;
            self.positions
                .lock()
                .expect("positions")
                .get(&instruction.leg_id)
                .cloned()
                .ok_or_else(|| StrategyError::NotFound("missing position".to_string()))
        }
    }

    #[derive(Default)]
    struct TestTradeContextStore {
        contexts: Mutex<BTreeMap<String, (i64, String, serde_json::Value)>>,
        fail_save: Mutex<bool>,
        fail_update: Mutex<bool>,
    }

    impl TestTradeContextStore {
        fn set_fail_save(&self, fail: bool) {
            *self.fail_save.lock().expect("fail save") = fail;
        }

        fn set_fail_update(&self, fail: bool) {
            *self.fail_update.lock().expect("fail update") = fail;
        }

        fn insert(
            &self,
            position_id: &str,
            ssu_id: i64,
            trigger_instrument: &str,
            metadata: serde_json::Value,
        ) {
            self.contexts.lock().expect("contexts").insert(
                position_id.to_string(),
                (ssu_id, trigger_instrument.to_string(), metadata),
            );
        }

        fn metadata(&self, position_id: &str) -> Option<serde_json::Value> {
            self.contexts
                .lock()
                .expect("contexts")
                .get(position_id)
                .map(|(_, _, metadata)| metadata.clone())
        }
    }

    impl StrategyTradeContextStore for TestTradeContextStore {
        fn save_context(
            &self,
            position_id: &str,
            ssu_id: i64,
            _strategy_key: &str,
            trigger_instrument: &str,
            metadata: &serde_json::Value,
            _updated_at: u64,
        ) -> Result<(), StrategyError> {
            if *self.fail_save.lock().expect("fail save") {
                return Err(StrategyError::Io("forced context save failure".to_string()));
            }
            self.insert(position_id, ssu_id, trigger_instrument, metadata.clone());
            Ok(())
        }

        fn load_context(
            &self,
            position_id: &str,
        ) -> Result<Option<serde_json::Value>, StrategyError> {
            Ok(self.metadata(position_id))
        }

        fn load_open_contexts(
            &self,
            ssu_id: i64,
            trigger_instrument: &str,
        ) -> Result<Vec<(String, serde_json::Value)>, StrategyError> {
            Ok(self
                .contexts
                .lock()
                .expect("contexts")
                .iter()
                .filter(|(_, (stored_ssu_id, stored_instrument, _))| {
                    *stored_ssu_id == ssu_id && stored_instrument == trigger_instrument
                })
                .map(|(position_id, (_, _, metadata))| (position_id.clone(), metadata.clone()))
                .collect())
        }

        fn update_context(
            &self,
            position_id: &str,
            metadata: &serde_json::Value,
            _updated_at: u64,
        ) -> Result<(), StrategyError> {
            if *self.fail_update.lock().expect("fail update") {
                return Err(StrategyError::Io(
                    "forced context update failure".to_string(),
                ));
            }
            let mut contexts = self.contexts.lock().expect("contexts");
            let Some((_, _, stored)) = contexts.get_mut(position_id) else {
                return Err(StrategyError::NotFound("missing context".to_string()));
            };
            *stored = metadata.clone();
            Ok(())
        }

        fn delete_context(&self, position_id: &str) -> Result<(), StrategyError> {
            self.contexts.lock().expect("contexts").remove(position_id);
            Ok(())
        }
    }

    #[test]
    fn parses_valid_ssu() {
        let settings =
            ExponentialEdgeSettings::from_ssu(&complete_ssu(complete_params())).expect("settings");
        assert_eq!(settings.timeframe, Timeframe::FiveMinute);
        assert_eq!(settings.state_capacity(), 8);
        assert!(!settings.profit_trail_enabled);
        assert_eq!(
            settings.profit_trail_activation_r,
            DEFAULT_PROFIT_TRAIL_ACTIVATION_R
        );
    }

    #[test]
    fn rejects_invalid_retracement_range() {
        let mut value: serde_json::Value = serde_json::from_str(&complete_params()).unwrap();
        value["max_retracement_bars"] = serde_json::json!(0);
        let error = ExponentialEdgeSettings::from_ssu(&complete_ssu(value.to_string()))
            .expect_err("invalid");
        assert!(format!("{error}").contains("invalid retracement"));
    }

    #[test]
    fn rejects_invalid_profit_trail_config() {
        let mut value: serde_json::Value = serde_json::from_str(&complete_params()).unwrap();
        value["profit_trail_enabled"] = serde_json::json!(true);
        value["profit_trail_activation_r"] = serde_json::json!(3.0);
        value["profit_trail_giveback_pct"] = serde_json::json!(1.0);
        value["profit_trail_min_lock_r"] = serde_json::json!(1.0);

        let error = ExponentialEdgeSettings::from_ssu(&complete_ssu(value.to_string()))
            .expect_err("invalid");

        assert!(format!("{error}").contains("profit_trail_giveback_pct"));
    }

    #[test]
    fn ema_seeds_and_updates() {
        let mut ema = EmaIndicator::new(3);
        ema.on_close(10.0);
        ema.on_close(13.0);
        assert_eq!(ema.value(), None);
        ema.on_close(16.0);
        assert_eq!(ema.value(), Some(13.0));
        ema.on_close(19.0);
        assert_eq!(ema.value(), Some(16.0));
    }

    #[test]
    fn atr_seeds_and_updates() {
        let mut atr = AtrIndicator::new(2);
        atr.on_bar(&test_bar(0, 10.0, 8.0, 9.0));
        atr.on_bar(&test_bar(1, 12.0, 9.0, 11.0));
        assert_eq!(atr.value(), None);
        atr.on_bar(&test_bar(2, 13.0, 10.0, 12.0));
        assert_eq!(atr.value(), Some(3.0));
        atr.on_bar(&test_bar(3, 15.0, 11.0, 14.0));
        assert_eq!(atr.value(), Some(3.5));
    }

    #[test]
    fn ker_computes_directional_efficiency() {
        let mut ker = KerIndicator::new(3);
        for close in [10.0, 12.0, 11.0, 15.0] {
            ker.on_close(close);
        }
        assert!((ker.value().unwrap() - (5.0 / 7.0)).abs() < 0.000001);
    }

    #[test]
    fn adx_readiness_after_two_periods_plus_previous_bar() {
        let mut adx = AdxIndicator::new(2);
        for index in 0..4 {
            adx.on_bar(&test_bar(
                index,
                10.0 + index as f64,
                8.0,
                9.0 + index as f64,
            ));
            assert!(adx.value().is_none());
        }
        adx.on_bar(&test_bar(4, 14.0, 8.0, 13.0));
        assert!(adx.value().is_some());
    }

    #[test]
    fn ring_tree_matches_bruteforce_after_wrap() {
        let bars = vec![
            test_bar(0, 11.0, 10.0, 10.5),
            test_bar(1, 12.0, 9.0, 11.0),
            test_bar(2, 10.0, 8.0, 9.0),
            test_bar(3, 14.0, 11.0, 13.0),
            test_bar(4, 13.0, 7.0, 8.0),
            test_bar(5, 16.0, 12.0, 15.0),
        ];
        let mut tree = RingTrendTree::new(4);
        for bar in &bars {
            tree.insert(bar);
        }
        let summary = tree.query_active().expect("summary");
        let active = &bars[2..];
        let (long_height, long_start, long_extreme) = brute_long(active);
        let best_long = summary.best_long.expect("long");
        assert_eq!(best_long.height, long_height);
        assert_eq!(best_long.start_at, long_start);
        assert_eq!(best_long.extreme_at, long_extreme);
    }

    #[test]
    fn empty_pre_trigger_pullback_range_uses_current_bar() {
        let mut tree = RingTrendTree::new(3);
        tree.insert(&test_bar(0, 10.0, 8.0, 9.0));
        tree.insert(&test_bar(1, 13.0, 9.0, 12.0));
        tree.insert(&test_bar(2, 15.0, 11.0, 14.0));
        let latest = tree.latest_logical_index().unwrap();
        let summary = tree.query_active().unwrap();
        let leg = summary.best_long.unwrap();
        assert_eq!(leg.extreme_index, latest);
        assert!(tree.query_logical(leg.extreme_index + 1, latest).is_none());
    }

    #[test]
    fn finalizing_current_bar_advances_after_warmup() {
        let settings =
            ExponentialEdgeSettings::from_ssu(&complete_ssu(complete_params())).expect("settings");
        let mut state = RuntimeState::new(&settings);
        state
            .process_warmup_bar(&test_bar(0, 10.0, 8.0, 9.0))
            .unwrap();
        let current = test_bar(1, 11.0, 9.0, 10.0);
        state.prepare_current_bar(&current).unwrap();
        state.finalize_current_bar(&current).unwrap();
        assert_eq!(state.last_processed_closed_end, Some(current.end_at));
        assert_eq!(state.trend.len(), 2);
    }

    #[test]
    fn duplicate_closed_bar_does_not_run_exit_logic_twice() {
        let ssu = complete_ssu(complete_params());
        let strategy = ExponentialEdgeStrategy::default();
        let position_book = Arc::new(TestPositionBook::default());
        let trade_contexts = Arc::new(TestTradeContextStore::default());
        position_book.insert(open_position("P1", SignalSide::Long, 105.0));
        trade_contexts.insert(
            "P1",
            ssu.ssu_id,
            "BTCUSD",
            context_metadata("P1", SignalSide::Long, 105.0, 50.0, None),
        );
        let closes = [100.0, 102.0, 104.0, 106.0, 108.0, 110.0, 112.0, 100.0];
        let bars = closes
            .iter()
            .enumerate()
            .map(|(index, close)| test_bar(index as u64, close + 2.0, close - 2.0, *close))
            .collect::<Vec<_>>();
        let (ctx, update, event) =
            build_context(&ssu, &bars, position_book.clone(), trade_contexts.clone());

        let first = strategy
            .on_price_updated(&ctx, &ssu, &event, &update)
            .expect("first");
        let second = strategy
            .on_price_updated(&ctx, &ssu, &event, &update)
            .expect("duplicate");

        assert!(first.is_empty());
        assert!(second.is_empty());
        let metadata = trade_contexts.metadata("P1").expect("context");
        assert_eq!(required_u64(&metadata, "ema_fail_bars").unwrap(), 1);
        assert_eq!(
            position_book.position("P1").expect("position").status,
            PositionStatus::Open
        );
    }

    #[test]
    fn stop_exit_runs_without_entry_indicator_readiness() {
        let ssu = complete_ssu(complete_params());
        let strategy = ExponentialEdgeStrategy::default();
        let position_book = Arc::new(TestPositionBook::default());
        let trade_contexts = Arc::new(TestTradeContextStore::default());
        position_book.insert(open_position("P1", SignalSide::Long, 105.0));
        trade_contexts.insert(
            "P1",
            ssu.ssu_id,
            "BTCUSD",
            context_metadata("P1", SignalSide::Long, 105.0, 99.0, None),
        );
        let bars = vec![test_bar(0, 106.0, 98.0, 100.0)];
        let (ctx, update, event) =
            build_context(&ssu, &bars, position_book.clone(), trade_contexts.clone());

        let signals = strategy
            .on_price_updated(&ctx, &ssu, &event, &update)
            .expect("signals");

        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].metadata["exit_reason"], "stop");
        assert_eq!(
            position_book.position("P1").expect("position").status,
            PositionStatus::Closed
        );
        assert!(trade_contexts.metadata("P1").is_none());
    }

    #[test]
    fn profit_trail_activates_then_exits_on_giveback() {
        let mut params: serde_json::Value = serde_json::from_str(&complete_params()).unwrap();
        params["profit_trail_enabled"] = serde_json::json!(true);
        params["profit_trail_activation_r"] = serde_json::json!(2.0);
        params["profit_trail_giveback_pct"] = serde_json::json!(0.5);
        params["profit_trail_min_lock_r"] = serde_json::json!(1.0);
        let ssu = complete_ssu(params.to_string());
        let strategy = ExponentialEdgeStrategy::default();
        let position_book = Arc::new(TestPositionBook::default());
        let trade_contexts = Arc::new(TestTradeContextStore::default());
        position_book.insert(open_position("P1", SignalSide::Long, 100.0));

        let mut metadata = context_metadata("P1", SignalSide::Long, 100.0, 90.0, None);
        metadata["profit_trail_enabled"] = serde_json::json!(true);
        metadata["profit_trail_activation_r"] = serde_json::json!(2.0);
        metadata["profit_trail_giveback_pct"] = serde_json::json!(0.5);
        metadata["profit_trail_min_lock_r"] = serde_json::json!(1.0);
        trade_contexts.insert("P1", ssu.ssu_id, "BTCUSD", metadata);

        let first_bar = test_bar(0, 125.0, 101.0, 120.0);
        let (ctx, update, event) = build_context(
            &ssu,
            std::slice::from_ref(&first_bar),
            position_book.clone(),
            trade_contexts.clone(),
        );
        let first = strategy
            .on_price_updated(&ctx, &ssu, &event, &update)
            .expect("activation bar");
        assert!(first.is_empty());

        let metadata = trade_contexts.metadata("P1").expect("context");
        assert_eq!(metadata["profit_trail_activated"], serde_json::json!(true));
        assert_eq!(
            metadata["profit_trail_best_price"],
            serde_json::json!(125.0)
        );
        assert_eq!(
            metadata["profit_trail_stop_price"],
            serde_json::json!(112.5)
        );

        let second_bar = test_bar(1, 123.0, 112.0, 113.0);
        let (ctx, update, event) = build_context(
            &ssu,
            &[first_bar, second_bar],
            position_book.clone(),
            trade_contexts.clone(),
        );
        let signals = strategy
            .on_price_updated(&ctx, &ssu, &event, &update)
            .expect("trail exit");

        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].metadata["exit_reason"], "profit_trail");
        assert_eq!(
            signals[0]
                .primary_instruction()
                .unwrap()
                .price_policy
                .reference_price,
            Some(112.5)
        );
        assert_eq!(
            position_book.position("P1").expect("position").status,
            PositionStatus::Closed
        );
    }

    #[test]
    fn missing_trade_context_is_recovery_error() {
        let ssu = complete_ssu(complete_params());
        let strategy = ExponentialEdgeStrategy::default();
        let position_book = Arc::new(TestPositionBook::default());
        let trade_contexts = Arc::new(TestTradeContextStore::default());
        position_book.insert(open_position("P1", SignalSide::Long, 105.0));
        let bars = vec![test_bar(0, 106.0, 98.0, 100.0)];
        let (ctx, update, event) =
            build_context(&ssu, &bars, position_book.clone(), trade_contexts.clone());

        let error = strategy
            .on_price_updated(&ctx, &ssu, &event, &update)
            .expect_err("missing context");

        assert!(format!("{error}").contains("missing trade context"));
        assert_eq!(
            position_book.position("P1").expect("position").status,
            PositionStatus::Open
        );
    }

    #[test]
    fn warmup_recovery_replay_survives_context_update_failure() {
        let ssu = complete_ssu(complete_params());
        let strategy = ExponentialEdgeStrategy::default();
        let position_book = Arc::new(TestPositionBook::default());
        let trade_contexts = Arc::new(TestTradeContextStore::default());
        position_book.insert(open_position("P1", SignalSide::Long, 105.0));
        trade_contexts.insert(
            "P1",
            ssu.ssu_id,
            "BTCUSD",
            context_metadata("P1", SignalSide::Long, 105.0, 99.0, None),
        );
        let bars = vec![
            test_bar(0, 106.0, 98.0, 100.0),
            test_bar(1, 106.0, 100.0, 101.0),
            test_bar(2, 106.0, 100.0, 102.0),
        ];
        let (ctx, update, event) =
            build_context(&ssu, &bars, position_book.clone(), trade_contexts.clone());

        trade_contexts.set_fail_update(true);
        let error = strategy
            .on_price_updated(&ctx, &ssu, &event, &update)
            .expect_err("first recovery update fails");
        assert!(format!("{error}").contains("forced context update failure"));
        assert_eq!(
            position_book.position("P1").expect("position").status,
            PositionStatus::Open
        );

        trade_contexts.set_fail_update(false);
        let signals = strategy
            .on_price_updated(&ctx, &ssu, &event, &update)
            .expect("retry replays warmup recovery");

        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].metadata["exit_reason"], "recovery_breach");
        assert_eq!(
            signals[0].metadata["recovery_breach_reason"],
            serde_json::json!("stop")
        );
        assert_eq!(
            position_book.position("P1").expect("position").status,
            PositionStatus::Closed
        );
        assert!(trade_contexts.metadata("P1").is_none());
    }

    #[test]
    fn entry_context_save_failure_does_not_open_or_finalize_entry() {
        let ssu = complete_ssu(complete_params());
        let strategy = ExponentialEdgeStrategy::default();
        let position_book = Arc::new(TestPositionBook::default());
        let trade_contexts = Arc::new(TestTradeContextStore::default());
        let bars = entry_bars();
        let (ctx, update, event) =
            build_context(&ssu, &bars, position_book.clone(), trade_contexts.clone());

        trade_contexts.set_fail_save(true);
        let error = strategy
            .on_price_updated(&ctx, &ssu, &event, &update)
            .expect_err("save context failure");
        assert!(format!("{error}").contains("forced context save failure"));
        assert_eq!(position_book.open_count(), 0);

        trade_contexts.set_fail_save(false);
        let signals = strategy
            .on_price_updated(&ctx, &ssu, &event, &update)
            .expect("retry");
        assert_eq!(signals.len(), 1);
        assert_eq!(position_book.open_count(), 1);
        let position_id = signals[0].instructions[0].leg_id.clone();
        assert!(trade_contexts.metadata(&position_id).is_some());

        let duplicate = strategy
            .on_price_updated(&ctx, &ssu, &event, &update)
            .expect("duplicate");
        assert!(duplicate.is_empty());
    }

    fn brute_long(bars: &[Bar]) -> (f64, u64, u64) {
        let mut best = (f64::MIN, 0, 0);
        for i in 0..bars.len() {
            for j in i + 1..bars.len() {
                let height = bars[j].high - bars[i].low;
                if height > best.0
                    || ((height - best.0).abs() <= f64::EPSILON
                        && (bars[j].end_at > best.2
                            || (bars[j].end_at == best.2 && bars[i].end_at > best.1)))
                {
                    best = (height, bars[i].end_at, bars[j].end_at);
                }
            }
        }
        best
    }
}
