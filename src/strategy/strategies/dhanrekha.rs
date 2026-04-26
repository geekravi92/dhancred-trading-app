use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Mutex;

use chrono::{Datelike, FixedOffset, TimeZone, Utc};
use serde::Deserialize;

use crate::strategy::{
    Bar, PositionStatus, PriceUpdated, SignalSide, SsuConfig, Strategy, StrategyContext,
    StrategyError, StrategySignal, Timeframe, TimeframeUpdate,
};

#[derive(Debug, Default)]
pub(crate) struct DhanrekhaStrategy {
    settings: Mutex<BTreeMap<i64, DhanrekhaSettings>>,
    states: Mutex<BTreeMap<StateKey, RuntimeState>>,
}

impl Strategy for DhanrekhaStrategy {
    fn strategy_key(&self) -> &'static str {
        "dhanrekha"
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
        self.bootstrap_state(ctx, &state_key, &settings, &event.trigger_instrument)?;

        let mut exits = self.manage_open_positions(ctx, ssu, event, &settings, &closed_bar)?;
        if !exits.is_empty() {
            self.advance_state(&state_key, &settings, &closed_bar, false)?;
            return Ok(exits);
        }

        let setups = self.advance_state(&state_key, &settings, &closed_bar, true)?;
        for setup in setups {
            if !settings.enabled_sides.contains(&setup.side) {
                continue;
            }
            if !self.entry_policy_allows(ctx, ssu, &event.trigger_instrument, setup.side)? {
                continue;
            }

            let entry_price =
                current_ltp(ctx, &event.trigger_instrument).unwrap_or(setup.entry_price);
            let Some((stop_price, target_price)) = setup.stop_and_target(entry_price, &settings)
            else {
                continue;
            };

            let mut signal = StrategySignal::single_leg_entry(
                ssu.ssu_id,
                self.strategy_key(),
                &event.trigger_instrument,
                setup.side,
                entry_price,
                setup.reason(&settings),
                closed_bar.end_at,
            );
            signal.metadata = setup.entry_metadata(entry_price, stop_price, target_price);
            signal.instructions[0].metadata = serde_json::json!({
                "setup_id": setup.setup_id,
                "mode": setup.mode.as_str(),
                "zone_id": setup.zone_id,
                "stop_price": stop_price,
                "target_enabled": settings.target_enabled,
                "target_price": target_price,
            });

            let position = match ctx.strategy_positions.open_position(&signal, ssu) {
                Ok(position) => position,
                Err(StrategyError::Rule(_)) => continue,
                Err(error) => return Err(error),
            };
            let metadata = setup.trade_context_metadata(
                &signal,
                &position.position_id,
                entry_price,
                stop_price,
                target_price,
            );
            ctx.trade_contexts.save_context(
                &position.position_id,
                ssu.ssu_id,
                self.strategy_key(),
                &event.trigger_instrument,
                &metadata,
                closed_bar.end_at,
            )?;

            exits.push(signal);
            return Ok(exits);
        }

        Ok(Vec::new())
    }
}

impl DhanrekhaStrategy {
    fn settings_for(&self, ssu: &SsuConfig) -> Result<DhanrekhaSettings, StrategyError> {
        if let Some(settings) = self
            .settings
            .lock()
            .expect("dhanrekha settings lock poisoned")
            .get(&ssu.ssu_id)
            .cloned()
        {
            return Ok(settings);
        }

        let settings = DhanrekhaSettings::from_ssu(ssu)?;
        self.settings
            .lock()
            .expect("dhanrekha settings lock poisoned")
            .insert(ssu.ssu_id, settings.clone());
        Ok(settings)
    }

    fn bootstrap_state(
        &self,
        ctx: &StrategyContext,
        state_key: &StateKey,
        settings: &DhanrekhaSettings,
        instrument: &str,
    ) -> Result<(), StrategyError> {
        let needs_bootstrap = self
            .states
            .lock()
            .expect("dhanrekha state lock poisoned")
            .get(state_key)
            .is_none_or(|state| state.last_processed_closed_end.is_none());
        if !needs_bootstrap {
            return Ok(());
        }

        let bars = ctx.timeframes.recent_bars(
            instrument,
            settings.timeframe,
            settings.bootstrap_bar_count(),
        );
        let mut states = self.states.lock().expect("dhanrekha state lock poisoned");
        let state = states
            .entry(state_key.clone())
            .or_insert_with(|| RuntimeState::new(settings));
        for bar in bars {
            state.on_closed_bar(settings, &bar, false)?;
        }
        Ok(())
    }

    fn advance_state(
        &self,
        state_key: &StateKey,
        settings: &DhanrekhaSettings,
        closed_bar: &Bar,
        may_emit: bool,
    ) -> Result<Vec<DetectedSetup>, StrategyError> {
        let mut states = self.states.lock().expect("dhanrekha state lock poisoned");
        let state = states
            .entry(state_key.clone())
            .or_insert_with(|| RuntimeState::new(settings));
        state.on_closed_bar(settings, closed_bar, may_emit)
    }

    fn entry_policy_allows(
        &self,
        ctx: &StrategyContext,
        ssu: &SsuConfig,
        trigger_instrument: &str,
        side: SignalSide,
    ) -> Result<bool, StrategyError> {
        let open_positions = ctx.strategy_positions.list_open_by_ssu(ssu.ssu_id)?;
        let matching = open_positions
            .into_iter()
            .filter(|position| position.trigger_instrument == trigger_instrument)
            .filter(|position| position.status == PositionStatus::Open)
            .collect::<Vec<_>>();

        match self.settings_for(ssu)?.entry_policy {
            EntryPolicy::SinglePosition => Ok(matching.is_empty()),
            EntryPolicy::Independent => {
                Ok(!matching.into_iter().any(|position| position.side == side))
            }
        }
    }

    fn manage_open_positions(
        &self,
        ctx: &StrategyContext,
        ssu: &SsuConfig,
        event: &PriceUpdated,
        settings: &DhanrekhaSettings,
        closed_bar: &Bar,
    ) -> Result<Vec<StrategySignal>, StrategyError> {
        let mut exits = Vec::new();
        let open_positions = ctx.strategy_positions.list_open_by_ssu(ssu.ssu_id)?;
        for position in open_positions
            .into_iter()
            .filter(|position| position.trigger_instrument == event.trigger_instrument)
            .filter(|position| position.status == PositionStatus::Open)
        {
            let Some(metadata) = ctx.trade_contexts.load_context(&position.position_id)? else {
                return Err(StrategyError::Parse(format!(
                    "dhanrekha missing trade context for open position {}",
                    position.position_id
                )));
            };
            let mut context = TradeContext::from_metadata(&metadata)?;
            if closed_bar.end_at <= context.last_exit_check_bar_end_at {
                continue;
            }

            let exit_reason = match position.side {
                SignalSide::Long => {
                    if closed_bar.low <= context.stop_price {
                        Some("stop")
                    } else if context
                        .target_price
                        .is_some_and(|target| closed_bar.high >= target)
                    {
                        Some("target")
                    } else {
                        None
                    }
                }
                SignalSide::Short => {
                    if closed_bar.high >= context.stop_price {
                        Some("stop")
                    } else if context
                        .target_price
                        .is_some_and(|target| closed_bar.low <= target)
                    {
                        Some("target")
                    } else {
                        None
                    }
                }
            }
            .or_else(|| {
                if settings.max_hold_bars > 0 && context.hold_bars + 1 >= settings.max_hold_bars {
                    Some("max_hold")
                } else {
                    None
                }
            });

            if let Some(reason) = exit_reason {
                let price = current_ltp(ctx, &event.trigger_instrument).unwrap_or(closed_bar.close);
                let mut signal = StrategySignal::single_leg_exit(
                    ssu.ssu_id,
                    self.strategy_key(),
                    &event.trigger_instrument,
                    position.side,
                    price,
                    format!(
                        "dhanrekha_exit|reason={reason}|mode={}|zone_id={}|closed_bar_end={}",
                        context.mode, context.zone_id, closed_bar.end_at
                    ),
                    closed_bar.end_at,
                );
                signal.campaign_id = context.campaign_id.clone();
                signal.signal_id = format!(
                    "SIG-{}-{}-EXIT-{}",
                    ssu.ssu_id, closed_bar.end_at, position.position_id
                );
                signal.instructions[0].instruction_id = format!("{}-I1", signal.signal_id);
                signal.instructions[0].leg_id = position.position_id.clone();
                signal.metadata = serde_json::json!({
                    "exit_reason": reason,
                    "position_id": position.position_id,
                    "mode": context.mode,
                    "zone_id": context.zone_id,
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
                context.hold_bars = context.hold_bars.saturating_add(1);
                context.last_exit_check_bar_end_at = closed_bar.end_at;
                ctx.trade_contexts.update_context(
                    &position.position_id,
                    &context.to_metadata(),
                    closed_bar.end_at,
                )?;
            }
        }

        Ok(exits)
    }
}

#[derive(Clone, Debug)]
struct DhanrekhaSettings {
    timeframe: Timeframe,
    enabled_modes: BTreeSet<SetupMode>,
    enabled_sides: Vec<SignalSide>,
    level_sources: BTreeSet<LevelSource>,
    period_timezone: PeriodTimezone,
    entry_policy: EntryPolicy,
    atr_period: usize,
    pivot_left_bars: usize,
    pivot_right_bars: usize,
    donchian_lookback_bars: usize,
    zone_atr_mult: f64,
    max_active_zones: usize,
    max_zone_age_bars: u64,
    max_broken_closes: u32,
    min_zone_score: f64,
    touch_tolerance_atr: f64,
    break_close_beyond_atr: f64,
    pivot_level_score: f64,
    prev_day_level_score: f64,
    prev_week_level_score: f64,
    donchian_level_score: f64,
    touch_score: f64,
    broken_close_penalty: f64,
    sweep_min_penetration_atr: f64,
    sweep_max_depth_atr: f64,
    max_reclaim_bars: u64,
    breakout_min_close_beyond_atr: f64,
    breakout_accept_closes: u32,
    max_retest_bars: u64,
    retest_tolerance_atr: f64,
    retest_max_penetration_atr: f64,
    min_retest_close_location: f64,
    stop_buffer_atr: f64,
    target_enabled: bool,
    target_r_multiple: f64,
    max_hold_bars: u64,
}

impl DhanrekhaSettings {
    fn from_ssu(ssu: &SsuConfig) -> Result<Self, StrategyError> {
        #[derive(Deserialize)]
        struct Raw {
            timeframe: Option<String>,
            enabled_modes: Option<Vec<String>>,
            enabled_sides: Option<Vec<String>>,
            level_sources: Option<Vec<String>>,
            period_timezone: Option<String>,
            entry_policy: Option<String>,
            atr_period: Option<usize>,
            pivot_left_bars: Option<usize>,
            pivot_right_bars: Option<usize>,
            donchian_lookback_bars: Option<usize>,
            zone_atr_mult: Option<f64>,
            max_active_zones: Option<usize>,
            max_zone_age_bars: Option<u64>,
            max_broken_closes: Option<u32>,
            min_zone_score: Option<f64>,
            touch_tolerance_atr: Option<f64>,
            break_close_beyond_atr: Option<f64>,
            pivot_level_score: Option<f64>,
            prev_day_level_score: Option<f64>,
            prev_week_level_score: Option<f64>,
            donchian_level_score: Option<f64>,
            touch_score: Option<f64>,
            broken_close_penalty: Option<f64>,
            sweep_min_penetration_atr: Option<f64>,
            sweep_max_depth_atr: Option<f64>,
            max_reclaim_bars: Option<u64>,
            breakout_min_close_beyond_atr: Option<f64>,
            breakout_accept_closes: Option<u32>,
            max_retest_bars: Option<u64>,
            retest_tolerance_atr: Option<f64>,
            retest_max_penetration_atr: Option<f64>,
            min_retest_close_location: Option<f64>,
            stop_buffer_atr: Option<f64>,
            target_enabled: Option<bool>,
            target_r_multiple: Option<f64>,
            max_hold_bars: Option<u64>,
        }

        let raw: Raw = serde_json::from_str(&ssu.params_json).map_err(|error| {
            StrategyError::Parse(format!(
                "invalid dhanrekha params_json for SSU {}: {error}",
                ssu.ssu_id
            ))
        })?;
        let timeframe = parse_timeframe(&require(raw.timeframe, "timeframe", ssu.ssu_id)?)?;
        if !ssu.required_timeframes.contains(&timeframe) {
            return Err(StrategyError::Config(format!(
                "SSU {} dhanrekha timeframe {} is not registered",
                ssu.ssu_id,
                timeframe_label(timeframe)
            )));
        }

        let enabled_modes = require(raw.enabled_modes, "enabled_modes", ssu.ssu_id)?
            .iter()
            .map(|mode| parse_setup_mode(mode))
            .collect::<Result<BTreeSet<_>, _>>()?;
        let enabled_sides = require(raw.enabled_sides, "enabled_sides", ssu.ssu_id)?
            .iter()
            .map(|side| parse_side(side))
            .collect::<Result<Vec<_>, _>>()?;
        let level_sources = require(raw.level_sources, "level_sources", ssu.ssu_id)?
            .iter()
            .map(|source| parse_level_source(source))
            .collect::<Result<BTreeSet<_>, _>>()?;
        let settings = Self {
            timeframe,
            enabled_modes,
            enabled_sides,
            level_sources,
            period_timezone: parse_period_timezone(&require(
                raw.period_timezone,
                "period_timezone",
                ssu.ssu_id,
            )?)?,
            entry_policy: parse_entry_policy(&require(
                raw.entry_policy,
                "entry_policy",
                ssu.ssu_id,
            )?)?,
            atr_period: require(raw.atr_period, "atr_period", ssu.ssu_id)?,
            pivot_left_bars: require(raw.pivot_left_bars, "pivot_left_bars", ssu.ssu_id)?,
            pivot_right_bars: require(raw.pivot_right_bars, "pivot_right_bars", ssu.ssu_id)?,
            donchian_lookback_bars: require(
                raw.donchian_lookback_bars,
                "donchian_lookback_bars",
                ssu.ssu_id,
            )?,
            zone_atr_mult: require_non_negative(raw.zone_atr_mult, "zone_atr_mult", ssu.ssu_id)?,
            max_active_zones: require(raw.max_active_zones, "max_active_zones", ssu.ssu_id)?,
            max_zone_age_bars: require(raw.max_zone_age_bars, "max_zone_age_bars", ssu.ssu_id)?,
            max_broken_closes: require(raw.max_broken_closes, "max_broken_closes", ssu.ssu_id)?,
            min_zone_score: require_non_negative(raw.min_zone_score, "min_zone_score", ssu.ssu_id)?,
            touch_tolerance_atr: require_non_negative(
                raw.touch_tolerance_atr,
                "touch_tolerance_atr",
                ssu.ssu_id,
            )?,
            break_close_beyond_atr: require_non_negative(
                raw.break_close_beyond_atr,
                "break_close_beyond_atr",
                ssu.ssu_id,
            )?,
            pivot_level_score: require_positive(
                raw.pivot_level_score,
                "pivot_level_score",
                ssu.ssu_id,
            )?,
            prev_day_level_score: require_positive(
                raw.prev_day_level_score,
                "prev_day_level_score",
                ssu.ssu_id,
            )?,
            prev_week_level_score: require_positive(
                raw.prev_week_level_score,
                "prev_week_level_score",
                ssu.ssu_id,
            )?,
            donchian_level_score: require_positive(
                raw.donchian_level_score,
                "donchian_level_score",
                ssu.ssu_id,
            )?,
            touch_score: require_non_negative(raw.touch_score, "touch_score", ssu.ssu_id)?,
            broken_close_penalty: require_non_negative(
                raw.broken_close_penalty,
                "broken_close_penalty",
                ssu.ssu_id,
            )?,
            sweep_min_penetration_atr: require_non_negative(
                raw.sweep_min_penetration_atr,
                "sweep_min_penetration_atr",
                ssu.ssu_id,
            )?,
            sweep_max_depth_atr: require_positive(
                raw.sweep_max_depth_atr,
                "sweep_max_depth_atr",
                ssu.ssu_id,
            )?,
            max_reclaim_bars: require(raw.max_reclaim_bars, "max_reclaim_bars", ssu.ssu_id)?,
            breakout_min_close_beyond_atr: require_non_negative(
                raw.breakout_min_close_beyond_atr,
                "breakout_min_close_beyond_atr",
                ssu.ssu_id,
            )?,
            breakout_accept_closes: require(
                raw.breakout_accept_closes,
                "breakout_accept_closes",
                ssu.ssu_id,
            )?,
            max_retest_bars: require(raw.max_retest_bars, "max_retest_bars", ssu.ssu_id)?,
            retest_tolerance_atr: require_non_negative(
                raw.retest_tolerance_atr,
                "retest_tolerance_atr",
                ssu.ssu_id,
            )?,
            retest_max_penetration_atr: require_non_negative(
                raw.retest_max_penetration_atr,
                "retest_max_penetration_atr",
                ssu.ssu_id,
            )?,
            min_retest_close_location: require_ratio(
                raw.min_retest_close_location,
                "min_retest_close_location",
                ssu.ssu_id,
            )?,
            stop_buffer_atr: require_non_negative(
                raw.stop_buffer_atr,
                "stop_buffer_atr",
                ssu.ssu_id,
            )?,
            target_enabled: require(raw.target_enabled, "target_enabled", ssu.ssu_id)?,
            target_r_multiple: require(raw.target_r_multiple, "target_r_multiple", ssu.ssu_id)?,
            max_hold_bars: require(raw.max_hold_bars, "max_hold_bars", ssu.ssu_id)?,
        };
        settings.validate(ssu.ssu_id)?;
        Ok(settings)
    }

    fn validate(&self, ssu_id: i64) -> Result<(), StrategyError> {
        if self.enabled_modes.is_empty() {
            return Err(config_error(ssu_id, "enabled_modes must not be empty"));
        }
        if self.enabled_sides.is_empty() {
            return Err(config_error(ssu_id, "enabled_sides must not be empty"));
        }
        if self.level_sources.is_empty() {
            return Err(config_error(ssu_id, "level_sources must not be empty"));
        }
        if self.atr_period == 0
            || self.pivot_left_bars == 0
            || self.pivot_right_bars == 0
            || self.donchian_lookback_bars == 0
            || self.max_active_zones == 0
            || self.max_zone_age_bars == 0
            || self.max_reclaim_bars == 0
            || self.breakout_accept_closes == 0
            || self.max_retest_bars == 0
        {
            return Err(config_error(
                ssu_id,
                "periods, windows, active-zone cap, reclaim/retest windows, and accept closes must be positive",
            ));
        }
        if self.sweep_max_depth_atr < self.sweep_min_penetration_atr {
            return Err(config_error(
                ssu_id,
                "sweep_max_depth_atr must be >= sweep_min_penetration_atr",
            ));
        }
        if self.target_enabled
            && (!self.target_r_multiple.is_finite() || self.target_r_multiple <= 0.0)
        {
            return Err(config_error(
                ssu_id,
                "target_r_multiple must be positive when target_enabled=true",
            ));
        }
        if !self.target_enabled && !self.target_r_multiple.is_finite() {
            return Err(config_error(ssu_id, "target_r_multiple must be finite"));
        }
        Ok(())
    }

    fn bootstrap_bar_count(&self) -> usize {
        [
            self.atr_period + 1,
            self.pivot_left_bars + self.pivot_right_bars + 1,
            self.donchian_lookback_bars + 1,
            self.max_reclaim_bars as usize + self.max_retest_bars as usize + 1,
        ]
        .into_iter()
        .max()
        .unwrap_or(1)
        .saturating_add(self.max_active_zones)
    }

    fn source_score(&self, source: LevelSource) -> f64 {
        match source {
            LevelSource::Pivot => self.pivot_level_score,
            LevelSource::PrevDay => self.prev_day_level_score,
            LevelSource::PrevWeek => self.prev_week_level_score,
            LevelSource::Donchian => self.donchian_level_score,
        }
    }

    fn zone_half_width(&self, atr: f64) -> f64 {
        atr * self.zone_atr_mult
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
    bar_index: u64,
    prev_close: Option<f64>,
    atr: AtrState,
    pivot_window: VecDeque<Bar>,
    donchian_highs: MonotonicQueue,
    donchian_lows: MonotonicQueue,
    day: PeriodTracker,
    week: PeriodTracker,
    zones: Vec<Zone>,
    pending: Vec<PendingBreach>,
    next_zone_id: u64,
}

impl RuntimeState {
    fn new(settings: &DhanrekhaSettings) -> Self {
        Self {
            last_processed_closed_end: None,
            bar_index: 0,
            prev_close: None,
            atr: AtrState::new(settings.atr_period),
            pivot_window: VecDeque::new(),
            donchian_highs: MonotonicQueue::new(QueueKind::High),
            donchian_lows: MonotonicQueue::new(QueueKind::Low),
            day: PeriodTracker::new(PeriodKind::Day),
            week: PeriodTracker::new(PeriodKind::Week),
            zones: Vec::new(),
            pending: Vec::new(),
            next_zone_id: 1,
        }
    }

    fn on_closed_bar(
        &mut self,
        settings: &DhanrekhaSettings,
        bar: &Bar,
        may_emit: bool,
    ) -> Result<Vec<DetectedSetup>, StrategyError> {
        if self
            .last_processed_closed_end
            .is_some_and(|end_at| bar.end_at <= end_at)
        {
            return Ok(Vec::new());
        }

        let current_index = self.bar_index;
        self.atr.on_bar(bar);
        let Some(atr) = self.atr.latest else {
            self.push_deferred_structures(settings, bar, current_index)?;
            self.prev_close = Some(bar.close);
            self.last_processed_closed_end = Some(bar.end_at);
            self.bar_index = self.bar_index.saturating_add(1);
            return Ok(Vec::new());
        };

        self.add_period_levels(settings, bar, current_index, atr)?;
        self.add_donchian_levels(settings, bar, current_index, atr)?;
        self.add_pivot_levels(settings, bar, current_index, atr)?;
        self.update_zone_health(settings, bar, current_index, atr);
        self.prune(settings, current_index);

        let mut setups = Vec::new();
        setups.extend(self.update_pending(settings, bar, current_index, atr, may_emit));
        setups.extend(self.start_new_breaches(settings, bar, current_index, atr, may_emit));

        self.push_donchian_bar(settings, bar, current_index);
        self.prev_close = Some(bar.close);
        self.last_processed_closed_end = Some(bar.end_at);
        self.bar_index = self.bar_index.saturating_add(1);
        Ok(setups)
    }

    fn push_deferred_structures(
        &mut self,
        settings: &DhanrekhaSettings,
        bar: &Bar,
        current_index: u64,
    ) -> Result<(), StrategyError> {
        let _ = self.day.on_bar(bar, settings.period_timezone)?;
        let _ = self.week.on_bar(bar, settings.period_timezone)?;
        self.push_pivot_bar(bar);
        self.push_donchian_bar(settings, bar, current_index);
        Ok(())
    }

    fn add_period_levels(
        &mut self,
        settings: &DhanrekhaSettings,
        bar: &Bar,
        current_index: u64,
        atr: f64,
    ) -> Result<(), StrategyError> {
        for level in self.day.on_bar(bar, settings.period_timezone)? {
            if settings.level_sources.contains(&LevelSource::PrevDay) {
                self.add_level(
                    settings,
                    level.high,
                    LevelSource::PrevDay,
                    bar.start_at.saturating_sub(1),
                    current_index,
                    atr,
                );
                self.add_level(
                    settings,
                    level.low,
                    LevelSource::PrevDay,
                    bar.start_at.saturating_sub(1),
                    current_index,
                    atr,
                );
            }
        }
        for level in self.week.on_bar(bar, settings.period_timezone)? {
            if settings.level_sources.contains(&LevelSource::PrevWeek) {
                self.add_level(
                    settings,
                    level.high,
                    LevelSource::PrevWeek,
                    bar.start_at.saturating_sub(1),
                    current_index,
                    atr,
                );
                self.add_level(
                    settings,
                    level.low,
                    LevelSource::PrevWeek,
                    bar.start_at.saturating_sub(1),
                    current_index,
                    atr,
                );
            }
        }
        Ok(())
    }

    fn add_donchian_levels(
        &mut self,
        settings: &DhanrekhaSettings,
        bar: &Bar,
        current_index: u64,
        atr: f64,
    ) -> Result<(), StrategyError> {
        if !settings.level_sources.contains(&LevelSource::Donchian) {
            return Ok(());
        }
        self.donchian_highs
            .expire(current_index, settings.donchian_lookback_bars as u64);
        self.donchian_lows
            .expire(current_index, settings.donchian_lookback_bars as u64);
        if let Some(high) = self.donchian_highs.front_value() {
            self.add_level(
                settings,
                high,
                LevelSource::Donchian,
                bar.start_at.saturating_sub(1),
                current_index,
                atr,
            );
        }
        if let Some(low) = self.donchian_lows.front_value() {
            self.add_level(
                settings,
                low,
                LevelSource::Donchian,
                bar.start_at.saturating_sub(1),
                current_index,
                atr,
            );
        }
        Ok(())
    }

    fn add_pivot_levels(
        &mut self,
        settings: &DhanrekhaSettings,
        bar: &Bar,
        current_index: u64,
        atr: f64,
    ) -> Result<(), StrategyError> {
        self.push_pivot_bar(bar);
        if !settings.level_sources.contains(&LevelSource::Pivot) {
            return Ok(());
        }
        let window_size = settings.pivot_left_bars + settings.pivot_right_bars + 1;
        if self.pivot_window.len() < window_size {
            return Ok(());
        }

        let candidate_index = settings.pivot_left_bars;
        let candidate = self
            .pivot_window
            .get(candidate_index)
            .expect("pivot candidate in window")
            .clone();
        let left = self.pivot_window.iter().take(candidate_index);
        let right = self.pivot_window.iter().skip(candidate_index + 1);
        let pivot_low = left.clone().all(|left_bar| candidate.low <= left_bar.low)
            && right.clone().all(|right_bar| candidate.low < right_bar.low);
        let pivot_high = self
            .pivot_window
            .iter()
            .take(candidate_index)
            .all(|left_bar| candidate.high >= left_bar.high)
            && self
                .pivot_window
                .iter()
                .skip(candidate_index + 1)
                .all(|right_bar| candidate.high > right_bar.high);

        if pivot_low {
            self.add_level(
                settings,
                candidate.low,
                LevelSource::Pivot,
                bar.end_at,
                current_index,
                atr,
            );
        }
        if pivot_high {
            self.add_level(
                settings,
                candidate.high,
                LevelSource::Pivot,
                bar.end_at,
                current_index,
                atr,
            );
        }
        self.pivot_window.pop_front();
        Ok(())
    }

    fn push_pivot_bar(&mut self, bar: &Bar) {
        self.pivot_window.push_back(bar.clone());
    }

    fn push_donchian_bar(&mut self, settings: &DhanrekhaSettings, bar: &Bar, current_index: u64) {
        self.donchian_highs.push(current_index, bar.high);
        self.donchian_lows.push(current_index, bar.low);
        self.donchian_highs.expire(
            current_index.saturating_add(1),
            settings.donchian_lookback_bars as u64,
        );
        self.donchian_lows.expire(
            current_index.saturating_add(1),
            settings.donchian_lookback_bars as u64,
        );
    }

    fn add_level(
        &mut self,
        settings: &DhanrekhaSettings,
        price: f64,
        source: LevelSource,
        created_at: u64,
        created_index: u64,
        atr: f64,
    ) {
        if !price.is_finite() || price <= 0.0 {
            return;
        }
        let half_width = settings.zone_half_width(atr);
        let score = settings.source_score(source);
        if let Some(zone) = self.zones.iter_mut().find(|zone| {
            (price >= zone.lower - half_width && price <= zone.upper + half_width)
                || (zone.price - price).abs() <= half_width
        }) {
            let total_score = (zone.score + score).max(score);
            zone.price = ((zone.price * zone.score.max(1.0)) + (price * score))
                / (zone.score.max(1.0) + score);
            zone.lower = zone.price - half_width;
            zone.upper = zone.price + half_width;
            zone.score = total_score;
            zone.sources.insert(source);
            zone.last_seen_at = created_at;
            zone.last_seen_index = created_index;
            return;
        }

        let id = self.next_zone_id;
        self.next_zone_id = self.next_zone_id.saturating_add(1);
        self.zones.push(Zone {
            id,
            price,
            lower: price - half_width,
            upper: price + half_width,
            sources: BTreeSet::from([source]),
            touches: 0,
            broken_closes: 0,
            score,
            created_at,
            created_index,
            last_seen_at: created_at,
            last_seen_index: created_index,
            last_touched_at: None,
        });
    }

    fn update_zone_health(
        &mut self,
        settings: &DhanrekhaSettings,
        bar: &Bar,
        current_index: u64,
        atr: f64,
    ) {
        let tolerance = settings.touch_tolerance_atr * atr;
        let break_buffer = settings.break_close_beyond_atr * atr;
        for zone in &mut self.zones {
            let touched = bar.low <= zone.upper + tolerance && bar.high >= zone.lower - tolerance;
            if touched && zone.last_touched_at != Some(bar.end_at) {
                zone.touches = zone.touches.saturating_add(1);
                zone.score += settings.touch_score;
                zone.last_touched_at = Some(bar.end_at);
                zone.last_seen_at = bar.end_at;
                zone.last_seen_index = current_index;
            }

            let bearish_body_break =
                bar.open >= zone.upper && bar.close <= zone.lower - break_buffer;
            let bullish_body_break =
                bar.open <= zone.lower && bar.close >= zone.upper + break_buffer;
            if bearish_body_break || bullish_body_break {
                zone.broken_closes = zone.broken_closes.saturating_add(1);
                zone.score = (zone.score - settings.broken_close_penalty).max(0.0);
            }
        }
    }

    fn update_pending(
        &mut self,
        settings: &DhanrekhaSettings,
        bar: &Bar,
        current_index: u64,
        atr: f64,
        may_emit: bool,
    ) -> Vec<DetectedSetup> {
        let zones = self
            .zones
            .iter()
            .map(|zone| (zone.id, zone.clone()))
            .collect::<BTreeMap<_, _>>();
        let mut setups = Vec::new();
        let mut next_pending = Vec::new();
        let mut pending = std::mem::take(&mut self.pending);
        for mut breach in pending.drain(..) {
            let Some(zone) = zones.get(&breach.zone_id).cloned() else {
                continue;
            };
            breach.update_with_bar(bar);
            let bars_waited = current_index.saturating_sub(breach.started_index);
            let setup = match breach.direction {
                BreachDirection::Up => {
                    self.evaluate_up_breach(settings, &zone, &mut breach, bar, bars_waited, atr)
                }
                BreachDirection::Down => {
                    self.evaluate_down_breach(settings, &zone, &mut breach, bar, bars_waited, atr)
                }
            };
            if let Some(setup) = setup {
                if may_emit {
                    setups.push(setup);
                }
                continue;
            }
            if bars_waited <= settings.max_reclaim_bars.max(settings.max_retest_bars) {
                next_pending.push(breach);
            }
        }
        self.pending = next_pending;
        setups
    }

    fn evaluate_up_breach(
        &self,
        settings: &DhanrekhaSettings,
        zone: &Zone,
        breach: &mut PendingBreach,
        bar: &Bar,
        bars_waited: u64,
        atr: f64,
    ) -> Option<DetectedSetup> {
        let penetration = breach.extreme - zone.upper;
        let sweep_depth_ok = penetration >= settings.sweep_min_penetration_atr * atr
            && penetration <= settings.sweep_max_depth_atr * atr;
        if settings.enabled_modes.contains(&SetupMode::SweepReversal)
            && settings.enabled_sides.contains(&SignalSide::Short)
            && bars_waited <= settings.max_reclaim_bars
            && sweep_depth_ok
            && bar.close <= zone.lower
        {
            return Some(DetectedSetup::new(
                SetupMode::SweepReversal,
                SignalSide::Short,
                zone,
                bar,
                atr,
                breach.extreme,
            ));
        }

        if bar.close >= zone.upper + settings.breakout_min_close_beyond_atr * atr {
            breach.accept_closes = breach.accept_closes.saturating_add(1);
        }
        breach.accepted |= breach.accept_closes >= settings.breakout_accept_closes;
        if settings.enabled_modes.contains(&SetupMode::BreakoutRetest)
            && settings.enabled_sides.contains(&SignalSide::Long)
            && breach.accepted
            && bars_waited > 0
            && bars_waited <= settings.max_retest_bars
            && bar.low <= zone.upper + settings.retest_tolerance_atr * atr
            && bar.low >= zone.lower - settings.retest_max_penetration_atr * atr
            && bar.close >= zone.upper
            && close_location(bar) >= settings.min_retest_close_location
        {
            return Some(DetectedSetup::new(
                SetupMode::BreakoutRetest,
                SignalSide::Long,
                zone,
                bar,
                atr,
                bar.low.min(zone.lower),
            ));
        }
        None
    }

    fn evaluate_down_breach(
        &self,
        settings: &DhanrekhaSettings,
        zone: &Zone,
        breach: &mut PendingBreach,
        bar: &Bar,
        bars_waited: u64,
        atr: f64,
    ) -> Option<DetectedSetup> {
        let penetration = zone.lower - breach.extreme;
        let sweep_depth_ok = penetration >= settings.sweep_min_penetration_atr * atr
            && penetration <= settings.sweep_max_depth_atr * atr;
        if settings.enabled_modes.contains(&SetupMode::SweepReversal)
            && settings.enabled_sides.contains(&SignalSide::Long)
            && bars_waited <= settings.max_reclaim_bars
            && sweep_depth_ok
            && bar.close >= zone.upper
        {
            return Some(DetectedSetup::new(
                SetupMode::SweepReversal,
                SignalSide::Long,
                zone,
                bar,
                atr,
                breach.extreme,
            ));
        }

        if bar.close <= zone.lower - settings.breakout_min_close_beyond_atr * atr {
            breach.accept_closes = breach.accept_closes.saturating_add(1);
        }
        breach.accepted |= breach.accept_closes >= settings.breakout_accept_closes;
        if settings.enabled_modes.contains(&SetupMode::BreakoutRetest)
            && settings.enabled_sides.contains(&SignalSide::Short)
            && breach.accepted
            && bars_waited > 0
            && bars_waited <= settings.max_retest_bars
            && bar.high >= zone.lower - settings.retest_tolerance_atr * atr
            && bar.high <= zone.upper + settings.retest_max_penetration_atr * atr
            && bar.close <= zone.lower
            && close_location(bar) <= 1.0 - settings.min_retest_close_location
        {
            return Some(DetectedSetup::new(
                SetupMode::BreakoutRetest,
                SignalSide::Short,
                zone,
                bar,
                atr,
                bar.high.max(zone.upper),
            ));
        }
        None
    }

    fn start_new_breaches(
        &mut self,
        settings: &DhanrekhaSettings,
        bar: &Bar,
        current_index: u64,
        atr: f64,
        may_emit: bool,
    ) -> Vec<DetectedSetup> {
        let Some(prev_close) = self.prev_close else {
            return Vec::new();
        };
        let mut zones = self
            .zones
            .iter()
            .filter(|zone| zone.score >= settings.min_zone_score)
            .filter(|zone| zone.created_at < bar.start_at)
            .cloned()
            .collect::<Vec<_>>();
        zones.sort_by(|a, b| {
            distance_to_zone(bar.close, a)
                .partial_cmp(&distance_to_zone(bar.close, b))
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| {
                    b.score
                        .partial_cmp(&a.score)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
        });

        let mut setups = Vec::new();
        for zone in zones {
            if prev_close <= zone.upper
                && bar.high >= zone.upper + settings.sweep_min_penetration_atr * atr
                && !self.has_pending(zone.id, BreachDirection::Up)
            {
                if let Some(setup) =
                    self.start_up_breach(settings, &zone, bar, current_index, atr, may_emit)
                {
                    setups.push(setup);
                }
            }
            if prev_close >= zone.lower
                && bar.low <= zone.lower - settings.sweep_min_penetration_atr * atr
                && !self.has_pending(zone.id, BreachDirection::Down)
            {
                if let Some(setup) =
                    self.start_down_breach(settings, &zone, bar, current_index, atr, may_emit)
                {
                    setups.push(setup);
                }
            }
        }
        setups
    }

    fn start_up_breach(
        &mut self,
        settings: &DhanrekhaSettings,
        zone: &Zone,
        bar: &Bar,
        current_index: u64,
        atr: f64,
        may_emit: bool,
    ) -> Option<DetectedSetup> {
        let penetration = bar.high - zone.upper;
        if settings.enabled_modes.contains(&SetupMode::SweepReversal)
            && settings.enabled_sides.contains(&SignalSide::Short)
            && penetration <= settings.sweep_max_depth_atr * atr
            && bar.close <= zone.lower
        {
            let setup = DetectedSetup::new(
                SetupMode::SweepReversal,
                SignalSide::Short,
                zone,
                bar,
                atr,
                bar.high,
            );
            return may_emit.then_some(setup);
        }
        self.pending.push(PendingBreach::new(
            zone.id,
            BreachDirection::Up,
            current_index,
            bar.end_at,
            bar.high,
            (bar.close >= zone.upper + settings.breakout_min_close_beyond_atr * atr) as u32,
            settings.breakout_accept_closes,
        ));
        None
    }

    fn start_down_breach(
        &mut self,
        settings: &DhanrekhaSettings,
        zone: &Zone,
        bar: &Bar,
        current_index: u64,
        atr: f64,
        may_emit: bool,
    ) -> Option<DetectedSetup> {
        let penetration = zone.lower - bar.low;
        if settings.enabled_modes.contains(&SetupMode::SweepReversal)
            && settings.enabled_sides.contains(&SignalSide::Long)
            && penetration <= settings.sweep_max_depth_atr * atr
            && bar.close >= zone.upper
        {
            let setup = DetectedSetup::new(
                SetupMode::SweepReversal,
                SignalSide::Long,
                zone,
                bar,
                atr,
                bar.low,
            );
            return may_emit.then_some(setup);
        }
        self.pending.push(PendingBreach::new(
            zone.id,
            BreachDirection::Down,
            current_index,
            bar.end_at,
            bar.low,
            (bar.close <= zone.lower - settings.breakout_min_close_beyond_atr * atr) as u32,
            settings.breakout_accept_closes,
        ));
        None
    }

    fn has_pending(&self, zone_id: u64, direction: BreachDirection) -> bool {
        self.pending
            .iter()
            .any(|breach| breach.zone_id == zone_id && breach.direction == direction)
    }

    fn prune(&mut self, settings: &DhanrekhaSettings, current_index: u64) {
        self.zones.retain(|zone| {
            current_index.saturating_sub(zone.created_index) <= settings.max_zone_age_bars
                && zone.broken_closes <= settings.max_broken_closes
        });
        self.zones.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| b.last_seen_index.cmp(&a.last_seen_index))
        });
        if self.zones.len() > settings.max_active_zones {
            self.zones.truncate(settings.max_active_zones);
        }
        let active_ids = self
            .zones
            .iter()
            .map(|zone| zone.id)
            .collect::<BTreeSet<_>>();
        self.pending
            .retain(|breach| active_ids.contains(&breach.zone_id));
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum SetupMode {
    SweepReversal,
    BreakoutRetest,
}

impl SetupMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::SweepReversal => "sweep_reversal",
            Self::BreakoutRetest => "breakout_retest",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum LevelSource {
    Pivot,
    PrevDay,
    PrevWeek,
    Donchian,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EntryPolicy {
    SinglePosition,
    Independent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PeriodTimezone {
    Utc,
    Ist,
}

impl PeriodTimezone {
    fn offset(self) -> FixedOffset {
        match self {
            Self::Utc => FixedOffset::east_opt(0).expect("valid UTC offset"),
            Self::Ist => FixedOffset::east_opt(5 * 60 * 60 + 30 * 60).expect("valid IST offset"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PeriodKind {
    Day,
    Week,
}

#[derive(Clone, Debug)]
struct CompletedPeriod {
    high: f64,
    low: f64,
}

#[derive(Clone, Debug)]
struct PeriodTracker {
    kind: PeriodKind,
    key: Option<String>,
    high: f64,
    low: f64,
}

impl PeriodTracker {
    fn new(kind: PeriodKind) -> Self {
        Self {
            kind,
            key: None,
            high: f64::NEG_INFINITY,
            low: f64::INFINITY,
        }
    }

    fn on_bar(
        &mut self,
        bar: &Bar,
        timezone: PeriodTimezone,
    ) -> Result<Vec<CompletedPeriod>, StrategyError> {
        let key = period_key(self.kind, timezone, bar.start_at)?;
        let mut completed = Vec::new();
        match self.key.as_ref() {
            None => {
                self.key = Some(key);
                self.high = bar.high;
                self.low = bar.low;
            }
            Some(current) if current == &key => {
                self.high = self.high.max(bar.high);
                self.low = self.low.min(bar.low);
            }
            Some(_) => {
                if self.high.is_finite() && self.low.is_finite() {
                    completed.push(CompletedPeriod {
                        high: self.high,
                        low: self.low,
                    });
                }
                self.key = Some(key);
                self.high = bar.high;
                self.low = bar.low;
            }
        }
        Ok(completed)
    }
}

#[derive(Clone, Debug)]
struct AtrState {
    period: usize,
    prev_close: Option<f64>,
    values: VecDeque<f64>,
    sum: f64,
    latest: Option<f64>,
}

impl AtrState {
    fn new(period: usize) -> Self {
        Self {
            period,
            prev_close: None,
            values: VecDeque::new(),
            sum: 0.0,
            latest: None,
        }
    }

    fn on_bar(&mut self, bar: &Bar) {
        let tr = match self.prev_close {
            Some(prev_close) => (bar.high - bar.low)
                .max((bar.high - prev_close).abs())
                .max((bar.low - prev_close).abs()),
            None => bar.high - bar.low,
        };
        self.prev_close = Some(bar.close);
        if !tr.is_finite() || tr <= 0.0 {
            return;
        }
        self.values.push_back(tr);
        self.sum += tr;
        while self.values.len() > self.period {
            if let Some(value) = self.values.pop_front() {
                self.sum -= value;
            }
        }
        if self.values.len() == self.period {
            self.latest = Some(self.sum / self.period as f64);
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QueueKind {
    High,
    Low,
}

#[derive(Clone, Debug)]
struct MonotonicQueue {
    kind: QueueKind,
    values: VecDeque<(u64, f64)>,
}

impl MonotonicQueue {
    fn new(kind: QueueKind) -> Self {
        Self {
            kind,
            values: VecDeque::new(),
        }
    }

    fn push(&mut self, index: u64, value: f64) {
        match self.kind {
            QueueKind::High => {
                while self
                    .values
                    .back()
                    .is_some_and(|(_, existing)| *existing <= value)
                {
                    self.values.pop_back();
                }
            }
            QueueKind::Low => {
                while self
                    .values
                    .back()
                    .is_some_and(|(_, existing)| *existing >= value)
                {
                    self.values.pop_back();
                }
            }
        }
        self.values.push_back((index, value));
    }

    fn expire(&mut self, current_index: u64, lookback: u64) {
        let min_index = current_index.saturating_sub(lookback);
        while self
            .values
            .front()
            .is_some_and(|(index, _)| *index < min_index)
        {
            self.values.pop_front();
        }
    }

    fn front_value(&self) -> Option<f64> {
        self.values.front().map(|(_, value)| *value)
    }
}

#[derive(Clone, Debug)]
struct Zone {
    id: u64,
    price: f64,
    lower: f64,
    upper: f64,
    sources: BTreeSet<LevelSource>,
    touches: u32,
    broken_closes: u32,
    score: f64,
    created_at: u64,
    created_index: u64,
    last_seen_at: u64,
    last_seen_index: u64,
    last_touched_at: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BreachDirection {
    Up,
    Down,
}

#[derive(Clone, Debug)]
struct PendingBreach {
    zone_id: u64,
    direction: BreachDirection,
    started_index: u64,
    extreme: f64,
    accept_closes: u32,
    accepted: bool,
}

impl PendingBreach {
    fn new(
        zone_id: u64,
        direction: BreachDirection,
        started_index: u64,
        _started_at: u64,
        extreme: f64,
        accept_closes: u32,
        required_accept_closes: u32,
    ) -> Self {
        Self {
            zone_id,
            direction,
            started_index,
            extreme,
            accept_closes,
            accepted: accept_closes >= required_accept_closes,
        }
    }

    fn update_with_bar(&mut self, bar: &Bar) {
        match self.direction {
            BreachDirection::Up => self.extreme = self.extreme.max(bar.high),
            BreachDirection::Down => self.extreme = self.extreme.min(bar.low),
        }
    }
}

#[derive(Clone, Debug)]
struct DetectedSetup {
    setup_id: String,
    mode: SetupMode,
    side: SignalSide,
    zone_id: u64,
    zone_price: f64,
    zone_lower: f64,
    zone_upper: f64,
    entry_price: f64,
    stop_anchor: f64,
    atr: f64,
    bar_end_at: u64,
}

impl DetectedSetup {
    fn new(
        mode: SetupMode,
        side: SignalSide,
        zone: &Zone,
        bar: &Bar,
        atr: f64,
        stop_anchor: f64,
    ) -> Self {
        Self {
            setup_id: format!(
                "DR-{}-{}-{}-{}",
                zone.id,
                mode.as_str(),
                side_label(side),
                bar.end_at
            ),
            mode,
            side,
            zone_id: zone.id,
            zone_price: zone.price,
            zone_lower: zone.lower,
            zone_upper: zone.upper,
            entry_price: bar.close,
            stop_anchor,
            atr,
            bar_end_at: bar.end_at,
        }
    }

    fn stop_and_target(
        &self,
        entry_price: f64,
        settings: &DhanrekhaSettings,
    ) -> Option<(f64, Option<f64>)> {
        let stop_buffer = settings.stop_buffer_atr * self.atr;
        let stop_price = match self.side {
            SignalSide::Long => self.stop_anchor - stop_buffer,
            SignalSide::Short => self.stop_anchor + stop_buffer,
        };
        let risk = match self.side {
            SignalSide::Long => entry_price - stop_price,
            SignalSide::Short => stop_price - entry_price,
        };
        if !risk.is_finite() || risk <= 0.0 {
            return None;
        }
        let target_price = settings.target_enabled.then(|| match self.side {
            SignalSide::Long => entry_price + settings.target_r_multiple * risk,
            SignalSide::Short => entry_price - settings.target_r_multiple * risk,
        });
        Some((stop_price, target_price))
    }

    fn reason(&self, settings: &DhanrekhaSettings) -> String {
        format!(
            "dhanrekha_entry|mode={}|side={}|tf={}|zone_id={}|zone={:.4}-{:.4}|bar_end={}",
            self.mode.as_str(),
            side_label(self.side),
            timeframe_label(settings.timeframe),
            self.zone_id,
            self.zone_lower,
            self.zone_upper,
            self.bar_end_at
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
            "mode": self.mode.as_str(),
            "side": side_label(self.side),
            "zone_id": self.zone_id,
            "zone_price": self.zone_price,
            "zone_lower": self.zone_lower,
            "zone_upper": self.zone_upper,
            "entry_price": entry_price,
            "stop_price": stop_price,
            "target_price": target_price,
            "atr": self.atr,
            "bar_end_at": self.bar_end_at,
        })
    }

    fn trade_context_metadata(
        &self,
        signal: &StrategySignal,
        position_id: &str,
        entry_price: f64,
        stop_price: f64,
        target_price: Option<f64>,
    ) -> serde_json::Value {
        serde_json::json!({
            "strategy_key": "dhanrekha",
            "position_id": position_id,
            "campaign_id": signal.campaign_id,
            "setup_id": self.setup_id,
            "mode": self.mode.as_str(),
            "side": side_label(self.side),
            "zone_id": self.zone_id,
            "zone_price": self.zone_price,
            "zone_lower": self.zone_lower,
            "zone_upper": self.zone_upper,
            "entry_bar_end_at": self.bar_end_at,
            "entry_price": entry_price,
            "stop_price": stop_price,
            "target_price": target_price,
            "hold_bars": 0_u64,
            "last_exit_check_bar_end_at": self.bar_end_at,
        })
    }
}

#[derive(Clone, Debug)]
struct TradeContext {
    campaign_id: String,
    mode: String,
    zone_id: u64,
    stop_price: f64,
    target_price: Option<f64>,
    hold_bars: u64,
    last_exit_check_bar_end_at: u64,
}

impl TradeContext {
    fn from_metadata(metadata: &serde_json::Value) -> Result<Self, StrategyError> {
        Ok(Self {
            campaign_id: required_string(metadata, "campaign_id")?,
            mode: required_string(metadata, "mode")?,
            zone_id: required_u64(metadata, "zone_id")?,
            stop_price: required_f64(metadata, "stop_price")?,
            target_price: optional_f64(metadata, "target_price"),
            hold_bars: required_u64(metadata, "hold_bars")?,
            last_exit_check_bar_end_at: required_u64(metadata, "last_exit_check_bar_end_at")?,
        })
    }

    fn to_metadata(&self) -> serde_json::Value {
        serde_json::json!({
            "strategy_key": "dhanrekha",
            "campaign_id": self.campaign_id,
            "mode": self.mode,
            "zone_id": self.zone_id,
            "stop_price": self.stop_price,
            "target_price": self.target_price,
            "hold_bars": self.hold_bars,
            "last_exit_check_bar_end_at": self.last_exit_check_bar_end_at,
        })
    }
}

fn current_ltp(ctx: &StrategyContext, instrument: &str) -> Option<f64> {
    ctx.prices
        .get_price(instrument)
        .map(|snapshot| snapshot.ltp)
}

fn distance_to_zone(price: f64, zone: &Zone) -> f64 {
    if price < zone.lower {
        zone.lower - price
    } else if price > zone.upper {
        price - zone.upper
    } else {
        0.0
    }
}

fn close_location(bar: &Bar) -> f64 {
    let range = bar.high - bar.low;
    if !range.is_finite() || range <= 0.0 {
        0.5
    } else {
        ((bar.close - bar.low) / range).clamp(0.0, 1.0)
    }
}

fn period_key(
    kind: PeriodKind,
    timezone: PeriodTimezone,
    start_at: u64,
) -> Result<String, StrategyError> {
    let dt = Utc
        .timestamp_millis_opt(start_at as i64)
        .single()
        .ok_or_else(|| StrategyError::Parse(format!("invalid bar timestamp {start_at}")))?
        .with_timezone(&timezone.offset());
    Ok(match kind {
        PeriodKind::Day => format!("{}-{:02}-{:02}", dt.year(), dt.month(), dt.day()),
        PeriodKind::Week => {
            let week = dt.iso_week();
            format!("{}-W{:02}", week.year(), week.week())
        }
    })
}

fn parse_timeframe(value: &str) -> Result<Timeframe, StrategyError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1m" | "one_minute" => Ok(Timeframe::OneMinute),
        "3m" | "three_minute" => Ok(Timeframe::ThreeMinute),
        "5m" | "five_minute" => Ok(Timeframe::FiveMinute),
        "15m" | "fifteen_minute" => Ok(Timeframe::FifteenMinute),
        "30m" | "thirty_minute" => Ok(Timeframe::ThirtyMinute),
        "75m" | "seventy_five_minute" => Ok(Timeframe::SeventyFiveMinute),
        "1h" | "one_hour" => Ok(Timeframe::OneHour),
        "4h" | "four_hour" => Ok(Timeframe::FourHour),
        "1d" | "one_day" => Ok(Timeframe::OneDay),
        value => Err(StrategyError::Parse(format!(
            "unsupported dhanrekha timeframe {value}"
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

fn parse_side(value: &str) -> Result<SignalSide, StrategyError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "long" => Ok(SignalSide::Long),
        "short" => Ok(SignalSide::Short),
        other => Err(StrategyError::Parse(format!(
            "unsupported dhanrekha side {other}; expected long or short"
        ))),
    }
}

fn side_label(side: SignalSide) -> &'static str {
    match side {
        SignalSide::Long => "long",
        SignalSide::Short => "short",
    }
}

fn parse_setup_mode(value: &str) -> Result<SetupMode, StrategyError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "sweep_reversal" => Ok(SetupMode::SweepReversal),
        "breakout_retest" => Ok(SetupMode::BreakoutRetest),
        other => Err(StrategyError::Parse(format!(
            "unsupported dhanrekha enabled mode {other}"
        ))),
    }
}

fn parse_level_source(value: &str) -> Result<LevelSource, StrategyError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "pivot" | "pivots" => Ok(LevelSource::Pivot),
        "prev_day" | "previous_day" => Ok(LevelSource::PrevDay),
        "prev_week" | "previous_week" => Ok(LevelSource::PrevWeek),
        "donchian" => Ok(LevelSource::Donchian),
        other => Err(StrategyError::Parse(format!(
            "unsupported dhanrekha level source {other}"
        ))),
    }
}

fn parse_entry_policy(value: &str) -> Result<EntryPolicy, StrategyError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "single_position" => Ok(EntryPolicy::SinglePosition),
        "independent" => Ok(EntryPolicy::Independent),
        other => Err(StrategyError::Parse(format!(
            "unsupported dhanrekha entry_policy {other}"
        ))),
    }
}

fn parse_period_timezone(value: &str) -> Result<PeriodTimezone, StrategyError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "utc" => Ok(PeriodTimezone::Utc),
        "ist" | "asia/kolkata" => Ok(PeriodTimezone::Ist),
        other => Err(StrategyError::Parse(format!(
            "unsupported dhanrekha period_timezone {other}; expected utc or ist"
        ))),
    }
}

fn require<T>(value: Option<T>, field: &str, ssu_id: i64) -> Result<T, StrategyError> {
    value.ok_or_else(|| {
        StrategyError::Config(format!(
            "SSU {ssu_id} dhanrekha missing required field {field}"
        ))
    })
}

fn require_positive(value: Option<f64>, field: &str, ssu_id: i64) -> Result<f64, StrategyError> {
    let value = require(value, field, ssu_id)?;
    if value.is_finite() && value > 0.0 {
        Ok(value)
    } else {
        Err(config_error(
            ssu_id,
            &format!("{field} must be finite and positive"),
        ))
    }
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
        Err(config_error(
            ssu_id,
            &format!("{field} must be finite and non-negative"),
        ))
    }
}

fn require_ratio(value: Option<f64>, field: &str, ssu_id: i64) -> Result<f64, StrategyError> {
    let value = require(value, field, ssu_id)?;
    if value.is_finite() && (0.0..=1.0).contains(&value) {
        Ok(value)
    } else {
        Err(config_error(
            ssu_id,
            &format!("{field} must be finite and between 0 and 1"),
        ))
    }
}

fn config_error(ssu_id: i64, message: &str) -> StrategyError {
    StrategyError::Config(format!("SSU {ssu_id} dhanrekha {message}"))
}

fn required_f64(metadata: &serde_json::Value, field: &str) -> Result<f64, StrategyError> {
    metadata
        .get(field)
        .and_then(serde_json::Value::as_f64)
        .ok_or_else(|| StrategyError::Parse(format!("dhanrekha metadata missing f64 {field}")))
}

fn optional_f64(metadata: &serde_json::Value, field: &str) -> Option<f64> {
    metadata.get(field).and_then(serde_json::Value::as_f64)
}

fn required_u64(metadata: &serde_json::Value, field: &str) -> Result<u64, StrategyError> {
    metadata
        .get(field)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| StrategyError::Parse(format!("dhanrekha metadata missing u64 {field}")))
}

fn required_string(metadata: &serde_json::Value, field: &str) -> Result<String, StrategyError> {
    metadata
        .get(field)
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| StrategyError::Parse(format!("dhanrekha metadata missing string {field}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_require_registered_timeframe() {
        let mut ssu = complete_ssu();
        ssu.required_timeframes = vec![Timeframe::OneMinute];

        let error = DhanrekhaSettings::from_ssu(&ssu).expect_err("timeframe mismatch");

        assert!(error.to_string().contains("timeframe 5m is not registered"));
    }

    #[test]
    fn pivot_level_is_confirmed_only_after_right_bars_close() {
        let mut settings = complete_settings();
        settings.level_sources = BTreeSet::from([LevelSource::Pivot]);
        let mut state = RuntimeState::new(&settings);

        state
            .on_closed_bar(&settings, &bar(0, 100.0, 101.0, 100.0, 100.5), false)
            .expect("bar 0");
        state
            .on_closed_bar(&settings, &bar(1, 100.5, 101.0, 90.0, 96.0), false)
            .expect("bar 1");
        state
            .on_closed_bar(&settings, &bar(2, 96.0, 99.0, 94.0, 97.0), false)
            .expect("bar 2");

        assert!(state.zones.iter().all(|zone| zone.price != 90.0));

        state
            .on_closed_bar(&settings, &bar(3, 97.0, 100.0, 95.0, 99.0), false)
            .expect("bar 3");

        assert!(
            state
                .zones
                .iter()
                .any(|zone| (zone.price - 90.0).abs() < 0.01)
        );
    }

    #[test]
    fn sweep_reversal_can_reclaim_after_close_beyond_zone() {
        let settings = complete_settings();
        let mut state = RuntimeState::new(&settings);
        seed_manual_zone(&mut state, 100.0, 99.5, 100.5);
        seed_atr(&mut state, &settings);
        state.prev_close = Some(101.0);

        let first = state
            .on_closed_bar(&settings, &bar(10, 101.0, 101.2, 98.8, 99.0), true)
            .expect("breach bar");
        assert!(first.is_empty());

        let second = state
            .on_closed_bar(&settings, &bar(11, 99.0, 101.4, 98.9, 100.8), true)
            .expect("reclaim bar");

        assert_eq!(second.len(), 1);
        assert_eq!(second[0].mode, SetupMode::SweepReversal);
        assert_eq!(second[0].side, SignalSide::Long);
    }

    #[test]
    fn breakout_retest_requires_later_retest_candle() {
        let settings = complete_settings();
        let mut state = RuntimeState::new(&settings);
        seed_manual_zone(&mut state, 100.0, 99.5, 100.5);
        seed_atr(&mut state, &settings);
        state.prev_close = Some(99.0);

        let breakout = state
            .on_closed_bar(&settings, &bar(10, 99.0, 103.0, 98.8, 102.0), true)
            .expect("breakout bar");
        assert!(breakout.is_empty());

        let retest = state
            .on_closed_bar(&settings, &bar(11, 102.0, 102.4, 100.4, 101.8), true)
            .expect("retest bar");

        assert_eq!(retest.len(), 1);
        assert_eq!(retest[0].mode, SetupMode::BreakoutRetest);
        assert_eq!(retest[0].side, SignalSide::Long);
    }

    fn complete_settings() -> DhanrekhaSettings {
        DhanrekhaSettings::from_ssu(&complete_ssu()).expect("settings")
    }

    fn complete_ssu() -> SsuConfig {
        SsuConfig {
            ssu_id: 42,
            strategy_key: "dhanrekha".to_string(),
            enabled: true,
            trade_gap_secs: 0,
            max_overlap: 1,
            max_positions_per_day: 10,
            required_timeframes: vec![Timeframe::FiveMinute],
            indicator_specs: Vec::new(),
            params_json: complete_params(),
        }
    }

    fn complete_params() -> String {
        serde_json::json!({
            "timeframe": "5m",
            "enabled_modes": ["sweep_reversal", "breakout_retest"],
            "enabled_sides": ["long", "short"],
            "level_sources": ["pivot", "donchian", "prev_day", "prev_week"],
            "period_timezone": "utc",
            "entry_policy": "single_position",
            "atr_period": 3,
            "pivot_left_bars": 1,
            "pivot_right_bars": 2,
            "donchian_lookback_bars": 5,
            "zone_atr_mult": 0.10,
            "max_active_zones": 16,
            "max_zone_age_bars": 200,
            "max_broken_closes": 20,
            "min_zone_score": 1.0,
            "touch_tolerance_atr": 0.10,
            "break_close_beyond_atr": 0.10,
            "pivot_level_score": 2.0,
            "prev_day_level_score": 3.0,
            "prev_week_level_score": 4.0,
            "donchian_level_score": 1.0,
            "touch_score": 0.25,
            "broken_close_penalty": 0.50,
            "sweep_min_penetration_atr": 0.10,
            "sweep_max_depth_atr": 2.0,
            "max_reclaim_bars": 3,
            "breakout_min_close_beyond_atr": 0.25,
            "breakout_accept_closes": 1,
            "max_retest_bars": 5,
            "retest_tolerance_atr": 0.30,
            "retest_max_penetration_atr": 0.50,
            "min_retest_close_location": 0.60,
            "stop_buffer_atr": 0.20,
            "target_enabled": true,
            "target_r_multiple": 2.0,
            "max_hold_bars": 20
        })
        .to_string()
    }

    fn seed_manual_zone(state: &mut RuntimeState, price: f64, lower: f64, upper: f64) {
        state.zones.push(Zone {
            id: 1,
            price,
            lower,
            upper,
            sources: BTreeSet::from([LevelSource::Pivot]),
            touches: 0,
            broken_closes: 0,
            score: 5.0,
            created_at: 1,
            created_index: 1,
            last_seen_at: 1,
            last_seen_index: 1,
            last_touched_at: None,
        });
        state.next_zone_id = 2;
        state.bar_index = 10;
    }

    fn seed_atr(state: &mut RuntimeState, settings: &DhanrekhaSettings) {
        state.atr = AtrState::new(settings.atr_period);
        for index in 0..settings.atr_period {
            state
                .atr
                .on_bar(&bar(index as u64, 100.0, 101.0, 99.0, 100.0));
        }
    }

    fn bar(index: u64, open: f64, high: f64, low: f64, close: f64) -> Bar {
        let start = index * 300_000;
        Bar {
            instrument: "BTCUSD".to_string(),
            timeframe: Timeframe::FiveMinute,
            start_at: start,
            end_at: start + 300_000,
            open,
            high,
            low,
            close,
            volume: 0.0,
            is_closed: true,
        }
    }
}
