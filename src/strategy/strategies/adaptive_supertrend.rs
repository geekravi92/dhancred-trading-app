use std::collections::{BTreeMap, VecDeque};
use std::sync::Mutex;

use serde::Deserialize;

use crate::strategy::diagnostics;
use crate::strategy::{
    Bar, CandleSnapshot, MarketEvent, PositionStatus, SignalSide, SsuConfig, Strategy,
    StrategyContext, StrategyError, StrategySignal, TickSnapshot, TimedCandle, Timeframe,
};

const WARMUP_FLOOR: usize = 50;
const ER_LOW_THRESH: f64 = 0.25;
const ER_HIGH_THRESH: f64 = 0.50;
const VOL_LOW_THRESH: f64 = 0.7;
const VOL_HIGH_THRESH: f64 = 1.3;
const MULT_SMOOTH_ALPHA: f64 = 0.15;
const PARTIAL_EXIT_FRACTION: f64 = 1.0 / 3.0;

#[derive(Clone, Copy, Debug)]
struct ExitPlan {
    reason: &'static str,
    price: f64,
    realized_r: f64,
}

#[derive(Clone, Copy, Debug)]
struct PartialExitPlan {
    level: &'static str,
    price: f64,
    target_price: f64,
    trigger_price: f64,
    realized_r: f64,
    fraction: f64,
    remaining_before: f64,
    remaining_after: f64,
}

#[derive(Debug, Default)]
pub(crate) struct AdaptiveSupertrendStrategy {
    settings: Mutex<BTreeMap<i64, AdaptiveSupertrendSettings>>,
    states: Mutex<BTreeMap<StateKey, AdaptiveSupertrendState>>,
}

impl Strategy for AdaptiveSupertrendStrategy {
    fn strategy_key(&self) -> &'static str {
        "adaptive_supertrend"
    }

    fn warmup(
        &self,
        ctx: &StrategyContext,
        ssu: &SsuConfig,
        instrument: &str,
    ) -> Result<(), StrategyError> {
        let settings = self.settings_for(ssu)?;
        let state_key = StateKey::new(ssu.ssu_id, instrument, settings.timeframe);
        let mut states = self
            .states
            .lock()
            .expect("adaptive supertrend state lock poisoned");
        let state = states
            .entry(state_key)
            .or_insert_with(|| AdaptiveSupertrendState::new(&settings));
        replay_warmup_bars(state, ctx, ssu, instrument, &settings, None)
    }

    fn on_market_event(
        &self,
        ctx: &StrategyContext,
        ssu: &SsuConfig,
        event: &MarketEvent,
    ) -> Result<Vec<StrategySignal>, StrategyError> {
        let settings = self.settings_for(ssu)?;
        match event {
            MarketEvent::Tick(snapshot) => self.on_tick_snapshot(ctx, ssu, snapshot, &settings),
            MarketEvent::Candles(snapshot) => {
                self.on_candle_snapshot(ctx, ssu, snapshot, &settings)
            }
        }
    }
}

impl AdaptiveSupertrendStrategy {
    fn on_tick_snapshot(
        &self,
        ctx: &StrategyContext,
        ssu: &SsuConfig,
        snapshot: &TickSnapshot,
        settings: &AdaptiveSupertrendSettings,
    ) -> Result<Vec<StrategySignal>, StrategyError> {
        let mut signals = Vec::new();
        for (instrument, tick) in &snapshot.ticks {
            signals.extend(self.manage_open_positions_on_tick(
                ctx,
                ssu,
                instrument,
                snapshot.event_ts,
                tick.price,
                settings,
            )?);
        }
        Ok(signals)
    }

    fn on_candle_snapshot(
        &self,
        ctx: &StrategyContext,
        ssu: &SsuConfig,
        snapshot: &CandleSnapshot,
        settings: &AdaptiveSupertrendSettings,
    ) -> Result<Vec<StrategySignal>, StrategyError> {
        let mut all_signals = Vec::new();
        for (instrument, candles_by_timeframe) in &snapshot.candles {
            let Some(timed_candle) = candles_by_timeframe.get(&settings.timeframe) else {
                continue;
            };
            let closed_bar = bar_from_timed_candle(instrument, settings.timeframe, timed_candle);
            let state_key = StateKey::new(ssu.ssu_id, instrument, settings.timeframe);
            let Some(point) =
                self.advance_state(ctx, ssu, &state_key, settings, &closed_bar, instrument)?
            else {
                continue;
            };

            let mut signals =
                self.manage_open_positions(ctx, ssu, instrument, settings, &closed_bar, &point)?;
            let exit_signal_count = signals.len();
            let mut entry_reasons = Vec::new();
            let mut entry_signal_emitted = false;

            if let Some(side) = point.entry_side {
                if settings.enabled_sides.contains(&side) {
                    entry_reasons = entry_block_reasons(settings, &point);
                    if entry_reasons.is_empty() {
                        if let Some(entry_signal) = self.try_open_entry(
                            ctx,
                            ssu,
                            instrument,
                            settings,
                            &closed_bar,
                            &point,
                            side,
                        )? {
                            entry_signal_emitted = true;
                            signals.push(entry_signal);
                        } else {
                            entry_reasons.push(
                                "entry_open_rejected:position_rule_or_invalid_risk".to_string(),
                            );
                        }
                    }
                } else {
                    entry_reasons.push(format!("side_disabled:{}", side_label(side)));
                }
            } else {
                entry_reasons = no_entry_reasons(settings, &point);
            }

            if diagnostics::closed_candle_decisions_enabled() {
                log_closed_bar_decision(
                    "market_event",
                    ssu,
                    instrument,
                    snapshot.event_ts,
                    settings,
                    &closed_bar,
                    &point,
                    exit_signal_count,
                    entry_signal_emitted,
                    &entry_reasons,
                );
            }
            all_signals.extend(signals);
        }
        Ok(all_signals)
    }

    fn settings_for(&self, ssu: &SsuConfig) -> Result<AdaptiveSupertrendSettings, StrategyError> {
        if let Some(settings) = self
            .settings
            .lock()
            .expect("adaptive supertrend settings lock poisoned")
            .get(&ssu.ssu_id)
            .cloned()
        {
            return Ok(settings);
        }

        let settings = AdaptiveSupertrendSettings::from_ssu(ssu)?;
        self.settings
            .lock()
            .expect("adaptive supertrend settings lock poisoned")
            .insert(ssu.ssu_id, settings.clone());
        Ok(settings)
    }

    fn advance_state(
        &self,
        ctx: &StrategyContext,
        ssu: &SsuConfig,
        state_key: &StateKey,
        settings: &AdaptiveSupertrendSettings,
        closed_bar: &Bar,
        instrument: &str,
    ) -> Result<Option<IndicatorPoint>, StrategyError> {
        let mut states = self
            .states
            .lock()
            .expect("adaptive supertrend state lock poisoned");
        let state = states
            .entry(state_key.clone())
            .or_insert_with(|| AdaptiveSupertrendState::new(settings));

        if state
            .last_processed_closed_end
            .is_some_and(|end_at| closed_bar.end_at <= end_at)
        {
            return Ok(None);
        }

        if state.is_empty() {
            replay_warmup_bars(
                state,
                ctx,
                ssu,
                instrument,
                settings,
                Some(closed_bar.end_at),
            )?;
        }

        state.on_closed_bar(closed_bar, settings, true)
    }

    fn manage_open_positions_on_tick(
        &self,
        ctx: &StrategyContext,
        ssu: &SsuConfig,
        instrument: &str,
        at: u64,
        tick_price: f64,
        settings: &AdaptiveSupertrendSettings,
    ) -> Result<Vec<StrategySignal>, StrategyError> {
        if !tick_price.is_finite() || tick_price <= 0.0 {
            return Ok(Vec::new());
        }

        let mut signals = Vec::new();
        let open_positions = ctx.strategy_positions.list_open_by_ssu(ssu.ssu_id)?;
        for position in open_positions
            .into_iter()
            .filter(|position| position.trade_instrument == instrument)
            .filter(|position| position.status == PositionStatus::Open)
        {
            let Some(mut metadata) = ctx.trade_contexts.load_context(&position.position_id)? else {
                continue;
            };
            let entry_price = required_f64(&metadata, "entry_price")?;
            let risk = required_f64(&metadata, "risk")?;
            let stop_price = required_f64(&metadata, "stop_price")?;
            let mut active_stop_price =
                optional_f64(&metadata, "active_stop_price").unwrap_or(stop_price);
            let tp1_price = required_f64(&metadata, "tp1_price")?;
            let tp2_price = required_f64(&metadata, "tp2_price")?;
            let tp3_price = required_f64(&metadata, "tp3_price")?;
            let tp1_r = required_f64(&metadata, "tp1_r")?;
            let tp2_r = required_f64(&metadata, "tp2_r")?;
            let tp3_r = required_f64(&metadata, "tp3_r")?;
            let entry_bar_end_at = required_u64(&metadata, "entry_bar_end_at")?;
            let mut hit_tp1 = required_bool(&metadata, "hit_tp1")?;
            let mut hit_tp2 = required_bool(&metadata, "hit_tp2")?;
            let mut hit_tp3 = required_bool(&metadata, "hit_tp3")?;

            if at <= entry_bar_end_at {
                continue;
            }

            let mut partial_plans = Vec::new();
            let mut exit_plan = None;

            if settings.exit_mode == ExitMode::PartialBook {
                if price_stopped(position.side, tick_price, stop_price) {
                    exit_plan = Some(ExitPlan {
                        reason: "stop",
                        price: tick_price,
                        realized_r: price_to_r(position.side, entry_price, risk, tick_price)
                            .max(-1.0),
                    });
                } else {
                    let mut remaining_fraction = partial_book_remaining_fraction(hit_tp1, hit_tp2);
                    if !hit_tp1 && price_reached(position.side, tick_price, tp1_price) {
                        let remaining_after = (remaining_fraction - PARTIAL_EXIT_FRACTION).max(0.0);
                        partial_plans.push(PartialExitPlan {
                            level: "tp1",
                            price: tp1_price,
                            target_price: tp1_price,
                            trigger_price: tick_price,
                            realized_r: tp1_r,
                            fraction: PARTIAL_EXIT_FRACTION,
                            remaining_before: remaining_fraction,
                            remaining_after,
                        });
                        remaining_fraction = remaining_after;
                        hit_tp1 = true;
                    }
                    if !hit_tp2 && price_reached(position.side, tick_price, tp2_price) {
                        let remaining_after = (remaining_fraction - PARTIAL_EXIT_FRACTION).max(0.0);
                        partial_plans.push(PartialExitPlan {
                            level: "tp2",
                            price: tp2_price,
                            target_price: tp2_price,
                            trigger_price: tick_price,
                            realized_r: tp2_r,
                            fraction: PARTIAL_EXIT_FRACTION,
                            remaining_before: remaining_fraction,
                            remaining_after,
                        });
                        hit_tp2 = true;
                    }
                    if price_reached(position.side, tick_price, tp3_price) {
                        hit_tp3 = true;
                        exit_plan = Some(ExitPlan {
                            reason: "tp3",
                            price: tp3_price,
                            realized_r: tp3_r,
                        });
                    }
                }
            } else {
                if price_stopped(position.side, tick_price, active_stop_price) {
                    exit_plan = Some(ExitPlan {
                        reason: "stop",
                        price: tick_price,
                        realized_r: price_to_r(position.side, entry_price, risk, tick_price)
                            .max(-1.0),
                    });
                } else {
                    if !hit_tp1 && price_reached(position.side, tick_price, tp1_price) {
                        hit_tp1 = true;
                        active_stop_price =
                            favorable_stop(position.side, active_stop_price, entry_price);
                    }
                    if !hit_tp2 && price_reached(position.side, tick_price, tp2_price) {
                        hit_tp2 = true;
                        active_stop_price =
                            favorable_stop(position.side, active_stop_price, tp1_price);
                    }
                    if !hit_tp3 && price_reached(position.side, tick_price, tp3_price) {
                        hit_tp3 = true;
                        match settings.exit_mode {
                            ExitMode::TrailThenExit => {
                                exit_plan = Some(ExitPlan {
                                    reason: "tp3",
                                    price: tp3_price,
                                    realized_r: tp3_r,
                                });
                            }
                            ExitMode::TrailSupertrend | ExitMode::TrailChandelier => {
                                active_stop_price =
                                    favorable_stop(position.side, active_stop_price, tp2_price);
                            }
                            ExitMode::PartialBook => {}
                        }
                    }
                }
            }

            metadata["hit_tp1"] = serde_json::json!(hit_tp1);
            metadata["hit_tp2"] = serde_json::json!(hit_tp2);
            metadata["hit_tp3"] = serde_json::json!(hit_tp3);
            metadata["active_stop_price"] = serde_json::json!(active_stop_price);
            metadata["last_tick_price"] = serde_json::json!(tick_price);
            metadata["last_tick_at"] = serde_json::json!(at);

            for partial in partial_plans {
                let position_id = position.position_id.clone();
                let mut signal = StrategySignal::single_leg_partial_exit(
                    ssu.ssu_id,
                    self.strategy_key(),
                    instrument,
                    position.side,
                    partial.price,
                    partial.fraction,
                    format!(
                        "adaptive_supertrend_partial_exit|level={}|mode=tick|tf={}|tick_at={}",
                        partial.level,
                        timeframe_label(settings.timeframe),
                        at
                    ),
                    at,
                );
                signal.signal_id = format!(
                    "SIG-{}-{}-{}-{}-{}",
                    ssu.ssu_id,
                    at,
                    partial_exit_signal_label(position.side),
                    partial.level.to_ascii_uppercase(),
                    position_id
                );
                signal.campaign_id = required_string(&metadata, "campaign_id")?;
                signal.instructions[0].instruction_id = format!("{}-I1", signal.signal_id);
                signal.instructions[0].leg_id = position_id.clone();
                signal.instructions[0].metadata = serde_json::json!({
                    "partial_exit": true,
                    "partial_level": partial.level,
                    "exit_fraction": partial.fraction,
                    "remaining_fraction_before": partial.remaining_before,
                    "remaining_fraction_after": partial.remaining_after,
                    "target_price": partial.target_price,
                    "trigger_price": partial.trigger_price,
                    "execution_price": partial.price,
                    "target_r": partial.realized_r,
                    "position_id": position_id.clone(),
                    "evaluation_mode": "tick",
                });
                signal.metadata = serde_json::json!({
                    "exit_reason": format!("{}_partial", partial.level),
                    "exit_mode": exit_mode_label(settings.exit_mode),
                    "position_id": position_id.clone(),
                    "tick_at": at,
                    "partial_level": partial.level,
                    "exit_fraction": partial.fraction,
                    "remaining_fraction_before": partial.remaining_before,
                    "remaining_fraction_after": partial.remaining_after,
                    "target_price": partial.target_price,
                    "trigger_price": partial.trigger_price,
                    "execution_price": partial.price,
                    "target_r": partial.realized_r,
                    "realized_r": partial.realized_r,
                    "hit_tp1": hit_tp1,
                    "hit_tp2": hit_tp2,
                    "hit_tp3": hit_tp3,
                    "evaluation_mode": "tick",
                });
                ctx.strategy_positions.partial_close_position(&signal)?;
                signals.push(signal);
            }

            if let Some(plan) = exit_plan {
                let mut signal = StrategySignal::single_leg_exit(
                    ssu.ssu_id,
                    self.strategy_key(),
                    instrument,
                    position.side,
                    plan.price,
                    format!(
                        "adaptive_supertrend_exit|reason={}|mode=tick|tf={}|tick_at={}",
                        plan.reason,
                        timeframe_label(settings.timeframe),
                        at
                    ),
                    at,
                );
                signal.signal_id = format!(
                    "SIG-{}-{}-{}-{}",
                    ssu.ssu_id,
                    at,
                    exit_signal_label(position.side),
                    position.position_id
                );
                signal.campaign_id = required_string(&metadata, "campaign_id")?;
                signal.instructions[0].instruction_id = format!("{}-I1", signal.signal_id);
                signal.instructions[0].leg_id = position.position_id.clone();
                signal.metadata = serde_json::json!({
                    "exit_reason": plan.reason,
                    "exit_mode": exit_mode_label(settings.exit_mode),
                    "position_id": position.position_id,
                    "tick_at": at,
                    "realized_r": plan.realized_r,
                    "hit_tp1": hit_tp1,
                    "hit_tp2": hit_tp2,
                    "hit_tp3": hit_tp3,
                    "active_stop_price": active_stop_price,
                    "trigger_price": tick_price,
                    "exit_price": plan.price,
                    "evaluation_mode": "tick",
                });
                match ctx.strategy_positions.close_position(&signal) {
                    Ok(_) => {
                        ctx.trade_contexts.delete_context(&position.position_id)?;
                        signals.push(signal);
                    }
                    Err(StrategyError::Rule(_)) => {}
                    Err(error) => return Err(error),
                }
            } else if !signals.is_empty() || hit_tp1 || hit_tp2 || hit_tp3 {
                ctx.trade_contexts
                    .update_context(&position.position_id, &metadata, at)?;
            }
        }

        Ok(signals)
    }

    fn manage_open_positions(
        &self,
        ctx: &StrategyContext,
        ssu: &SsuConfig,
        instrument: &str,
        settings: &AdaptiveSupertrendSettings,
        closed_bar: &Bar,
        point: &IndicatorPoint,
    ) -> Result<Vec<StrategySignal>, StrategyError> {
        let mut exits = Vec::new();
        let open_positions = ctx.strategy_positions.list_open_by_ssu(ssu.ssu_id)?;
        for position in open_positions
            .into_iter()
            .filter(|position| position.trade_instrument == instrument)
            .filter(|position| position.status == PositionStatus::Open)
        {
            let Some(mut metadata) = ctx.trade_contexts.load_context(&position.position_id)? else {
                continue;
            };
            let entry_price = required_f64(&metadata, "entry_price")?;
            let risk = required_f64(&metadata, "risk")?;
            let stop_price = required_f64(&metadata, "stop_price")?;
            let mut active_stop_price =
                optional_f64(&metadata, "active_stop_price").unwrap_or(stop_price);
            let tp1_price = required_f64(&metadata, "tp1_price")?;
            let tp2_price = required_f64(&metadata, "tp2_price")?;
            let tp3_price = required_f64(&metadata, "tp3_price")?;
            let tp1_r = required_f64(&metadata, "tp1_r")?;
            let tp2_r = required_f64(&metadata, "tp2_r")?;
            let tp3_r = required_f64(&metadata, "tp3_r")?;
            let entry_bar_end_at = required_u64(&metadata, "entry_bar_end_at")?;
            let mut hit_tp1 = required_bool(&metadata, "hit_tp1")?;
            let mut hit_tp2 = required_bool(&metadata, "hit_tp2")?;
            let mut hit_tp3 = required_bool(&metadata, "hit_tp3")?;

            if closed_bar.end_at <= entry_bar_end_at {
                continue;
            }

            let timeout_hit = closed_bar.end_at.saturating_sub(entry_bar_end_at)
                >= settings.trade_timeout_bars as u64 * timeframe_millis(settings.timeframe);
            let opposite_flip = point.entry_side.is_some_and(|side| side != position.side);

            let mut partial_plans = Vec::new();
            let exit_plan = if settings.exit_mode == ExitMode::PartialBook {
                let tp1_reached = reached(position.side, closed_bar, tp1_price);
                let tp2_reached = reached(position.side, closed_bar, tp2_price);
                let tp3_reached = reached(position.side, closed_bar, tp3_price);
                let sl_hit = stopped(position.side, closed_bar, stop_price);

                let mut remaining_fraction = partial_book_remaining_fraction(hit_tp1, hit_tp2);
                if !hit_tp1 && tp1_reached {
                    let remaining_after = (remaining_fraction - PARTIAL_EXIT_FRACTION).max(0.0);
                    partial_plans.push(PartialExitPlan {
                        level: "tp1",
                        price: tp1_price,
                        target_price: tp1_price,
                        trigger_price: closed_bar.close,
                        realized_r: tp1_r,
                        fraction: PARTIAL_EXIT_FRACTION,
                        remaining_before: remaining_fraction,
                        remaining_after,
                    });
                    remaining_fraction = remaining_after;
                    hit_tp1 = true;
                }
                if !hit_tp2 && tp2_reached {
                    let remaining_after = (remaining_fraction - PARTIAL_EXIT_FRACTION).max(0.0);
                    partial_plans.push(PartialExitPlan {
                        level: "tp2",
                        price: tp2_price,
                        target_price: tp2_price,
                        trigger_price: closed_bar.close,
                        realized_r: tp2_r,
                        fraction: PARTIAL_EXIT_FRACTION,
                        remaining_before: remaining_fraction,
                        remaining_after,
                    });
                    remaining_fraction = remaining_after;
                    hit_tp2 = true;
                }
                if tp3_reached {
                    hit_tp3 = true;
                }

                let exit_plan = if hit_tp3 {
                    Some(ExitPlan {
                        reason: "tp3",
                        price: tp3_price,
                        realized_r: tp3_r,
                    })
                } else if sl_hit {
                    Some(ExitPlan {
                        reason: "stop",
                        price: stop_price,
                        realized_r: price_to_r(position.side, entry_price, risk, stop_price)
                            .max(-1.0),
                    })
                } else if timeout_hit {
                    Some(ExitPlan {
                        reason: "timeout",
                        price: closed_bar.close,
                        realized_r: price_to_r(position.side, entry_price, risk, closed_bar.close),
                    })
                } else if opposite_flip {
                    Some(ExitPlan {
                        reason: "opposite_flip",
                        price: closed_bar.close,
                        realized_r: price_to_r(position.side, entry_price, risk, closed_bar.close),
                    })
                } else {
                    None
                };
                exit_plan
            } else {
                let mut exit_plan = None;

                if stopped(position.side, closed_bar, active_stop_price) {
                    let realized_r =
                        price_to_r(position.side, entry_price, risk, active_stop_price).max(-1.0);
                    exit_plan = Some(ExitPlan {
                        reason: "stop",
                        price: active_stop_price,
                        realized_r,
                    });
                }

                if exit_plan.is_none() {
                    if !hit_tp1 && reached(position.side, closed_bar, tp1_price) {
                        hit_tp1 = true;
                        active_stop_price =
                            favorable_stop(position.side, active_stop_price, entry_price);
                    }
                    if !hit_tp2 && reached(position.side, closed_bar, tp2_price) {
                        hit_tp2 = true;
                        active_stop_price =
                            favorable_stop(position.side, active_stop_price, tp1_price);
                    }
                    if !hit_tp3 && reached(position.side, closed_bar, tp3_price) {
                        hit_tp3 = true;
                        match settings.exit_mode {
                            ExitMode::TrailThenExit => {
                                exit_plan = Some(ExitPlan {
                                    reason: "tp3",
                                    price: tp3_price,
                                    realized_r: tp3_r,
                                });
                            }
                            ExitMode::TrailSupertrend | ExitMode::TrailChandelier => {
                                active_stop_price =
                                    favorable_stop(position.side, active_stop_price, tp2_price);
                            }
                            ExitMode::PartialBook => {}
                        }
                    }
                }

                if exit_plan.is_none() && hit_tp3 {
                    match settings.exit_mode {
                        ExitMode::TrailSupertrend => {
                            active_stop_price = favorable_stop(
                                position.side,
                                active_stop_price,
                                point.supertrend_line,
                            );
                            let close_break = match position.side {
                                SignalSide::Long => closed_bar.close < point.supertrend_line,
                                SignalSide::Short => closed_bar.close > point.supertrend_line,
                            };
                            if close_break {
                                let realized_r =
                                    price_to_r(position.side, entry_price, risk, closed_bar.close)
                                        .max(tp2_r)
                                        .max(-1.0);
                                exit_plan = Some(ExitPlan {
                                    reason: "supertrend_trail",
                                    price: synthetic_exit_price(
                                        position.side,
                                        entry_price,
                                        risk,
                                        realized_r,
                                    ),
                                    realized_r,
                                });
                            }
                        }
                        ExitMode::TrailChandelier => {
                            if point.atr > 0.0 && point.atr.is_finite() {
                                match position.side {
                                    SignalSide::Long => {
                                        let high_since_tp3 =
                                            optional_f64(&metadata, "trail_high_since_tp3")
                                                .unwrap_or(closed_bar.high)
                                                .max(closed_bar.high);
                                        metadata["trail_high_since_tp3"] =
                                            serde_json::json!(high_since_tp3);
                                        let candidate = high_since_tp3
                                            - settings.chandelier_atr_mult * point.atr;
                                        active_stop_price = favorable_stop(
                                            position.side,
                                            active_stop_price,
                                            candidate,
                                        );
                                    }
                                    SignalSide::Short => {
                                        let low_since_tp3 =
                                            optional_f64(&metadata, "trail_low_since_tp3")
                                                .unwrap_or(closed_bar.low)
                                                .min(closed_bar.low);
                                        metadata["trail_low_since_tp3"] =
                                            serde_json::json!(low_since_tp3);
                                        let candidate = low_since_tp3
                                            + settings.chandelier_atr_mult * point.atr;
                                        active_stop_price = favorable_stop(
                                            position.side,
                                            active_stop_price,
                                            candidate,
                                        );
                                    }
                                }
                            }
                        }
                        ExitMode::PartialBook | ExitMode::TrailThenExit => {}
                    }
                }

                if exit_plan.is_none() && (timeout_hit || opposite_flip) {
                    let reason = if timeout_hit {
                        "timeout"
                    } else {
                        "opposite_flip"
                    };
                    let floor_r = if hit_tp3 {
                        tp2_r
                    } else {
                        price_to_r(position.side, entry_price, risk, active_stop_price)
                    };
                    let realized_r = price_to_r(position.side, entry_price, risk, closed_bar.close)
                        .max(floor_r)
                        .max(-1.0);
                    exit_plan = Some(ExitPlan {
                        reason,
                        price: synthetic_exit_price(position.side, entry_price, risk, realized_r),
                        realized_r,
                    });
                }

                exit_plan
            };

            metadata["hit_tp1"] = serde_json::json!(hit_tp1);
            metadata["hit_tp2"] = serde_json::json!(hit_tp2);
            metadata["hit_tp3"] = serde_json::json!(hit_tp3);
            metadata["active_stop_price"] = serde_json::json!(active_stop_price);
            metadata["exit_mode"] = serde_json::json!(exit_mode_label(settings.exit_mode));

            for partial in partial_plans {
                let position_id = position.position_id.clone();
                let mut signal = StrategySignal::single_leg_partial_exit(
                    ssu.ssu_id,
                    self.strategy_key(),
                    instrument,
                    position.side,
                    partial.price,
                    partial.fraction,
                    format!(
                        "adaptive_supertrend_partial_exit|level={}|tf={}|closed_bar_end={}",
                        partial.level,
                        timeframe_label(settings.timeframe),
                        closed_bar.end_at
                    ),
                    closed_bar.end_at,
                );
                signal.signal_id = format!(
                    "SIG-{}-{}-{}-{}-{}",
                    ssu.ssu_id,
                    closed_bar.end_at,
                    partial_exit_signal_label(position.side),
                    partial.level.to_ascii_uppercase(),
                    position_id
                );
                signal.campaign_id = required_string(&metadata, "campaign_id")?;
                signal.instructions[0].instruction_id = format!("{}-I1", signal.signal_id);
                signal.instructions[0].leg_id = position_id.clone();
                signal.instructions[0].metadata = serde_json::json!({
                    "partial_exit": true,
                    "partial_level": partial.level,
                    "exit_fraction": partial.fraction,
                    "remaining_fraction_before": partial.remaining_before,
                    "remaining_fraction_after": partial.remaining_after,
                    "target_price": partial.target_price,
                    "trigger_price": partial.trigger_price,
                    "execution_price": partial.price,
                    "target_r": partial.realized_r,
                    "position_id": position_id.clone(),
                });
                signal.metadata = serde_json::json!({
                    "exit_reason": format!("{}_partial", partial.level),
                    "exit_mode": exit_mode_label(settings.exit_mode),
                    "position_id": position_id.clone(),
                    "closed_bar_end": closed_bar.end_at,
                    "partial_level": partial.level,
                    "exit_fraction": partial.fraction,
                    "remaining_fraction_before": partial.remaining_before,
                    "remaining_fraction_after": partial.remaining_after,
                    "target_price": partial.target_price,
                    "trigger_price": partial.trigger_price,
                    "execution_price": partial.price,
                    "target_r": partial.realized_r,
                    "realized_r": partial.realized_r,
                    "hit_tp1": hit_tp1,
                    "hit_tp2": hit_tp2,
                    "hit_tp3": hit_tp3,
                });
                ctx.strategy_positions.partial_close_position(&signal)?;
                exits.push(signal);
            }

            if let Some(plan) = exit_plan {
                let mut signal = StrategySignal::single_leg_exit(
                    ssu.ssu_id,
                    self.strategy_key(),
                    instrument,
                    position.side,
                    plan.price,
                    format!(
                        "adaptive_supertrend_exit|reason={}|tf={}|closed_bar_end={}",
                        plan.reason,
                        timeframe_label(settings.timeframe),
                        closed_bar.end_at
                    ),
                    closed_bar.end_at,
                );
                signal.signal_id = format!(
                    "SIG-{}-{}-{}-{}",
                    ssu.ssu_id,
                    closed_bar.end_at,
                    exit_signal_label(position.side),
                    position.position_id
                );
                signal.campaign_id = required_string(&metadata, "campaign_id")?;
                signal.instructions[0].instruction_id = format!("{}-I1", signal.signal_id);
                signal.instructions[0].leg_id = position.position_id.clone();
                signal.metadata = serde_json::json!({
                    "exit_reason": plan.reason,
                    "exit_mode": exit_mode_label(settings.exit_mode),
                    "position_id": position.position_id,
                    "closed_bar_end": closed_bar.end_at,
                    "realized_r": plan.realized_r,
                    "hit_tp1": hit_tp1,
                    "hit_tp2": hit_tp2,
                    "hit_tp3": hit_tp3,
                    "active_stop_price": active_stop_price,
                    "exit_price": plan.price,
                    "synthetic_exit_price": plan.price,
                });
                match ctx.strategy_positions.close_position(&signal) {
                    Ok(_) => {
                        ctx.trade_contexts.delete_context(&position.position_id)?;
                        exits.push(signal);
                    }
                    Err(StrategyError::Rule(_)) => {}
                    Err(error) => return Err(error),
                }
            } else {
                ctx.trade_contexts.update_context(
                    &position.position_id,
                    &metadata,
                    closed_bar.end_at,
                )?;
            }
        }
        Ok(exits)
    }

    fn try_open_entry(
        &self,
        ctx: &StrategyContext,
        ssu: &SsuConfig,
        instrument: &str,
        settings: &AdaptiveSupertrendSettings,
        closed_bar: &Bar,
        point: &IndicatorPoint,
        side: SignalSide,
    ) -> Result<Option<StrategySignal>, StrategyError> {
        if point.atr <= 0.0 || !point.atr.is_finite() {
            return Ok(None);
        }
        if !settings.entry_filters_pass(point) {
            return Ok(None);
        }

        let entry_price = closed_bar.close;
        let stop_price = match side {
            SignalSide::Long => {
                let pivot_base = point.last_pivot_low.unwrap_or(closed_bar.low);
                let raw_stop = pivot_base - settings.effective_sl_mult * point.atr;
                let min_stop = entry_price - settings.effective_sl_mult * point.atr;
                raw_stop.min(min_stop)
            }
            SignalSide::Short => {
                let pivot_base = point.last_pivot_high.unwrap_or(closed_bar.high);
                let raw_stop = pivot_base + settings.effective_sl_mult * point.atr;
                let min_stop = entry_price + settings.effective_sl_mult * point.atr;
                raw_stop.max(min_stop)
            }
        };
        let risk = match side {
            SignalSide::Long => entry_price - stop_price,
            SignalSide::Short => stop_price - entry_price,
        };
        if !risk.is_finite() || risk <= 0.0 {
            return Ok(None);
        }

        let tp1_price = target_price(side, entry_price, risk, point.tp1_r);
        let tp2_price = target_price(side, entry_price, risk, point.tp2_r);
        let tp3_price = target_price(side, entry_price, risk, point.tp3_r);

        let mut signal = StrategySignal::single_leg_entry(
            ssu.ssu_id,
            self.strategy_key(),
            instrument,
            side,
            entry_price,
            format!(
                "adaptive_supertrend_entry|side={}|tf={}|closed_bar_end={}|tqi={:.4}|er={:.4}|vol_ratio={:.4}",
                side_label(side),
                timeframe_label(settings.timeframe),
                closed_bar.end_at,
                point.tqi,
                point.er,
                point.vol_ratio
            ),
            closed_bar.end_at,
        );
        signal.metadata = serde_json::json!({
            "timeframe": timeframe_label(settings.timeframe),
            "entry_price": entry_price,
            "stop_price": stop_price,
            "risk": risk,
            "tp1_price": tp1_price,
            "tp2_price": tp2_price,
            "tp3_price": tp3_price,
            "tp1_r": point.tp1_r,
            "tp2_r": point.tp2_r,
            "tp3_r": point.tp3_r,
            "tp_mode": if settings.tp_mode == TpMode::Dynamic { "dynamic" } else { "fixed" },
            "exit_mode": exit_mode_label(settings.exit_mode),
            "tp_scale": point.tp_scale,
            "tqi": point.tqi,
            "tqi_er": point.tqi_er,
            "tqi_vol": point.tqi_vol,
            "tqi_struct": point.tqi_struct,
            "tqi_momentum": point.tqi_momentum,
            "er": point.er,
            "vol_ratio": point.vol_ratio,
            "pre_flip_trend_age": point.pre_flip_trend_age,
            "entry_filters": settings.entry_filters_metadata(),
            "atr": point.atr,
            "supertrend_line": point.supertrend_line,
            "trend": point.trend,
            "flip_reason": point.flip_reason,
        });
        signal.instructions[0].metadata = serde_json::json!({
            "stop_price": stop_price,
            "target_price": tp3_price,
            "tp1_price": tp1_price,
            "tp2_price": tp2_price,
            "tp3_price": tp3_price,
            "risk": risk,
            "tp1_r": point.tp1_r,
            "tp2_r": point.tp2_r,
            "tp3_r": point.tp3_r,
        });

        let position = match ctx.strategy_positions.open_position(&signal, ssu) {
            Ok(position) => position,
            Err(StrategyError::Rule(_)) => return Ok(None),
            Err(error) => return Err(error),
        };
        let metadata = serde_json::json!({
            "campaign_id": signal.campaign_id,
            "entry_price": entry_price,
            "entry_bar_end_at": closed_bar.end_at,
            "stop_price": stop_price,
            "risk": risk,
            "tp1_price": tp1_price,
            "tp2_price": tp2_price,
            "tp3_price": tp3_price,
            "tp1_r": point.tp1_r,
            "tp2_r": point.tp2_r,
            "tp3_r": point.tp3_r,
            "exit_mode": exit_mode_label(settings.exit_mode),
            "initial_stop_price": stop_price,
            "active_stop_price": stop_price,
            "tp_scale": point.tp_scale,
            "tqi": point.tqi,
            "er": point.er,
            "vol_ratio": point.vol_ratio,
            "pre_flip_trend_age": point.pre_flip_trend_age,
            "regime_cell": point.regime_cell,
            "hit_tp1": false,
            "hit_tp2": false,
            "hit_tp3": false,
        });
        ctx.trade_contexts.save_context(
            &position.position_id,
            ssu.ssu_id,
            self.strategy_key(),
            instrument,
            &metadata,
            closed_bar.end_at,
        )?;

        Ok(Some(signal))
    }
}

fn bar_from_timed_candle(
    instrument: &str,
    timeframe: Timeframe,
    timed_candle: &TimedCandle,
) -> Bar {
    Bar {
        instrument: instrument.to_string(),
        timeframe,
        start_at: timed_candle.start_ts,
        end_at: timed_candle.end_ts,
        open: timed_candle.candle.open,
        high: timed_candle.candle.high,
        low: timed_candle.candle.low,
        close: timed_candle.candle.close,
        volume: timed_candle.candle.volume,
        is_closed: true,
    }
}

#[derive(Clone, Debug)]
struct AdaptiveSupertrendSettings {
    timeframe: Timeframe,
    enabled_sides: Vec<SignalSide>,
    lookback_bars: usize,
    source: Source,
    effective_atr_len: usize,
    effective_base_mult: f64,
    effective_er_len: usize,
    effective_sl_mult: f64,
    atr_baseline_len: usize,
    use_adaptive: bool,
    adapt_strength: f64,
    use_tqi: bool,
    quality_strength: f64,
    quality_curve: f64,
    smooth_multipliers: bool,
    use_asym_bands: bool,
    asym_strength: f64,
    use_eff_atr: bool,
    use_char_flip: bool,
    char_flip_min_age: usize,
    char_flip_high: f64,
    char_flip_low: f64,
    tqi_weight_er: f64,
    tqi_weight_vol: f64,
    tqi_weight_struct: f64,
    tqi_weight_momentum: f64,
    tqi_struct_len: usize,
    tqi_mom_len: usize,
    pivot_len: usize,
    volume_z_len: usize,
    tp_mode: TpMode,
    tp1_r: f64,
    tp2_r: f64,
    tp3_r: f64,
    dyn_tp_tqi_weight: f64,
    dyn_tp_vol_weight: f64,
    dyn_tp_min_scale: f64,
    dyn_tp_max_scale: f64,
    dyn_tp_floor_r1: f64,
    dyn_tp_ceil_r3: f64,
    trade_timeout_bars: usize,
    exit_mode: ExitMode,
    chandelier_atr_mult: f64,
    min_entry_tqi: f64,
    min_entry_er: f64,
    min_entry_tp_scale: f64,
    min_entry_trend_age: usize,
    min_entry_vol_ratio: f64,
    max_entry_vol_ratio: f64,
}

#[derive(Default, Deserialize)]
struct RawAdaptiveSupertrendSettings {
    timeframe: Option<String>,
    enabled_sides: Option<Vec<String>>,
    preset: Option<String>,
    lookback_bars: Option<usize>,
    source: Option<String>,
    atr_length: Option<usize>,
    base_band_width_atr: Option<f64>,
    use_adaptive: Option<bool>,
    efficiency_window: Option<usize>,
    adaptation_strength: Option<f64>,
    atr_baseline_length: Option<usize>,
    use_tqi: Option<bool>,
    quality_influence: Option<f64>,
    quality_curve_power: Option<f64>,
    smooth_adaptive_multipliers: Option<bool>,
    asymmetric_bands: Option<bool>,
    asymmetry_strength: Option<f64>,
    efficiency_weighted_atr: Option<bool>,
    character_flip: Option<bool>,
    character_flip_min_age: Option<usize>,
    character_flip_high_tqi: Option<f64>,
    character_flip_low_tqi: Option<f64>,
    weight_efficiency: Option<f64>,
    weight_volatility_regime: Option<f64>,
    weight_structure: Option<f64>,
    weight_momentum_persist: Option<f64>,
    structure_window: Option<usize>,
    momentum_persist_window: Option<usize>,
    pivot_strength: Option<usize>,
    volume_z_window: Option<usize>,
    sl_buffer_atr: Option<f64>,
    tp_mode: Option<String>,
    tp1_r: Option<f64>,
    tp2_r: Option<f64>,
    tp3_r: Option<f64>,
    dyn_tp_tqi_weight: Option<f64>,
    dyn_tp_vol_weight: Option<f64>,
    dyn_tp_min_scale: Option<f64>,
    dyn_tp_max_scale: Option<f64>,
    dyn_tp_floor_r1: Option<f64>,
    dyn_tp_ceil_r3: Option<f64>,
    trade_timeout_bars: Option<usize>,
    exit_mode: Option<String>,
    chandelier_atr_mult: Option<f64>,
    min_entry_tqi: Option<f64>,
    min_entry_er: Option<f64>,
    min_entry_tp_scale: Option<f64>,
    min_entry_trend_age: Option<usize>,
    min_entry_vol_ratio: Option<f64>,
    max_entry_vol_ratio: Option<f64>,
}

impl AdaptiveSupertrendSettings {
    fn from_ssu(ssu: &SsuConfig) -> Result<Self, StrategyError> {
        let raw = if ssu.params_json.trim().is_empty() {
            RawAdaptiveSupertrendSettings::default()
        } else {
            serde_json::from_str::<RawAdaptiveSupertrendSettings>(&ssu.params_json).map_err(
                |error| {
                    StrategyError::Parse(format!(
                        "invalid adaptive_supertrend params_json for SSU {}: {error}",
                        ssu.ssu_id
                    ))
                },
            )?
        };
        let timeframe = parse_timeframe(&require(raw.timeframe, "timeframe", ssu.ssu_id)?)?;
        if !ssu.required_timeframes.contains(&timeframe) {
            return Err(StrategyError::Config(format!(
                "SSU {} adaptive_supertrend timeframe {} is not registered",
                ssu.ssu_id,
                timeframe_label(timeframe)
            )));
        }

        let preset = raw.preset.unwrap_or_else(|| "Auto".to_string());
        let resolved_preset = resolve_preset(&preset, timeframe);
        let atr_len_input = raw.atr_length.unwrap_or(13).clamp(5, 100);
        let base_mult_input = raw.base_band_width_atr.unwrap_or(2.0).clamp(0.5, 5.0);
        let er_len_input = raw.efficiency_window.unwrap_or(20).clamp(5, 100);
        let sl_mult_input = raw.sl_buffer_atr.unwrap_or(1.5).clamp(0.3, 5.0);

        let (effective_atr_len, effective_base_mult, effective_er_len, effective_sl_mult) =
            preset_values(
                &resolved_preset,
                atr_len_input,
                base_mult_input,
                er_len_input,
                sl_mult_input,
            );

        let mut sides = Vec::new();
        for side in raw
            .enabled_sides
            .unwrap_or_else(|| vec!["long".to_string(), "short".to_string()])
        {
            sides.push(parse_side(&side)?);
        }
        if sides.is_empty() {
            return Err(StrategyError::Config(format!(
                "SSU {} adaptive_supertrend enabled_sides must not be empty",
                ssu.ssu_id
            )));
        }

        let (tp1_r, tp2_r, tp3_r) = sort_three(
            raw.tp1_r.unwrap_or(1.0).clamp(0.5, 10.0),
            raw.tp2_r.unwrap_or(2.0).clamp(0.5, 10.0),
            raw.tp3_r.unwrap_or(3.0).clamp(0.5, 10.0),
        );

        let mut settings = Self {
            timeframe,
            enabled_sides: sides,
            lookback_bars: raw.lookback_bars.unwrap_or(600).max(64),
            source: parse_source(raw.source.as_deref().unwrap_or("close"))?,
            effective_atr_len,
            effective_base_mult,
            effective_er_len,
            effective_sl_mult,
            atr_baseline_len: raw.atr_baseline_length.unwrap_or(100).clamp(20, 500),
            use_adaptive: raw.use_adaptive.unwrap_or(true),
            adapt_strength: raw.adaptation_strength.unwrap_or(0.5).clamp(0.0, 1.0),
            use_tqi: raw.use_tqi.unwrap_or(true),
            quality_strength: raw.quality_influence.unwrap_or(0.4).clamp(0.0, 1.0),
            quality_curve: raw.quality_curve_power.unwrap_or(1.5).clamp(1.0, 3.0),
            smooth_multipliers: raw.smooth_adaptive_multipliers.unwrap_or(true),
            use_asym_bands: raw.asymmetric_bands.unwrap_or(true),
            asym_strength: raw.asymmetry_strength.unwrap_or(0.5).clamp(0.0, 1.0),
            use_eff_atr: raw.efficiency_weighted_atr.unwrap_or(true),
            use_char_flip: raw.character_flip.unwrap_or(true),
            char_flip_min_age: raw.character_flip_min_age.unwrap_or(5).clamp(1, 50),
            char_flip_high: raw.character_flip_high_tqi.unwrap_or(0.55).clamp(0.3, 0.9),
            char_flip_low: raw.character_flip_low_tqi.unwrap_or(0.25).clamp(0.0, 0.5),
            tqi_weight_er: raw.weight_efficiency.unwrap_or(0.35).clamp(0.0, 1.0),
            tqi_weight_vol: raw.weight_volatility_regime.unwrap_or(0.20).clamp(0.0, 1.0),
            tqi_weight_struct: raw.weight_structure.unwrap_or(0.25).clamp(0.0, 1.0),
            tqi_weight_momentum: raw.weight_momentum_persist.unwrap_or(0.20).clamp(0.0, 1.0),
            tqi_struct_len: raw.structure_window.unwrap_or(20).clamp(5, 100),
            tqi_mom_len: raw.momentum_persist_window.unwrap_or(10).clamp(3, 50),
            pivot_len: raw.pivot_strength.unwrap_or(3).clamp(2, 10),
            volume_z_len: raw.volume_z_window.unwrap_or(20).clamp(5, 100),
            tp_mode: parse_tp_mode(raw.tp_mode.as_deref().unwrap_or("Fixed"))?,
            tp1_r,
            tp2_r,
            tp3_r,
            dyn_tp_tqi_weight: raw.dyn_tp_tqi_weight.unwrap_or(0.6).clamp(0.0, 1.0),
            dyn_tp_vol_weight: raw.dyn_tp_vol_weight.unwrap_or(0.4).clamp(0.0, 1.0),
            dyn_tp_min_scale: raw.dyn_tp_min_scale.unwrap_or(0.5).clamp(0.2, 1.0),
            dyn_tp_max_scale: raw.dyn_tp_max_scale.unwrap_or(2.0).clamp(1.0, 4.0),
            dyn_tp_floor_r1: raw.dyn_tp_floor_r1.unwrap_or(0.5).clamp(0.2, 2.0),
            dyn_tp_ceil_r3: raw.dyn_tp_ceil_r3.unwrap_or(8.0).clamp(2.0, 20.0),
            trade_timeout_bars: raw.trade_timeout_bars.unwrap_or(100).clamp(10, 500),
            exit_mode: parse_exit_mode(raw.exit_mode.as_deref().unwrap_or("partial_book"))?,
            chandelier_atr_mult: raw.chandelier_atr_mult.unwrap_or(2.0).clamp(0.1, 10.0),
            min_entry_tqi: raw.min_entry_tqi.unwrap_or(0.0).clamp(0.0, 1.0),
            min_entry_er: raw.min_entry_er.unwrap_or(0.0).clamp(0.0, 1.0),
            min_entry_tp_scale: raw.min_entry_tp_scale.unwrap_or(0.0).clamp(0.0, 4.0),
            min_entry_trend_age: raw.min_entry_trend_age.unwrap_or(0).clamp(0, 500),
            min_entry_vol_ratio: raw.min_entry_vol_ratio.unwrap_or(0.0).clamp(0.0, 10.0),
            max_entry_vol_ratio: raw.max_entry_vol_ratio.unwrap_or(10.0).clamp(0.0, 10.0),
        };
        if settings.min_entry_vol_ratio > settings.max_entry_vol_ratio {
            return Err(StrategyError::Config(format!(
                "SSU {} adaptive_supertrend min_entry_vol_ratio must be <= max_entry_vol_ratio",
                ssu.ssu_id
            )));
        }
        settings.lookback_bars = settings.lookback_bars.max(settings.min_warmup_bars() + 5);
        Ok(settings)
    }

    fn min_warmup_bars(&self) -> usize {
        WARMUP_FLOOR
            .max(self.effective_atr_len)
            .max(self.effective_er_len)
            .max(self.volume_z_len)
            .max(self.pivot_len * 2 + 1)
            .max(self.tqi_mom_len)
            .max(self.tqi_struct_len)
            + 10
    }

    fn state_capacity(&self) -> usize {
        self.lookback_bars
            .max(self.atr_baseline_len + self.effective_er_len + 10)
            .max(self.tqi_struct_len + self.tqi_mom_len + 10)
    }

    fn entry_filters_pass(&self, point: &IndicatorPoint) -> bool {
        point.tqi >= self.min_entry_tqi
            && point.er >= self.min_entry_er
            && point.tp_scale >= self.min_entry_tp_scale
            && point.pre_flip_trend_age >= self.min_entry_trend_age
            && point.vol_ratio >= self.min_entry_vol_ratio
            && point.vol_ratio <= self.max_entry_vol_ratio
    }

    fn entry_filters_metadata(&self) -> serde_json::Value {
        serde_json::json!({
            "min_entry_tqi": self.min_entry_tqi,
            "min_entry_er": self.min_entry_er,
            "min_entry_tp_scale": self.min_entry_tp_scale,
            "min_entry_trend_age": self.min_entry_trend_age,
            "min_entry_vol_ratio": self.min_entry_vol_ratio,
            "max_entry_vol_ratio": self.max_entry_vol_ratio,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Source {
    Open,
    High,
    Low,
    Close,
    Hl2,
    Hlc3,
    Hlcc4,
    Ohlc4,
}

impl Source {
    fn value(self, bar: &Bar) -> f64 {
        match self {
            Self::Open => bar.open,
            Self::High => bar.high,
            Self::Low => bar.low,
            Self::Close => bar.close,
            Self::Hl2 => (bar.high + bar.low) / 2.0,
            Self::Hlc3 => (bar.high + bar.low + bar.close) / 3.0,
            Self::Hlcc4 => (bar.high + bar.low + bar.close + bar.close) / 4.0,
            Self::Ohlc4 => (bar.open + bar.high + bar.low + bar.close) / 4.0,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TpMode {
    Fixed,
    Dynamic,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExitMode {
    PartialBook,
    TrailThenExit,
    TrailSupertrend,
    TrailChandelier,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
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
struct IndicatorPoint {
    entry_side: Option<SignalSide>,
    flip_reason: &'static str,
    prev_trend: i32,
    trend: i32,
    supertrend_line: f64,
    atr: f64,
    er: f64,
    vol_ratio: f64,
    tqi: f64,
    tqi_er: f64,
    tqi_vol: f64,
    tqi_struct: f64,
    tqi_momentum: f64,
    tp_scale: f64,
    tp1_r: f64,
    tp2_r: f64,
    tp3_r: f64,
    processed_bars: usize,
    pre_flip_trend_age: usize,
    last_pivot_high: Option<f64>,
    last_pivot_low: Option<f64>,
    regime_cell: usize,
}

#[derive(Debug)]
struct AdaptiveSupertrendState {
    last_close: Option<f64>,
    tr_rma: WilderRma,
    atr_baseline: RollingMean,
    er: RollingEfficiency,
    volume_stats: RollingStats,
    structure: RollingExtrema,
    momentum: RollingMomentum,
    pivot_tracker: PivotTracker,
    pending_pivot_high: Option<f64>,
    pending_pivot_low: Option<f64>,
    lower_band: Option<f64>,
    upper_band: Option<f64>,
    trend: i32,
    trend_start_index: usize,
    active_mult_sm: Option<f64>,
    passive_mult_sm: Option<f64>,
    last_tqi: Option<f64>,
    last_pivot_high: Option<f64>,
    last_pivot_low: Option<f64>,
    last_processed_closed_end: Option<u64>,
    processed_bars: usize,
}

impl AdaptiveSupertrendState {
    fn new(settings: &AdaptiveSupertrendSettings) -> Self {
        Self {
            last_close: None,
            tr_rma: WilderRma::new(settings.effective_atr_len),
            atr_baseline: RollingMean::new(settings.atr_baseline_len),
            er: RollingEfficiency::new(settings.effective_er_len),
            volume_stats: RollingStats::new(settings.volume_z_len),
            structure: RollingExtrema::new(settings.tqi_struct_len),
            momentum: RollingMomentum::new(settings.tqi_mom_len),
            pivot_tracker: PivotTracker::new(settings.pivot_len),
            pending_pivot_high: None,
            pending_pivot_low: None,
            lower_band: None,
            upper_band: None,
            trend: 1,
            trend_start_index: 0,
            active_mult_sm: None,
            passive_mult_sm: None,
            last_tqi: None,
            last_pivot_high: None,
            last_pivot_low: None,
            last_processed_closed_end: None,
            processed_bars: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.processed_bars == 0
    }

    fn on_closed_bar(
        &mut self,
        bar: &Bar,
        settings: &AdaptiveSupertrendSettings,
        may_emit: bool,
    ) -> Result<Option<IndicatorPoint>, StrategyError> {
        if self
            .last_processed_closed_end
            .is_some_and(|end_at| bar.end_at <= end_at)
        {
            return Ok(None);
        }
        if bar.timeframe != settings.timeframe {
            return Err(StrategyError::Rule(format!(
                "adaptive_supertrend expected {} bar, got {}",
                timeframe_label(settings.timeframe),
                timeframe_label(bar.timeframe)
            )));
        }

        self.apply_pending_pivots();

        let prev_close = self.last_close.unwrap_or(bar.close);
        let true_range = (bar.high - bar.low)
            .max((bar.high - prev_close).abs())
            .max((bar.low - prev_close).abs());
        let raw_atr = self.tr_rma.next(true_range).unwrap_or(0.0);
        let atr_baseline_mean = self.atr_baseline.push(raw_atr);
        let atr_baseline = if self.atr_baseline.is_full() {
            atr_baseline_mean
        } else {
            raw_atr
        };
        let vol_ratio = safe_div(raw_atr, atr_baseline, 1.0);

        let er = self.er.push(bar.close);
        let atr_value = if settings.use_eff_atr {
            raw_atr * (0.5 + 0.5 * er)
        } else {
            raw_atr
        };

        let tqi_er = er.clamp(0.0, 1.0);
        let tqi_vol = self.tqi_volatility(bar, vol_ratio);
        let (struct_hi, struct_lo) = self.structure.push(bar.high, bar.low);
        let price_pos = safe_div(bar.close - struct_lo, struct_hi - struct_lo, 0.5);
        let tqi_struct = ((price_pos - 0.5).abs() * 2.0).clamp(0.0, 1.0);
        let tqi_momentum = self.momentum.push(bar.close);
        let tqi_weight_sum = settings.tqi_weight_er
            + settings.tqi_weight_vol
            + settings.tqi_weight_struct
            + settings.tqi_weight_momentum;
        let tqi = if settings.use_tqi {
            safe_div(
                tqi_er * settings.tqi_weight_er
                    + tqi_vol * settings.tqi_weight_vol
                    + tqi_struct * settings.tqi_weight_struct
                    + tqi_momentum * settings.tqi_weight_momentum,
                if tqi_weight_sum > 0.0 {
                    tqi_weight_sum
                } else {
                    1.0
                },
                0.5,
            )
            .clamp(0.0, 1.0)
        } else {
            0.5
        };

        let legacy_adapt_factor = if settings.use_adaptive {
            1.0 + settings.adapt_strength * (0.5 - er)
        } else {
            1.0
        };
        let quality_deviation = if settings.use_tqi {
            (1.0 - tqi).powf(settings.quality_curve)
        } else {
            0.5
        };
        let tqi_mult = 1.0 - settings.quality_strength
            + settings.quality_strength * (0.6 + 0.8 * quality_deviation);
        let sym_mult = settings.effective_base_mult * legacy_adapt_factor * tqi_mult;
        let (mut active_mult_raw, mut passive_mult_raw) = (sym_mult, sym_mult);
        if settings.use_tqi && settings.use_asym_bands {
            let asym_tighten = 1.0 - settings.asym_strength * tqi * 0.3;
            let asym_widen = 1.0 + settings.asym_strength * tqi * 0.4;
            active_mult_raw = sym_mult * asym_tighten;
            passive_mult_raw = sym_mult * asym_widen;
        }
        let active_mult = smooth_value(
            self.active_mult_sm,
            active_mult_raw,
            settings.smooth_multipliers,
        );
        let passive_mult = smooth_value(
            self.passive_mult_sm,
            passive_mult_raw,
            settings.smooth_multipliers,
        );
        self.active_mult_sm = Some(active_mult);
        self.passive_mult_sm = Some(passive_mult);

        let prev_trend = self.trend;
        let prev_lower = self.lower_band;
        let prev_upper = self.upper_band;
        let lower_mult = if prev_trend == 1 {
            active_mult
        } else {
            passive_mult
        };
        let upper_mult = if prev_trend == 1 {
            passive_mult
        } else {
            active_mult
        };
        let source = settings.source.value(bar);
        let lower_raw = source - lower_mult * atr_value;
        let upper_raw = source + upper_mult * atr_value;
        let lower = match prev_lower {
            None => lower_raw,
            Some(prev) if prev_close > prev => lower_raw.max(prev),
            Some(_) => lower_raw,
        };
        let upper = match prev_upper {
            None => upper_raw,
            Some(prev) if prev_close < prev => upper_raw.min(prev),
            Some(_) => upper_raw,
        };
        self.lower_band = Some(lower);
        self.upper_band = Some(upper);

        let price_flip_up = prev_trend == -1 && prev_upper.is_some_and(|band| bar.close > band);
        let price_flip_down = prev_trend == 1 && prev_lower.is_some_and(|band| bar.close < band);
        let prev_tqi = self.last_tqi.unwrap_or(0.5);
        let trend_age = self.processed_bars.saturating_sub(self.trend_start_index);
        let char_base = settings.use_char_flip
            && settings.use_tqi
            && prev_tqi > settings.char_flip_high
            && tqi < settings.char_flip_low
            && trend_age >= settings.char_flip_min_age;
        let char_flip_down = char_base && prev_trend == 1 && bar.close < source;
        let char_flip_up = char_base && prev_trend == -1 && bar.close > source;

        let final_flip_up = price_flip_up || char_flip_up;
        let final_flip_down = price_flip_down || char_flip_down;
        self.trend = if final_flip_up {
            1
        } else if final_flip_down {
            -1
        } else {
            prev_trend
        };
        if self.trend != prev_trend {
            self.trend_start_index = self.processed_bars;
        }
        let entry_side = if may_emit && self.processed_bars + 1 >= settings.min_warmup_bars() {
            if self.trend == 1 && prev_trend == -1 {
                Some(SignalSide::Long)
            } else if self.trend == -1 && prev_trend == 1 {
                Some(SignalSide::Short)
            } else {
                None
            }
        } else {
            None
        };
        let flip_reason = if char_flip_up || char_flip_down {
            "character_flip"
        } else if price_flip_up || price_flip_down {
            "price_flip"
        } else {
            "none"
        };
        let (tp_scale, tp1_r, tp2_r, tp3_r) = dynamic_tp(
            settings,
            tqi,
            vol_ratio,
            settings.tp1_r,
            settings.tp2_r,
            settings.tp3_r,
        );
        let regime_cell = cell_idx(er_bin(er), vol_bin(vol_ratio));

        let point = IndicatorPoint {
            entry_side,
            flip_reason,
            prev_trend,
            trend: self.trend,
            supertrend_line: if self.trend == 1 { lower } else { upper },
            atr: atr_value,
            er,
            vol_ratio,
            tqi,
            tqi_er,
            tqi_vol,
            tqi_struct,
            tqi_momentum,
            tp_scale,
            tp1_r,
            tp2_r,
            tp3_r,
            processed_bars: self.processed_bars + 1,
            pre_flip_trend_age: trend_age,
            last_pivot_high: self.last_pivot_high,
            last_pivot_low: self.last_pivot_low,
            regime_cell,
        };

        let pivots = self.pivot_tracker.push(bar);
        self.pending_pivot_high = pivots.high;
        self.pending_pivot_low = pivots.low;
        self.last_close = Some(bar.close);
        self.last_tqi = Some(tqi);
        self.processed_bars += 1;
        self.last_processed_closed_end = Some(bar.end_at);

        Ok(Some(point))
    }

    fn apply_pending_pivots(&mut self) {
        if let Some(pivot) = self.pending_pivot_high.take() {
            self.last_pivot_high = Some(pivot);
        }
        if let Some(pivot) = self.pending_pivot_low.take() {
            self.last_pivot_low = Some(pivot);
        }
    }

    fn tqi_volatility(&mut self, bar: &Bar, vol_ratio: f64) -> f64 {
        let stats = self.volume_stats.push(bar.volume);
        if bar.volume > 0.0 && self.volume_stats.is_full() {
            let avg = stats.mean;
            let stdev = stats.stdev;
            let z = safe_div(bar.volume - avg, stdev, 0.0);
            map_clamp(z, -1.0, 2.0, 0.0, 1.0)
        } else {
            map_clamp(vol_ratio, 0.6, 1.8, 0.0, 1.0)
        }
    }
}

#[derive(Debug)]
struct WilderRma {
    period: usize,
    seed: VecDeque<f64>,
    seed_sum: f64,
    value: Option<f64>,
}

impl WilderRma {
    fn new(period: usize) -> Self {
        Self {
            period,
            seed: VecDeque::with_capacity(period),
            seed_sum: 0.0,
            value: None,
        }
    }

    fn next(&mut self, input: f64) -> Option<f64> {
        if let Some(prev) = self.value {
            let next = (prev * (self.period as f64 - 1.0) + input) / self.period as f64;
            self.value = Some(next);
            return self.value;
        }
        self.seed.push_back(input);
        self.seed_sum += input;
        if self.seed.len() < self.period {
            return None;
        }
        let seeded = self.seed_sum / self.period as f64;
        self.value = Some(seeded);
        self.value
    }
}

#[derive(Clone, Copy, Debug)]
struct RollingStatsPoint {
    mean: f64,
    stdev: f64,
}

#[derive(Debug)]
struct RollingMean {
    capacity: usize,
    values: VecDeque<f64>,
    sum: f64,
}

impl RollingMean {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            values: VecDeque::with_capacity(capacity),
            sum: 0.0,
        }
    }

    fn push(&mut self, value: f64) -> f64 {
        self.values.push_back(value);
        self.sum += value;
        if self.values.len() > self.capacity {
            if let Some(removed) = self.values.pop_front() {
                self.sum -= removed;
            }
        }
        safe_div(self.sum, self.values.len() as f64, 0.0)
    }

    fn is_full(&self) -> bool {
        self.values.len() >= self.capacity
    }
}

#[derive(Debug)]
struct RollingStats {
    capacity: usize,
    values: VecDeque<f64>,
    sum: f64,
    sum_sq: f64,
}

impl RollingStats {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            values: VecDeque::with_capacity(capacity),
            sum: 0.0,
            sum_sq: 0.0,
        }
    }

    fn push(&mut self, value: f64) -> RollingStatsPoint {
        self.values.push_back(value);
        self.sum += value;
        self.sum_sq += value * value;
        if self.values.len() > self.capacity {
            if let Some(removed) = self.values.pop_front() {
                self.sum -= removed;
                self.sum_sq -= removed * removed;
            }
        }

        let count = self.values.len() as f64;
        let mean = safe_div(self.sum, count, 0.0);
        let variance = safe_div(self.sum_sq, count, 0.0) - mean * mean;
        RollingStatsPoint {
            mean,
            stdev: variance.max(0.0).sqrt(),
        }
    }

    fn is_full(&self) -> bool {
        self.values.len() >= self.capacity
    }
}

#[derive(Debug)]
struct RollingEfficiency {
    len: usize,
    closes: VecDeque<f64>,
    diffs: VecDeque<f64>,
    volatility_sum: f64,
}

impl RollingEfficiency {
    fn new(len: usize) -> Self {
        Self {
            len,
            closes: VecDeque::with_capacity(len + 1),
            diffs: VecDeque::with_capacity(len),
            volatility_sum: 0.0,
        }
    }

    fn push(&mut self, close: f64) -> f64 {
        if let Some(prev) = self.closes.back().copied() {
            let diff = (close - prev).abs();
            self.diffs.push_back(diff);
            self.volatility_sum += diff;
            if self.diffs.len() > self.len {
                if let Some(removed) = self.diffs.pop_front() {
                    self.volatility_sum -= removed;
                }
            }
        }

        self.closes.push_back(close);
        if self.closes.len() > self.len + 1 {
            self.closes.pop_front();
        }

        if self.closes.len() <= self.len || self.diffs.len() < self.len {
            return 0.0;
        }
        let old_close = self.closes.front().copied().unwrap_or(close);
        safe_div((close - old_close).abs(), self.volatility_sum, 0.0)
    }
}

#[derive(Debug)]
struct RollingMomentum {
    len: usize,
    closes: VecDeque<f64>,
    diffs: VecDeque<f64>,
    positive: usize,
    negative: usize,
}

impl RollingMomentum {
    fn new(len: usize) -> Self {
        Self {
            len,
            closes: VecDeque::with_capacity(len + 1),
            diffs: VecDeque::with_capacity(len),
            positive: 0,
            negative: 0,
        }
    }

    fn push(&mut self, close: f64) -> f64 {
        if let Some(prev) = self.closes.back().copied() {
            let diff = close - prev;
            self.add_diff(diff);
        }

        self.closes.push_back(close);
        if self.closes.len() > self.len + 1 {
            self.closes.pop_front();
        }

        if self.closes.len() <= self.len || self.diffs.len() < self.len {
            return 0.0;
        }
        let old_close = self.closes.front().copied().unwrap_or(close);
        let window_change = close - old_close;
        if window_change > 0.0 {
            self.positive as f64 / self.len as f64
        } else if window_change < 0.0 {
            self.negative as f64 / self.len as f64
        } else {
            0.0
        }
    }

    fn add_diff(&mut self, diff: f64) {
        self.diffs.push_back(diff);
        if diff > 0.0 {
            self.positive += 1;
        } else if diff < 0.0 {
            self.negative += 1;
        }

        if self.diffs.len() > self.len {
            if let Some(removed) = self.diffs.pop_front() {
                if removed > 0.0 {
                    self.positive -= 1;
                } else if removed < 0.0 {
                    self.negative -= 1;
                }
            }
        }
    }
}

#[derive(Debug)]
struct RollingExtrema {
    capacity: usize,
    next_index: usize,
    max_values: MonotonicQueue,
    min_values: MonotonicQueue,
}

impl RollingExtrema {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            next_index: 0,
            max_values: MonotonicQueue::max(),
            min_values: MonotonicQueue::min(),
        }
    }

    fn push(&mut self, high: f64, low: f64) -> (f64, f64) {
        let index = self.next_index;
        self.next_index += 1;
        self.max_values.push(index, high);
        self.min_values.push(index, low);
        let min_index = index.saturating_add(1).saturating_sub(self.capacity);
        self.max_values.expire_before(min_index);
        self.min_values.expire_before(min_index);
        (
            self.max_values.front_value().unwrap_or(high),
            self.min_values.front_value().unwrap_or(low),
        )
    }
}

#[derive(Debug)]
struct PivotUpdate {
    high: Option<f64>,
    low: Option<f64>,
}

#[derive(Debug)]
struct PivotTracker {
    pivot_len: usize,
    needed: usize,
    next_seq: usize,
    window: VecDeque<PivotBar>,
    left_high: MonotonicQueue,
    left_low: MonotonicQueue,
    right_high: MonotonicQueue,
    right_low: MonotonicQueue,
}

impl PivotTracker {
    fn new(pivot_len: usize) -> Self {
        let needed = pivot_len * 2 + 1;
        Self {
            pivot_len,
            needed,
            next_seq: 0,
            window: VecDeque::with_capacity(needed),
            left_high: MonotonicQueue::max(),
            left_low: MonotonicQueue::min(),
            right_high: MonotonicQueue::max(),
            right_low: MonotonicQueue::min(),
        }
    }

    fn push(&mut self, bar: &Bar) -> PivotUpdate {
        let seq = self.next_seq;
        self.next_seq += 1;

        self.window.push_back(PivotBar {
            seq,
            high: bar.high,
            low: bar.low,
        });
        if self.window.len() > self.needed {
            self.window.pop_front();
        }

        self.right_high.push(seq, bar.high);
        self.right_low.push(seq, bar.low);

        if seq > self.pivot_len {
            let left_seq = seq - self.pivot_len - 1;
            if let Some(left_bar) = self.window_value(left_seq) {
                self.left_high.push(left_seq, left_bar.high);
                self.left_low.push(left_seq, left_bar.low);
            }
        }

        if seq < self.needed - 1 {
            return PivotUpdate {
                high: None,
                low: None,
            };
        }

        let left_min_seq = seq - (self.pivot_len * 2);
        let right_min_seq = seq - self.pivot_len + 1;
        self.left_high.expire_before(left_min_seq);
        self.left_low.expire_before(left_min_seq);
        self.right_high.expire_before(right_min_seq);
        self.right_low.expire_before(right_min_seq);

        let center_seq = seq - self.pivot_len;
        let Some(center) = self.window_value(center_seq) else {
            return PivotUpdate {
                high: None,
                low: None,
            };
        };

        let left_high = self.left_high.front_value().unwrap_or(f64::NEG_INFINITY);
        let right_high = self.right_high.front_value().unwrap_or(f64::NEG_INFINITY);
        let left_low = self.left_low.front_value().unwrap_or(f64::INFINITY);
        let right_low = self.right_low.front_value().unwrap_or(f64::INFINITY);

        PivotUpdate {
            high: (center.high > left_high && center.high > right_high).then_some(center.high),
            low: (center.low < left_low && center.low < right_low).then_some(center.low),
        }
    }

    fn window_value(&self, seq: usize) -> Option<PivotBar> {
        let front_seq = self.window.front()?.seq;
        let offset = seq.checked_sub(front_seq)?;
        self.window
            .get(offset)
            .copied()
            .filter(|bar| bar.seq == seq)
    }
}

#[derive(Clone, Copy, Debug)]
struct PivotBar {
    seq: usize,
    high: f64,
    low: f64,
}

#[derive(Clone, Copy, Debug)]
enum MonotonicMode {
    Max,
    Min,
}

#[derive(Debug)]
struct MonotonicQueue {
    mode: MonotonicMode,
    values: VecDeque<(usize, f64)>,
}

impl MonotonicQueue {
    fn max() -> Self {
        Self {
            mode: MonotonicMode::Max,
            values: VecDeque::new(),
        }
    }

    fn min() -> Self {
        Self {
            mode: MonotonicMode::Min,
            values: VecDeque::new(),
        }
    }

    fn push(&mut self, index: usize, value: f64) {
        while self
            .values
            .back()
            .is_some_and(|(_, back)| self.should_remove(*back, value))
        {
            self.values.pop_back();
        }
        self.values.push_back((index, value));
    }

    fn expire_before(&mut self, min_index: usize) {
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

    fn should_remove(&self, existing: f64, incoming: f64) -> bool {
        match self.mode {
            MonotonicMode::Max => existing <= incoming,
            MonotonicMode::Min => existing >= incoming,
        }
    }
}

fn dynamic_tp(
    settings: &AdaptiveSupertrendSettings,
    tqi: f64,
    vol_ratio: f64,
    tp1: f64,
    tp2: f64,
    tp3: f64,
) -> (f64, f64, f64, f64) {
    if settings.tp_mode == TpMode::Fixed {
        return (1.0, tp1, tp2, tp3);
    }
    let tqi_comp = tqi.clamp(0.0, 1.0);
    let vol_comp = map_clamp(vol_ratio, 0.5, 2.0, 0.0, 1.0).clamp(0.0, 1.0);
    let weight_sum = settings.dyn_tp_tqi_weight + settings.dyn_tp_vol_weight;
    let raw = safe_div(
        tqi_comp * settings.dyn_tp_tqi_weight + vol_comp * settings.dyn_tp_vol_weight,
        if weight_sum > 0.0 { weight_sum } else { 1.0 },
        0.5,
    );
    let scale =
        settings.dyn_tp_min_scale + raw * (settings.dyn_tp_max_scale - settings.dyn_tp_min_scale);
    let tp1_floor = settings.dyn_tp_floor_r1;
    let tp2_floor = settings.dyn_tp_floor_r1 * safe_div(tp2, tp1.max(0.01), 1.0);
    let tp3_floor = settings.dyn_tp_floor_r1 * safe_div(tp3, tp1.max(0.01), 1.0);
    let eff1 = (tp1 * scale).clamp(tp1_floor, settings.dyn_tp_ceil_r3);
    let eff2 = (tp2 * scale).clamp(tp2_floor, settings.dyn_tp_ceil_r3);
    let eff3 = (tp3 * scale).clamp(tp3_floor, settings.dyn_tp_ceil_r3);
    let (s1, s2, s3) = sort_three(eff1, eff2, eff3);
    (scale, s1, s2, s3)
}

fn partial_book_remaining_fraction(hit_tp1: bool, hit_tp2: bool) -> f64 {
    let mut remaining = 1.0;
    if hit_tp1 {
        remaining -= PARTIAL_EXIT_FRACTION;
    }
    if hit_tp2 {
        remaining -= PARTIAL_EXIT_FRACTION;
    }
    remaining.max(0.0)
}

fn target_price(side: SignalSide, entry: f64, risk: f64, r: f64) -> f64 {
    match side {
        SignalSide::Long => entry + risk * r,
        SignalSide::Short => entry - risk * r,
    }
}

fn price_to_r(side: SignalSide, entry: f64, risk: f64, price: f64) -> f64 {
    match side {
        SignalSide::Long => safe_div(price - entry, risk, 0.0),
        SignalSide::Short => safe_div(entry - price, risk, 0.0),
    }
}

fn favorable_stop(side: SignalSide, current: f64, candidate: f64) -> f64 {
    match side {
        SignalSide::Long => current.max(candidate),
        SignalSide::Short => current.min(candidate),
    }
}

fn synthetic_exit_price(side: SignalSide, entry: f64, risk: f64, realized_r: f64) -> f64 {
    match side {
        SignalSide::Long => entry + risk * realized_r,
        SignalSide::Short => entry - risk * realized_r,
    }
}

fn reached(side: SignalSide, bar: &Bar, price: f64) -> bool {
    match side {
        SignalSide::Long => bar.high >= price,
        SignalSide::Short => bar.low <= price,
    }
}

fn stopped(side: SignalSide, bar: &Bar, price: f64) -> bool {
    match side {
        SignalSide::Long => bar.low <= price,
        SignalSide::Short => bar.high >= price,
    }
}

fn price_reached(side: SignalSide, current_price: f64, target_price: f64) -> bool {
    match side {
        SignalSide::Long => current_price >= target_price,
        SignalSide::Short => current_price <= target_price,
    }
}

fn price_stopped(side: SignalSide, current_price: f64, stop_price: f64) -> bool {
    match side {
        SignalSide::Long => current_price <= stop_price,
        SignalSide::Short => current_price >= stop_price,
    }
}

fn smooth_value(previous: Option<f64>, raw: f64, enabled: bool) -> f64 {
    match (previous, enabled) {
        (Some(prev), true) => prev * (1.0 - MULT_SMOOTH_ALPHA) + raw * MULT_SMOOTH_ALPHA,
        _ => raw,
    }
}

fn er_bin(er: f64) -> usize {
    if er < ER_LOW_THRESH {
        0
    } else if er < ER_HIGH_THRESH {
        1
    } else {
        2
    }
}

fn vol_bin(vol_ratio: f64) -> usize {
    if vol_ratio < VOL_LOW_THRESH {
        0
    } else if vol_ratio < VOL_HIGH_THRESH {
        1
    } else {
        2
    }
}

fn cell_idx(er_bin: usize, vol_bin: usize) -> usize {
    er_bin * 3 + vol_bin
}

fn map_clamp(value: f64, in_low: f64, in_high: f64, out_low: f64, out_high: f64) -> f64 {
    let t = safe_div(value - in_low, in_high - in_low, 0.0).clamp(0.0, 1.0);
    out_low + t * (out_high - out_low)
}

fn safe_div(num: f64, den: f64, fallback: f64) -> f64 {
    if num.is_finite() && den.is_finite() && den != 0.0 {
        num / den
    } else {
        fallback
    }
}

fn sort_three(a: f64, b: f64, c: f64) -> (f64, f64, f64) {
    let low = a.min(b).min(c);
    let high = a.max(b).max(c);
    let mid = a + b + c - low - high;
    (low, mid, high)
}

fn resolve_preset(input: &str, timeframe: Timeframe) -> String {
    if !input.eq_ignore_ascii_case("auto") {
        return input.to_string();
    }
    let minutes = timeframe_minutes(timeframe);
    if minutes <= 5 {
        "Scalping".to_string()
    } else if minutes <= 240 {
        "Default".to_string()
    } else {
        "Swing".to_string()
    }
}

fn preset_values(
    preset: &str,
    atr_len: usize,
    base_mult: f64,
    er_len: usize,
    sl_mult: f64,
) -> (usize, f64, usize, f64) {
    match preset.trim().to_ascii_lowercase().as_str() {
        "scalping" => (10, 1.5, 14, 1.0),
        "default" => (14, 2.0, 20, 1.5),
        "swing" => (21, 2.5, 30, 2.0),
        "crypto 24/7" | "crypto" => (14, 2.8, 20, 2.5),
        _ => (atr_len, base_mult, er_len, sl_mult),
    }
}

fn parse_side(value: &str) -> Result<SignalSide, StrategyError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "long" | "buy" => Ok(SignalSide::Long),
        "short" | "sell" => Ok(SignalSide::Short),
        other => Err(StrategyError::Parse(format!(
            "unsupported adaptive_supertrend side {other}; expected long or short"
        ))),
    }
}

fn parse_source(value: &str) -> Result<Source, StrategyError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "open" => Ok(Source::Open),
        "high" => Ok(Source::High),
        "low" => Ok(Source::Low),
        "close" => Ok(Source::Close),
        "hl2" => Ok(Source::Hl2),
        "hlc3" => Ok(Source::Hlc3),
        "hlcc4" => Ok(Source::Hlcc4),
        "ohlc4" => Ok(Source::Ohlc4),
        other => Err(StrategyError::Parse(format!(
            "unsupported adaptive_supertrend source {other}"
        ))),
    }
}

fn parse_tp_mode(value: &str) -> Result<TpMode, StrategyError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "fixed" => Ok(TpMode::Fixed),
        "dynamic" => Ok(TpMode::Dynamic),
        other => Err(StrategyError::Parse(format!(
            "unsupported adaptive_supertrend tp_mode {other}"
        ))),
    }
}

fn parse_exit_mode(value: &str) -> Result<ExitMode, StrategyError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "partial_book" | "partial" => Ok(ExitMode::PartialBook),
        "trail_then_exit" => Ok(ExitMode::TrailThenExit),
        "trail_supertrend" | "supertrend_trail" => Ok(ExitMode::TrailSupertrend),
        "trail_chandelier" | "chandelier_trail" => Ok(ExitMode::TrailChandelier),
        other => Err(StrategyError::Parse(format!(
            "unsupported adaptive_supertrend exit_mode {other}"
        ))),
    }
}

fn exit_mode_label(mode: ExitMode) -> &'static str {
    match mode {
        ExitMode::PartialBook => "partial_book",
        ExitMode::TrailThenExit => "trail_then_exit",
        ExitMode::TrailSupertrend => "trail_supertrend",
        ExitMode::TrailChandelier => "trail_chandelier",
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
            "unsupported adaptive_supertrend timeframe {other}"
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

fn timeframe_minutes(timeframe: Timeframe) -> usize {
    match timeframe {
        Timeframe::OneMinute => 1,
        Timeframe::ThreeMinute => 3,
        Timeframe::FiveMinute => 5,
        Timeframe::FifteenMinute => 15,
        Timeframe::ThirtyMinute => 30,
        Timeframe::SeventyFiveMinute => 75,
        Timeframe::OneHour => 60,
        Timeframe::FourHour => 240,
        Timeframe::OneDay => 1_440,
    }
}

fn timeframe_millis(timeframe: Timeframe) -> u64 {
    timeframe_minutes(timeframe) as u64 * 60_000
}

fn side_label(side: SignalSide) -> &'static str {
    match side {
        SignalSide::Long => "long",
        SignalSide::Short => "short",
    }
}

fn replay_warmup_bars(
    state: &mut AdaptiveSupertrendState,
    ctx: &StrategyContext,
    ssu: &SsuConfig,
    instrument: &str,
    settings: &AdaptiveSupertrendSettings,
    before_end: Option<u64>,
) -> Result<(), StrategyError> {
    if !state.is_empty() {
        return Ok(());
    }

    let log_warmup = diagnostics::warmup_replay_enabled();
    for bar in ctx
        .timeframes
        .recent_bars(instrument, settings.timeframe, settings.state_capacity())
        .into_iter()
        .filter(|bar| before_end.is_none_or(|end| bar.end_at < end))
    {
        let point = state.on_closed_bar(&bar, settings, log_warmup)?;
        if log_warmup {
            if let Some(point) = point {
                let (would_emit, reasons) = diagnostic_entry_decision(settings, &point);
                log_closed_bar_decision(
                    "warmup", ssu, instrument, bar.end_at, settings, &bar, &point, 0, would_emit,
                    &reasons,
                );
            }
        }
    }

    Ok(())
}

fn optional_side_label(side: Option<SignalSide>) -> &'static str {
    match side {
        Some(SignalSide::Long) => "long",
        Some(SignalSide::Short) => "short",
        None => "none",
    }
}

fn trend_label(trend: i32) -> &'static str {
    if trend == 1 { "bullish" } else { "bearish" }
}

fn no_entry_reasons(settings: &AdaptiveSupertrendSettings, point: &IndicatorPoint) -> Vec<String> {
    let mut reasons = Vec::new();
    if point.processed_bars < settings.min_warmup_bars() {
        reasons.push(format!(
            "warmup:{}/{}",
            point.processed_bars,
            settings.min_warmup_bars()
        ));
    }
    if point.flip_reason == "none" {
        reasons.push("no_trend_flip".to_string());
    } else if point.processed_bars < settings.min_warmup_bars() {
        reasons.push(format!("flip_ignored_during_warmup:{}", point.flip_reason));
    } else {
        reasons.push(format!("flip_without_entry_side:{}", point.flip_reason));
    }
    reasons
}

fn entry_block_reasons(
    settings: &AdaptiveSupertrendSettings,
    point: &IndicatorPoint,
) -> Vec<String> {
    let mut reasons = Vec::new();
    if point.atr <= 0.0 || !point.atr.is_finite() {
        reasons.push(format!("invalid_atr:{:.6}", point.atr));
    }
    if point.tqi < settings.min_entry_tqi {
        reasons.push(format!(
            "tqi:{:.4}<min:{:.4}",
            point.tqi, settings.min_entry_tqi
        ));
    }
    if point.er < settings.min_entry_er {
        reasons.push(format!(
            "er:{:.4}<min:{:.4}",
            point.er, settings.min_entry_er
        ));
    }
    if point.tp_scale < settings.min_entry_tp_scale {
        reasons.push(format!(
            "tp_scale:{:.4}<min:{:.4}",
            point.tp_scale, settings.min_entry_tp_scale
        ));
    }
    if point.pre_flip_trend_age < settings.min_entry_trend_age {
        reasons.push(format!(
            "trend_age:{}<min:{}",
            point.pre_flip_trend_age, settings.min_entry_trend_age
        ));
    }
    if point.vol_ratio < settings.min_entry_vol_ratio {
        reasons.push(format!(
            "vol_ratio:{:.4}<min:{:.4}",
            point.vol_ratio, settings.min_entry_vol_ratio
        ));
    }
    if point.vol_ratio > settings.max_entry_vol_ratio {
        reasons.push(format!(
            "vol_ratio:{:.4}>max:{:.4}",
            point.vol_ratio, settings.max_entry_vol_ratio
        ));
    }
    reasons
}

fn diagnostic_entry_decision(
    settings: &AdaptiveSupertrendSettings,
    point: &IndicatorPoint,
) -> (bool, Vec<String>) {
    let Some(side) = point.entry_side else {
        return (false, no_entry_reasons(settings, point));
    };
    if !settings.enabled_sides.contains(&side) {
        return (false, vec![format!("side_disabled:{}", side_label(side))]);
    }
    let reasons = entry_block_reasons(settings, point);
    (reasons.is_empty(), reasons)
}

fn log_closed_bar_decision(
    phase: &str,
    ssu: &SsuConfig,
    instrument: &str,
    at: u64,
    settings: &AdaptiveSupertrendSettings,
    closed_bar: &Bar,
    point: &IndicatorPoint,
    exit_signal_count: usize,
    entry_signal_emitted: bool,
    entry_reasons: &[String],
) {
    let decision = if phase == "warmup" && entry_signal_emitted {
        "would_entry_signal"
    } else if phase == "warmup" {
        "would_no_signal"
    } else if entry_signal_emitted && exit_signal_count > 0 {
        "entry_and_exit_signal"
    } else if entry_signal_emitted {
        "entry_signal"
    } else if exit_signal_count > 0 {
        "exit_signal_no_new_entry"
    } else {
        "no_signal"
    };
    let reason_text = if entry_signal_emitted {
        if phase == "warmup" {
            "entry_conditions_met".to_string()
        } else {
            "entry_emitted".to_string()
        }
    } else if entry_reasons.is_empty() {
        "none".to_string()
    } else {
        entry_reasons.join(",")
    };

    println!(
        "ADAPTIVE_SUPERTREND_DECISION | phase={} | ssu={} | instrument={} | tf={} | bar_end={} | tick_at={} | ohlc={:.4}/{:.4}/{:.4}/{:.4} | decision={} | entry_side={} | exit_signals={} | reason={} | trend={}->{} | flip={} | warmup={}/{} | tqi={:.4} er={:.4} vol_ratio={:.4} tp_scale={:.4} trend_age={} | tqi_parts=er:{:.4},vol:{:.4},struct:{:.4},mom:{:.4} | filters=min_tqi:{:.4},min_er:{:.4},min_tp_scale:{:.4},min_trend_age:{},vol_range:{:.4}-{:.4} | atr={:.4} st_line={:.4} r={:.2}/{:.2}/{:.2} exit_mode={}",
        phase,
        ssu.ssu_id,
        instrument,
        timeframe_label(settings.timeframe),
        closed_bar.end_at,
        at,
        closed_bar.open,
        closed_bar.high,
        closed_bar.low,
        closed_bar.close,
        decision,
        optional_side_label(point.entry_side),
        exit_signal_count,
        reason_text,
        trend_label(point.prev_trend),
        trend_label(point.trend),
        point.flip_reason,
        point.processed_bars,
        settings.min_warmup_bars(),
        point.tqi,
        point.er,
        point.vol_ratio,
        point.tp_scale,
        point.pre_flip_trend_age,
        point.tqi_er,
        point.tqi_vol,
        point.tqi_struct,
        point.tqi_momentum,
        settings.min_entry_tqi,
        settings.min_entry_er,
        settings.min_entry_tp_scale,
        settings.min_entry_trend_age,
        settings.min_entry_vol_ratio,
        settings.max_entry_vol_ratio,
        point.atr,
        point.supertrend_line,
        point.tp1_r,
        point.tp2_r,
        point.tp3_r,
        exit_mode_label(settings.exit_mode),
    );
}

fn exit_signal_label(side: SignalSide) -> &'static str {
    match side {
        SignalSide::Long => "EXIT_LONG",
        SignalSide::Short => "EXIT_SHORT",
    }
}

fn partial_exit_signal_label(side: SignalSide) -> &'static str {
    match side {
        SignalSide::Long => "EXIT_LONG_PARTIAL",
        SignalSide::Short => "EXIT_SHORT_PARTIAL",
    }
}

fn require<T>(value: Option<T>, field: &str, ssu_id: i64) -> Result<T, StrategyError> {
    value.ok_or_else(|| {
        StrategyError::Config(format!(
            "SSU {ssu_id} adaptive_supertrend params_json missing {field}"
        ))
    })
}

fn required_f64(metadata: &serde_json::Value, field: &str) -> Result<f64, StrategyError> {
    metadata
        .get(field)
        .and_then(serde_json::Value::as_f64)
        .ok_or_else(|| {
            StrategyError::Parse(format!("adaptive_supertrend metadata missing {field}"))
        })
}

fn optional_f64(metadata: &serde_json::Value, field: &str) -> Option<f64> {
    metadata.get(field).and_then(serde_json::Value::as_f64)
}

fn required_u64(metadata: &serde_json::Value, field: &str) -> Result<u64, StrategyError> {
    metadata
        .get(field)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| {
            StrategyError::Parse(format!("adaptive_supertrend metadata missing {field}"))
        })
}

fn required_bool(metadata: &serde_json::Value, field: &str) -> Result<bool, StrategyError> {
    metadata
        .get(field)
        .and_then(serde_json::Value::as_bool)
        .ok_or_else(|| {
            StrategyError::Parse(format!("adaptive_supertrend metadata missing {field}"))
        })
}

fn required_string(metadata: &serde_json::Value, field: &str) -> Result<String, StrategyError> {
    metadata
        .get(field)
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| {
            StrategyError::Parse(format!("adaptive_supertrend metadata missing {field}"))
        })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::strategy::{
        InMemoryPriceStore, PriceStore, SharedTimeframeEngine, SqliteStrategyPositionBook,
        SqliteStrategyTradeContextStore, StrategyPositionBook, StrategySignalType,
        StrategyTradeContextStore, TimeframeEngine,
    };

    fn test_ssu(params_json: serde_json::Value) -> SsuConfig {
        SsuConfig {
            ssu_id: 900,
            strategy_key: "adaptive_supertrend".to_string(),
            enabled: true,
            trade_gap_secs: 0,
            max_overlap: 1,
            max_positions_per_day: 0,
            required_timeframes: vec![Timeframe::FiveMinute],
            indicator_specs: Vec::new(),
            params_json: params_json.to_string(),
        }
    }

    fn bar(index: u64, close: f64) -> Bar {
        Bar {
            instrument: "BTCUSD".to_string(),
            timeframe: Timeframe::FiveMinute,
            start_at: index * 300_000,
            end_at: (index + 1) * 300_000,
            open: close,
            high: close + 10.0,
            low: close - 10.0,
            close,
            volume: 100.0 + index as f64,
            is_closed: true,
        }
    }

    fn temp_sqlite(name: &str) -> String {
        format!(
            "{}/{}-{}.sqlite",
            std::env::temp_dir().display(),
            name,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        )
    }

    #[test]
    fn settings_resolve_auto_preset() {
        let settings = AdaptiveSupertrendSettings::from_ssu(&test_ssu(serde_json::json!({
            "timeframe": "5m"
        })))
        .expect("settings");

        assert_eq!(settings.effective_atr_len, 10);
        assert_eq!(settings.effective_base_mult, 1.5);
        assert_eq!(settings.effective_er_len, 14);
    }

    #[test]
    fn state_emits_flip_after_warmup() {
        let settings = AdaptiveSupertrendSettings::from_ssu(&test_ssu(serde_json::json!({
            "timeframe": "5m",
            "preset": "Custom",
            "atr_length": 5,
            "efficiency_window": 5,
            "base_band_width_atr": 1.0,
            "lookback_bars": 80,
            "character_flip": false
        })))
        .expect("settings");
        let mut state = AdaptiveSupertrendState::new(&settings);
        let mut latest = None;
        for index in 0..80 {
            let close = if index < 55 {
                1000.0 + index as f64
            } else {
                1100.0 - (index - 54) as f64 * 20.0
            };
            latest = state
                .on_closed_bar(&bar(index, close), &settings, true)
                .expect("bar");
        }

        assert!(latest.is_some());
        assert_eq!(state.trend, -1);
    }

    #[test]
    fn tick_update_emits_partial_exit_without_closing_position() {
        let strategy = AdaptiveSupertrendStrategy::default();
        let ssu = test_ssu(serde_json::json!({
            "timeframe": "5m",
            "preset": "Custom",
            "tp_mode": "Fixed"
        }));
        let prices = Arc::new(InMemoryPriceStore::new());
        prices.put_price("BTCUSD", 110.0, 2_000);
        let timeframes = Arc::new(SharedTimeframeEngine::new(64));
        let sqlite_path = temp_sqlite("adaptive-supertrend-tick");
        let positions =
            Arc::new(SqliteStrategyPositionBook::new(sqlite_path.clone()).expect("positions"));
        let trade_contexts =
            Arc::new(SqliteStrategyTradeContextStore::new(sqlite_path).expect("contexts"));
        let entry = StrategySignal::single_leg_entry(
            ssu.ssu_id,
            "adaptive_supertrend",
            "BTCUSD",
            SignalSide::Long,
            100.0,
            "entry".to_string(),
            1_000,
        );
        let position = positions.open_position(&entry, &ssu).expect("open");
        let metadata = serde_json::json!({
            "campaign_id": entry.campaign_id,
            "entry_price": 100.0,
            "entry_bar_end_at": 1_000_u64,
            "stop_price": 90.0,
            "active_stop_price": 90.0,
            "risk": 10.0,
            "tp1_price": 110.0,
            "tp2_price": 120.0,
            "tp3_price": 130.0,
            "tp1_r": 1.0,
            "tp2_r": 2.0,
            "tp3_r": 3.0,
            "hit_tp1": false,
            "hit_tp2": false,
            "hit_tp3": false
        });
        trade_contexts
            .save_context(
                &position.position_id,
                ssu.ssu_id,
                "adaptive_supertrend",
                "BTCUSD",
                &metadata,
                1_000,
            )
            .expect("context");
        let ctx = StrategyContext {
            prices: prices as Arc<dyn PriceStore>,
            timeframes: timeframes as Arc<dyn TimeframeEngine>,
            strategy_positions: positions.clone() as Arc<dyn StrategyPositionBook>,
            trade_contexts: trade_contexts.clone() as Arc<dyn StrategyTradeContextStore>,
        };
        let mut ticks = std::collections::BTreeMap::new();
        ticks.insert(
            "BTCUSD".to_string(),
            crate::strategy::Tick {
                price: 110.0,
                volume: 0.0,
            },
        );
        let event = MarketEvent::Tick(TickSnapshot {
            event_ts: 2_000,
            ticks,
        });

        let signals = strategy.on_market_event(&ctx, &ssu, &event).expect("tick");

        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].signal_type, StrategySignalType::ExitLongPartial);
        assert_eq!(
            signals[0].metadata["evaluation_mode"].as_str(),
            Some("tick")
        );
        assert_eq!(
            positions.list_open_by_ssu(ssu.ssu_id).expect("open").len(),
            1
        );
        let updated = trade_contexts
            .load_context(&position.position_id)
            .expect("load")
            .expect("metadata");
        assert_eq!(updated["hit_tp1"].as_bool(), Some(true));
    }

    #[test]
    fn dynamic_tp_sorts_and_applies_floor() {
        let settings = AdaptiveSupertrendSettings::from_ssu(&test_ssu(serde_json::json!({
            "timeframe": "5m",
            "tp_mode": "Dynamic",
            "tp1_r": 1.0,
            "tp2_r": 2.0,
            "tp3_r": 3.0
        })))
        .expect("settings");
        let (_, tp1, tp2, tp3) = dynamic_tp(&settings, 0.0, 0.5, 1.0, 2.0, 3.0);

        assert!(tp1 >= 0.5);
        assert!(tp1 <= tp2 && tp2 <= tp3);
    }

    #[test]
    fn settings_parse_exit_modes() {
        let trail_then_exit = AdaptiveSupertrendSettings::from_ssu(&test_ssu(serde_json::json!({
            "timeframe": "5m",
            "exit_mode": "trail_then_exit"
        })))
        .expect("trail_then_exit settings");
        assert_eq!(trail_then_exit.exit_mode, ExitMode::TrailThenExit);

        let supertrend = AdaptiveSupertrendSettings::from_ssu(&test_ssu(serde_json::json!({
            "timeframe": "5m",
            "exit_mode": "trail_supertrend"
        })))
        .expect("trail_supertrend settings");
        assert_eq!(supertrend.exit_mode, ExitMode::TrailSupertrend);

        let chandelier = AdaptiveSupertrendSettings::from_ssu(&test_ssu(serde_json::json!({
            "timeframe": "5m",
            "exit_mode": "trail_chandelier",
            "chandelier_atr_mult": 2.5
        })))
        .expect("trail_chandelier settings");
        assert_eq!(chandelier.exit_mode, ExitMode::TrailChandelier);
        assert_close(chandelier.chandelier_atr_mult, 2.5);
    }

    #[test]
    fn favorable_stop_only_moves_toward_profit() {
        assert_close(favorable_stop(SignalSide::Long, 100.0, 110.0), 110.0);
        assert_close(favorable_stop(SignalSide::Long, 100.0, 90.0), 100.0);
        assert_close(favorable_stop(SignalSide::Short, 100.0, 90.0), 90.0);
        assert_close(favorable_stop(SignalSide::Short, 100.0, 110.0), 100.0);
    }

    #[test]
    fn entry_filters_default_to_neutral_and_gate_signals() {
        let default_settings = AdaptiveSupertrendSettings::from_ssu(&test_ssu(serde_json::json!({
            "timeframe": "5m"
        })))
        .expect("default settings");
        assert!(default_settings.entry_filters_pass(&point_with_filters(0.0, 0.0, 0.0, 0, 0.0)));

        let filtered_settings =
            AdaptiveSupertrendSettings::from_ssu(&test_ssu(serde_json::json!({
                "timeframe": "5m",
                "min_entry_tqi": 0.55,
                "min_entry_er": 0.25,
                "min_entry_tp_scale": 1.1,
                "min_entry_trend_age": 8,
                "min_entry_vol_ratio": 0.7,
                "max_entry_vol_ratio": 2.0
            })))
            .expect("filtered settings");

        assert!(
            !filtered_settings.entry_filters_pass(&point_with_filters(0.54, 0.25, 1.1, 8, 1.0))
        );
        assert!(
            !filtered_settings.entry_filters_pass(&point_with_filters(0.55, 0.24, 1.1, 8, 1.0))
        );
        assert!(
            !filtered_settings.entry_filters_pass(&point_with_filters(0.55, 0.25, 1.0, 8, 1.0))
        );
        assert!(
            !filtered_settings.entry_filters_pass(&point_with_filters(0.55, 0.25, 1.1, 7, 1.0))
        );
        assert!(
            !filtered_settings.entry_filters_pass(&point_with_filters(0.55, 0.25, 1.1, 8, 2.1))
        );
        assert!(filtered_settings.entry_filters_pass(&point_with_filters(0.55, 0.25, 1.1, 8, 1.0)));
    }

    #[test]
    fn entry_filter_vol_ratio_range_must_be_ordered() {
        let error = AdaptiveSupertrendSettings::from_ssu(&test_ssu(serde_json::json!({
            "timeframe": "5m",
            "min_entry_vol_ratio": 2.0,
            "max_entry_vol_ratio": 1.0
        })))
        .expect_err("invalid vol ratio range should fail");

        assert!(
            error
                .to_string()
                .contains("min_entry_vol_ratio must be <= max_entry_vol_ratio")
        );
    }

    #[test]
    fn rolling_windows_match_bounded_formulas() {
        let closes = [
            100.0, 102.0, 101.0, 105.0, 104.0, 108.0, 111.0, 109.0, 112.0,
        ];
        let mut er = RollingEfficiency::new(4);
        let mut momentum = RollingMomentum::new(3);
        let mut extrema = RollingExtrema::new(4);

        let mut seen_closes = Vec::new();
        let mut highs = Vec::new();
        let mut lows = Vec::new();
        for (index, close) in closes.into_iter().enumerate() {
            let high = close + (index % 3) as f64;
            let low = close - (index % 4) as f64;
            seen_closes.push(close);
            highs.push(high);
            lows.push(low);

            assert_close(er.push(close), brute_er(&seen_closes, 4));
            assert_close(momentum.push(close), brute_momentum(&seen_closes, 3));

            let (rolling_high, rolling_low) = extrema.push(high, low);
            let start = highs.len().saturating_sub(4);
            let brute_high = highs[start..]
                .iter()
                .copied()
                .fold(f64::NEG_INFINITY, f64::max);
            let brute_low = lows[start..].iter().copied().fold(f64::INFINITY, f64::min);
            assert_close(rolling_high, brute_high);
            assert_close(rolling_low, brute_low);
        }
    }

    fn brute_er(closes: &[f64], len: usize) -> f64 {
        if closes.len() <= len {
            return 0.0;
        }
        let current = closes[closes.len() - 1];
        let old = closes[closes.len() - 1 - len];
        let start = closes.len() - len;
        let mut volatility = 0.0;
        for index in start..closes.len() {
            volatility += (closes[index] - closes[index - 1]).abs();
        }
        safe_div((current - old).abs(), volatility, 0.0)
    }

    fn point_with_filters(
        tqi: f64,
        er: f64,
        tp_scale: f64,
        pre_flip_trend_age: usize,
        vol_ratio: f64,
    ) -> IndicatorPoint {
        IndicatorPoint {
            entry_side: Some(SignalSide::Long),
            flip_reason: "price_flip",
            prev_trend: -1,
            trend: 1,
            supertrend_line: 100.0,
            atr: 1.0,
            er,
            vol_ratio,
            tqi,
            tqi_er: er,
            tqi_vol: 0.5,
            tqi_struct: 0.5,
            tqi_momentum: 0.5,
            tp_scale,
            tp1_r: 1.0,
            tp2_r: 2.0,
            tp3_r: 3.0,
            processed_bars: 100,
            pre_flip_trend_age,
            last_pivot_high: None,
            last_pivot_low: None,
            regime_cell: 0,
        }
    }

    fn brute_momentum(closes: &[f64], len: usize) -> f64 {
        if closes.len() <= len {
            return 0.0;
        }
        let current = closes[closes.len() - 1];
        let old = closes[closes.len() - 1 - len];
        let window_change = current - old;
        if window_change == 0.0 {
            return 0.0;
        }
        let start = closes.len() - len;
        let mut aligned = 0;
        for index in start..closes.len() {
            let diff = closes[index] - closes[index - 1];
            if (window_change > 0.0 && diff > 0.0) || (window_change < 0.0 && diff < 0.0) {
                aligned += 1;
            }
        }
        aligned as f64 / len as f64
    }

    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 1e-12,
            "actual={actual} expected={expected}"
        );
    }
}
