use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use chrono::{Datelike, FixedOffset};
use serde::Serialize;

use crate::strategy::{
    PositionStatus, SignalSide, SsuConfig, StrategyError, StrategyPosition, StrategyPositionBook,
    StrategySignal, StrategySignalType, StrategyTradeContextStore, TradeAction,
};

const IST_OFFSET_SECONDS: i32 = 5 * 60 * 60 + 30 * 60;
const MIN_REMAINING_QTY: f64 = 0.000000001;

#[derive(Clone, Debug)]
pub struct BacktestExecutionConfig {
    pub slippage_pct: f64,
    pub brokerage_pct: f64,
    pub entry_fee_pct: Option<f64>,
    pub exit_fee_pct: Option<f64>,
    pub fee_tax_pct: f64,
    pub fixed_fee_per_order: f64,
    pub funding_rate_pct: f64,
    pub funding_interval_hours: u64,
    pub funding_charge_mode: FundingChargeMode,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FundingChargeMode {
    Disabled,
    Signed,
    Absolute,
}

impl FundingChargeMode {
    pub fn parse(value: &str) -> Result<Self, StrategyError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "" | "disabled" | "off" | "none" => Ok(Self::Disabled),
            "signed" => Ok(Self::Signed),
            "absolute" | "abs" | "conservative" => Ok(Self::Absolute),
            other => Err(StrategyError::Config(format!(
                "unsupported funding_charge_mode {other}; expected disabled, signed, or absolute"
            ))),
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Signed => "signed",
            Self::Absolute => "absolute",
        }
    }
}

impl Default for BacktestExecutionConfig {
    fn default() -> Self {
        Self {
            slippage_pct: 0.0,
            brokerage_pct: 0.0,
            entry_fee_pct: None,
            exit_fee_pct: None,
            fee_tax_pct: 0.0,
            fixed_fee_per_order: 0.0,
            funding_rate_pct: 0.0,
            funding_interval_hours: 8,
            funding_charge_mode: FundingChargeMode::Disabled,
        }
    }
}

impl BacktestExecutionConfig {
    pub fn validate(&self) -> Result<(), StrategyError> {
        let entry_fee_pct = self.entry_fee_pct.unwrap_or(0.0);
        let exit_fee_pct = self.exit_fee_pct.unwrap_or(0.0);
        if !self.slippage_pct.is_finite()
            || !self.brokerage_pct.is_finite()
            || !entry_fee_pct.is_finite()
            || !exit_fee_pct.is_finite()
            || !self.fee_tax_pct.is_finite()
            || !self.fixed_fee_per_order.is_finite()
            || !self.funding_rate_pct.is_finite()
            || self.slippage_pct < 0.0
            || self.brokerage_pct < 0.0
            || entry_fee_pct < 0.0
            || exit_fee_pct < 0.0
            || self.fee_tax_pct < 0.0
            || self.fixed_fee_per_order < 0.0
            || (self.funding_charge_mode != FundingChargeMode::Disabled
                && self.funding_interval_hours == 0)
        {
            return Err(StrategyError::Config(
                "backtest execution costs must be finite and non-negative".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct BacktestPartialExit {
    pub signal_id: String,
    pub reason: String,
    pub qty: f64,
    pub raw_exit_price: f64,
    pub exit_price: f64,
    pub exit_at: u64,
    pub exit_charges: f64,
    pub exit_slippage: f64,
    pub funding_charges: f64,
    pub gross_pnl: f64,
    pub net_pnl: f64,
}

#[derive(Clone, Debug)]
pub struct BacktestTrade {
    pub position_id: String,
    pub entry_signal_id: String,
    pub exit_signal_id: Option<String>,
    pub entry_metadata: serde_json::Value,
    pub ssu_id: i64,
    pub strategy_key: String,
    pub instrument: String,
    pub side: SignalSide,
    pub status: PositionStatus,
    pub qty: f64,
    pub remaining_qty: f64,
    pub raw_entry_price: f64,
    pub entry_price: f64,
    pub entry_at: u64,
    pub entry_charges: f64,
    pub entry_slippage: f64,
    pub raw_exit_price: Option<f64>,
    pub exit_price: Option<f64>,
    pub exit_at: Option<u64>,
    pub exit_reason: Option<String>,
    pub exit_charges: f64,
    pub exit_slippage: f64,
    pub funding_charges: f64,
    pub gross_pnl: Option<f64>,
    pub charges: f64,
    pub net_pnl: Option<f64>,
    pub partial_exits: Vec<BacktestPartialExit>,
}

#[derive(Debug)]
pub struct BacktestPositionBook {
    execution: BacktestExecutionConfig,
    state: Mutex<PositionState>,
}

impl BacktestPositionBook {
    pub fn new(execution: BacktestExecutionConfig) -> Result<Self, StrategyError> {
        execution.validate()?;
        Ok(Self {
            execution,
            state: Mutex::new(PositionState::default()),
        })
    }

    pub fn trades(&self) -> Vec<BacktestTrade> {
        self.state
            .lock()
            .expect("backtest position book lock poisoned")
            .trades
            .values()
            .cloned()
            .collect()
    }
}

impl StrategyPositionBook for BacktestPositionBook {
    fn list_open_by_ssu(&self, ssu_id: i64) -> Result<Vec<StrategyPosition>, StrategyError> {
        let state = self
            .state
            .lock()
            .expect("backtest position book lock poisoned");
        let Some(position_ids) = state.open_by_ssu.get(&ssu_id) else {
            return Ok(Vec::new());
        };
        Ok(position_ids
            .iter()
            .filter_map(|position_id| state.positions.get(position_id))
            .cloned()
            .collect())
    }

    fn last_entry_time_by_ssu(&self, ssu_id: i64) -> Result<Option<u64>, StrategyError> {
        Ok(self
            .state
            .lock()
            .expect("backtest position book lock poisoned")
            .last_entry_by_ssu
            .get(&ssu_id)
            .copied())
    }

    fn entries_today_by_ssu(&self, ssu_id: i64, ist_day: &str) -> Result<u32, StrategyError> {
        Ok(self
            .state
            .lock()
            .expect("backtest position book lock poisoned")
            .entries_by_ssu_day
            .get(&(ssu_id, ist_day.to_string()))
            .copied()
            .unwrap_or(0))
    }

    fn open_position(
        &self,
        signal: &StrategySignal,
        ssu: &SsuConfig,
    ) -> Result<StrategyPosition, StrategyError> {
        if !signal.signal_type.is_entry() {
            return Err(StrategyError::Rule(format!(
                "signal {} is not an entry signal",
                signal.signal_id
            )));
        }
        let side = signal_side(signal)?;
        let instruction = signal.primary_instruction().ok_or_else(|| {
            StrategyError::Rule(format!(
                "signal {} has no trade instructions",
                signal.signal_id
            ))
        })?;
        let raw_price = signal_price(signal)?;
        let qty = instruction.quantity_ratio.abs();
        if !qty.is_finite() || qty <= 0.0 {
            return Err(StrategyError::Rule(format!(
                "signal {} primary instruction has invalid quantity_ratio",
                signal.signal_id
            )));
        }
        let entry_price = fill_price(instruction.action, raw_price, self.execution.slippage_pct);
        let entry_charges = trading_fee(
            entry_price,
            qty,
            fee_pct(self.execution.entry_fee_pct, &self.execution),
            &self.execution,
        );
        let entry_slippage = (entry_price - raw_price).abs() * qty;

        let mut state = self
            .state
            .lock()
            .expect("backtest position book lock poisoned");
        enforce_ssu_limits(&state, signal, ssu)?;

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
        let trade = BacktestTrade {
            position_id: position.position_id.clone(),
            entry_signal_id: signal.signal_id.clone(),
            exit_signal_id: None,
            entry_metadata: signal.metadata.clone(),
            ssu_id: signal.ssu_id,
            strategy_key: signal.strategy_key.clone(),
            instrument: instruction.instrument_name.clone(),
            side,
            status: PositionStatus::Open,
            qty,
            remaining_qty: qty,
            raw_entry_price: raw_price,
            entry_price,
            entry_at: signal.generated_at,
            entry_charges,
            entry_slippage,
            raw_exit_price: None,
            exit_price: None,
            exit_at: None,
            exit_reason: None,
            exit_charges: 0.0,
            exit_slippage: 0.0,
            funding_charges: 0.0,
            gross_pnl: None,
            charges: entry_charges,
            net_pnl: None,
            partial_exits: Vec::new(),
        };
        state
            .positions
            .insert(position.position_id.clone(), position.clone());
        state.trades.insert(position.position_id.clone(), trade);
        state.index_open_position(&position)?;
        Ok(position)
    }

    fn partial_close_position(
        &self,
        signal: &StrategySignal,
    ) -> Result<StrategyPosition, StrategyError> {
        if !signal.signal_type.is_partial_exit() {
            return Err(StrategyError::Rule(format!(
                "signal {} is not a partial exit signal",
                signal.signal_id
            )));
        }
        let side = signal_side(signal)?;
        let instruction = signal.primary_instruction().ok_or_else(|| {
            StrategyError::Rule(format!(
                "signal {} has no trade instructions",
                signal.signal_id
            ))
        })?;
        let requested_qty = instruction.quantity_ratio.abs();
        if !requested_qty.is_finite() || requested_qty <= 0.0 || requested_qty >= 1.0 {
            return Err(StrategyError::Rule(format!(
                "signal {} partial exit quantity_ratio must be between 0 and 1",
                signal.signal_id
            )));
        }
        let raw_exit_price = signal_price(signal)?;
        let exit_price = fill_price(
            instruction.action,
            raw_exit_price,
            self.execution.slippage_pct,
        );

        let mut state = self
            .state
            .lock()
            .expect("backtest position book lock poisoned");
        let position_id = state
            .positions
            .get(&instruction.leg_id)
            .filter(|position| {
                position.ssu_id == signal.ssu_id
                    && position.trade_instrument == instruction.instrument_name
                    && position.side == side
                    && position.status == PositionStatus::Open
            })
            .map(|position| position.position_id.clone())
            .or_else(|| {
                state.latest_open_position_id(signal.ssu_id, &instruction.instrument_name, side)
            });
        let Some(position_id) = position_id else {
            return Err(StrategyError::Rule(format!(
                "no open virtual position found for SSU {} instrument {}",
                signal.ssu_id, instruction.instrument_name
            )));
        };

        let trade = state.trades.get_mut(&position_id).ok_or_else(|| {
            StrategyError::NotFound(format!("missing backtest trade for {position_id}"))
        })?;
        if trade.status != PositionStatus::Open || trade.remaining_qty <= MIN_REMAINING_QTY {
            return Err(StrategyError::Rule(format!(
                "position {position_id} has no remaining quantity for partial exit"
            )));
        }
        if requested_qty >= trade.remaining_qty - MIN_REMAINING_QTY {
            return Err(StrategyError::Rule(format!(
                "signal {} partial exit quantity_ratio would close the full position",
                signal.signal_id
            )));
        }

        let exit_qty = requested_qty;
        let exit_charges = trading_fee(
            exit_price,
            exit_qty,
            fee_pct(self.execution.exit_fee_pct, &self.execution),
            &self.execution,
        );
        let exit_slippage = (exit_price - raw_exit_price).abs() * exit_qty;
        let funding_charges = funding_charge(
            side,
            trade.entry_price,
            exit_qty,
            trade.entry_at,
            signal.generated_at,
            &self.execution,
        );
        let gross_pnl = compute_pnl(side, trade.entry_price, exit_price) * exit_qty;
        let net_pnl = gross_pnl - exit_charges - funding_charges;

        trade.remaining_qty -= exit_qty;
        trade.exit_charges += exit_charges;
        trade.exit_slippage += exit_slippage;
        trade.funding_charges += funding_charges;
        trade.charges += exit_charges + funding_charges;
        trade.partial_exits.push(BacktestPartialExit {
            signal_id: signal.signal_id.clone(),
            reason: signal.reason.clone(),
            qty: exit_qty,
            raw_exit_price,
            exit_price,
            exit_at: signal.generated_at,
            exit_charges,
            exit_slippage,
            funding_charges,
            gross_pnl,
            net_pnl,
        });

        Ok(state
            .positions
            .get(&position_id)
            .expect("position must exist")
            .clone())
    }

    fn close_position(&self, signal: &StrategySignal) -> Result<StrategyPosition, StrategyError> {
        if !signal.signal_type.is_full_exit() {
            return Err(StrategyError::Rule(format!(
                "signal {} is not a full exit signal",
                signal.signal_id
            )));
        }
        let side = signal_side(signal)?;
        let instruction = signal.primary_instruction().ok_or_else(|| {
            StrategyError::Rule(format!(
                "signal {} has no trade instructions",
                signal.signal_id
            ))
        })?;
        let raw_exit_price = signal_price(signal)?;
        let exit_price = fill_price(
            instruction.action,
            raw_exit_price,
            self.execution.slippage_pct,
        );

        let mut state = self
            .state
            .lock()
            .expect("backtest position book lock poisoned");
        let position_id = state
            .positions
            .get(&instruction.leg_id)
            .filter(|position| {
                position.ssu_id == signal.ssu_id
                    && position.trade_instrument == instruction.instrument_name
                    && position.side == side
                    && position.status == PositionStatus::Open
            })
            .map(|position| position.position_id.clone())
            .or_else(|| {
                state.latest_open_position_id(signal.ssu_id, &instruction.instrument_name, side)
            });
        let Some(position_id) = position_id else {
            return Err(StrategyError::Rule(format!(
                "no open virtual position found for SSU {} instrument {}",
                signal.ssu_id, instruction.instrument_name
            )));
        };

        let (trade_entry_price, trade_qty, trade_entry_at, trade_charges, trade_partial_gross) = {
            let trade = state.trades.get(&position_id).ok_or_else(|| {
                StrategyError::NotFound(format!("missing backtest trade for {position_id}"))
            })?;
            if trade.remaining_qty <= MIN_REMAINING_QTY {
                return Err(StrategyError::Rule(format!(
                    "position {position_id} has no remaining quantity to close"
                )));
            }
            (
                trade.entry_price,
                trade.remaining_qty,
                trade.entry_at,
                trade.charges,
                trade
                    .partial_exits
                    .iter()
                    .map(|partial| partial.gross_pnl)
                    .sum::<f64>(),
            )
        };
        let exit_charges = trading_fee(
            exit_price,
            trade_qty,
            fee_pct(self.execution.exit_fee_pct, &self.execution),
            &self.execution,
        );
        let exit_slippage = (exit_price - raw_exit_price).abs() * trade_qty;
        let funding_charges = funding_charge(
            side,
            trade_entry_price,
            trade_qty,
            trade_entry_at,
            signal.generated_at,
            &self.execution,
        );
        let gross_pnl = compute_pnl(side, trade_entry_price, exit_price) * trade_qty;
        let total_charges = trade_charges + exit_charges + funding_charges;
        let gross_pnl = trade_partial_gross + gross_pnl;
        let net_pnl = gross_pnl - total_charges;

        {
            let trade = state.trades.get_mut(&position_id).ok_or_else(|| {
                StrategyError::NotFound(format!("missing backtest trade for {position_id}"))
            })?;
            trade.exit_signal_id = Some(signal.signal_id.clone());
            trade.status = PositionStatus::Closed;
            trade.raw_exit_price = Some(raw_exit_price);
            trade.exit_price = Some(exit_price);
            trade.exit_at = Some(signal.generated_at);
            trade.exit_reason = Some(signal.reason.clone());
            trade.exit_charges += exit_charges;
            trade.exit_slippage += exit_slippage;
            trade.funding_charges += funding_charges;
            trade.gross_pnl = Some(gross_pnl);
            trade.charges = total_charges;
            trade.net_pnl = Some(net_pnl);
            trade.remaining_qty = 0.0;
        }

        let open_position = state
            .positions
            .get(&position_id)
            .cloned()
            .expect("position must exist");
        state.remove_open_position(&open_position);
        let position = state
            .positions
            .get_mut(&position_id)
            .expect("position must exist");
        position.exit_price = Some(exit_price);
        position.exit_at = Some(signal.generated_at);
        position.exit_reason = Some(signal.reason.clone());
        position.pnl = Some(net_pnl);
        position.status = PositionStatus::Closed;
        Ok(position.clone())
    }
}

#[derive(Debug, Default)]
pub struct InMemoryTradeContextStore {
    contexts: Mutex<BTreeMap<String, ContextRecord>>,
}

impl StrategyTradeContextStore for InMemoryTradeContextStore {
    fn save_context(
        &self,
        position_id: &str,
        ssu_id: i64,
        _strategy_key: &str,
        trigger_instrument: &str,
        metadata: &serde_json::Value,
        updated_at: u64,
    ) -> Result<(), StrategyError> {
        self.contexts
            .lock()
            .expect("backtest context store lock poisoned")
            .insert(
                position_id.to_string(),
                ContextRecord {
                    ssu_id,
                    trigger_instrument: trigger_instrument.to_string(),
                    metadata: metadata.clone(),
                    updated_at,
                },
            );
        Ok(())
    }

    fn load_context(&self, position_id: &str) -> Result<Option<serde_json::Value>, StrategyError> {
        Ok(self
            .contexts
            .lock()
            .expect("backtest context store lock poisoned")
            .get(position_id)
            .map(|record| record.metadata.clone()))
    }

    fn load_open_contexts(
        &self,
        ssu_id: i64,
        trigger_instrument: &str,
    ) -> Result<Vec<(String, serde_json::Value)>, StrategyError> {
        Ok(self
            .contexts
            .lock()
            .expect("backtest context store lock poisoned")
            .iter()
            .filter(|(_, record)| {
                record.ssu_id == ssu_id && record.trigger_instrument == trigger_instrument
            })
            .map(|(position_id, record)| (position_id.clone(), record.metadata.clone()))
            .collect())
    }

    fn update_context(
        &self,
        position_id: &str,
        metadata: &serde_json::Value,
        updated_at: u64,
    ) -> Result<(), StrategyError> {
        let mut contexts = self
            .contexts
            .lock()
            .expect("backtest context store lock poisoned");
        let Some(record) = contexts.get_mut(position_id) else {
            return Err(StrategyError::NotFound(format!(
                "missing strategy trade context for {position_id}"
            )));
        };
        record.metadata = metadata.clone();
        record.updated_at = updated_at;
        Ok(())
    }

    fn delete_context(&self, position_id: &str) -> Result<(), StrategyError> {
        self.contexts
            .lock()
            .expect("backtest context store lock poisoned")
            .remove(position_id);
        Ok(())
    }
}

#[derive(Debug, Default)]
struct PositionState {
    positions: BTreeMap<String, StrategyPosition>,
    trades: BTreeMap<String, BacktestTrade>,
    open_by_ssu: BTreeMap<i64, BTreeSet<String>>,
    open_by_key: BTreeMap<OpenPositionKey, BTreeSet<OpenPositionOrder>>,
    last_entry_by_ssu: BTreeMap<i64, u64>,
    entries_by_ssu_day: BTreeMap<(i64, String), u32>,
}

impl PositionState {
    fn index_open_position(&mut self, position: &StrategyPosition) -> Result<(), StrategyError> {
        self.open_by_ssu
            .entry(position.ssu_id)
            .or_default()
            .insert(position.position_id.clone());
        self.open_by_key
            .entry(OpenPositionKey::new(
                position.ssu_id,
                &position.trade_instrument,
                position.side,
            ))
            .or_default()
            .insert(OpenPositionOrder {
                entry_at: position.entry_at,
                position_id: position.position_id.clone(),
            });
        self.last_entry_by_ssu
            .entry(position.ssu_id)
            .and_modify(|last| *last = (*last).max(position.entry_at))
            .or_insert(position.entry_at);
        let day = ist_day_key(position.entry_at)?;
        *self
            .entries_by_ssu_day
            .entry((position.ssu_id, day))
            .or_insert(0) += 1;
        Ok(())
    }

    fn remove_open_position(&mut self, position: &StrategyPosition) {
        let remove_ssu_key = if let Some(ids) = self.open_by_ssu.get_mut(&position.ssu_id) {
            ids.remove(&position.position_id);
            ids.is_empty()
        } else {
            false
        };
        if remove_ssu_key {
            self.open_by_ssu.remove(&position.ssu_id);
        }

        let key = OpenPositionKey::new(position.ssu_id, &position.trade_instrument, position.side);
        let remove_key = if let Some(orders) = self.open_by_key.get_mut(&key) {
            orders.remove(&OpenPositionOrder {
                entry_at: position.entry_at,
                position_id: position.position_id.clone(),
            });
            orders.is_empty()
        } else {
            false
        };
        if remove_key {
            self.open_by_key.remove(&key);
        }
    }

    fn open_count_by_ssu(&self, ssu_id: i64) -> u32 {
        self.open_by_ssu
            .get(&ssu_id)
            .map(|ids| ids.len() as u32)
            .unwrap_or(0)
    }

    fn latest_open_position_id(
        &self,
        ssu_id: i64,
        instrument: &str,
        side: SignalSide,
    ) -> Option<String> {
        self.open_by_key
            .get(&OpenPositionKey::new(ssu_id, instrument, side))
            .and_then(|orders| orders.iter().next_back())
            .map(|order| order.position_id.clone())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct OpenPositionKey {
    ssu_id: i64,
    instrument: String,
    side: SideKey,
}

impl OpenPositionKey {
    fn new(ssu_id: i64, instrument: &str, side: SignalSide) -> Self {
        Self {
            ssu_id,
            instrument: instrument.to_string(),
            side: SideKey::from(side),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum SideKey {
    Long,
    Short,
}

impl From<SignalSide> for SideKey {
    fn from(value: SignalSide) -> Self {
        match value {
            SignalSide::Long => Self::Long,
            SignalSide::Short => Self::Short,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct OpenPositionOrder {
    entry_at: u64,
    position_id: String,
}

#[derive(Clone, Debug)]
struct ContextRecord {
    ssu_id: i64,
    trigger_instrument: String,
    metadata: serde_json::Value,
    updated_at: u64,
}

fn enforce_ssu_limits(
    state: &PositionState,
    signal: &StrategySignal,
    ssu: &SsuConfig,
) -> Result<(), StrategyError> {
    if ssu.trade_gap_secs > 0 {
        if let Some(last_entry_at) = state.last_entry_by_ssu.get(&ssu.ssu_id).copied() {
            let blocked_until = last_entry_at + ssu.trade_gap_secs * 1_000;
            if signal.generated_at < blocked_until {
                return Err(StrategyError::Rule(format!(
                    "SSU {} trade_gap_secs blocked entry until {}",
                    ssu.ssu_id, blocked_until
                )));
            }
        }
    }

    if ssu.max_overlap > 0 {
        let open_positions = state.open_count_by_ssu(ssu.ssu_id);
        if open_positions >= ssu.max_overlap {
            return Err(StrategyError::Rule(format!(
                "SSU {} max_overlap reached",
                ssu.ssu_id
            )));
        }
    }

    if ssu.max_positions_per_day > 0 {
        let day = ist_day_key(signal.generated_at)?;
        let entries_today = state
            .entries_by_ssu_day
            .get(&(ssu.ssu_id, day.clone()))
            .copied()
            .unwrap_or(0);
        if entries_today >= ssu.max_positions_per_day {
            return Err(StrategyError::Rule(format!(
                "SSU {} max_positions_per_day reached for {}",
                ssu.ssu_id, day
            )));
        }
    }

    Ok(())
}

fn signal_side(signal: &StrategySignal) -> Result<SignalSide, StrategyError> {
    signal.signal_type.side().ok_or_else(|| {
        StrategyError::Rule(format!(
            "signal {} has no directional side",
            signal.signal_id
        ))
    })
}

fn signal_price(signal: &StrategySignal) -> Result<f64, StrategyError> {
    let instruction = signal.primary_instruction().ok_or_else(|| {
        StrategyError::Rule(format!(
            "signal {} has no trade instructions",
            signal.signal_id
        ))
    })?;
    instruction
        .price_policy
        .reference_price
        .or(instruction.price_policy.limit_price)
        .ok_or_else(|| {
            StrategyError::Rule(format!(
                "signal {} primary instruction has no reference or limit price",
                signal.signal_id
            ))
        })
}

fn fill_price(action: TradeAction, reference_price: f64, slippage_pct: f64) -> f64 {
    match action {
        TradeAction::Buy => reference_price * (1.0 + slippage_pct),
        TradeAction::Sell => reference_price * (1.0 - slippage_pct),
    }
}

fn fee_pct(configured: Option<f64>, execution: &BacktestExecutionConfig) -> f64 {
    configured.unwrap_or(execution.brokerage_pct)
}

fn trading_fee(price: f64, qty: f64, fee_pct: f64, execution: &BacktestExecutionConfig) -> f64 {
    let base_fee = price.abs() * qty * fee_pct;
    base_fee * (1.0 + execution.fee_tax_pct) + execution.fixed_fee_per_order
}

fn funding_charge(
    side: SignalSide,
    notional_price: f64,
    qty: f64,
    entry_at: u64,
    exit_at: u64,
    execution: &BacktestExecutionConfig,
) -> f64 {
    if execution.funding_charge_mode == FundingChargeMode::Disabled
        || execution.funding_rate_pct == 0.0
        || execution.funding_interval_hours == 0
        || exit_at < entry_at
    {
        return 0.0;
    }

    let interval_ms = execution.funding_interval_hours.saturating_mul(3_600_000);
    if interval_ms == 0 {
        return 0.0;
    }

    let first_snapshot = if entry_at % interval_ms == 0 {
        entry_at
    } else {
        ((entry_at / interval_ms) + 1) * interval_ms
    };
    let last_snapshot = (exit_at / interval_ms) * interval_ms;
    if first_snapshot > last_snapshot {
        return 0.0;
    }

    let snapshots = ((last_snapshot - first_snapshot) / interval_ms) + 1;
    let notional = notional_price.abs() * qty;
    match execution.funding_charge_mode {
        FundingChargeMode::Disabled => 0.0,
        FundingChargeMode::Absolute => {
            notional * execution.funding_rate_pct.abs() * snapshots as f64
        }
        FundingChargeMode::Signed => {
            let side_sign = match side {
                SignalSide::Long => 1.0,
                SignalSide::Short => -1.0,
            };
            notional * execution.funding_rate_pct * side_sign * snapshots as f64
        }
    }
}

fn compute_pnl(side: SignalSide, entry_price: f64, exit_price: f64) -> f64 {
    match side {
        SignalSide::Long => exit_price - entry_price,
        SignalSide::Short => entry_price - exit_price,
    }
}

fn ist_day_key(unix_millis: u64) -> Result<String, StrategyError> {
    let ist = FixedOffset::east_opt(IST_OFFSET_SECONDS)
        .ok_or_else(|| StrategyError::Config("failed to create IST fixed offset".to_string()))?;
    let utc = chrono::DateTime::from_timestamp_millis(unix_millis as i64)
        .ok_or_else(|| StrategyError::Parse(format!("invalid unix millis {unix_millis}")))?;
    let ist_time = utc.with_timezone(&ist);
    Ok(format!(
        "{:04}-{:02}-{:02}",
        ist_time.year(),
        ist_time.month(),
        ist_time.day()
    ))
}

pub fn side_label(side: SignalSide) -> &'static str {
    match side {
        SignalSide::Long => "LONG",
        SignalSide::Short => "SHORT",
    }
}

pub fn status_label(status: PositionStatus) -> &'static str {
    match status {
        PositionStatus::Open => "OPEN",
        PositionStatus::Closed => "CLOSED",
    }
}

#[allow(dead_code)]
fn signal_type_label(signal_type: StrategySignalType) -> &'static str {
    match signal_type {
        StrategySignalType::EntryLong => "ENTRY_LONG",
        StrategySignalType::EntryShort => "ENTRY_SHORT",
        StrategySignalType::ExitLong => "EXIT_LONG",
        StrategySignalType::ExitShort => "EXIT_SHORT",
        StrategySignalType::ExitLongPartial => "EXIT_LONG_PARTIAL",
        StrategySignalType::ExitShortPartial => "EXIT_SHORT_PARTIAL",
        StrategySignalType::Shift => "SHIFT",
        StrategySignalType::Rollover => "ROLLOVER",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategy::StrategySignal;

    #[test]
    fn slippage_penalizes_both_sides() {
        assert_eq!(fill_price(TradeAction::Buy, 100.0, 0.001), 100.1);
        assert_eq!(fill_price(TradeAction::Sell, 100.0, 0.001), 99.9);
    }

    #[test]
    fn position_book_records_net_pnl_after_slippage_and_charges() {
        let book = BacktestPositionBook::new(BacktestExecutionConfig {
            slippage_pct: 0.01,
            brokerage_pct: 0.001,
            fixed_fee_per_order: 1.0,
            ..BacktestExecutionConfig::default()
        })
        .expect("book");
        let ssu = SsuConfig {
            ssu_id: 1,
            strategy_key: "test".to_string(),
            enabled: true,
            trade_gap_secs: 0,
            max_overlap: 1,
            max_positions_per_day: 10,
            required_timeframes: Vec::new(),
            indicator_specs: Vec::new(),
            params_json: "{}".to_string(),
        };
        let entry = StrategySignal::single_leg_entry(
            1,
            "test",
            "BTC",
            SignalSide::Long,
            100.0,
            "entry".to_string(),
            1_000,
        );
        let exit = StrategySignal::single_leg_exit(
            1,
            "test",
            "BTC",
            SignalSide::Long,
            110.0,
            "exit".to_string(),
            2_000,
        );

        let position = book.open_position(&entry, &ssu).expect("open");
        assert!((position.entry_price - 101.0).abs() < 0.000001);
        let position = book.close_position(&exit).expect("close");

        let expected_charges = 101.0 * 0.001 + 1.0 + 108.9 * 0.001 + 1.0;
        let expected_net = (108.9 - 101.0) - expected_charges;
        assert!((position.pnl.expect("pnl") - expected_net).abs() < 0.000001);
        assert_eq!(book.trades().len(), 1);
    }

    #[test]
    fn partial_exit_keeps_position_open_and_closes_remaining_later() {
        let book = BacktestPositionBook::new(BacktestExecutionConfig::default()).expect("book");
        let ssu = SsuConfig {
            ssu_id: 1,
            strategy_key: "test".to_string(),
            enabled: true,
            trade_gap_secs: 0,
            max_overlap: 1,
            max_positions_per_day: 10,
            required_timeframes: Vec::new(),
            indicator_specs: Vec::new(),
            params_json: "{}".to_string(),
        };
        let entry = StrategySignal::single_leg_entry(
            1,
            "test",
            "BTC",
            SignalSide::Long,
            100.0,
            "entry".to_string(),
            1_000,
        );
        let mut partial = StrategySignal::single_leg_partial_exit(
            1,
            "test",
            "BTC",
            SignalSide::Long,
            106.0,
            1.0 / 3.0,
            "tp1 partial".to_string(),
            2_000,
        );
        let final_exit = StrategySignal::single_leg_exit(
            1,
            "test",
            "BTC",
            SignalSide::Long,
            112.0,
            "final exit".to_string(),
            3_000,
        );

        let position = book.open_position(&entry, &ssu).expect("open");
        partial.instructions[0].leg_id = position.position_id.clone();
        book.partial_close_position(&partial).expect("partial exit");
        assert_eq!(book.list_open_by_ssu(1).expect("open").len(), 1);

        let position = book.close_position(&final_exit).expect("close");
        let expected_net = (106.0 - 100.0) / 3.0 + (112.0 - 100.0) * (2.0 / 3.0);
        assert!((position.pnl.expect("pnl") - expected_net).abs() < 0.000001);
        let trades = book.trades();
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].partial_exits.len(), 1);
        assert!((trades[0].remaining_qty - 0.0).abs() < 0.000001);
    }

    #[test]
    fn position_book_releases_open_index_after_close() {
        let book = BacktestPositionBook::new(BacktestExecutionConfig::default()).expect("book");
        let ssu = SsuConfig {
            ssu_id: 1,
            strategy_key: "test".to_string(),
            enabled: true,
            trade_gap_secs: 0,
            max_overlap: 1,
            max_positions_per_day: 10,
            required_timeframes: Vec::new(),
            indicator_specs: Vec::new(),
            params_json: "{}".to_string(),
        };
        let entry = StrategySignal::single_leg_entry(
            1,
            "test",
            "BTC",
            SignalSide::Long,
            100.0,
            "entry".to_string(),
            1_000,
        );
        let blocked_entry = StrategySignal::single_leg_entry(
            1,
            "test",
            "BTC",
            SignalSide::Long,
            101.0,
            "entry".to_string(),
            2_000,
        );
        let exit = StrategySignal::single_leg_exit(
            1,
            "test",
            "BTC",
            SignalSide::Long,
            110.0,
            "exit".to_string(),
            3_000,
        );
        let next_entry = StrategySignal::single_leg_entry(
            1,
            "test",
            "BTC",
            SignalSide::Long,
            102.0,
            "entry".to_string(),
            4_000,
        );

        book.open_position(&entry, &ssu).expect("open");
        assert!(book.open_position(&blocked_entry, &ssu).is_err());

        book.close_position(&exit).expect("close");
        assert!(book.list_open_by_ssu(1).expect("open positions").is_empty());

        book.open_position(&next_entry, &ssu).expect("reopen");
        let open_positions = book.list_open_by_ssu(1).expect("open positions");
        assert_eq!(open_positions.len(), 1);
        assert_eq!(open_positions[0].entry_at, 4_000);
    }

    #[test]
    fn delta_fee_tax_and_funding_are_common_costs() {
        let execution = BacktestExecutionConfig {
            entry_fee_pct: Some(0.0005),
            exit_fee_pct: Some(0.0005),
            fee_tax_pct: 0.18,
            funding_rate_pct: 0.0001,
            funding_interval_hours: 8,
            funding_charge_mode: FundingChargeMode::Signed,
            ..BacktestExecutionConfig::default()
        };

        let entry_fee = trading_fee(
            100_000.0,
            1.0,
            fee_pct(execution.entry_fee_pct, &execution),
            &execution,
        );
        assert!((entry_fee - 59.0).abs() < 0.000001);

        let long_funding = funding_charge(
            SignalSide::Long,
            100_000.0,
            1.0,
            0,
            8 * 3_600_000,
            &execution,
        );
        assert!((long_funding - 20.0).abs() < 0.000001);

        let short_funding = funding_charge(
            SignalSide::Short,
            100_000.0,
            1.0,
            0,
            8 * 3_600_000,
            &execution,
        );
        assert!((short_funding + 20.0).abs() < 0.000001);
    }
}
