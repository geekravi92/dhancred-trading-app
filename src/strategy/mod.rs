pub(crate) mod diagnostics;
mod error;
mod history;
mod price_store;
mod runtime;
mod signal;
mod strategies;
mod timeframes;

use std::collections::BTreeMap;
use std::sync::Arc;

pub use error::StrategyError;
pub use history::{HistoricalReplayStore, SqliteHistoricalReplayStore};
pub use price_store::{InMemoryPriceStore, PriceStore};
pub use runtime::{
    BuiltinStrategyFactory, InMemorySignalSink, PositionStatus, SignalRouter, SignalSink,
    SqliteSsuRepository, SqliteStrategyPositionBook, SqliteStrategyTradeContextStore,
    SsuRepository, Strategy, StrategyFactory, StrategyPosition, StrategyPositionBook,
    StrategyRuntime, StrategyRuntimeHandle, StrategyTradeContextStore, start_strategy_runtime,
};
pub use signal::{
    InstrumentKind, PricePolicy, PricePolicyType, SignalSide, StrategySignal, StrategySignalType,
    TradeAction, TradeInstruction, instrument_kind_label, parse_instrument_kind,
    parse_price_policy, parse_price_policy_type, parse_signal_type, parse_trade_action,
    price_policy_type_label, serialize_price_policy, signal_type_label, trade_action_label,
};
pub(crate) use timeframes::bucket_bounds;
pub use timeframes::{SharedTimeframeEngine, TimeframeEngine};

pub use crate::feeder::Timeframe;

#[derive(Clone, Debug, PartialEq)]
pub struct PriceSnapshot {
    pub instrument: String,
    pub ltp: f64,
    pub updated_at: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Bar {
    pub instrument: String,
    pub timeframe: Timeframe,
    pub start_at: u64,
    pub end_at: u64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub is_closed: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Candle {
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TimedCandle {
    pub start_ts: u64,
    pub end_ts: u64,
    pub candle: Candle,
}

impl TimedCandle {
    pub fn from_bar(bar: &Bar) -> Self {
        Self {
            start_ts: bar.start_at,
            end_ts: bar.end_at,
            candle: Candle {
                open: bar.open,
                high: bar.high,
                low: bar.low,
                close: bar.close,
                volume: bar.volume,
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Tick {
    pub price: f64,
    pub volume: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TickSnapshot {
    pub event_ts: u64,
    pub ticks: BTreeMap<String, Tick>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CandleSnapshot {
    pub event_ts: u64,
    pub candles: BTreeMap<String, BTreeMap<Timeframe, TimedCandle>>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum MarketEvent {
    Tick(TickSnapshot),
    Candles(CandleSnapshot),
}

impl MarketEvent {
    pub fn event_ts(&self) -> u64 {
        match self {
            MarketEvent::Tick(snapshot) => snapshot.event_ts,
            MarketEvent::Candles(snapshot) => snapshot.event_ts,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndicatorSpec {
    pub key: String,
    pub timeframe: Timeframe,
    pub kind: String,
    pub params_json: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct IndicatorValue {
    pub key: String,
    pub timeframe: Timeframe,
    pub value: f64,
    pub as_of: u64,
    pub is_final: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TimeframeUpdate {
    pub instrument: String,
    pub tick_at: u64,
    pub closed_timeframes: Vec<Timeframe>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SsuConfig {
    pub ssu_id: i64,
    pub strategy_key: String,
    pub enabled: bool,
    pub trade_gap_secs: u64,
    pub max_overlap: u32,
    pub max_positions_per_day: u32,
    pub required_timeframes: Vec<Timeframe>,
    pub indicator_specs: Vec<IndicatorSpec>,
    pub params_json: String,
}

#[derive(Clone)]
pub struct StrategyContext {
    pub prices: Arc<dyn PriceStore>,
    pub timeframes: Arc<dyn TimeframeEngine>,
    pub strategy_positions: Arc<dyn StrategyPositionBook>,
    pub trade_contexts: Arc<dyn StrategyTradeContextStore>,
}
