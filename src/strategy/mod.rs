mod error;
mod history;
mod price_store;
mod runtime;
mod strategies;
mod timeframes;

use std::sync::Arc;

pub use error::StrategyError;
pub use history::{HistoricalReplayStore, SqliteHistoricalReplayStore};
pub use price_store::{InMemoryPriceStore, PriceStore};
pub use runtime::{
    BuiltinStrategyFactory, EntrySignal, ExitSignal, InMemorySignalSink, PositionStatus,
    RolloverSignal, SignalSide, ShiftSignal, SignalRouter, SignalSink, SqliteSsuRepository,
    SqliteStrategyPositionBook, Strategy, StrategyFactory, StrategyPosition,
    StrategyPositionBook, StrategyRuntime, StrategyRuntimeHandle, StrategySignal,
    SsuRepository, start_strategy_runtime,
};
pub use timeframes::{SharedTimeframeEngine, TimeframeEngine};

pub use crate::feeder::Timeframe;

#[derive(Clone, Debug, PartialEq)]
pub struct PriceSnapshot {
    pub instrument: String,
    pub ltp: f64,
    pub updated_at: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PriceUpdated {
    pub trigger_instrument: String,
    pub at: u64,
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
    pub is_closed: bool,
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
}
