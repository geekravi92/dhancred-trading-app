use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, RwLock};

use chrono::{Datelike, FixedOffset};
use rusqlite::params;

use crate::config::AppConfig;
use crate::feeder::{
    CandleAlignmentMap, InstrumentCatalog, InstrumentType, MarketSessionSchedule, Timeframe,
    candle_alignments_from_catalog, merge_candle_alignments,
};
use crate::notification::notify_message;
use crate::storage::strategy::StrategySqlite;
use crate::strategy::{
    Bar, CandleSnapshot, HistoricalReplayStore, InMemoryPriceStore, IndicatorSpec, MarketEvent,
    PriceStore, SharedTimeframeEngine, SignalSide, SsuConfig, StrategyContext, StrategyError,
    StrategySignal, Tick, TickSnapshot, TimedCandle, TimeframeEngine, TimeframeUpdate,
    instrument_kind_label, signal_type_label, trade_action_label,
};
const IST_OFFSET_SECONDS: i32 = 5 * 60 * 60 + 30 * 60;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PositionStatus {
    Open,
    Closed,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StrategyPosition {
    pub position_id: String,
    pub ssu_id: i64,
    pub trigger_instrument: String,
    pub trade_instrument: String,
    pub side: SignalSide,
    pub entry_price: f64,
    pub entry_at: u64,
    pub exit_price: Option<f64>,
    pub exit_at: Option<u64>,
    pub exit_reason: Option<String>,
    pub pnl: Option<f64>,
    pub status: PositionStatus,
}

pub trait Strategy: Send + Sync {
    fn strategy_key(&self) -> &'static str;

    fn warmup(
        &self,
        _ctx: &StrategyContext,
        _ssu: &SsuConfig,
        _instrument: &str,
    ) -> Result<(), StrategyError> {
        Ok(())
    }

    fn on_market_event(
        &self,
        ctx: &StrategyContext,
        ssu: &SsuConfig,
        event: &MarketEvent,
    ) -> Result<Vec<StrategySignal>, StrategyError>;
}

pub trait StrategyFactory: Send + Sync {
    fn get(&self, strategy_key: &str) -> Result<Arc<dyn Strategy>, StrategyError>;
}

pub trait SsuRepository: Send + Sync {
    fn load_active_ssus(&self) -> Result<Vec<SsuConfig>, StrategyError>;
}

pub trait StrategyPositionBook: Send + Sync {
    fn list_open_by_ssu(&self, ssu_id: i64) -> Result<Vec<StrategyPosition>, StrategyError>;
    fn last_entry_time_by_ssu(&self, ssu_id: i64) -> Result<Option<u64>, StrategyError>;
    fn entries_today_by_ssu(&self, ssu_id: i64, ist_day: &str) -> Result<u32, StrategyError>;
    fn open_position(
        &self,
        signal: &StrategySignal,
        ssu: &SsuConfig,
    ) -> Result<StrategyPosition, StrategyError>;
    fn partial_close_position(
        &self,
        signal: &StrategySignal,
    ) -> Result<StrategyPosition, StrategyError>;
    fn close_position(&self, signal: &StrategySignal) -> Result<StrategyPosition, StrategyError>;
}

pub trait StrategyTradeContextStore: Send + Sync {
    fn save_context(
        &self,
        position_id: &str,
        ssu_id: i64,
        strategy_key: &str,
        trigger_instrument: &str,
        metadata: &serde_json::Value,
        updated_at: u64,
    ) -> Result<(), StrategyError>;
    fn load_context(&self, position_id: &str) -> Result<Option<serde_json::Value>, StrategyError>;
    fn load_open_contexts(
        &self,
        ssu_id: i64,
        trigger_instrument: &str,
    ) -> Result<Vec<(String, serde_json::Value)>, StrategyError>;
    fn update_context(
        &self,
        position_id: &str,
        metadata: &serde_json::Value,
        updated_at: u64,
    ) -> Result<(), StrategyError>;
    fn delete_context(&self, position_id: &str) -> Result<(), StrategyError>;
}

pub trait SignalSink: Send + Sync {
    fn consume(&self, signal: &StrategySignal) -> Result<(), StrategyError>;
}

#[derive(Debug)]
pub struct InMemorySignalSink {
    messages: Mutex<Vec<String>>,
}

impl InMemorySignalSink {
    pub fn new() -> Self {
        Self {
            messages: Mutex::new(Vec::new()),
        }
    }

    pub fn messages(&self) -> Vec<String> {
        self.messages
            .lock()
            .expect("notifier lock poisoned")
            .clone()
    }
}

impl SignalSink for InMemorySignalSink {
    fn consume(&self, signal: &StrategySignal) -> Result<(), StrategyError> {
        self.messages
            .lock()
            .expect("signal sink lock poisoned")
            .push(signal_label(signal).to_string());
        Ok(())
    }
}

#[derive(Default)]
pub struct SignalRouter {
    sinks: Vec<Arc<dyn SignalSink>>,
}

impl SignalRouter {
    pub fn new(sinks: Vec<Arc<dyn SignalSink>>) -> Self {
        Self { sinks }
    }

    fn route(&self, signal: &StrategySignal) -> Result<(), StrategyError> {
        for sink in &self.sinks {
            sink.consume(signal)?;
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct BuiltinStrategyFactory;

impl StrategyFactory for BuiltinStrategyFactory {
    fn get(&self, strategy_key: &str) -> Result<Arc<dyn Strategy>, StrategyError> {
        crate::strategy::strategies::strategy_by_key(strategy_key)
    }
}

#[derive(Debug)]
pub struct SqliteSsuRepository {
    sqlite: StrategySqlite,
}

impl SqliteSsuRepository {
    pub fn new(path: impl Into<String>) -> Result<Self, StrategyError> {
        Ok(Self {
            sqlite: StrategySqlite::new(path.into())?,
        })
    }

    pub fn load_all_ssus(&self) -> Result<Vec<SsuConfig>, StrategyError> {
        self.load_ssus(false)
    }

    fn load_ssus(&self, only_enabled: bool) -> Result<Vec<SsuConfig>, StrategyError> {
        let connection = self.sqlite.open_connection()?;
        let query = if only_enabled {
            "\
            SELECT
                ssu_id,
                strategy_key,
                enabled,
                trade_gap_secs,
                max_overlap,
                max_positions_per_day,
                required_timeframes_json,
                indicator_specs_json,
                params_json
            FROM strategy_ssu
            WHERE enabled = 1
            ORDER BY ssu_id
            "
        } else {
            "\
            SELECT
                ssu_id,
                strategy_key,
                enabled,
                trade_gap_secs,
                max_overlap,
                max_positions_per_day,
                required_timeframes_json,
                indicator_specs_json,
                params_json
            FROM strategy_ssu
            ORDER BY ssu_id
            "
        };
        let mut statement = connection.prepare(query)?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, String>(8)?,
            ))
        })?;

        let mut configs = Vec::new();
        for row in rows {
            let (
                ssu_id,
                strategy_key,
                enabled,
                trade_gap_secs,
                max_overlap,
                max_positions_per_day,
                required_timeframes_json,
                indicator_specs_json,
                params_json,
            ) = row?;
            configs.push(SsuConfig {
                ssu_id,
                strategy_key,
                enabled: enabled != 0,
                trade_gap_secs: trade_gap_secs.max(0) as u64,
                max_overlap: max_overlap.max(0) as u32,
                max_positions_per_day: max_positions_per_day.max(0) as u32,
                required_timeframes: parse_timeframes_json(&required_timeframes_json)?,
                indicator_specs: parse_indicator_specs_json(&indicator_specs_json)?,
                params_json,
            });
        }

        Ok(configs)
    }
}

impl SsuRepository for SqliteSsuRepository {
    fn load_active_ssus(&self) -> Result<Vec<SsuConfig>, StrategyError> {
        self.load_ssus(true)
    }
}

#[derive(Debug)]
pub struct SqliteStrategyPositionBook {
    sqlite: StrategySqlite,
    state: Mutex<StrategyPositionState>,
}

impl SqliteStrategyPositionBook {
    pub fn new(path: impl Into<String>) -> Result<Self, StrategyError> {
        let sqlite = StrategySqlite::new(path.into())?;
        let connection = sqlite.open_connection()?;
        let mut statement = connection.prepare(
            "\
            SELECT
                position_id,
                ssu_id,
                trigger_instrument,
                trade_instrument,
                side,
                status,
                entry_price,
                entry_at,
                exit_price,
                exit_at,
                exit_reason,
                pnl
            FROM virtual_position
            ORDER BY entry_at, position_id
            ",
        )?;
        let rows = statement.query_map([], |row| {
            Ok(StrategyPosition {
                position_id: row.get(0)?,
                ssu_id: row.get(1)?,
                trigger_instrument: row.get(2)?,
                trade_instrument: row.get(3)?,
                side: parse_side(&row.get::<_, String>(4)?).map_err(to_rusqlite_error)?,
                status: parse_status(&row.get::<_, String>(5)?).map_err(to_rusqlite_error)?,
                entry_price: row.get(6)?,
                entry_at: row.get(7)?,
                exit_price: row.get(8)?,
                exit_at: row.get(9)?,
                exit_reason: row.get(10)?,
                pnl: row.get(11)?,
            })
        })?;

        let mut positions = BTreeMap::new();
        let mut next_id = 1_u64;
        for row in rows {
            let position = row?;
            next_id = next_id.max(extract_position_seq(&position.position_id).saturating_add(1));
            positions.insert(position.position_id.clone(), position);
        }

        Ok(Self {
            sqlite,
            state: Mutex::new(StrategyPositionState { positions, next_id }),
        })
    }
}

impl StrategyPositionBook for SqliteStrategyPositionBook {
    fn list_open_by_ssu(&self, ssu_id: i64) -> Result<Vec<StrategyPosition>, StrategyError> {
        Ok(self
            .state
            .lock()
            .expect("position store lock poisoned")
            .positions
            .values()
            .filter(|position| position.ssu_id == ssu_id && position.status == PositionStatus::Open)
            .cloned()
            .collect())
    }

    fn last_entry_time_by_ssu(&self, ssu_id: i64) -> Result<Option<u64>, StrategyError> {
        Ok(self
            .state
            .lock()
            .expect("position store lock poisoned")
            .positions
            .values()
            .filter(|position| position.ssu_id == ssu_id)
            .map(|position| position.entry_at)
            .max())
    }

    fn entries_today_by_ssu(&self, ssu_id: i64, ist_day: &str) -> Result<u32, StrategyError> {
        Ok(self
            .state
            .lock()
            .expect("position store lock poisoned")
            .positions
            .values()
            .filter(|position| position.ssu_id == ssu_id)
            .filter(|position| ist_day_key(position.entry_at).is_ok_and(|day| day == ist_day))
            .count() as u32)
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
        let side = signal.signal_type.side().ok_or_else(|| {
            StrategyError::Rule(format!(
                "signal {} has no directional side",
                signal.signal_id
            ))
        })?;
        let instruction = signal.primary_instruction().ok_or_else(|| {
            StrategyError::Rule(format!(
                "signal {} has no trade instructions",
                signal.signal_id
            ))
        })?;
        let entry_price = instruction
            .price_policy
            .reference_price
            .or(instruction.price_policy.limit_price)
            .ok_or_else(|| {
                StrategyError::Rule(format!(
                    "signal {} primary instruction has no reference or limit price",
                    signal.signal_id
                ))
            })?;

        let mut state = self.state.lock().expect("position store lock poisoned");
        if ssu.trade_gap_secs > 0 {
            if let Some(last_entry_at) = state
                .positions
                .values()
                .filter(|position| position.ssu_id == ssu.ssu_id)
                .map(|position| position.entry_at)
                .max()
            {
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
            let open_positions = state
                .positions
                .values()
                .filter(|position| {
                    position.ssu_id == ssu.ssu_id && position.status == PositionStatus::Open
                })
                .count() as u32;
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
                .positions
                .values()
                .filter(|position| position.ssu_id == ssu.ssu_id)
                .filter(|position| ist_day_key(position.entry_at).is_ok_and(|value| value == day))
                .count() as u32;
            if entries_today >= ssu.max_positions_per_day {
                return Err(StrategyError::Rule(format!(
                    "SSU {} max_positions_per_day reached for {}",
                    ssu.ssu_id, day
                )));
            }
        }

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
        persist_signal(&self.sqlite, signal)?;
        persist_open_position(&self.sqlite, &position)?;
        state.next_id += 1;
        state
            .positions
            .insert(position.position_id.clone(), position.clone());
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
        let side = signal.signal_type.side().ok_or_else(|| {
            StrategyError::Rule(format!(
                "signal {} has no directional side",
                signal.signal_id
            ))
        })?;
        let instruction = signal.primary_instruction().ok_or_else(|| {
            StrategyError::Rule(format!(
                "signal {} has no trade instructions",
                signal.signal_id
            ))
        })?;
        if !instruction.quantity_ratio.is_finite()
            || instruction.quantity_ratio <= 0.0
            || instruction.quantity_ratio >= 1.0
        {
            return Err(StrategyError::Rule(format!(
                "signal {} partial exit quantity_ratio must be between 0 and 1",
                signal.signal_id
            )));
        }

        let state = self.state.lock().expect("position store lock poisoned");
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
                state
                    .positions
                    .values()
                    .filter(|position| {
                        position.ssu_id == signal.ssu_id
                            && position.trade_instrument == instruction.instrument_name
                            && position.side == side
                            && position.status == PositionStatus::Open
                    })
                    .max_by_key(|position| position.entry_at)
                    .map(|position| position.position_id.clone())
            });
        let Some(position_id) = position_id else {
            return Err(StrategyError::Rule(format!(
                "no open virtual position found for SSU {} instrument {}",
                signal.ssu_id, instruction.instrument_name
            )));
        };
        let position = state
            .positions
            .get(&position_id)
            .expect("position must exist")
            .clone();
        drop(state);

        persist_signal(&self.sqlite, signal)?;
        Ok(position)
    }

    fn close_position(&self, signal: &StrategySignal) -> Result<StrategyPosition, StrategyError> {
        if !signal.signal_type.is_full_exit() {
            return Err(StrategyError::Rule(format!(
                "signal {} is not a full exit signal",
                signal.signal_id
            )));
        }
        let side = signal.signal_type.side().ok_or_else(|| {
            StrategyError::Rule(format!(
                "signal {} has no directional side",
                signal.signal_id
            ))
        })?;
        let instruction = signal.primary_instruction().ok_or_else(|| {
            StrategyError::Rule(format!(
                "signal {} has no trade instructions",
                signal.signal_id
            ))
        })?;
        let exit_price = instruction
            .price_policy
            .reference_price
            .or(instruction.price_policy.limit_price)
            .ok_or_else(|| {
                StrategyError::Rule(format!(
                    "signal {} primary instruction has no reference or limit price",
                    signal.signal_id
                ))
            })?;
        let mut state = self.state.lock().expect("position store lock poisoned");
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
                state
                    .positions
                    .values()
                    .filter(|position| {
                        position.ssu_id == signal.ssu_id
                            && position.trade_instrument == instruction.instrument_name
                            && position.side == side
                            && position.status == PositionStatus::Open
                    })
                    .max_by_key(|position| position.entry_at)
                    .map(|position| position.position_id.clone())
            });
        let Some(position_id) = position_id else {
            return Err(StrategyError::Rule(format!(
                "no open virtual position found for SSU {} instrument {}",
                signal.ssu_id, instruction.instrument_name
            )));
        };
        let position = state
            .positions
            .get_mut(&position_id)
            .expect("position must exist");
        position.exit_price = Some(exit_price);
        position.exit_at = Some(signal.generated_at);
        position.exit_reason = Some(signal.reason.clone());
        position.pnl = Some(compute_pnl(position.side, position.entry_price, exit_price));
        position.status = PositionStatus::Closed;
        persist_signal(&self.sqlite, signal)?;
        persist_close_position(&self.sqlite, position)?;
        Ok(position.clone())
    }
}

#[derive(Debug)]
pub struct SqliteStrategyTradeContextStore {
    sqlite: StrategySqlite,
}

impl SqliteStrategyTradeContextStore {
    pub fn new(path: impl Into<String>) -> Result<Self, StrategyError> {
        Ok(Self {
            sqlite: StrategySqlite::new(path.into())?,
        })
    }
}

impl StrategyTradeContextStore for SqliteStrategyTradeContextStore {
    fn save_context(
        &self,
        position_id: &str,
        ssu_id: i64,
        strategy_key: &str,
        trigger_instrument: &str,
        metadata: &serde_json::Value,
        updated_at: u64,
    ) -> Result<(), StrategyError> {
        let connection = self.sqlite.open_connection()?;
        connection.execute(
            "\
            INSERT OR REPLACE INTO strategy_trade_context (
                position_id,
                ssu_id,
                strategy_key,
                trigger_instrument,
                metadata_json,
                updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            ",
            params![
                position_id,
                ssu_id,
                strategy_key,
                trigger_instrument,
                serde_json::to_string(metadata)?,
                updated_at as i64,
            ],
        )?;
        Ok(())
    }

    fn load_context(&self, position_id: &str) -> Result<Option<serde_json::Value>, StrategyError> {
        let connection = self.sqlite.open_connection()?;
        let mut statement = connection.prepare(
            "\
            SELECT metadata_json
            FROM strategy_trade_context
            WHERE position_id = ?1
            ",
        )?;
        let mut rows = statement.query(params![position_id])?;
        let Some(row) = rows.next()? else {
            return Ok(None);
        };
        let metadata_json: String = row.get(0)?;
        Ok(Some(serde_json::from_str(&metadata_json)?))
    }

    fn load_open_contexts(
        &self,
        ssu_id: i64,
        trigger_instrument: &str,
    ) -> Result<Vec<(String, serde_json::Value)>, StrategyError> {
        let connection = self.sqlite.open_connection()?;
        let mut statement = connection.prepare(
            "\
            SELECT c.position_id, c.metadata_json
            FROM strategy_trade_context c
            INNER JOIN virtual_position v ON v.position_id = c.position_id
            WHERE c.ssu_id = ?1
              AND c.trigger_instrument = ?2
              AND v.status = 'OPEN'
            ORDER BY v.entry_at, c.position_id
            ",
        )?;
        let rows = statement.query_map(params![ssu_id, trigger_instrument], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut contexts = Vec::new();
        for row in rows {
            let (position_id, metadata_json) = row?;
            contexts.push((position_id, serde_json::from_str(&metadata_json)?));
        }
        Ok(contexts)
    }

    fn update_context(
        &self,
        position_id: &str,
        metadata: &serde_json::Value,
        updated_at: u64,
    ) -> Result<(), StrategyError> {
        let connection = self.sqlite.open_connection()?;
        let updated = connection.execute(
            "\
            UPDATE strategy_trade_context
            SET metadata_json = ?2, updated_at = ?3
            WHERE position_id = ?1
            ",
            params![
                position_id,
                serde_json::to_string(metadata)?,
                updated_at as i64,
            ],
        )?;
        if updated == 0 {
            return Err(StrategyError::NotFound(format!(
                "missing strategy trade context for {position_id}"
            )));
        }
        Ok(())
    }

    fn delete_context(&self, position_id: &str) -> Result<(), StrategyError> {
        let connection = self.sqlite.open_connection()?;
        connection.execute(
            "DELETE FROM strategy_trade_context WHERE position_id = ?1",
            params![position_id],
        )?;
        Ok(())
    }
}

#[derive(Debug)]
struct TelegramSignalSink;

impl SignalSink for TelegramSignalSink {
    fn consume(&self, signal: &StrategySignal) -> Result<(), StrategyError> {
        let component = "STRATEGY:SIGNAL";
        notify_message(component, signal_message(signal));
        Ok(())
    }
}

#[derive(Debug)]
struct StrategyPositionState {
    positions: BTreeMap<String, StrategyPosition>,
    next_id: u64,
}

#[derive(Clone)]
struct LoadedSsu {
    config: SsuConfig,
    strategy: Arc<dyn Strategy>,
}

pub struct StrategyRuntime {
    prices: Arc<dyn PriceStore>,
    timeframes: Arc<SharedTimeframeEngine>,
    strategy_positions: Arc<dyn StrategyPositionBook>,
    trade_contexts: Arc<dyn StrategyTradeContextStore>,
    repository: Arc<dyn SsuRepository>,
    factory: Arc<dyn StrategyFactory>,
    signal_router: SignalRouter,
    historical: Arc<dyn HistoricalReplayStore>,
    warmup_spot_instruments: Vec<String>,
    warmup_bars: usize,
    active_ssus: RwLock<Vec<LoadedSsu>>,
    instrument_locks: Mutex<BTreeMap<String, Arc<Mutex<()>>>>,
    lifecycle_gate: RwLock<()>,
}

pub type StrategyRuntimeHandle = StrategyRuntime;

impl StrategyRuntime {
    pub(crate) fn new(
        repository: Arc<dyn SsuRepository>,
        factory: Arc<dyn StrategyFactory>,
        historical: Arc<dyn HistoricalReplayStore>,
        strategy_positions: Arc<dyn StrategyPositionBook>,
        trade_contexts: Arc<dyn StrategyTradeContextStore>,
        signal_router: SignalRouter,
        warmup_spot_instruments: Vec<String>,
        warmup_bars: usize,
        recent_bars: usize,
        candle_alignments: CandleAlignmentMap,
    ) -> Self {
        Self {
            prices: Arc::new(InMemoryPriceStore::new()),
            timeframes: Arc::new(SharedTimeframeEngine::with_alignments(
                recent_bars,
                candle_alignments,
            )),
            strategy_positions,
            trade_contexts,
            repository,
            factory,
            signal_router,
            historical,
            warmup_spot_instruments,
            warmup_bars,
            active_ssus: RwLock::new(Vec::new()),
            instrument_locks: Mutex::new(BTreeMap::new()),
            lifecycle_gate: RwLock::new(()),
        }
    }

    pub fn reload_ssus(&self) -> Result<usize, StrategyError> {
        let _gate = self
            .lifecycle_gate
            .write()
            .expect("strategy lifecycle gate poisoned");
        let configs = self.repository.load_active_ssus()?;
        self.timeframes.reset_ssus();

        let mut loaded = Vec::with_capacity(configs.len());
        for config in configs {
            let strategy = self.factory.get(&config.strategy_key)?;
            self.timeframes.register_ssu(&config)?;

            let mut required = config
                .required_timeframes
                .iter()
                .copied()
                .collect::<BTreeSet<_>>();
            required.extend(config.indicator_specs.iter().map(|spec| spec.timeframe));
            for instrument in &self.warmup_spot_instruments {
                for timeframe in &required {
                    let existing = self.timeframes.recent_bars(
                        instrument,
                        *timeframe,
                        self.warmup_bars.max(1),
                    );
                    let bars = if existing.is_empty() {
                        self.historical.load_bars(
                            instrument,
                            *timeframe,
                            self.warmup_bars.max(1),
                        )?
                    } else {
                        existing
                    };
                    if !bars.is_empty() {
                        self.timeframes
                            .warmup(instrument, *timeframe, &bars, config.ssu_id)?;
                    }
                }

                let ctx = StrategyContext {
                    prices: Arc::clone(&self.prices),
                    timeframes: Arc::clone(&self.timeframes) as Arc<dyn TimeframeEngine>,
                    strategy_positions: Arc::clone(&self.strategy_positions),
                    trade_contexts: Arc::clone(&self.trade_contexts),
                };
                strategy.warmup(&ctx, &config, instrument)?;
            }

            loaded.push(LoadedSsu { config, strategy });
        }

        *self.active_ssus.write().expect("active SSU lock poisoned") = loaded;
        Ok(self
            .active_ssus
            .read()
            .expect("active SSU lock poisoned")
            .len())
    }

    pub fn on_tick(
        &self,
        instrument: &str,
        ltp: f64,
        at: u64,
        trigger_for_strategy: bool,
    ) -> Result<(), StrategyError> {
        let instrument_lock = self.instrument_lock(instrument);
        let _instrument_guard = instrument_lock.lock().expect("instrument lock poisoned");
        let _gate = self
            .lifecycle_gate
            .read()
            .expect("strategy lifecycle gate poisoned");

        self.prices.put_price(instrument, ltp, at);
        let tf_update = self.timeframes.on_tick(instrument, ltp, at)?;
        if !trigger_for_strategy {
            return Ok(());
        }

        let ctx = self.strategy_context();
        let mut ticks = BTreeMap::new();
        ticks.insert(
            instrument.to_string(),
            Tick {
                price: ltp,
                volume: 0.0,
            },
        );
        self.dispatch_market_event(
            &ctx,
            &MarketEvent::Tick(TickSnapshot {
                event_ts: at,
                ticks,
            }),
        )?;

        if let Some(snapshot) = self.candle_snapshot_from_update(&tf_update)? {
            self.dispatch_market_event(&ctx, &MarketEvent::Candles(snapshot))?;
        }

        Ok(())
    }

    pub fn warmup_closed_bars(&self, bars: &[Bar]) -> Result<(), StrategyError> {
        let _gate = self
            .lifecycle_gate
            .read()
            .expect("strategy lifecycle gate poisoned");
        let ssus = self
            .active_ssus
            .read()
            .expect("active SSU lock poisoned")
            .clone();

        for bar in bars {
            for loaded in &ssus {
                let required_by_ssu = loaded.config.required_timeframes.contains(&bar.timeframe)
                    || loaded
                        .config
                        .indicator_specs
                        .iter()
                        .any(|spec| spec.timeframe == bar.timeframe);
                if required_by_ssu {
                    self.timeframes.warmup(
                        &bar.instrument,
                        bar.timeframe,
                        std::slice::from_ref(bar),
                        loaded.config.ssu_id,
                    )?;
                }
            }
        }

        Ok(())
    }

    pub fn on_closed_bar(&self, bar: Bar, trigger_for_strategy: bool) -> Result<(), StrategyError> {
        self.on_closed_bars(std::slice::from_ref(&bar), trigger_for_strategy)
    }

    pub fn on_closed_bars(
        &self,
        bars: &[Bar],
        trigger_for_strategy: bool,
    ) -> Result<(), StrategyError> {
        if bars.is_empty() {
            return Ok(());
        }

        let event_ts = bars[0].end_at;
        if bars.iter().any(|bar| bar.end_at != event_ts) {
            return Err(StrategyError::Rule(
                "closed-bar snapshot requires all candles to share the same end_at".to_string(),
            ));
        }

        let instruments = bars
            .iter()
            .map(|bar| bar.instrument.as_str())
            .collect::<BTreeSet<_>>();
        let locks = instruments
            .iter()
            .map(|instrument| self.instrument_lock(instrument))
            .collect::<Vec<_>>();
        let _instrument_guards = locks
            .iter()
            .map(|lock| lock.lock().expect("instrument lock poisoned"))
            .collect::<Vec<_>>();
        let _gate = self
            .lifecycle_gate
            .read()
            .expect("strategy lifecycle gate poisoned");

        let mut candles = BTreeMap::new();
        for bar in bars {
            self.prices
                .put_price(&bar.instrument, bar.close, bar.end_at);
            let tf_update = self.timeframes.on_closed_bar(bar)?;
            if tf_update.closed_timeframes.contains(&bar.timeframe) {
                candles
                    .entry(bar.instrument.clone())
                    .or_insert_with(BTreeMap::new)
                    .insert(bar.timeframe, TimedCandle::from_bar(bar));
            }
        }

        if !trigger_for_strategy || candles.is_empty() {
            return Ok(());
        }

        let ctx = self.strategy_context();
        self.dispatch_market_event(
            &ctx,
            &MarketEvent::Candles(CandleSnapshot { event_ts, candles }),
        )?;

        Ok(())
    }

    pub fn active_ssu_count(&self) -> usize {
        self.active_ssus
            .read()
            .expect("active SSU lock poisoned")
            .len()
    }

    fn strategy_context(&self) -> StrategyContext {
        StrategyContext {
            prices: Arc::clone(&self.prices),
            timeframes: Arc::clone(&self.timeframes) as Arc<dyn TimeframeEngine>,
            strategy_positions: Arc::clone(&self.strategy_positions),
            trade_contexts: Arc::clone(&self.trade_contexts),
        }
    }

    fn dispatch_market_event(
        &self,
        ctx: &StrategyContext,
        event: &MarketEvent,
    ) -> Result<(), StrategyError> {
        let ssus = self
            .active_ssus
            .read()
            .expect("active SSU lock poisoned")
            .clone();
        for loaded in ssus {
            let signals = loaded
                .strategy
                .on_market_event(ctx, &loaded.config, event)?;
            for signal in signals {
                self.signal_router.route(&signal)?;
            }
        }
        Ok(())
    }

    fn candle_snapshot_from_update(
        &self,
        update: &TimeframeUpdate,
    ) -> Result<Option<CandleSnapshot>, StrategyError> {
        if update.closed_timeframes.is_empty() {
            return Ok(None);
        }

        let mut candles = BTreeMap::new();
        let mut event_ts = 0;
        for timeframe in &update.closed_timeframes {
            let Some(bar) = self
                .timeframes
                .last_closed_bar(&update.instrument, *timeframe)
            else {
                return Err(StrategyError::NotFound(format!(
                    "missing closed candle for {} {:?} at {}",
                    update.instrument, timeframe, update.tick_at
                )));
            };
            event_ts = event_ts.max(bar.end_at);
            candles
                .entry(update.instrument.clone())
                .or_insert_with(BTreeMap::new)
                .insert(*timeframe, TimedCandle::from_bar(&bar));
        }

        Ok(Some(CandleSnapshot { event_ts, candles }))
    }

    fn instrument_lock(&self, instrument: &str) -> Arc<Mutex<()>> {
        let mut locks = self
            .instrument_locks
            .lock()
            .expect("strategy instrument locks poisoned");
        locks
            .entry(instrument.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }
}

pub fn start_strategy_runtime(
    config: &AppConfig,
) -> Result<Option<Arc<StrategyRuntimeHandle>>, StrategyError> {
    let Some(strategy_config) = config.strategy.as_ref().filter(|config| config.enabled) else {
        return Ok(None);
    };
    let historical_config = config
        .historical_candles
        .as_ref()
        .filter(|config| config.enabled)
        .ok_or_else(|| {
            StrategyError::Config(
                "strategy runtime requires enabled historical_candles sqlite".to_string(),
            )
        })?;
    let warmup_spot_instruments = load_spot_instruments(config)?;
    let candle_alignments = load_candle_alignments(config)?;
    crate::strategy::diagnostics::configure(&config.strategy_runtime.diagnostics);
    let repository = Arc::new(SqliteSsuRepository::new(
        strategy_config.sqlite_path.clone(),
    )?);
    let strategy_positions = Arc::new(SqliteStrategyPositionBook::new(
        strategy_config.sqlite_path.clone(),
    )?);
    let trade_contexts = Arc::new(SqliteStrategyTradeContextStore::new(
        strategy_config.sqlite_path.clone(),
    )?);
    let historical = Arc::new(
        crate::strategy::SqliteHistoricalReplayStore::with_alignments(
            historical_config.sqlite_path.clone(),
            candle_alignments.clone(),
        ),
    );
    let runtime = Arc::new(StrategyRuntime::new(
        repository,
        Arc::new(BuiltinStrategyFactory),
        historical,
        strategy_positions,
        trade_contexts,
        SignalRouter::new(vec![Arc::new(TelegramSignalSink)]),
        warmup_spot_instruments,
        strategy_config.warmup_bars,
        strategy_config.recent_bars,
        candle_alignments,
    ));
    let loaded_ssus = runtime.reload_ssus()?;
    println!("Strategy runtime loaded: active_ssus={loaded_ssus}");
    Ok(Some(runtime))
}

fn load_spot_instruments(config: &AppConfig) -> Result<Vec<String>, StrategyError> {
    let mut instruments = BTreeSet::new();

    if config.feed_broker_enabled("DELTA") {
        let Some(delta) = config.brokers.delta.as_ref() else {
            return Err(StrategyError::Config(
                "missing brokers.delta config".to_string(),
            ));
        };
        let catalog = InstrumentCatalog::load_csv(&delta.base_instruments_csv)
            .map_err(|error| StrategyError::Config(error.to_string()))?;
        collect_spot_instruments(&catalog, &mut instruments);
    }

    if config.feed_broker_enabled("FYERS") {
        let Some(fyers) = config.brokers.fyers.as_ref() else {
            return Err(StrategyError::Config(
                "missing brokers.fyers config".to_string(),
            ));
        };
        let catalog = InstrumentCatalog::load_csv(&fyers.base_instruments_csv)
            .map_err(|error| StrategyError::Config(error.to_string()))?;
        collect_spot_instruments(&catalog, &mut instruments);
    }

    if config.feed_broker_enabled("ANGELONE") {
        let Some(angelone) = config.brokers.angelone.as_ref() else {
            return Err(StrategyError::Config(
                "missing brokers.angelone config".to_string(),
            ));
        };
        let catalog = InstrumentCatalog::load_csv(&angelone.base_instruments_csv)
            .map_err(|error| StrategyError::Config(error.to_string()))?;
        collect_spot_instruments(&catalog, &mut instruments);
    }

    Ok(instruments.into_iter().collect())
}

fn load_candle_alignments(config: &AppConfig) -> Result<CandleAlignmentMap, StrategyError> {
    let mut alignments = CandleAlignmentMap::new();
    let market_sessions = MarketSessionSchedule::from_configs(&config.market_sessions)
        .map_err(|error| StrategyError::Config(error.to_string()))?;

    if config.feed_broker_enabled("DELTA") {
        let Some(delta) = config.brokers.delta.as_ref() else {
            return Err(StrategyError::Config(
                "missing brokers.delta config".to_string(),
            ));
        };
        let catalog = InstrumentCatalog::load_csv(&delta.base_instruments_csv)
            .map_err(|error| StrategyError::Config(error.to_string()))?;
        merge_candle_alignments(
            &mut alignments,
            candle_alignments_from_catalog(&catalog, &market_sessions),
        );
    }

    if config.feed_broker_enabled("FYERS") {
        let Some(fyers) = config.brokers.fyers.as_ref() else {
            return Err(StrategyError::Config(
                "missing brokers.fyers config".to_string(),
            ));
        };
        let catalog = InstrumentCatalog::load_csv(&fyers.base_instruments_csv)
            .map_err(|error| StrategyError::Config(error.to_string()))?;
        merge_candle_alignments(
            &mut alignments,
            candle_alignments_from_catalog(&catalog, &market_sessions),
        );
    }

    if config.feed_broker_enabled("ANGELONE") {
        let Some(angelone) = config.brokers.angelone.as_ref() else {
            return Err(StrategyError::Config(
                "missing brokers.angelone config".to_string(),
            ));
        };
        let catalog = InstrumentCatalog::load_csv(&angelone.base_instruments_csv)
            .map_err(|error| StrategyError::Config(error.to_string()))?;
        merge_candle_alignments(
            &mut alignments,
            candle_alignments_from_catalog(&catalog, &market_sessions),
        );
    }

    Ok(alignments)
}

fn collect_spot_instruments(catalog: &InstrumentCatalog, instruments: &mut BTreeSet<String>) {
    for instrument in catalog
        .instruments()
        .filter(|instrument| instrument.instrument_type == InstrumentType::Spot)
        .filter(|instrument| instrument.tradable)
    {
        instruments.insert(instrument.instrument_name.to_string());
    }
}

fn parse_timeframes_json(value: &str) -> Result<Vec<Timeframe>, StrategyError> {
    if value.trim().is_empty() {
        return Ok(Vec::new());
    }

    serde_json::from_str::<Vec<String>>(value)?
        .into_iter()
        .map(|value| parse_timeframe(&value))
        .collect()
}

fn parse_indicator_specs_json(value: &str) -> Result<Vec<IndicatorSpec>, StrategyError> {
    #[derive(serde::Deserialize)]
    struct RawIndicatorSpec {
        key: String,
        timeframe: String,
        kind: String,
        #[serde(default)]
        params_json: serde_json::Value,
    }

    if value.trim().is_empty() {
        return Ok(Vec::new());
    }

    serde_json::from_str::<Vec<RawIndicatorSpec>>(value)?
        .into_iter()
        .map(|spec| {
            Ok(IndicatorSpec {
                key: spec.key,
                timeframe: parse_timeframe(&spec.timeframe)?,
                kind: spec.kind,
                params_json: if spec.params_json.is_null() {
                    "{}".to_string()
                } else {
                    spec.params_json.to_string()
                },
            })
        })
        .collect()
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
            "unsupported timeframe value {value}"
        ))),
    }
}

fn persist_open_position(
    sqlite: &StrategySqlite,
    position: &StrategyPosition,
) -> Result<(), StrategyError> {
    let connection = sqlite.open_connection()?;
    connection.execute(
        "\
        INSERT INTO virtual_position (
            position_id,
            ssu_id,
            trigger_instrument,
            trade_instrument,
            side,
            status,
            entry_price,
            entry_at,
            exit_price,
            exit_at,
            exit_reason,
            pnl
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
        ",
        params![
            position.position_id,
            position.ssu_id,
            position.trigger_instrument,
            position.trade_instrument,
            side_label(position.side),
            status_label(position.status),
            position.entry_price,
            position.entry_at as i64,
            position.exit_price,
            position.exit_at.map(|value| value as i64),
            position.exit_reason,
            position.pnl,
        ],
    )?;
    Ok(())
}

fn persist_close_position(
    sqlite: &StrategySqlite,
    position: &StrategyPosition,
) -> Result<(), StrategyError> {
    let connection = sqlite.open_connection()?;
    connection.execute(
        "\
        UPDATE virtual_position
        SET
            status = ?2,
            exit_price = ?3,
            exit_at = ?4,
            exit_reason = ?5,
            pnl = ?6
        WHERE position_id = ?1
        ",
        params![
            position.position_id,
            status_label(position.status),
            position.exit_price,
            position.exit_at.map(|value| value as i64),
            position.exit_reason,
            position.pnl,
        ],
    )?;
    Ok(())
}

fn persist_signal(sqlite: &StrategySqlite, signal: &StrategySignal) -> Result<(), StrategyError> {
    let mut connection = sqlite.open_connection()?;
    let transaction = connection.transaction()?;
    transaction.execute(
        "\
        INSERT INTO strategy_signal (
            signal_id,
            ssu_id,
            strategy_key,
            campaign_id,
            trigger_instrument,
            signal_type,
            generated_at,
            reason,
            metadata_json
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
        ",
        params![
            signal.signal_id,
            signal.ssu_id,
            signal.strategy_key,
            signal.campaign_id,
            signal.trigger_instrument,
            signal_type_label(signal.signal_type),
            signal.generated_at as i64,
            signal.reason,
            serde_json::to_string(&signal.metadata)?,
        ],
    )?;
    for instruction in &signal.instructions {
        transaction.execute(
            "\
            INSERT INTO strategy_signal_instruction (
                instruction_id,
                signal_id,
                leg_id,
                action,
                instrument_id,
                instrument_name,
                instrument_kind,
                leg_role,
                quantity_ratio,
                price_policy_json,
                metadata_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
            ",
            params![
                instruction.instruction_id,
                signal.signal_id,
                instruction.leg_id,
                trade_action_label(instruction.action),
                instruction.instrument_id,
                instruction.instrument_name,
                instrument_kind_label(instruction.instrument_kind),
                instruction.leg_role,
                instruction.quantity_ratio,
                serde_json::to_string(&instruction.price_policy)?,
                serde_json::to_string(&instruction.metadata)?,
            ],
        )?;
    }
    transaction.commit()?;
    Ok(())
}

fn extract_position_seq(position_id: &str) -> u64 {
    position_id
        .rsplit_once('-')
        .and_then(|(_, value)| value.parse::<u64>().ok())
        .unwrap_or(0)
}

fn compute_pnl(side: SignalSide, entry: f64, exit: f64) -> f64 {
    match side {
        SignalSide::Long => exit - entry,
        SignalSide::Short => entry - exit,
    }
}

fn side_label(side: SignalSide) -> &'static str {
    match side {
        SignalSide::Long => "LONG",
        SignalSide::Short => "SHORT",
    }
}

fn parse_side(value: &str) -> Result<SignalSide, StrategyError> {
    match value.trim().to_ascii_uppercase().as_str() {
        "LONG" => Ok(SignalSide::Long),
        "SHORT" => Ok(SignalSide::Short),
        value => Err(StrategyError::Parse(format!("unsupported side {value}"))),
    }
}

fn status_label(status: PositionStatus) -> &'static str {
    match status {
        PositionStatus::Open => "OPEN",
        PositionStatus::Closed => "CLOSED",
    }
}

fn parse_status(value: &str) -> Result<PositionStatus, StrategyError> {
    match value.trim().to_ascii_uppercase().as_str() {
        "OPEN" => Ok(PositionStatus::Open),
        "CLOSED" => Ok(PositionStatus::Closed),
        value => Err(StrategyError::Parse(format!(
            "unsupported position status {value}"
        ))),
    }
}

fn to_rusqlite_error(error: StrategyError) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
}

fn ist_day_key(unix_millis: u64) -> Result<String, StrategyError> {
    let ist = FixedOffset::east_opt(IST_OFFSET_SECONDS)
        .ok_or_else(|| StrategyError::Config("failed to create IST fixed offset".to_string()))?;
    let utc = chrono::DateTime::from_timestamp_millis(unix_millis as i64)
        .ok_or_else(|| StrategyError::Parse(format!("invalid unix millis {unix_millis}")))?;
    let ist_dt = utc.with_timezone(&ist);
    Ok(format!(
        "{:04}-{:02}-{:02}",
        ist_dt.year(),
        ist_dt.month(),
        ist_dt.day()
    ))
}

fn signal_label(signal: &StrategySignal) -> &'static str {
    signal_type_label(signal.signal_type)
}

fn signal_message(signal: &StrategySignal) -> String {
    format!(
        "SIGNAL | type={} | ssu={} | strategy={} | campaign={} | instructions={} | reason={} | at={}",
        signal_type_label(signal.signal_type),
        signal.ssu_id,
        signal.strategy_key,
        signal.campaign_id,
        signal.instructions.len(),
        signal.reason,
        signal.generated_at
    )
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::strategy::Bar;

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
    fn position_store_enforces_ssu_limits() {
        let store =
            SqliteStrategyPositionBook::new(temp_sqlite("strategy-position-store")).expect("store");
        let ssu = SsuConfig {
            ssu_id: 11,
            strategy_key: "noop".to_string(),
            enabled: true,
            trade_gap_secs: 60,
            max_overlap: 1,
            max_positions_per_day: 1,
            required_timeframes: vec![Timeframe::FiveMinute],
            indicator_specs: Vec::new(),
            params_json: "{}".to_string(),
        };
        let signal = StrategySignal::single_leg_entry(
            11,
            "noop",
            "NIFTY",
            SignalSide::Long,
            100.0,
            "entry".to_string(),
            1_700_000_000_000,
        );

        store.open_position(&signal, &ssu).expect("open");
        assert!(matches!(
            store.open_position(&signal, &ssu),
            Err(StrategyError::Rule(_))
        ));
    }

    struct FakeRepository {
        ssus: Vec<SsuConfig>,
    }

    impl SsuRepository for FakeRepository {
        fn load_active_ssus(&self) -> Result<Vec<SsuConfig>, StrategyError> {
            Ok(self.ssus.clone())
        }
    }

    struct FakeHistorical;

    impl HistoricalReplayStore for FakeHistorical {
        fn load_bars(
            &self,
            instrument: &str,
            timeframe: Timeframe,
            limit: usize,
        ) -> Result<Vec<Bar>, StrategyError> {
            Ok((0..limit.min(20))
                .map(|index| Bar {
                    instrument: instrument.to_string(),
                    timeframe,
                    start_at: 1_700_000_000_000 + index as u64 * 300_000,
                    end_at: 1_700_000_300_000 + index as u64 * 300_000,
                    open: 100.0 + index as f64,
                    high: 101.0 + index as f64,
                    low: 99.0 + index as f64,
                    close: 100.5 + index as f64,
                    volume: 0.0,
                    is_closed: true,
                })
                .collect())
        }
    }

    #[derive(Debug)]
    struct CountingStrategy {
        calls: Arc<AtomicUsize>,
    }

    impl Strategy for CountingStrategy {
        fn strategy_key(&self) -> &'static str {
            "counting"
        }

        fn on_market_event(
            &self,
            _ctx: &StrategyContext,
            _ssu: &SsuConfig,
            _event: &MarketEvent,
        ) -> Result<Vec<StrategySignal>, StrategyError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(Vec::new())
        }
    }

    #[derive(Debug)]
    struct RecordingStrategy {
        events: Arc<Mutex<Vec<MarketEvent>>>,
    }

    impl Strategy for RecordingStrategy {
        fn strategy_key(&self) -> &'static str {
            "recording"
        }

        fn on_market_event(
            &self,
            _ctx: &StrategyContext,
            _ssu: &SsuConfig,
            event: &MarketEvent,
        ) -> Result<Vec<StrategySignal>, StrategyError> {
            self.events
                .lock()
                .expect("events lock poisoned")
                .push(event.clone());
            Ok(Vec::new())
        }
    }

    #[derive(Debug)]
    struct WarmupCountingStrategy {
        calls: Arc<AtomicUsize>,
        bars_seen: Arc<AtomicUsize>,
    }

    impl Strategy for WarmupCountingStrategy {
        fn strategy_key(&self) -> &'static str {
            "warmup_counting"
        }

        fn warmup(
            &self,
            ctx: &StrategyContext,
            _ssu: &SsuConfig,
            instrument: &str,
        ) -> Result<(), StrategyError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.bars_seen.fetch_add(
                ctx.timeframes
                    .recent_bars(instrument, Timeframe::FiveMinute, 64)
                    .len(),
                Ordering::Relaxed,
            );
            Ok(())
        }

        fn on_market_event(
            &self,
            _ctx: &StrategyContext,
            _ssu: &SsuConfig,
            _event: &MarketEvent,
        ) -> Result<Vec<StrategySignal>, StrategyError> {
            Ok(Vec::new())
        }
    }

    struct FakeFactory {
        strategy: Arc<dyn Strategy>,
    }

    impl StrategyFactory for FakeFactory {
        fn get(&self, _strategy_key: &str) -> Result<Arc<dyn Strategy>, StrategyError> {
            Ok(Arc::clone(&self.strategy))
        }
    }

    #[test]
    fn runtime_dispatches_spot_trigger_to_all_active_ssus() {
        let calls = Arc::new(AtomicUsize::new(0));
        let runtime = StrategyRuntime::new(
            Arc::new(FakeRepository {
                ssus: vec![
                    SsuConfig {
                        ssu_id: 1,
                        strategy_key: "counting".to_string(),
                        enabled: true,
                        trade_gap_secs: 0,
                        max_overlap: 0,
                        max_positions_per_day: 0,
                        required_timeframes: vec![Timeframe::FiveMinute],
                        indicator_specs: vec![IndicatorSpec {
                            key: "ema20".to_string(),
                            timeframe: Timeframe::FiveMinute,
                            kind: "ema".to_string(),
                            params_json: r#"{"period":20}"#.to_string(),
                        }],
                        params_json: "{}".to_string(),
                    },
                    SsuConfig {
                        ssu_id: 2,
                        strategy_key: "counting".to_string(),
                        enabled: true,
                        trade_gap_secs: 0,
                        max_overlap: 0,
                        max_positions_per_day: 0,
                        required_timeframes: vec![Timeframe::FiveMinute],
                        indicator_specs: vec![IndicatorSpec {
                            key: "ema20".to_string(),
                            timeframe: Timeframe::FiveMinute,
                            kind: "ema".to_string(),
                            params_json: r#"{"period":20}"#.to_string(),
                        }],
                        params_json: "{}".to_string(),
                    },
                ],
            }),
            Arc::new(FakeFactory {
                strategy: Arc::new(CountingStrategy {
                    calls: Arc::clone(&calls),
                }),
            }),
            Arc::new(FakeHistorical),
            Arc::new(
                SqliteStrategyPositionBook::new(temp_sqlite("strategy-runtime"))
                    .expect("positions"),
            ),
            Arc::new(
                SqliteStrategyTradeContextStore::new(temp_sqlite("strategy-runtime-context"))
                    .expect("contexts"),
            ),
            SignalRouter::new(vec![Arc::new(InMemorySignalSink::new())]),
            vec!["NIFTY".to_string()],
            32,
            32,
            CandleAlignmentMap::new(),
        );

        runtime.reload_ssus().expect("reload");
        runtime
            .on_tick("NIFTY", 100.0, 1_700_000_000_000, true)
            .expect("tick");

        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn runtime_candle_snapshot_event_ts_uses_closed_candle_end() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let runtime = StrategyRuntime::new(
            Arc::new(FakeRepository {
                ssus: vec![SsuConfig {
                    ssu_id: 1,
                    strategy_key: "recording".to_string(),
                    enabled: true,
                    trade_gap_secs: 0,
                    max_overlap: 0,
                    max_positions_per_day: 0,
                    required_timeframes: vec![Timeframe::OneMinute],
                    indicator_specs: Vec::new(),
                    params_json: "{}".to_string(),
                }],
            }),
            Arc::new(FakeFactory {
                strategy: Arc::new(RecordingStrategy {
                    events: Arc::clone(&events),
                }),
            }),
            Arc::new(FakeHistorical),
            Arc::new(
                SqliteStrategyPositionBook::new(temp_sqlite("strategy-runtime-recording"))
                    .expect("positions"),
            ),
            Arc::new(
                SqliteStrategyTradeContextStore::new(temp_sqlite(
                    "strategy-runtime-recording-context",
                ))
                .expect("contexts"),
            ),
            SignalRouter::new(Vec::new()),
            Vec::new(),
            0,
            32,
            CandleAlignmentMap::new(),
        );

        runtime.reload_ssus().expect("reload");
        runtime.on_tick("NIFTY", 100.0, 10_000, true).expect("tick");
        runtime
            .on_tick("NIFTY", 101.0, 60_500, true)
            .expect("rollover tick");

        let events = events.lock().expect("events lock poisoned");
        assert!(matches!(&events[0], MarketEvent::Tick(snapshot) if snapshot.event_ts == 10_000));
        assert!(matches!(&events[1], MarketEvent::Tick(snapshot) if snapshot.event_ts == 60_500));
        match &events[2] {
            MarketEvent::Candles(snapshot) => {
                assert_eq!(snapshot.event_ts, 60_000);
                let candle = snapshot
                    .candles
                    .get("NIFTY")
                    .and_then(|by_timeframe| by_timeframe.get(&Timeframe::OneMinute))
                    .expect("one-minute candle");
                assert_eq!(candle.start_ts, 0);
                assert_eq!(candle.end_ts, 60_000);
            }
            MarketEvent::Tick(_) => panic!("expected candle snapshot"),
        }
    }

    #[test]
    fn runtime_calls_strategy_warmup_after_timeframe_warmup() {
        let calls = Arc::new(AtomicUsize::new(0));
        let bars_seen = Arc::new(AtomicUsize::new(0));
        let runtime = StrategyRuntime::new(
            Arc::new(FakeRepository {
                ssus: vec![SsuConfig {
                    ssu_id: 1,
                    strategy_key: "warmup_counting".to_string(),
                    enabled: true,
                    trade_gap_secs: 0,
                    max_overlap: 0,
                    max_positions_per_day: 0,
                    required_timeframes: vec![Timeframe::FiveMinute],
                    indicator_specs: Vec::new(),
                    params_json: "{}".to_string(),
                }],
            }),
            Arc::new(FakeFactory {
                strategy: Arc::new(WarmupCountingStrategy {
                    calls: Arc::clone(&calls),
                    bars_seen: Arc::clone(&bars_seen),
                }),
            }),
            Arc::new(FakeHistorical),
            Arc::new(
                SqliteStrategyPositionBook::new(temp_sqlite("strategy-runtime-warmup"))
                    .expect("positions"),
            ),
            Arc::new(
                SqliteStrategyTradeContextStore::new(temp_sqlite(
                    "strategy-runtime-warmup-context",
                ))
                .expect("contexts"),
            ),
            SignalRouter::new(vec![Arc::new(InMemorySignalSink::new())]),
            vec!["NIFTY".to_string()],
            32,
            32,
            CandleAlignmentMap::new(),
        );

        runtime.reload_ssus().expect("reload");

        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(bars_seen.load(Ordering::Relaxed), 20);
    }
}
