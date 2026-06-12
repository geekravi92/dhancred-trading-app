use std::collections::{BTreeSet, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use chrono::Utc;

use crate::backtest::candle_feed::HistoricalCandleFeed;
use crate::backtest::report::{BacktestReportInput, BacktestReportSummary, write_report};
pub use crate::backtest::stores::{BacktestExecutionConfig, FundingChargeMode};
use crate::backtest::stores::{BacktestPositionBook, InMemoryTradeContextStore};
use crate::config::AppConfig;
use crate::feeder::{
    CandleAlignmentMap, InstrumentCatalog, InstrumentType, Timeframe,
    candle_alignments_from_catalog, merge_candle_alignments,
};
use crate::strategy::{
    BuiltinStrategyFactory, HistoricalReplayStore, SignalRouter, SqliteHistoricalReplayStore,
    SqliteSsuRepository, SsuConfig, SsuRepository, StrategyError, StrategyRuntime,
};

#[derive(Clone, Debug, Default)]
pub struct BacktestExecutionOverrides {
    pub slippage_pct: Option<f64>,
    pub brokerage_pct: Option<f64>,
    pub entry_fee_pct: Option<f64>,
    pub exit_fee_pct: Option<f64>,
    pub fee_tax_pct: Option<f64>,
    pub fixed_fee_per_order: Option<f64>,
    pub funding_rate_pct: Option<f64>,
    pub funding_interval_hours: Option<u64>,
    pub funding_charge_mode: Option<FundingChargeMode>,
}

#[derive(Clone, Debug)]
pub struct BacktestRequest {
    pub config_path: PathBuf,
    pub from: u64,
    pub to: u64,
    pub instruments: Vec<String>,
    pub strategy_key: Option<String>,
    pub ssu_ids: Vec<i64>,
    pub output_dir: Option<PathBuf>,
    pub warmup_bars: Option<usize>,
    pub recent_bars: Option<usize>,
    pub execution: BacktestExecutionOverrides,
}

#[derive(Clone, Debug)]
pub struct BacktestOutcome {
    pub output_dir: PathBuf,
    pub orderbook_path: PathBuf,
    pub summary_path: PathBuf,
    pub warmup_bars: usize,
    pub replay_bars: usize,
    pub trades: usize,
    pub open_positions: usize,
    pub net_pnl: f64,
}

pub fn run_backtest(request: BacktestRequest) -> Result<BacktestOutcome, StrategyError> {
    if request.from >= request.to {
        return Err(StrategyError::Config(
            "backtest --from must be before --to".to_string(),
        ));
    }
    if request.strategy_key.is_none() && request.ssu_ids.is_empty() {
        return Err(StrategyError::Config(
            "backtest requires explicit --strategy or --ssu to avoid running the wrong SSU"
                .to_string(),
        ));
    }

    let config = AppConfig::load(&request.config_path)
        .map_err(|error| StrategyError::Config(error.to_string()))?;
    let strategy_config = config
        .strategy
        .as_ref()
        .ok_or_else(|| StrategyError::Config("missing [strategy] config".to_string()))?;
    let historical_config = config
        .historical_candles
        .as_ref()
        .ok_or_else(|| StrategyError::Config("missing [historical_candles] config".to_string()))?;
    let historical_sqlite_path = resolve_historical_sqlite_path(&config, historical_config);

    let execution = resolve_execution_config(&config, &request)?;
    execution.validate()?;
    let instruments = resolve_instruments(&config, &request)?;
    let candle_alignments = load_candle_alignments(&config)?;
    let ssus = load_ssus(strategy_config.sqlite_path.as_str(), &request)?;
    let timeframes = required_timeframes(&ssus)?;
    let warmup_bars = request.warmup_bars.unwrap_or(strategy_config.warmup_bars);
    let recent_bars = request.recent_bars.unwrap_or(strategy_config.recent_bars);
    let output_dir = resolve_output_dir(&config, &request);

    let position_book = Arc::new(BacktestPositionBook::new(execution.clone())?);
    let runtime = StrategyRuntime::new(
        Arc::new(StaticSsuRepository { ssus: ssus.clone() }),
        Arc::new(BuiltinStrategyFactory),
        Arc::new(SqliteHistoricalReplayStore::with_alignments(
            historical_sqlite_path.clone(),
            candle_alignments.clone(),
        )) as Arc<dyn HistoricalReplayStore>,
        position_book.clone(),
        Arc::new(InMemoryTradeContextStore::default()),
        SignalRouter::new(Vec::new()),
        Vec::new(),
        0,
        recent_bars,
        candle_alignments.clone(),
    );
    runtime.reload_ssus()?;

    let feed =
        HistoricalCandleFeed::open_with_alignments(&historical_sqlite_path, candle_alignments)?;
    let warmup = feed.load_warmup_bars(&instruments, &timeframes, request.from, warmup_bars)?;
    runtime.warmup_closed_bars(&warmup)?;

    let replay = feed.load_replay_bars(&instruments, &timeframes, request.from, request.to)?;
    let mut index = 0;
    while index < replay.len() {
        let end_at = replay[index].end_at;
        let start = index;
        while index < replay.len() && replay[index].end_at == end_at {
            index += 1;
        }
        runtime.on_closed_bars(&replay[start..index], true)?;
    }

    let report = write_report(BacktestReportInput {
        output_dir,
        strategy_filter: request.strategy_key.clone(),
        instruments,
        from: request.from,
        to: request.to,
        warmup_bars: warmup.len(),
        replay_bars: replay.len(),
        slippage_pct: execution.slippage_pct,
        brokerage_pct: execution.brokerage_pct,
        entry_fee_pct: execution.entry_fee_pct,
        exit_fee_pct: execution.exit_fee_pct,
        fee_tax_pct: execution.fee_tax_pct,
        fixed_fee_per_order: execution.fixed_fee_per_order,
        funding_rate_pct: execution.funding_rate_pct,
        funding_interval_hours: execution.funding_interval_hours,
        funding_charge_mode: execution.funding_charge_mode,
        ssus,
        trades: position_book.trades(),
    })?;

    Ok(outcome_from_report(report, warmup.len()))
}

fn resolve_historical_sqlite_path(
    config: &AppConfig,
    historical_config: &crate::config::HistoricalCandlesSection,
) -> String {
    config
        .backtest
        .as_ref()
        .and_then(|backtest| backtest.historical_candles_sqlite_path.clone())
        .unwrap_or_else(|| historical_config.sqlite_path.clone())
}

fn outcome_from_report(report: BacktestReportSummary, warmup_bars: usize) -> BacktestOutcome {
    BacktestOutcome {
        output_dir: report.output_dir,
        orderbook_path: report.orderbook_path,
        summary_path: report.summary_path,
        warmup_bars,
        replay_bars: report.replay_bars,
        trades: report.trades,
        open_positions: report.open_positions,
        net_pnl: report.net_pnl,
    }
}

fn resolve_execution_config(
    config: &AppConfig,
    request: &BacktestRequest,
) -> Result<BacktestExecutionConfig, StrategyError> {
    let mut execution = config
        .backtest
        .as_ref()
        .and_then(|backtest| backtest.execution.as_ref())
        .map(|execution| {
            Ok::<BacktestExecutionConfig, StrategyError>(BacktestExecutionConfig {
                slippage_pct: execution.slippage_pct,
                brokerage_pct: execution.brokerage_pct,
                entry_fee_pct: execution.entry_fee_pct,
                exit_fee_pct: execution.exit_fee_pct,
                fee_tax_pct: execution.fee_tax_pct,
                fixed_fee_per_order: execution.fixed_fee_per_order,
                funding_rate_pct: execution.funding_rate_pct,
                funding_interval_hours: execution.funding_interval_hours,
                funding_charge_mode: FundingChargeMode::parse(&execution.funding_charge_mode)?,
            })
        })
        .transpose()?
        .unwrap_or_default();

    if let Some(value) = request.execution.slippage_pct {
        execution.slippage_pct = value;
    }
    if let Some(value) = request.execution.brokerage_pct {
        execution.brokerage_pct = value;
    }
    if let Some(value) = request.execution.entry_fee_pct {
        execution.entry_fee_pct = Some(value);
    }
    if let Some(value) = request.execution.exit_fee_pct {
        execution.exit_fee_pct = Some(value);
    }
    if let Some(value) = request.execution.fee_tax_pct {
        execution.fee_tax_pct = value;
    }
    if let Some(value) = request.execution.fixed_fee_per_order {
        execution.fixed_fee_per_order = value;
    }
    if let Some(value) = request.execution.funding_rate_pct {
        execution.funding_rate_pct = value;
    }
    if let Some(value) = request.execution.funding_interval_hours {
        execution.funding_interval_hours = value;
    }
    if let Some(value) = request.execution.funding_charge_mode {
        execution.funding_charge_mode = value;
    }
    execution.validate()?;
    Ok(execution)
}

fn resolve_instruments(
    config: &AppConfig,
    request: &BacktestRequest,
) -> Result<Vec<String>, StrategyError> {
    let mut instruments = request
        .instruments
        .iter()
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect::<BTreeSet<_>>();

    if instruments.is_empty() {
        collect_spot_instruments(config, &mut instruments)?;
    }
    if instruments.is_empty() {
        return Err(StrategyError::Config(
            "backtest requires at least one --instrument or configured spot instrument".to_string(),
        ));
    }

    Ok(instruments.into_iter().collect())
}

fn collect_spot_instruments(
    config: &AppConfig,
    instruments: &mut BTreeSet<String>,
) -> Result<(), StrategyError> {
    if let Some(delta) = config.brokers.delta.as_ref().filter(|delta| delta.enabled) {
        let catalog = InstrumentCatalog::load_csv(&delta.base_instruments_csv)
            .map_err(|error| StrategyError::Config(error.to_string()))?;
        for instrument in catalog
            .instruments()
            .filter(|instrument| instrument.instrument_type == InstrumentType::Spot)
            .filter(|instrument| instrument.tradable)
        {
            instruments.insert(instrument.instrument_name.to_string());
        }
    }

    if let Some(fyers) = config.brokers.fyers.as_ref().filter(|fyers| fyers.enabled) {
        let catalog = InstrumentCatalog::load_csv(&fyers.base_instruments_csv)
            .map_err(|error| StrategyError::Config(error.to_string()))?;
        for instrument in catalog
            .instruments()
            .filter(|instrument| instrument.instrument_type == InstrumentType::Spot)
            .filter(|instrument| instrument.tradable)
        {
            instruments.insert(instrument.instrument_name.to_string());
        }
    }

    Ok(())
}

fn load_candle_alignments(config: &AppConfig) -> Result<CandleAlignmentMap, StrategyError> {
    let mut alignments = CandleAlignmentMap::new();

    if let Some(delta) = config.brokers.delta.as_ref().filter(|delta| delta.enabled) {
        let catalog = InstrumentCatalog::load_csv(&delta.base_instruments_csv)
            .map_err(|error| StrategyError::Config(error.to_string()))?;
        merge_candle_alignments(&mut alignments, candle_alignments_from_catalog(&catalog));
    }

    if let Some(fyers) = config.brokers.fyers.as_ref().filter(|fyers| fyers.enabled) {
        let catalog = InstrumentCatalog::load_csv(&fyers.base_instruments_csv)
            .map_err(|error| StrategyError::Config(error.to_string()))?;
        merge_candle_alignments(&mut alignments, candle_alignments_from_catalog(&catalog));
    }

    Ok(alignments)
}

fn load_ssus(
    sqlite_path: &str,
    request: &BacktestRequest,
) -> Result<Vec<SsuConfig>, StrategyError> {
    let repository = SqliteSsuRepository::new(sqlite_path)?;
    let mut ssus = repository.load_all_ssus()?;
    if let Some(strategy_key) = request.strategy_key.as_ref() {
        let wanted = strategy_key.trim().to_ascii_lowercase();
        ssus.retain(|ssu| ssu.strategy_key.trim().eq_ignore_ascii_case(&wanted));
    }
    if !request.ssu_ids.is_empty() {
        let wanted = request.ssu_ids.iter().copied().collect::<HashSet<_>>();
        ssus.retain(|ssu| wanted.contains(&ssu.ssu_id));
    }
    if ssus.is_empty() {
        return Err(StrategyError::Config(
            "no active SSUs matched the backtest filters".to_string(),
        ));
    }
    Ok(ssus)
}

fn required_timeframes(ssus: &[SsuConfig]) -> Result<BTreeSet<Timeframe>, StrategyError> {
    let mut timeframes = BTreeSet::new();
    for ssu in ssus {
        timeframes.extend(ssu.required_timeframes.iter().copied());
        timeframes.extend(ssu.indicator_specs.iter().map(|spec| spec.timeframe));
    }
    if timeframes.is_empty() {
        return Err(StrategyError::Config(
            "matched SSUs do not declare any required timeframes".to_string(),
        ));
    }
    Ok(timeframes)
}

fn resolve_output_dir(config: &AppConfig, request: &BacktestRequest) -> PathBuf {
    if let Some(output_dir) = request.output_dir.as_ref() {
        return output_dir.clone();
    }

    let root = config
        .backtest
        .as_ref()
        .and_then(|backtest| backtest.output_dir.as_ref())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("backtests"));
    root.join(format!("run-{}", Utc::now().format("%Y%m%d-%H%M%S")))
}

#[derive(Debug)]
struct StaticSsuRepository {
    ssus: Vec<SsuConfig>,
}

impl SsuRepository for StaticSsuRepository {
    fn load_active_ssus(&self) -> Result<Vec<SsuConfig>, StrategyError> {
        Ok(self.ssus.clone())
    }
}
