use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use crate::backtest::stores::{BacktestTrade, FundingChargeMode, side_label, status_label};
use crate::strategy::{PositionStatus, SsuConfig, StrategyError, Timeframe};

#[derive(Clone, Debug)]
pub struct BacktestReportInput {
    pub output_dir: PathBuf,
    pub strategy_filter: Option<String>,
    pub instruments: Vec<String>,
    pub from: u64,
    pub to: u64,
    pub warmup_bars: usize,
    pub replay_bars: usize,
    pub slippage_pct: f64,
    pub brokerage_pct: f64,
    pub entry_fee_pct: Option<f64>,
    pub exit_fee_pct: Option<f64>,
    pub fee_tax_pct: f64,
    pub fixed_fee_per_order: f64,
    pub funding_rate_pct: f64,
    pub funding_interval_hours: u64,
    pub funding_charge_mode: FundingChargeMode,
    pub ssus: Vec<SsuConfig>,
    pub trades: Vec<BacktestTrade>,
}

#[derive(Clone, Debug)]
pub struct BacktestReportSummary {
    pub output_dir: PathBuf,
    pub orderbook_path: PathBuf,
    pub summary_path: PathBuf,
    pub replay_bars: usize,
    pub trades: usize,
    pub open_positions: usize,
    pub net_pnl: f64,
}

pub fn write_report(input: BacktestReportInput) -> Result<BacktestReportSummary, StrategyError> {
    fs::create_dir_all(&input.output_dir)?;
    let orderbook_path = input.output_dir.join("orderbook.csv");
    let summary_path = input.output_dir.join("summary.csv");
    let ssu_report_index = SsuReportIndex::new(&input.ssus)?;
    let setup_metric_keys = collect_entry_metadata_keys(&input.trades);
    write_orderbook(
        &orderbook_path,
        &input.trades,
        &ssu_report_index,
        &setup_metric_keys,
    )?;
    let metrics = SummaryMetrics::from_trades(&input.trades);
    write_summary(&summary_path, &input, &ssu_report_index)?;
    Ok(BacktestReportSummary {
        output_dir: input.output_dir,
        orderbook_path,
        summary_path,
        replay_bars: input.replay_bars,
        trades: metrics.closed_trades,
        open_positions: metrics.open_positions,
        net_pnl: metrics.net_pnl,
    })
}

fn write_orderbook(
    path: &Path,
    trades: &[BacktestTrade],
    ssu_report_index: &SsuReportIndex,
    setup_metric_keys: &[String],
) -> Result<(), StrategyError> {
    let mut writer = BufWriter::new(File::create(path)?);
    let mut header = vec![
        "position_id".to_string(),
        "ssu_id".to_string(),
        "strategy".to_string(),
        "ssu_enabled".to_string(),
        "ssu_timeframes".to_string(),
        "ssu_trade_gap_secs".to_string(),
        "ssu_max_overlap".to_string(),
        "ssu_max_positions_per_day".to_string(),
    ];
    header.extend(
        ssu_report_index
            .param_keys
            .iter()
            .map(|key| format!("ssu_param_{key}")),
    );
    header.extend(setup_metric_keys.iter().map(|key| format!("setup_{key}")));
    header.extend(
        [
            "instrument",
            "side",
            "status",
            "qty",
            "remaining_qty",
            "entry_at",
            "entry_price",
            "exit_at",
            "exit_price",
            "exit_reason",
            "gross_pnl",
            "charges",
            "net_pnl",
            "raw_entry_price",
            "raw_exit_price",
            "entry_slippage",
            "exit_slippage",
            "entry_charges",
            "exit_charges",
            "funding_charges",
            "partial_exits_json",
            "entry_signal_id",
            "exit_signal_id",
        ]
        .into_iter()
        .map(str::to_string),
    );
    csv_row(&mut writer, &header)?;

    let mut sorted = trades.to_vec();
    sorted.sort_by_key(|trade| (trade.entry_at, trade.exit_at.unwrap_or(u64::MAX)));
    for trade in sorted {
        let mut row = vec![
            trade.position_id,
            trade.ssu_id.to_string(),
            trade.strategy_key,
        ];
        push_ssu_values(&mut row, trade.ssu_id, ssu_report_index);
        push_metadata_values(&mut row, &trade.entry_metadata, setup_metric_keys);
        row.extend([
            trade.instrument,
            side_label(trade.side).to_string(),
            status_label(trade.status).to_string(),
            fmt_f64(trade.qty),
            fmt_f64(trade.remaining_qty),
            trade.entry_at.to_string(),
            fmt_f64(trade.entry_price),
            trade
                .exit_at
                .map(|value| value.to_string())
                .unwrap_or_default(),
            trade.exit_price.map(fmt_f64).unwrap_or_default(),
            trade.exit_reason.unwrap_or_default(),
            trade.gross_pnl.map(fmt_f64).unwrap_or_default(),
            fmt_f64(trade.charges),
            trade.net_pnl.map(fmt_f64).unwrap_or_default(),
            fmt_f64(trade.raw_entry_price),
            trade.raw_exit_price.map(fmt_f64).unwrap_or_default(),
            fmt_f64(trade.entry_slippage),
            fmt_f64(trade.exit_slippage),
            fmt_f64(trade.entry_charges),
            fmt_f64(trade.exit_charges),
            fmt_f64(trade.funding_charges),
            serde_json::to_string(&trade.partial_exits)?,
            trade.entry_signal_id,
            trade.exit_signal_id.unwrap_or_default(),
        ]);
        csv_row(&mut writer, &row)?;
    }

    writer.flush()?;
    Ok(())
}

fn collect_entry_metadata_keys(trades: &[BacktestTrade]) -> Vec<String> {
    let mut keys = BTreeSet::new();
    for trade in trades {
        let mut metadata = BTreeMap::new();
        flatten_json_value("", &trade.entry_metadata, &mut metadata);
        keys.extend(metadata.into_keys());
    }
    keys.into_iter().collect()
}

fn push_metadata_values(row: &mut Vec<String>, metadata: &serde_json::Value, keys: &[String]) {
    let mut flattened = BTreeMap::new();
    flatten_json_value("", metadata, &mut flattened);
    row.extend(
        keys.iter()
            .map(|key| flattened.get(key).cloned().unwrap_or_default()),
    );
}

fn write_summary(
    path: &Path,
    input: &BacktestReportInput,
    ssu_report_index: &SsuReportIndex,
) -> Result<(), StrategyError> {
    let mut writer = BufWriter::new(File::create(path)?);
    let mut header = vec![
        "status".to_string(),
        "ssu_id".to_string(),
        "strategy".to_string(),
        "ssu_enabled".to_string(),
        "ssu_timeframes".to_string(),
        "ssu_trade_gap_secs".to_string(),
        "ssu_max_overlap".to_string(),
        "ssu_max_positions_per_day".to_string(),
    ];
    header.extend(
        ssu_report_index
            .param_keys
            .iter()
            .map(|key| format!("ssu_param_{key}")),
    );
    header.extend(
        [
            "instruments",
            "from",
            "to",
            "warmup_bars",
            "replay_bars",
            "slippage_pct",
            "brokerage_pct",
            "entry_fee_pct",
            "exit_fee_pct",
            "fee_tax_pct",
            "fixed_fee_per_order",
            "funding_rate_pct",
            "funding_interval_hours",
            "funding_charge_mode",
            "positions",
            "trades",
            "open_positions",
            "wins",
            "losses",
            "win_rate",
            "gross_pnl",
            "charges",
            "net_pnl",
            "max_drawdown",
            "profit_factor",
            "expectancy",
            "avg_win",
            "avg_loss",
            "avg_trade",
        ]
        .into_iter()
        .map(str::to_string),
    );
    csv_row(&mut writer, &header)?;
    for row in summary_rows(input) {
        let mut values = vec![
            report_status(&row.trades, &row.metrics),
            row.ssu_id.to_string(),
            row.strategy_key.clone(),
        ];
        push_ssu_values(&mut values, row.ssu_id, ssu_report_index);
        values.extend([
            row.instruments.clone(),
            input.from.to_string(),
            input.to.to_string(),
            input.warmup_bars.to_string(),
            input.replay_bars.to_string(),
            fmt_f64(input.slippage_pct),
            fmt_f64(input.brokerage_pct),
            input.entry_fee_pct.map(fmt_f64).unwrap_or_default(),
            input.exit_fee_pct.map(fmt_f64).unwrap_or_default(),
            fmt_f64(input.fee_tax_pct),
            fmt_f64(input.fixed_fee_per_order),
            fmt_f64(input.funding_rate_pct),
            input.funding_interval_hours.to_string(),
            input.funding_charge_mode.label().to_string(),
            row.trades.len().to_string(),
            row.metrics.closed_trades.to_string(),
            row.metrics.open_positions.to_string(),
            row.metrics.wins.to_string(),
            row.metrics.losses.to_string(),
            fmt_f64(row.metrics.win_rate),
            fmt_f64(row.metrics.gross_pnl),
            fmt_f64(row.metrics.charges),
            fmt_f64(row.metrics.net_pnl),
            fmt_f64(row.metrics.max_drawdown),
            row.metrics.profit_factor.map(fmt_f64).unwrap_or_default(),
            fmt_f64(row.metrics.expectancy),
            row.metrics.avg_win.map(fmt_f64).unwrap_or_default(),
            row.metrics.avg_loss.map(fmt_f64).unwrap_or_default(),
            fmt_f64(row.metrics.avg_trade),
        ]);
        csv_row(&mut writer, &values)?;
    }
    writer.flush()?;
    Ok(())
}

#[derive(Clone, Debug)]
struct SsuReportIndex {
    configs: BTreeMap<i64, SsuReportConfig>,
    param_keys: Vec<String>,
}

impl SsuReportIndex {
    fn new(ssus: &[SsuConfig]) -> Result<Self, StrategyError> {
        let mut param_keys = BTreeSet::new();
        let mut configs = BTreeMap::new();
        for ssu in ssus {
            let params = flatten_params_json(&ssu.params_json)?;
            param_keys.extend(params.keys().cloned());
            configs.insert(
                ssu.ssu_id,
                SsuReportConfig {
                    enabled: ssu.enabled,
                    trade_gap_secs: ssu.trade_gap_secs,
                    max_overlap: ssu.max_overlap,
                    max_positions_per_day: ssu.max_positions_per_day,
                    timeframes: timeframes_label(&ssu.required_timeframes),
                    params,
                },
            );
        }
        Ok(Self {
            configs,
            param_keys: param_keys.into_iter().collect(),
        })
    }
}

#[derive(Clone, Debug)]
struct SsuReportConfig {
    enabled: bool,
    trade_gap_secs: u64,
    max_overlap: u32,
    max_positions_per_day: u32,
    timeframes: String,
    params: BTreeMap<String, String>,
}

fn push_ssu_values(row: &mut Vec<String>, ssu_id: i64, ssu_report_index: &SsuReportIndex) {
    let Some(config) = ssu_report_index.configs.get(&ssu_id) else {
        row.extend([
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
        ]);
        row.extend(ssu_report_index.param_keys.iter().map(|_| String::new()));
        return;
    };

    row.extend([
        config.enabled.to_string(),
        config.timeframes.clone(),
        config.trade_gap_secs.to_string(),
        config.max_overlap.to_string(),
        config.max_positions_per_day.to_string(),
    ]);
    row.extend(
        ssu_report_index
            .param_keys
            .iter()
            .map(|key| config.params.get(key).cloned().unwrap_or_default()),
    );
}

fn flatten_params_json(params_json: &str) -> Result<BTreeMap<String, String>, StrategyError> {
    let value = serde_json::from_str::<serde_json::Value>(params_json).map_err(|error| {
        StrategyError::Parse(format!(
            "invalid SSU params_json for backtest report: {error}"
        ))
    })?;
    let mut params = BTreeMap::new();
    flatten_json_value("", &value, &mut params);
    Ok(params)
}

fn flatten_json_value(
    prefix: &str,
    value: &serde_json::Value,
    params: &mut BTreeMap<String, String>,
) {
    match value {
        serde_json::Value::Object(object) => {
            for (key, child) in object {
                let child_key = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{prefix}_{key}")
                };
                flatten_json_value(&child_key, child, params);
            }
        }
        _ => {
            params.insert(prefix.to_string(), json_value_label(value));
        }
    }
}

fn json_value_label(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => String::new(),
        serde_json::Value::Bool(value) => value.to_string(),
        serde_json::Value::Number(value) => value.to_string(),
        serde_json::Value::String(value) => value.clone(),
        serde_json::Value::Array(values) => values
            .iter()
            .map(json_value_label)
            .collect::<Vec<_>>()
            .join("|"),
        serde_json::Value::Object(_) => value.to_string(),
    }
}

#[derive(Clone, Debug)]
struct SsuSummaryRow {
    ssu_id: i64,
    strategy_key: String,
    instruments: String,
    trades: Vec<BacktestTrade>,
    metrics: SummaryMetrics,
}

fn summary_rows(input: &BacktestReportInput) -> Vec<SsuSummaryRow> {
    let mut by_ssu = std::collections::BTreeMap::<i64, Vec<BacktestTrade>>::new();
    for trade in &input.trades {
        by_ssu.entry(trade.ssu_id).or_default().push(trade.clone());
    }

    let mut rows = Vec::new();
    for ssu in &input.ssus {
        let trades = by_ssu.remove(&ssu.ssu_id).unwrap_or_default();
        rows.push(SsuSummaryRow {
            ssu_id: ssu.ssu_id,
            strategy_key: ssu.strategy_key.clone(),
            instruments: instruments_label(&trades, input),
            metrics: SummaryMetrics::from_trades(&trades),
            trades,
        });
    }

    for (ssu_id, trades) in by_ssu {
        let strategy_key = trades
            .first()
            .map(|trade| trade.strategy_key.clone())
            .or_else(|| input.strategy_filter.clone())
            .unwrap_or_else(|| "UNKNOWN".to_string());
        rows.push(SsuSummaryRow {
            ssu_id,
            strategy_key,
            instruments: instruments_label(&trades, input),
            metrics: SummaryMetrics::from_trades(&trades),
            trades,
        });
    }

    rows.sort_by_key(|row| row.ssu_id);
    rows
}

fn instruments_label(trades: &[BacktestTrade], input: &BacktestReportInput) -> String {
    let mut instruments = trades
        .iter()
        .map(|trade| trade.instrument.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    if instruments.is_empty() {
        instruments.extend(input.instruments.iter().map(String::as_str));
    }
    instruments.into_iter().collect::<Vec<_>>().join("|")
}

fn timeframes_label(timeframes: &[Timeframe]) -> String {
    timeframes
        .iter()
        .map(|timeframe| timeframe_label(*timeframe))
        .collect::<Vec<_>>()
        .join("|")
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

fn report_status(trades: &[BacktestTrade], metrics: &SummaryMetrics) -> String {
    if trades.is_empty() {
        "NO_POSITIONS".to_string()
    } else if metrics.open_positions > 0 {
        "HAS_OPEN_POSITIONS".to_string()
    } else {
        "CLOSED".to_string()
    }
}

#[derive(Clone, Debug)]
struct SummaryMetrics {
    closed_trades: usize,
    open_positions: usize,
    wins: usize,
    losses: usize,
    win_rate: f64,
    gross_pnl: f64,
    charges: f64,
    net_pnl: f64,
    max_drawdown: f64,
    profit_factor: Option<f64>,
    expectancy: f64,
    avg_win: Option<f64>,
    avg_loss: Option<f64>,
    avg_trade: f64,
}

impl SummaryMetrics {
    fn from_trades(trades: &[BacktestTrade]) -> Self {
        let mut closed = trades
            .iter()
            .filter(|trade| trade.status == PositionStatus::Closed)
            .cloned()
            .collect::<Vec<_>>();
        closed.sort_by_key(|trade| trade.exit_at.unwrap_or(u64::MAX));

        let closed_trades = closed.len();
        let open_positions = trades
            .iter()
            .filter(|trade| trade.status == PositionStatus::Open)
            .count();
        let gross_pnl = closed
            .iter()
            .filter_map(|trade| trade.gross_pnl)
            .sum::<f64>();
        let charges = closed.iter().map(|trade| trade.charges).sum::<f64>();
        let net_values = closed
            .iter()
            .filter_map(|trade| trade.net_pnl)
            .collect::<Vec<_>>();
        let net_pnl = net_values.iter().sum::<f64>();
        let wins = net_values.iter().filter(|pnl| **pnl > 0.0).count();
        let losses = net_values.iter().filter(|pnl| **pnl < 0.0).count();
        let win_rate = if closed_trades == 0 {
            0.0
        } else {
            wins as f64 / closed_trades as f64
        };
        let gross_wins = net_values.iter().filter(|pnl| **pnl > 0.0).sum::<f64>();
        let gross_losses = net_values
            .iter()
            .filter(|pnl| **pnl < 0.0)
            .sum::<f64>()
            .abs();
        let profit_factor = if gross_losses > 0.0 {
            Some(gross_wins / gross_losses)
        } else {
            None
        };
        let avg_win = if wins > 0 {
            Some(gross_wins / wins as f64)
        } else {
            None
        };
        let avg_loss = if losses > 0 {
            Some(-gross_losses / losses as f64)
        } else {
            None
        };
        let expectancy = if closed_trades == 0 {
            0.0
        } else {
            net_pnl / closed_trades as f64
        };
        let avg_trade = expectancy;
        let max_drawdown = max_drawdown(&net_values);

        Self {
            closed_trades,
            open_positions,
            wins,
            losses,
            win_rate,
            gross_pnl,
            charges,
            net_pnl,
            max_drawdown,
            profit_factor,
            expectancy,
            avg_win,
            avg_loss,
            avg_trade,
        }
    }
}

fn max_drawdown(values: &[f64]) -> f64 {
    let mut equity = 0.0;
    let mut peak = 0.0;
    let mut max_drawdown = 0.0;
    for value in values {
        equity += value;
        if equity > peak {
            peak = equity;
        }
        let drawdown = peak - equity;
        if drawdown > max_drawdown {
            max_drawdown = drawdown;
        }
    }
    max_drawdown
}

fn csv_row(writer: &mut impl Write, values: &[impl AsRef<str>]) -> Result<(), StrategyError> {
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            writer.write_all(b",")?;
        }
        write_csv_field(writer, value.as_ref())?;
    }
    writer.write_all(b"\n")?;
    Ok(())
}

fn write_csv_field(writer: &mut impl Write, value: &str) -> Result<(), StrategyError> {
    if value.contains(',') || value.contains('"') || value.contains('\n') || value.contains('\r') {
        writer.write_all(b"\"")?;
        for byte in value.bytes() {
            if byte == b'"' {
                writer.write_all(b"\"\"")?;
            } else {
                writer.write_all(&[byte])?;
            }
        }
        writer.write_all(b"\"")?;
    } else {
        writer.write_all(value.as_bytes())?;
    }
    Ok(())
}

fn fmt_f64(value: f64) -> String {
    if value.is_finite() {
        format!("{value:.6}")
    } else {
        String::new()
    }
}
