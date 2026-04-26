use std::env;
use std::path::PathBuf;

use chrono::{NaiveDate, NaiveDateTime, TimeZone, Utc};
use dhancred_trading_app::backtest::{
    BacktestExecutionOverrides, BacktestRequest, FundingChargeMode, run_backtest,
};

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        print_help();
        return Ok(());
    }

    let request = parse_args(args)?;
    let outcome = run_backtest(request).map_err(|error| error.to_string())?;
    println!("backtest complete");
    println!("output_dir={}", outcome.output_dir.display());
    println!("orderbook={}", outcome.orderbook_path.display());
    println!("summary={}", outcome.summary_path.display());
    println!(
        "warmup_bars={} replay_bars={} trades={} open_positions={} net_pnl={:.6}",
        outcome.warmup_bars,
        outcome.replay_bars,
        outcome.trades,
        outcome.open_positions,
        outcome.net_pnl
    );
    Ok(())
}

fn parse_args(args: Vec<String>) -> Result<BacktestRequest, String> {
    let mut config_path = env::var("FEEDER_CONFIG_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("config/feeder.toml"));
    let mut from = None;
    let mut to = None;
    let mut instruments = Vec::new();
    let mut strategy_key = None;
    let mut ssu_ids = Vec::new();
    let mut output_dir = None;
    let mut warmup_bars = None;
    let mut recent_bars = None;
    let mut execution = BacktestExecutionOverrides::default();

    let mut index = 0;
    while index < args.len() {
        let flag = &args[index];
        index += 1;
        match flag.as_str() {
            "--config" => config_path = PathBuf::from(take_value(&args, &mut index, flag)?),
            "--from" => from = Some(parse_time(&take_value(&args, &mut index, flag)?)?),
            "--to" => to = Some(parse_time(&take_value(&args, &mut index, flag)?)?),
            "--instrument" | "--instruments" => {
                instruments.push(take_value(&args, &mut index, flag)?)
            }
            "--strategy" => strategy_key = Some(take_value(&args, &mut index, flag)?),
            "--ssu" | "--ssu-id" => {
                let value = take_value(&args, &mut index, flag)?;
                for part in value
                    .split(',')
                    .map(str::trim)
                    .filter(|part| !part.is_empty())
                {
                    ssu_ids.push(
                        part.parse::<i64>()
                            .map_err(|error| format!("invalid {flag} value {part}: {error}"))?,
                    );
                }
            }
            "--output" | "--output-dir" => {
                output_dir = Some(PathBuf::from(take_value(&args, &mut index, flag)?))
            }
            "--warmup-bars" => {
                warmup_bars = Some(parse_usize(&take_value(&args, &mut index, flag)?, flag)?)
            }
            "--recent-bars" => {
                recent_bars = Some(parse_usize(&take_value(&args, &mut index, flag)?, flag)?)
            }
            "--slippage-pct" => {
                execution.slippage_pct =
                    Some(parse_f64(&take_value(&args, &mut index, flag)?, flag)?)
            }
            "--brokerage-pct" => {
                execution.brokerage_pct =
                    Some(parse_f64(&take_value(&args, &mut index, flag)?, flag)?)
            }
            "--entry-fee-pct" => {
                execution.entry_fee_pct =
                    Some(parse_f64(&take_value(&args, &mut index, flag)?, flag)?)
            }
            "--exit-fee-pct" => {
                execution.exit_fee_pct =
                    Some(parse_f64(&take_value(&args, &mut index, flag)?, flag)?)
            }
            "--fee-tax-pct" => {
                execution.fee_tax_pct =
                    Some(parse_f64(&take_value(&args, &mut index, flag)?, flag)?)
            }
            "--fixed-fee-per-order" => {
                execution.fixed_fee_per_order =
                    Some(parse_f64(&take_value(&args, &mut index, flag)?, flag)?)
            }
            "--funding-rate-pct" => {
                execution.funding_rate_pct =
                    Some(parse_f64(&take_value(&args, &mut index, flag)?, flag)?)
            }
            "--funding-interval-hours" => {
                execution.funding_interval_hours =
                    Some(parse_u64(&take_value(&args, &mut index, flag)?, flag)?)
            }
            "--funding-mode" | "--funding-charge-mode" => {
                execution.funding_charge_mode = Some(
                    FundingChargeMode::parse(&take_value(&args, &mut index, flag)?)
                        .map_err(|error| error.to_string())?,
                )
            }
            other => return Err(format!("unknown argument {other}; use --help")),
        }
    }

    Ok(BacktestRequest {
        config_path,
        from: from.ok_or_else(|| "missing required --from".to_string())?,
        to: to.ok_or_else(|| "missing required --to".to_string())?,
        instruments,
        strategy_key,
        ssu_ids,
        output_dir,
        warmup_bars,
        recent_bars,
        execution,
    })
}

fn take_value(args: &[String], index: &mut usize, flag: &str) -> Result<String, String> {
    let Some(value) = args.get(*index) else {
        return Err(format!("missing value for {flag}"));
    };
    *index += 1;
    Ok(value.clone())
}

fn parse_time(value: &str) -> Result<u64, String> {
    if value.chars().all(|ch| ch.is_ascii_digit()) {
        return value
            .parse::<u64>()
            .map_err(|error| format!("invalid unix millis {value}: {error}"));
    }

    let naive = if let Ok(date) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
        date.and_hms_opt(0, 0, 0)
            .ok_or_else(|| format!("invalid date {value}"))?
    } else if let Ok(time) = NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M") {
        time
    } else if let Ok(time) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M") {
        time
    } else if let Ok(time) = NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S") {
        time
    } else if let Ok(time) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S") {
        time
    } else {
        return Err(format!(
            "invalid time {value}; use unix millis, YYYY-MM-DD, or YYYY-MM-DDTHH:MM"
        ));
    };

    let parsed = Utc.from_utc_datetime(&naive);
    Ok(parsed.timestamp_millis() as u64)
}

fn parse_usize(value: &str, flag: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .map_err(|error| format!("invalid {flag} value {value}: {error}"))
}

fn parse_u64(value: &str, flag: &str) -> Result<u64, String> {
    value
        .parse::<u64>()
        .map_err(|error| format!("invalid {flag} value {value}: {error}"))
}

fn parse_f64(value: &str, flag: &str) -> Result<f64, String> {
    value
        .parse::<f64>()
        .map_err(|error| format!("invalid {flag} value {value}: {error}"))
}

fn print_help() {
    println!(
        "\
Usage:
  cargo run --bin backtest -- --from 2026-01-01 --to 2026-01-31 --instrument BTC

Options:
  --config PATH                 default: FEEDER_CONFIG_PATH or config/feeder.toml
  --from TIME                   unix millis, YYYY-MM-DD, or YYYY-MM-DDTHH:MM in UTC
  --to TIME                     unix millis, YYYY-MM-DD, or YYYY-MM-DDTHH:MM in UTC
  --instrument NAME             repeatable; comma-separated also supported
  --strategy KEY                optional active SSU filter
  --ssu ID                      repeatable; comma-separated also supported
  --output DIR                  exact output directory
  --slippage-pct VALUE          e.g. 0.001 for 0.1%
  --brokerage-pct VALUE         legacy flat fee pct per side
  --entry-fee-pct VALUE         overrides brokerage pct for entries
  --exit-fee-pct VALUE          overrides brokerage pct for exits
  --fee-tax-pct VALUE           tax/surcharge on trading fee, e.g. 0.18 GST
  --fixed-fee-per-order VALUE
  --funding-rate-pct VALUE      per funding interval; signed positive means longs pay shorts
  --funding-interval-hours N    default 8
  --funding-mode MODE           disabled, signed, or absolute
  --warmup-bars N
  --recent-bars N
"
    );
}
