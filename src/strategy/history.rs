use crate::feeder::{Candle, InstrumentName, Timeframe};
use crate::storage::historical_candles::HistoricalCandleStore;
use crate::strategy::timeframes::bucket_bounds_ist;
use crate::strategy::{Bar, StrategyError};

pub trait HistoricalReplayStore: Send + Sync {
    fn load_bars(
        &self,
        instrument: &str,
        timeframe: Timeframe,
        limit: usize,
    ) -> Result<Vec<Bar>, StrategyError>;
}

#[derive(Clone, Debug)]
pub struct SqliteHistoricalReplayStore {
    sqlite_path: String,
}

impl SqliteHistoricalReplayStore {
    pub fn new(sqlite_path: impl Into<String>) -> Self {
        Self {
            sqlite_path: sqlite_path.into(),
        }
    }
}

impl HistoricalReplayStore for SqliteHistoricalReplayStore {
    fn load_bars(
        &self,
        instrument: &str,
        timeframe: Timeframe,
        limit: usize,
    ) -> Result<Vec<Bar>, StrategyError> {
        let store = HistoricalCandleStore::open(&self.sqlite_path)
            .map_err(|error| StrategyError::Io(error.to_string()))?;
        let instrument_name = InstrumentName::new(instrument);

        match timeframe {
            Timeframe::OneMinute | Timeframe::OneDay => store
                .load_recent_candles(&instrument_name, timeframe, limit)
                .map_err(|error| StrategyError::Io(error.to_string()))
                .map(|candles| candles.into_iter().map(bar_from_candle).collect()),
            Timeframe::ThreeMinute
            | Timeframe::FiveMinute
            | Timeframe::FifteenMinute
            | Timeframe::OneHour => {
                let source = store
                    .load_recent_candles(
                        &instrument_name,
                        Timeframe::OneMinute,
                        limit.saturating_mul(timeframe_factor(timeframe)).saturating_add(8),
                    )
                    .map_err(|error| StrategyError::Io(error.to_string()))?;
                Ok(aggregate_bars(instrument, timeframe, &source, limit))
            }
        }
    }
}

fn bar_from_candle(candle: Candle) -> Bar {
    Bar {
        instrument: candle.instrument_name.to_string(),
        timeframe: candle.timeframe,
        start_at: candle.start_time.as_u64(),
        end_at: candle.end_time.as_u64(),
        open: candle.open.as_f64(),
        high: candle.high.as_f64(),
        low: candle.low.as_f64(),
        close: candle.close.as_f64(),
        is_closed: true,
    }
}

fn aggregate_bars(
    instrument: &str,
    timeframe: Timeframe,
    source: &[Candle],
    limit: usize,
) -> Vec<Bar> {
    let mut aggregated: Vec<Bar> = Vec::new();

    for candle in source {
        let start_at = candle.start_time.as_u64();
        let Ok((bucket_start, bucket_end)) = bucket_bounds_ist(timeframe, start_at) else {
            continue;
        };
        match aggregated.last_mut() {
            Some(bar) if bar.start_at == bucket_start => {
                bar.high = bar.high.max(candle.high.as_f64());
                bar.low = bar.low.min(candle.low.as_f64());
                bar.close = candle.close.as_f64();
                bar.end_at = bucket_end;
            }
            _ => aggregated.push(Bar {
                instrument: instrument.to_string(),
                timeframe,
                start_at: bucket_start,
                end_at: bucket_end,
                open: candle.open.as_f64(),
                high: candle.high.as_f64(),
                low: candle.low.as_f64(),
                close: candle.close.as_f64(),
                is_closed: true,
            }),
        }
    }

    if aggregated.len() > limit {
        aggregated.drain(0..aggregated.len() - limit);
    }

    aggregated
}

fn timeframe_factor(timeframe: Timeframe) -> usize {
    match timeframe {
        Timeframe::OneMinute => 1,
        Timeframe::ThreeMinute => 3,
        Timeframe::FiveMinute => 5,
        Timeframe::FifteenMinute => 15,
        Timeframe::OneHour => 60,
        Timeframe::OneDay => 1,
    }
}
