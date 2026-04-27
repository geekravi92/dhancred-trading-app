use crate::feeder::{Candle, CandleAlignment, CandleAlignmentMap, InstrumentName, Timeframe};
use crate::storage::historical_candles::HistoricalCandleStore;
use crate::strategy::{Bar, StrategyError, bucket_bounds};

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
    alignments: CandleAlignmentMap,
}

impl SqliteHistoricalReplayStore {
    pub fn new(sqlite_path: impl Into<String>) -> Self {
        Self::with_alignments(sqlite_path, CandleAlignmentMap::new())
    }

    pub fn with_alignments(sqlite_path: impl Into<String>, alignments: CandleAlignmentMap) -> Self {
        Self {
            sqlite_path: sqlite_path.into(),
            alignments,
        }
    }

    fn alignment_for(&self, instrument: &str) -> CandleAlignment {
        self.alignments
            .get(instrument)
            .copied()
            .unwrap_or(CandleAlignment::UTC)
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
            Timeframe::OneMinute => store
                .load_recent_candles(&instrument_name, timeframe, limit)
                .map_err(|error| StrategyError::Io(error.to_string()))
                .map(|candles| candles.into_iter().map(bar_from_candle).collect()),
            Timeframe::ThreeMinute
            | Timeframe::FiveMinute
            | Timeframe::FifteenMinute
            | Timeframe::ThirtyMinute
            | Timeframe::SeventyFiveMinute
            | Timeframe::OneHour
            | Timeframe::FourHour
            | Timeframe::OneDay => {
                let direct = store
                    .load_recent_candles(&instrument_name, timeframe, limit)
                    .map_err(|error| StrategyError::Io(error.to_string()))?;
                if !direct.is_empty() {
                    return Ok(direct.into_iter().map(bar_from_candle).collect());
                }
                let source = store
                    .load_recent_candles(
                        &instrument_name,
                        Timeframe::OneMinute,
                        limit
                            .saturating_add(2)
                            .saturating_mul(timeframe_factor(timeframe)),
                    )
                    .map_err(|error| StrategyError::Io(error.to_string()))?;
                Ok(aggregate_bars(
                    instrument,
                    timeframe,
                    &source,
                    limit,
                    self.alignment_for(instrument),
                ))
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
        volume: candle.volume,
        is_closed: true,
    }
}

fn aggregate_bars(
    instrument: &str,
    timeframe: Timeframe,
    source: &[Candle],
    limit: usize,
    alignment: CandleAlignment,
) -> Vec<Bar> {
    let mut aggregated: Vec<Bar> = Vec::new();

    for candle in source {
        let start_at = candle.start_time.as_u64();
        let Ok((bucket_start, bucket_end)) = bucket_bounds(timeframe, start_at, alignment) else {
            continue;
        };
        match aggregated.last_mut() {
            Some(bar) if bar.start_at == bucket_start => {
                bar.high = bar.high.max(candle.high.as_f64());
                bar.low = bar.low.min(candle.low.as_f64());
                bar.close = candle.close.as_f64();
                bar.volume += candle.volume;
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
                volume: candle.volume,
                is_closed: true,
            }),
        }
    }

    drop_incomplete_edge_buckets(&mut aggregated, source);

    if aggregated.len() > limit {
        aggregated.drain(0..aggregated.len() - limit);
    }

    aggregated
}

fn drop_incomplete_edge_buckets(aggregated: &mut Vec<Bar>, source: &[Candle]) {
    if aggregated.is_empty() || source.is_empty() {
        return;
    }

    let first_source_start = source
        .first()
        .map(|candle| candle.start_time.as_u64())
        .unwrap_or_default();
    if aggregated
        .first()
        .is_some_and(|bar| first_source_start > bar.start_at)
    {
        aggregated.remove(0);
    }

    let last_source_end = source
        .last()
        .map(|candle| candle.end_time.as_u64())
        .unwrap_or_default();
    if aggregated
        .last()
        .is_some_and(|bar| last_source_end < bar.end_at)
    {
        aggregated.pop();
    }
}

fn timeframe_factor(timeframe: Timeframe) -> usize {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feeder::{InstrumentName, Price, UnixMillis};

    const MINUTE: u64 = 60_000;

    fn minute_candle(start_minute: u64, close: f64) -> Candle {
        Candle::new(
            InstrumentName::new("BTC"),
            Timeframe::OneMinute,
            UnixMillis::new(start_minute * MINUTE),
            UnixMillis::new((start_minute + 1) * MINUTE),
            Price::new(close).expect("open"),
            Price::new(close + 1.0).expect("high"),
            Price::new(close - 1.0).expect("low"),
            Price::new(close).expect("close"),
            0.0,
        )
    }

    fn minute_source(start_minute: u64, end_minute_exclusive: u64) -> Vec<Candle> {
        (start_minute..end_minute_exclusive)
            .map(|minute| minute_candle(minute, 100.0 + minute as f64))
            .collect()
    }

    #[test]
    fn aggregated_warmup_drops_latest_incomplete_bucket() {
        let source = minute_source(0, 40);
        let bars = aggregate_bars("BTC", Timeframe::OneHour, &source, 10, CandleAlignment::UTC);

        assert!(bars.is_empty());
    }

    #[test]
    fn aggregated_warmup_keeps_only_completed_edge_buckets() {
        let source = minute_source(0, 100);
        let bars = aggregate_bars("BTC", Timeframe::OneHour, &source, 10, CandleAlignment::UTC);

        assert_eq!(bars.len(), 1);
        assert_eq!(bars[0].start_at, 0);
        assert_eq!(bars[0].end_at, 60 * MINUTE);
    }

    #[test]
    fn aggregated_warmup_drops_oldest_incomplete_bucket() {
        let source = minute_source(20, 120);
        let bars = aggregate_bars("BTC", Timeframe::OneHour, &source, 10, CandleAlignment::UTC);

        assert_eq!(bars.len(), 1);
        assert_eq!(bars[0].start_at, 60 * MINUTE);
        assert_eq!(bars[0].end_at, 120 * MINUTE);
    }
}
