use std::collections::BTreeSet;

use crate::feeder::{Candle, CandleAlignment, CandleAlignmentMap, InstrumentName, Timeframe};
use crate::storage::historical_candles::HistoricalCandleStore;
use crate::strategy::{Bar, StrategyError, bucket_bounds};

pub struct HistoricalCandleFeed {
    store: HistoricalCandleStore,
    alignments: CandleAlignmentMap,
}

impl HistoricalCandleFeed {
    pub fn open_with_alignments(
        sqlite_path: &str,
        alignments: CandleAlignmentMap,
    ) -> Result<Self, StrategyError> {
        Ok(Self {
            store: HistoricalCandleStore::open(sqlite_path)
                .map_err(|error| StrategyError::Io(error.to_string()))?,
            alignments,
        })
    }

    fn alignment_for(&self, instrument: &str) -> CandleAlignment {
        self.alignments
            .get(instrument)
            .copied()
            .unwrap_or(CandleAlignment::UTC)
    }

    pub fn load_warmup_bars(
        &self,
        instruments: &[String],
        timeframes: &BTreeSet<Timeframe>,
        before_millis: u64,
        limit: usize,
    ) -> Result<Vec<Bar>, StrategyError> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let mut bars = Vec::new();
        for instrument in instruments {
            for timeframe in timeframes {
                bars.extend(self.load_bars_before(instrument, *timeframe, before_millis, limit)?);
            }
        }
        sort_bars(&mut bars);
        Ok(bars)
    }

    pub fn load_replay_bars(
        &self,
        instruments: &[String],
        timeframes: &BTreeSet<Timeframe>,
        from_millis: u64,
        to_millis: u64,
    ) -> Result<Vec<Bar>, StrategyError> {
        let mut bars = Vec::new();
        for instrument in instruments {
            for timeframe in timeframes {
                bars.extend(self.load_bars_between(
                    instrument,
                    *timeframe,
                    from_millis,
                    to_millis,
                )?);
            }
        }
        sort_bars(&mut bars);
        Ok(bars)
    }

    fn load_bars_before(
        &self,
        instrument: &str,
        timeframe: Timeframe,
        before_millis: u64,
        limit: usize,
    ) -> Result<Vec<Bar>, StrategyError> {
        let instrument_name = InstrumentName::new(instrument);
        let direct = self
            .store
            .load_candles_before(&instrument_name, timeframe, before_millis, limit)
            .map_err(|error| StrategyError::Io(error.to_string()))?;
        if !direct.is_empty() || !can_aggregate_from_one_minute(timeframe) {
            return Ok(direct.into_iter().map(bar_from_candle).collect());
        }

        let source_limit = limit
            .saturating_add(2)
            .saturating_mul(timeframe_factor(timeframe));
        let source = self
            .store
            .load_candles_before(
                &instrument_name,
                Timeframe::OneMinute,
                before_millis,
                source_limit,
            )
            .map_err(|error| StrategyError::Io(error.to_string()))?;
        let mut aggregated = aggregate_bars(
            instrument,
            timeframe,
            &source,
            self.alignment_for(instrument),
        )?;
        if aggregated.len() > limit {
            aggregated.drain(0..aggregated.len() - limit);
        }
        Ok(aggregated)
    }

    fn load_bars_between(
        &self,
        instrument: &str,
        timeframe: Timeframe,
        from_millis: u64,
        to_millis: u64,
    ) -> Result<Vec<Bar>, StrategyError> {
        let instrument_name = InstrumentName::new(instrument);
        let direct = self
            .store
            .load_candles_between(&instrument_name, timeframe, from_millis, to_millis)
            .map_err(|error| StrategyError::Io(error.to_string()))?;
        if !direct.is_empty() || !can_aggregate_from_one_minute(timeframe) {
            return Ok(direct.into_iter().map(bar_from_candle).collect());
        }

        let source = self
            .store
            .load_candles_between(
                &instrument_name,
                Timeframe::OneMinute,
                from_millis,
                to_millis,
            )
            .map_err(|error| StrategyError::Io(error.to_string()))?;
        aggregate_bars(
            instrument,
            timeframe,
            &source,
            self.alignment_for(instrument),
        )
    }
}

fn sort_bars(bars: &mut [Bar]) {
    bars.sort_by(|left, right| {
        left.end_at
            .cmp(&right.end_at)
            .then_with(|| timeframe_factor(left.timeframe).cmp(&timeframe_factor(right.timeframe)))
            .then_with(|| left.instrument.cmp(&right.instrument))
            .then_with(|| left.start_at.cmp(&right.start_at))
    });
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
    alignment: CandleAlignment,
) -> Result<Vec<Bar>, StrategyError> {
    let mut aggregated: Vec<Bar> = Vec::new();

    for candle in source {
        let start_at = candle.start_time.as_u64();
        let (bucket_start, bucket_end) = bucket_bounds(timeframe, start_at, alignment)?;
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

    Ok(aggregated)
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

fn can_aggregate_from_one_minute(timeframe: Timeframe) -> bool {
    matches!(
        timeframe,
        Timeframe::ThreeMinute
            | Timeframe::FiveMinute
            | Timeframe::FifteenMinute
            | Timeframe::ThirtyMinute
            | Timeframe::SeventyFiveMinute
            | Timeframe::OneHour
            | Timeframe::FourHour
            | Timeframe::OneDay
    )
}

pub fn timeframe_factor(timeframe: Timeframe) -> usize {
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
    fn aggregation_drops_latest_incomplete_bucket() {
        let source = minute_source(0, 40);
        let bars = aggregate_bars("BTC", Timeframe::OneHour, &source, CandleAlignment::UTC)
            .expect("aggregate");

        assert!(bars.is_empty());
    }

    #[test]
    fn aggregation_keeps_only_completed_edge_buckets() {
        let source = minute_source(20, 120);
        let bars = aggregate_bars("BTC", Timeframe::OneHour, &source, CandleAlignment::UTC)
            .expect("aggregate");

        assert_eq!(bars.len(), 1);
        assert_eq!(bars[0].start_at, 60 * MINUTE);
        assert_eq!(bars[0].end_at, 120 * MINUTE);
    }
}
