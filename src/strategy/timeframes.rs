use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::RwLock;

use crate::feeder::{CandleAlignment, CandleAlignmentMap};
use crate::strategy::{
    Bar, IndicatorSpec, IndicatorValue, SsuConfig, StrategyError, Timeframe, TimeframeUpdate,
};

pub trait TimeframeEngine: Send + Sync {
    fn register_ssu(&self, ssu: &SsuConfig) -> Result<(), StrategyError>;
    fn warmup(
        &self,
        instrument: &str,
        timeframe: Timeframe,
        bars: &[Bar],
        ssu_id: i64,
    ) -> Result<(), StrategyError>;
    fn on_tick(
        &self,
        instrument: &str,
        ltp: f64,
        at: u64,
    ) -> Result<TimeframeUpdate, StrategyError>;
    fn current_bar(&self, instrument: &str, timeframe: Timeframe) -> Option<Bar>;
    fn last_closed_bar(&self, instrument: &str, timeframe: Timeframe) -> Option<Bar>;
    fn recent_bars(&self, instrument: &str, timeframe: Timeframe, count: usize) -> Vec<Bar>;
    fn indicator_value(&self, ssu_id: i64, instrument: &str, key: &str) -> Option<IndicatorValue>;
}

#[derive(Debug)]
pub struct SharedTimeframeEngine {
    state: RwLock<EngineState>,
    alignments: CandleAlignmentMap,
}

impl SharedTimeframeEngine {
    pub fn new(max_recent_bars: usize) -> Self {
        Self::with_alignments(max_recent_bars, CandleAlignmentMap::new())
    }

    pub fn with_alignments(max_recent_bars: usize, alignments: CandleAlignmentMap) -> Self {
        Self {
            state: RwLock::new(EngineState::new(max_recent_bars)),
            alignments,
        }
    }

    pub fn reset_ssus(&self) {
        let mut state = self.state.write().expect("timeframe engine lock poisoned");
        state.registrations.clear();
        state.indicators.clear();
        state.required_timeframes.clear();
    }

    fn alignment_for(&self, instrument: &str) -> CandleAlignment {
        self.alignments
            .get(instrument)
            .copied()
            .unwrap_or(CandleAlignment::UTC)
    }

    pub fn on_closed_bar(&self, bar: &Bar) -> Result<TimeframeUpdate, StrategyError> {
        if !bar.is_closed {
            return Err(StrategyError::Rule(format!(
                "direct timeframe replay requires a closed bar for {} {:?} at {}",
                bar.instrument, bar.timeframe, bar.end_at
            )));
        }

        let mut state = self.state.write().expect("timeframe engine lock poisoned");
        let max_recent_bars = state.max_recent_bars;
        let timeframe = bar.timeframe;
        let instrument = bar.instrument.clone();

        {
            let series = state
                .bars
                .entry(instrument.clone())
                .or_default()
                .entry(timeframe)
                .or_default();
            if series
                .closed
                .back()
                .is_some_and(|existing| existing.end_at >= bar.end_at)
            {
                return Ok(TimeframeUpdate {
                    instrument,
                    tick_at: bar.end_at,
                    closed_timeframes: Vec::new(),
                });
            }
            if series
                .current
                .as_ref()
                .is_some_and(|current| current.start_at <= bar.start_at)
            {
                series.current = None;
            }
            merge_closed_bar(series, bar.clone(), max_recent_bars);
        }

        let specs = state
            .registrations
            .iter()
            .filter(|(_, registration)| registration.required_timeframes.contains(&timeframe))
            .flat_map(|(ssu_id, registration)| {
                registration
                    .indicator_specs
                    .iter()
                    .filter(move |spec| spec.timeframe == timeframe)
                    .map(move |spec| (*ssu_id, spec.clone()))
            })
            .collect::<Vec<_>>();

        for (ssu_id, spec) in &specs {
            let runtime = state
                .indicators
                .entry(IndicatorRuntimeKey::new(*ssu_id, &instrument, &spec.key))
                .or_insert(IndicatorRuntime::new(spec)?);
            runtime.apply_closed(bar.close, bar.end_at)?;
        }

        Ok(TimeframeUpdate {
            instrument,
            tick_at: bar.end_at,
            closed_timeframes: vec![timeframe],
        })
    }
}

impl TimeframeEngine for SharedTimeframeEngine {
    fn register_ssu(&self, ssu: &SsuConfig) -> Result<(), StrategyError> {
        let mut state = self.state.write().expect("timeframe engine lock poisoned");
        let mut required = ssu
            .required_timeframes
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        for spec in &ssu.indicator_specs {
            validate_indicator_spec(spec)?;
            required.insert(spec.timeframe);
        }
        state.required_timeframes.extend(required.iter().copied());
        state.registrations.insert(
            ssu.ssu_id,
            SsuRegistration {
                indicator_specs: ssu.indicator_specs.clone(),
                required_timeframes: required,
            },
        );
        Ok(())
    }

    fn warmup(
        &self,
        instrument: &str,
        timeframe: Timeframe,
        bars: &[Bar],
        ssu_id: i64,
    ) -> Result<(), StrategyError> {
        let mut state = self.state.write().expect("timeframe engine lock poisoned");
        let max_recent_bars = state.max_recent_bars;
        let series = state
            .bars
            .entry(instrument.to_string())
            .or_default()
            .entry(timeframe)
            .or_default();
        for bar in bars {
            if bar.is_closed {
                merge_closed_bar(series, bar.clone(), max_recent_bars);
            }
        }

        let Some(registration) = state.registrations.get(&ssu_id).cloned() else {
            return Err(StrategyError::NotFound(format!(
                "missing registered SSU {ssu_id} for warmup"
            )));
        };

        for spec in registration
            .indicator_specs
            .iter()
            .filter(|spec| spec.timeframe == timeframe)
        {
            let runtime = state
                .indicators
                .entry(IndicatorRuntimeKey::new(ssu_id, instrument, &spec.key))
                .or_insert(IndicatorRuntime::new(spec)?);
            for bar in bars.iter().filter(|bar| bar.is_closed) {
                runtime.apply_closed(bar.close, bar.end_at)?;
            }
        }

        Ok(())
    }

    fn on_tick(
        &self,
        instrument: &str,
        ltp: f64,
        at: u64,
    ) -> Result<TimeframeUpdate, StrategyError> {
        let mut state = self.state.write().expect("timeframe engine lock poisoned");
        let required_timeframes = state
            .required_timeframes
            .iter()
            .copied()
            .collect::<Vec<_>>();
        let mut closed_timeframes = Vec::new();

        for timeframe in required_timeframes {
            let max_recent_bars = state.max_recent_bars;
            let alignment = self.alignment_for(instrument);
            let (closed_bar, current_bar) = {
                let series = state
                    .bars
                    .entry(instrument.to_string())
                    .or_default()
                    .entry(timeframe)
                    .or_default();
                let closed_bar = update_series(
                    series,
                    instrument,
                    timeframe,
                    ltp,
                    at,
                    max_recent_bars,
                    alignment,
                )?;
                let current_bar = series.current.clone();
                (closed_bar, current_bar)
            };
            let specs = state
                .registrations
                .iter()
                .filter(|(_, registration)| registration.required_timeframes.contains(&timeframe))
                .flat_map(|(ssu_id, registration)| {
                    registration
                        .indicator_specs
                        .iter()
                        .filter(move |spec| spec.timeframe == timeframe)
                        .map(move |spec| (*ssu_id, spec.clone()))
                })
                .collect::<Vec<_>>();

            if let Some(closed_bar) = closed_bar.clone() {
                closed_timeframes.push(timeframe);
                for (ssu_id, spec) in &specs {
                    let runtime = state
                        .indicators
                        .entry(IndicatorRuntimeKey::new(*ssu_id, instrument, &spec.key))
                        .or_insert(IndicatorRuntime::new(spec)?);
                    runtime.apply_closed(closed_bar.close, closed_bar.end_at)?;
                }
            }

            if let Some(current_bar) = current_bar {
                for (ssu_id, spec) in &specs {
                    let runtime = state
                        .indicators
                        .entry(IndicatorRuntimeKey::new(*ssu_id, instrument, &spec.key))
                        .or_insert(IndicatorRuntime::new(spec)?);
                    runtime.apply_live(current_bar.close, at);
                }
            }
        }

        Ok(TimeframeUpdate {
            instrument: instrument.to_string(),
            tick_at: at,
            closed_timeframes,
        })
    }

    fn current_bar(&self, instrument: &str, timeframe: Timeframe) -> Option<Bar> {
        self.state
            .read()
            .expect("timeframe engine lock poisoned")
            .bars
            .get(instrument)
            .and_then(|bars| bars.get(&timeframe))
            .and_then(|series| series.current.clone())
    }

    fn last_closed_bar(&self, instrument: &str, timeframe: Timeframe) -> Option<Bar> {
        self.state
            .read()
            .expect("timeframe engine lock poisoned")
            .bars
            .get(instrument)
            .and_then(|bars| bars.get(&timeframe))
            .and_then(|series| series.closed.back().cloned())
    }

    fn recent_bars(&self, instrument: &str, timeframe: Timeframe, count: usize) -> Vec<Bar> {
        self.state
            .read()
            .expect("timeframe engine lock poisoned")
            .bars
            .get(instrument)
            .and_then(|bars| bars.get(&timeframe))
            .map(|series| {
                series
                    .closed
                    .iter()
                    .rev()
                    .take(count)
                    .cloned()
                    .collect::<Vec<_>>()
                    .into_iter()
                    .rev()
                    .collect()
            })
            .unwrap_or_default()
    }

    fn indicator_value(&self, ssu_id: i64, instrument: &str, key: &str) -> Option<IndicatorValue> {
        self.state
            .read()
            .expect("timeframe engine lock poisoned")
            .indicators
            .get(&IndicatorRuntimeKey::new(ssu_id, instrument, key))
            .and_then(IndicatorRuntime::latest_value)
    }
}

#[derive(Debug)]
struct EngineState {
    max_recent_bars: usize,
    bars: BTreeMap<String, BTreeMap<Timeframe, TimeframeSeries>>,
    registrations: BTreeMap<i64, SsuRegistration>,
    required_timeframes: BTreeSet<Timeframe>,
    indicators: BTreeMap<IndicatorRuntimeKey, IndicatorRuntime>,
}

impl EngineState {
    fn new(max_recent_bars: usize) -> Self {
        Self {
            max_recent_bars,
            bars: BTreeMap::new(),
            registrations: BTreeMap::new(),
            required_timeframes: BTreeSet::new(),
            indicators: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug)]
struct SsuRegistration {
    indicator_specs: Vec<IndicatorSpec>,
    required_timeframes: BTreeSet<Timeframe>,
}

#[derive(Clone, Debug, Default)]
struct TimeframeSeries {
    current: Option<Bar>,
    closed: VecDeque<Bar>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct IndicatorRuntimeKey {
    ssu_id: i64,
    instrument: String,
    key: String,
}

impl IndicatorRuntimeKey {
    fn new(ssu_id: i64, instrument: &str, key: &str) -> Self {
        Self {
            ssu_id,
            instrument: instrument.to_string(),
            key: key.to_string(),
        }
    }
}

#[derive(Clone, Debug)]
struct IndicatorRuntime {
    key: String,
    timeframe: Timeframe,
    algorithm: IndicatorAlgorithm,
    latest_value: Option<IndicatorValue>,
    last_closed_at: Option<u64>,
}

impl IndicatorRuntime {
    fn new(spec: &IndicatorSpec) -> Result<Self, StrategyError> {
        Ok(Self {
            key: spec.key.clone(),
            timeframe: spec.timeframe,
            algorithm: IndicatorAlgorithm::from_spec(spec)?,
            latest_value: None,
            last_closed_at: None,
        })
    }

    fn apply_closed(&mut self, close: f64, as_of: u64) -> Result<(), StrategyError> {
        if self.last_closed_at.is_some_and(|last| as_of <= last) {
            return Ok(());
        }

        if let Some(value) = self.algorithm.apply_closed(close)? {
            self.latest_value = Some(IndicatorValue {
                key: self.key.clone(),
                timeframe: self.timeframe,
                value,
                as_of,
                is_final: true,
            });
            self.last_closed_at = Some(as_of);
        }
        Ok(())
    }

    fn apply_live(&mut self, close: f64, as_of: u64) {
        if let Some(value) = self.algorithm.preview(close) {
            self.latest_value = Some(IndicatorValue {
                key: self.key.clone(),
                timeframe: self.timeframe,
                value,
                as_of,
                is_final: false,
            });
        }
    }

    fn latest_value(&self) -> Option<IndicatorValue> {
        self.latest_value.clone()
    }
}

#[derive(Clone, Debug)]
enum IndicatorAlgorithm {
    Ema(EmaState),
}

impl IndicatorAlgorithm {
    fn from_spec(spec: &IndicatorSpec) -> Result<Self, StrategyError> {
        match spec.kind.trim().to_ascii_lowercase().as_str() {
            "ema" => Ok(Self::Ema(EmaState::new(parse_ema_period(
                &spec.params_json,
            )?)?)),
            value => Err(StrategyError::Unsupported(format!(
                "unsupported indicator kind {value}"
            ))),
        }
    }

    fn apply_closed(&mut self, close: f64) -> Result<Option<f64>, StrategyError> {
        match self {
            Self::Ema(state) => Ok(state.apply_closed(close)),
        }
    }

    fn preview(&self, close: f64) -> Option<f64> {
        match self {
            Self::Ema(state) => state.preview(close),
        }
    }
}

#[derive(Clone, Debug)]
struct EmaState {
    period: usize,
    alpha: f64,
    seed: Vec<f64>,
    last_final: Option<f64>,
}

impl EmaState {
    fn new(period: usize) -> Result<Self, StrategyError> {
        if period == 0 {
            return Err(StrategyError::Config(
                "EMA period must be positive".to_string(),
            ));
        }

        Ok(Self {
            period,
            alpha: 2.0 / (period as f64 + 1.0),
            seed: Vec::with_capacity(period),
            last_final: None,
        })
    }

    fn apply_closed(&mut self, close: f64) -> Option<f64> {
        if let Some(previous) = self.last_final {
            let next = self.alpha * close + (1.0 - self.alpha) * previous;
            self.last_final = Some(next);
            return Some(next);
        }

        self.seed.push(close);
        if self.seed.len() < self.period {
            return None;
        }

        let initial = self.seed.iter().sum::<f64>() / self.seed.len() as f64;
        self.last_final = Some(initial);
        Some(initial)
    }

    fn preview(&self, close: f64) -> Option<f64> {
        if let Some(previous) = self.last_final {
            return Some(self.alpha * close + (1.0 - self.alpha) * previous);
        }

        let mut seed = self.seed.clone();
        seed.push(close);
        if seed.len() < self.period {
            return None;
        }

        Some(seed.iter().sum::<f64>() / seed.len() as f64)
    }
}

fn validate_indicator_spec(spec: &IndicatorSpec) -> Result<(), StrategyError> {
    let _ = IndicatorAlgorithm::from_spec(spec)?;
    Ok(())
}

fn parse_ema_period(params_json: &str) -> Result<usize, StrategyError> {
    let value = serde_json::from_str::<serde_json::Value>(params_json).unwrap_or_else(|_| {
        serde_json::json!({
            "period": params_json.trim().parse::<usize>().unwrap_or(0)
        })
    });
    let period = value
        .get("period")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| {
            StrategyError::Parse(format!("EMA params missing integer period: {params_json}"))
        })?;
    Ok(period as usize)
}

fn update_series(
    series: &mut TimeframeSeries,
    instrument: &str,
    timeframe: Timeframe,
    ltp: f64,
    at: u64,
    max_recent_bars: usize,
    alignment: CandleAlignment,
) -> Result<Option<Bar>, StrategyError> {
    let (start_at, end_at) = bucket_bounds(timeframe, at, alignment)?;
    let next_bar = || Bar {
        instrument: instrument.to_string(),
        timeframe,
        start_at,
        end_at,
        open: ltp,
        high: ltp,
        low: ltp,
        close: ltp,
        volume: 0.0,
        is_closed: false,
    };

    match series.current.as_mut() {
        None => {
            series.current = Some(next_bar());
            Ok(None)
        }
        Some(current) if current.start_at == start_at => {
            current.high = current.high.max(ltp);
            current.low = current.low.min(ltp);
            current.close = ltp;
            Ok(None)
        }
        Some(current) if current.start_at > start_at => Ok(None),
        Some(_) => {
            let mut closed = series.current.take().expect("current bar must exist");
            closed.is_closed = true;
            merge_closed_bar(series, closed.clone(), max_recent_bars);
            series.current = Some(next_bar());
            Ok(Some(closed))
        }
    }
}

fn merge_closed_bar(series: &mut TimeframeSeries, bar: Bar, max_recent_bars: usize) {
    if series
        .closed
        .back()
        .is_some_and(|existing| existing.end_at == bar.end_at)
    {
        return;
    }

    series.closed.push_back(bar);
    while series.closed.len() > max_recent_bars {
        let _ = series.closed.pop_front();
    }
}

pub(crate) fn bucket_bounds(
    timeframe: Timeframe,
    unix_millis: u64,
    alignment: CandleAlignment,
) -> Result<(u64, u64), StrategyError> {
    let duration = timeframe_duration_millis(timeframe);
    let offset = i128::from(alignment.anchor_offset_seconds) * 1_000;
    let adjusted = i128::from(unix_millis) - offset;
    let start = adjusted.div_euclid(duration) * duration + offset;
    let mut end = start + duration;

    if let Some(close_offset_seconds) = alignment.session_close_offset_seconds {
        let day = (start - offset).div_euclid(timeframe_duration_millis(Timeframe::OneDay));
        let session_close = day * timeframe_duration_millis(Timeframe::OneDay)
            + i128::from(close_offset_seconds) * 1_000;
        if i128::from(unix_millis) < session_close && start < session_close && end > session_close {
            end = session_close;
        }
    }

    if start < 0 || end < 0 || start > i128::from(u64::MAX) || end > i128::from(u64::MAX) {
        return Err(StrategyError::Parse(format!(
            "computed invalid candle bucket for {unix_millis}"
        )));
    }

    Ok((start as u64, end as u64))
}

fn timeframe_duration_millis(timeframe: Timeframe) -> i128 {
    match timeframe {
        Timeframe::OneMinute => 60_000,
        Timeframe::ThreeMinute => 3 * 60_000,
        Timeframe::FiveMinute => 5 * 60_000,
        Timeframe::FifteenMinute => 15 * 60_000,
        Timeframe::ThirtyMinute => 30 * 60_000,
        Timeframe::SeventyFiveMinute => 75 * 60_000,
        Timeframe::OneHour => 60 * 60_000,
        Timeframe::FourHour => 4 * 60 * 60_000,
        Timeframe::OneDay => 24 * 60 * 60_000,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategy::IndicatorSpec;
    use chrono::TimeZone;
    use chrono::Utc;

    fn ema_spec() -> IndicatorSpec {
        IndicatorSpec {
            key: "ema20".to_string(),
            timeframe: Timeframe::FiveMinute,
            kind: "ema".to_string(),
            params_json: r#"{"period":20}"#.to_string(),
        }
    }

    #[test]
    fn warmup_seeds_ema_before_first_live_tick() {
        let engine = SharedTimeframeEngine::new(64);
        engine
            .register_ssu(&SsuConfig {
                ssu_id: 7,
                strategy_key: "noop".to_string(),
                enabled: true,
                trade_gap_secs: 0,
                max_overlap: 0,
                max_positions_per_day: 0,
                required_timeframes: vec![Timeframe::FiveMinute],
                indicator_specs: vec![ema_spec()],
                params_json: "{}".to_string(),
            })
            .expect("register");

        let bars = (0..20)
            .map(|index| Bar {
                instrument: "NIFTY".to_string(),
                timeframe: Timeframe::FiveMinute,
                start_at: index * 300_000,
                end_at: (index + 1) * 300_000,
                open: 100.0 + index as f64,
                high: 101.0 + index as f64,
                low: 99.0 + index as f64,
                close: 100.5 + index as f64,
                volume: 0.0,
                is_closed: true,
            })
            .collect::<Vec<_>>();

        engine
            .warmup("NIFTY", Timeframe::FiveMinute, &bars, 7)
            .expect("warmup");

        let indicator = engine
            .indicator_value(7, "NIFTY", "ema20")
            .expect("indicator");
        assert!(indicator.is_final);
        assert!(indicator.value > 0.0);
    }

    #[test]
    fn one_hour_buckets_are_utc_aligned() {
        let inside_hour = Utc
            .with_ymd_and_hms(2025, 1, 7, 0, 45, 0)
            .single()
            .expect("time")
            .timestamp_millis() as u64;
        let expected_start = Utc
            .with_ymd_and_hms(2025, 1, 7, 0, 0, 0)
            .single()
            .expect("start")
            .timestamp_millis() as u64;
        let expected_end = Utc
            .with_ymd_and_hms(2025, 1, 7, 1, 0, 0)
            .single()
            .expect("end")
            .timestamp_millis() as u64;

        assert_eq!(
            bucket_bounds(Timeframe::OneHour, inside_hour, CandleAlignment::UTC).expect("bucket"),
            (expected_start, expected_end)
        );
    }

    #[test]
    fn one_hour_buckets_can_follow_indian_market_anchor() {
        let inside_hour = Utc
            .with_ymd_and_hms(2025, 1, 7, 4, 0, 0)
            .single()
            .expect("time")
            .timestamp_millis() as u64;
        let expected_start = Utc
            .with_ymd_and_hms(2025, 1, 7, 3, 45, 0)
            .single()
            .expect("start")
            .timestamp_millis() as u64;
        let expected_end = Utc
            .with_ymd_and_hms(2025, 1, 7, 4, 45, 0)
            .single()
            .expect("end")
            .timestamp_millis() as u64;

        assert_eq!(
            bucket_bounds(
                Timeframe::OneHour,
                inside_hour,
                CandleAlignment::INDIAN_MARKET
            )
            .expect("bucket"),
            (expected_start, expected_end)
        );
    }

    #[test]
    fn indian_market_last_hour_clips_to_session_close() {
        let inside_last_hour = Utc
            .with_ymd_and_hms(2025, 1, 7, 9, 50, 0)
            .single()
            .expect("time")
            .timestamp_millis() as u64;
        let expected_start = Utc
            .with_ymd_and_hms(2025, 1, 7, 9, 45, 0)
            .single()
            .expect("start")
            .timestamp_millis() as u64;
        let expected_end = Utc
            .with_ymd_and_hms(2025, 1, 7, 10, 0, 0)
            .single()
            .expect("end")
            .timestamp_millis() as u64;

        assert_eq!(
            bucket_bounds(
                Timeframe::OneHour,
                inside_last_hour,
                CandleAlignment::INDIAN_MARKET
            )
            .expect("bucket"),
            (expected_start, expected_end)
        );
    }

    #[test]
    fn live_tick_updates_current_bar_and_non_final_ema() {
        let engine = SharedTimeframeEngine::new(64);
        engine
            .register_ssu(&SsuConfig {
                ssu_id: 7,
                strategy_key: "noop".to_string(),
                enabled: true,
                trade_gap_secs: 0,
                max_overlap: 0,
                max_positions_per_day: 0,
                required_timeframes: vec![Timeframe::FiveMinute],
                indicator_specs: vec![ema_spec()],
                params_json: "{}".to_string(),
            })
            .expect("register");
        let bars = (0..20)
            .map(|index| Bar {
                instrument: "NIFTY".to_string(),
                timeframe: Timeframe::FiveMinute,
                start_at: 1_700_000_000_000 + index * 300_000,
                end_at: 1_700_000_300_000 + index * 300_000,
                open: 100.0 + index as f64,
                high: 101.0 + index as f64,
                low: 99.0 + index as f64,
                close: 100.5 + index as f64,
                volume: 0.0,
                is_closed: true,
            })
            .collect::<Vec<_>>();
        engine
            .warmup("NIFTY", Timeframe::FiveMinute, &bars, 7)
            .expect("warmup");

        let tick_at = 1_700_006_100_000;
        engine.on_tick("NIFTY", 140.0, tick_at).expect("tick");

        let current = engine
            .current_bar("NIFTY", Timeframe::FiveMinute)
            .expect("current");
        assert!(!current.is_closed);
        assert_eq!(current.close, 140.0);

        let indicator = engine
            .indicator_value(7, "NIFTY", "ema20")
            .expect("indicator");
        assert!(!indicator.is_final);
        assert!(indicator.value > 0.0);
    }

    #[test]
    fn rollover_finalizes_bar_and_marks_closed_timeframe() {
        let engine = SharedTimeframeEngine::new(64);
        engine
            .register_ssu(&SsuConfig {
                ssu_id: 7,
                strategy_key: "noop".to_string(),
                enabled: true,
                trade_gap_secs: 0,
                max_overlap: 0,
                max_positions_per_day: 0,
                required_timeframes: vec![Timeframe::FiveMinute],
                indicator_specs: vec![ema_spec()],
                params_json: "{}".to_string(),
            })
            .expect("register");

        let first_tick = 1_700_006_100_000;
        engine.on_tick("NIFTY", 140.0, first_tick).expect("tick");
        let second_tick = first_tick + 300_000;
        let update = engine.on_tick("NIFTY", 141.0, second_tick).expect("tick");

        assert_eq!(update.closed_timeframes, vec![Timeframe::FiveMinute]);
        let last_closed = engine
            .last_closed_bar("NIFTY", Timeframe::FiveMinute)
            .expect("closed");
        assert!(last_closed.is_closed);
        assert_eq!(last_closed.close, 140.0);
    }
}
