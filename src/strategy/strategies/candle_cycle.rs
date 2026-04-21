use std::collections::BTreeMap;
use std::sync::Mutex;

use serde::Deserialize;

use crate::strategy::{
    EntrySignal, ExitSignal, PositionStatus, PriceUpdated, SignalSide, Strategy,
    StrategyContext, StrategyError, StrategySignal, SsuConfig, Timeframe, TimeframeUpdate,
};

#[derive(Debug, Default)]
pub(crate) struct CandleCycleStrategy {
    settings: Mutex<BTreeMap<i64, CandleCycleSettings>>,
    states: Mutex<BTreeMap<StateKey, CycleState>>,
}

impl Strategy for CandleCycleStrategy {
    fn strategy_key(&self) -> &'static str {
        "candle_cycle"
    }

    fn on_price_updated(
        &self,
        ctx: &StrategyContext,
        ssu: &SsuConfig,
        event: &PriceUpdated,
        tf_update: &TimeframeUpdate,
    ) -> Result<Vec<StrategySignal>, StrategyError> {
        let settings = self.settings_for(ssu)?;
        if !tf_update.closed_timeframes.contains(&settings.timeframe) {
            return Ok(Vec::new());
        }

        let Some(closed_bar) = ctx
            .timeframes
            .last_closed_bar(&event.trigger_instrument, settings.timeframe)
        else {
            return Ok(Vec::new());
        };

        let state_key = StateKey::new(ssu.ssu_id, &event.trigger_instrument);
        let mut states = self.states.lock().expect("candle cycle state lock poisoned");
        let state = states.entry(state_key).or_default();
        if state
            .last_processed_closed_end
            .is_some_and(|end_at| end_at >= closed_bar.end_at)
        {
            return Ok(Vec::new());
        }
        state.last_processed_closed_end = Some(closed_bar.end_at);

        let has_open_position = ctx
            .strategy_positions
            .list_open_by_ssu(ssu.ssu_id)?
            .into_iter()
            .any(|position| {
                position.trade_instrument == event.trigger_instrument
                    && position.side == settings.side
                    && position.status == PositionStatus::Open
            });
        let execution_price = current_ltp(ctx, &event.trigger_instrument).unwrap_or(closed_bar.close);

        if has_open_position {
            state.bars_since_entry = state.bars_since_entry.saturating_add(1);
            if state.bars_since_entry < settings.hold_candles {
                return Ok(Vec::new());
            }

            state.bars_since_entry = 0;
            state.cooldown_remaining = settings.cooldown_candles;
            let signal = ExitSignal {
                ssu_id: ssu.ssu_id,
                trigger_instrument: event.trigger_instrument.clone(),
                trade_instrument: event.trigger_instrument.clone(),
                side: settings.side,
                price: execution_price,
                reason: format!(
                    "candle_cycle_exit|timeframe={}|held={}|closed_bar_end={}",
                    timeframe_label(settings.timeframe),
                    settings.hold_candles,
                    closed_bar.end_at
                ),
                at: event.at,
            };
            return match ctx.strategy_positions.close_position(&signal) {
                Ok(_) => Ok(vec![StrategySignal::Exit(signal)]),
                Err(StrategyError::Rule(_)) | Err(StrategyError::NotFound(_)) => Ok(Vec::new()),
                Err(error) => Err(error),
            };
        }

        if state.cooldown_remaining > 0 {
            state.cooldown_remaining -= 1;
            if state.cooldown_remaining > 0 {
                return Ok(Vec::new());
            }
        }

        state.bars_since_entry = 0;
        let signal = EntrySignal {
            ssu_id: ssu.ssu_id,
            trigger_instrument: event.trigger_instrument.clone(),
            trade_instrument: event.trigger_instrument.clone(),
            side: settings.side,
            price: execution_price,
            reason: format!(
                "candle_cycle_entry|timeframe={}|hold={}|cooldown={}|closed_bar_end={}",
                timeframe_label(settings.timeframe),
                settings.hold_candles,
                settings.cooldown_candles,
                closed_bar.end_at
            ),
            at: event.at,
        };
        match ctx.strategy_positions.open_position(&signal, ssu) {
            Ok(_) => Ok(vec![StrategySignal::Entry(signal)]),
            Err(StrategyError::Rule(_)) => Ok(Vec::new()),
            Err(error) => Err(error),
        }
    }
}

impl CandleCycleStrategy {
    fn settings_for(&self, ssu: &SsuConfig) -> Result<CandleCycleSettings, StrategyError> {
        if let Some(settings) = self
            .settings
            .lock()
            .expect("candle cycle settings lock poisoned")
            .get(&ssu.ssu_id)
            .cloned()
        {
            return Ok(settings);
        }

        let parsed = CandleCycleSettings::from_ssu(ssu)?;
        self.settings
            .lock()
            .expect("candle cycle settings lock poisoned")
            .insert(ssu.ssu_id, parsed.clone());
        Ok(parsed)
    }
}

#[derive(Clone, Debug)]
struct CandleCycleSettings {
    timeframe: Timeframe,
    hold_candles: u32,
    cooldown_candles: u32,
    side: SignalSide,
}

impl CandleCycleSettings {
    fn from_ssu(ssu: &SsuConfig) -> Result<Self, StrategyError> {
        #[derive(Deserialize, Default)]
        struct RawSettings {
            timeframe: Option<String>,
            hold_candles: Option<u32>,
            cooldown_candles: Option<u32>,
            side: Option<String>,
        }

        let raw = if ssu.params_json.trim().is_empty() {
            RawSettings::default()
        } else {
            serde_json::from_str::<RawSettings>(&ssu.params_json).map_err(|error| {
                StrategyError::Parse(format!(
                    "invalid candle_cycle params_json for SSU {}: {error}",
                    ssu.ssu_id
                ))
            })?
        };

        let timeframe = match raw.timeframe.as_deref() {
            Some(value) => parse_timeframe(value)?,
            None => default_timeframe(ssu)?,
        };
        if !ssu.required_timeframes.contains(&timeframe)
            && !ssu
                .indicator_specs
                .iter()
                .any(|spec| spec.timeframe == timeframe)
        {
            return Err(StrategyError::Config(format!(
                "SSU {} candle_cycle timeframe {} is not registered in required_timeframes or indicator_specs",
                ssu.ssu_id,
                timeframe_label(timeframe)
            )));
        }

        let hold_candles = raw.hold_candles.unwrap_or(3);
        if hold_candles == 0 {
            return Err(StrategyError::Config(format!(
                "SSU {} candle_cycle hold_candles must be positive",
                ssu.ssu_id
            )));
        }

        Ok(Self {
            timeframe,
            hold_candles,
            cooldown_candles: raw.cooldown_candles.unwrap_or(2),
            side: parse_side(raw.side.as_deref().unwrap_or("long"))?,
        })
    }
}

#[derive(Clone, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
struct StateKey {
    ssu_id: i64,
    instrument: String,
}

impl StateKey {
    fn new(ssu_id: i64, instrument: &str) -> Self {
        Self {
            ssu_id,
            instrument: instrument.to_string(),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct CycleState {
    last_processed_closed_end: Option<u64>,
    bars_since_entry: u32,
    cooldown_remaining: u32,
}

fn current_ltp(ctx: &StrategyContext, instrument: &str) -> Option<f64> {
    ctx.prices.get_price(instrument).map(|snapshot| snapshot.ltp)
}

fn default_timeframe(ssu: &SsuConfig) -> Result<Timeframe, StrategyError> {
    ssu.required_timeframes
        .first()
        .copied()
        .or_else(|| ssu.indicator_specs.first().map(|spec| spec.timeframe))
        .ok_or_else(|| {
            StrategyError::Config(format!(
                "SSU {} candle_cycle requires at least one timeframe",
                ssu.ssu_id
            ))
        })
}

fn parse_side(value: &str) -> Result<SignalSide, StrategyError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "long" => Ok(SignalSide::Long),
        "short" => Ok(SignalSide::Short),
        other => Err(StrategyError::Parse(format!(
            "unsupported candle_cycle side {other}; expected long or short"
        ))),
    }
}

fn parse_timeframe(value: &str) -> Result<Timeframe, StrategyError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1m" | "one_minute" | "oneminute" => Ok(Timeframe::OneMinute),
        "3m" | "three_minute" | "threeminute" => Ok(Timeframe::ThreeMinute),
        "5m" | "five_minute" | "fiveminute" => Ok(Timeframe::FiveMinute),
        "15m" | "fifteen_minute" | "fifteenminute" => Ok(Timeframe::FifteenMinute),
        "1h" | "one_hour" | "onehour" | "60m" => Ok(Timeframe::OneHour),
        "1d" | "one_day" | "oneday" => Ok(Timeframe::OneDay),
        other => Err(StrategyError::Parse(format!(
            "unsupported candle_cycle timeframe {other}"
        ))),
    }
}

fn timeframe_label(timeframe: Timeframe) -> &'static str {
    match timeframe {
        Timeframe::OneMinute => "1m",
        Timeframe::ThreeMinute => "3m",
        Timeframe::FiveMinute => "5m",
        Timeframe::FifteenMinute => "15m",
        Timeframe::OneHour => "1h",
        Timeframe::OneDay => "1d",
    }
}
