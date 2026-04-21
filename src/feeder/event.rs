use std::fmt;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct InstrumentName(String);

impl InstrumentName {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for InstrumentName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum FeedChannel {
    PriceTick,
    PriceCandle(Timeframe),
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Timeframe {
    OneMinute,
    ThreeMinute,
    FiveMinute,
    FifteenMinute,
    OneHour,
    OneDay,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct UnixMillis(u64);

impl UnixMillis {
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn as_u64(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd)]
pub struct Price(f64);

impl Price {
    pub fn new(value: f64) -> Result<Self, &'static str> {
        if value.is_finite() && value > 0.0 {
            Ok(Self(value))
        } else {
            Err("price must be finite and positive")
        }
    }

    pub fn as_f64(self) -> f64 {
        self.0
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum PriceEvent {
    Tick(PriceTick),
    Candle(Candle),
}

impl PriceEvent {
    pub fn instrument_name(&self) -> &InstrumentName {
        match self {
            Self::Tick(tick) => &tick.instrument_name,
            Self::Candle(candle) => &candle.instrument_name,
        }
    }

    pub fn channel(&self) -> FeedChannel {
        match self {
            Self::Tick(_) => FeedChannel::PriceTick,
            Self::Candle(candle) => FeedChannel::PriceCandle(candle.timeframe),
        }
    }

    pub fn price(&self) -> Price {
        match self {
            Self::Tick(tick) => tick.price,
            Self::Candle(candle) => candle.price,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct PriceTick {
    pub instrument_name: InstrumentName,
    pub price: Price,
    pub time: UnixMillis,
}

impl PriceTick {
    pub fn new(instrument_name: InstrumentName, price: Price, time: UnixMillis) -> Self {
        Self {
            instrument_name,
            price,
            time,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Candle {
    pub instrument_name: InstrumentName,
    pub timeframe: Timeframe,
    pub start_time: UnixMillis,
    pub end_time: UnixMillis,
    pub open: Price,
    pub high: Price,
    pub low: Price,
    pub close: Price,
    pub price: Price,
    pub volume: f64,
}

impl Candle {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        instrument_name: InstrumentName,
        timeframe: Timeframe,
        start_time: UnixMillis,
        end_time: UnixMillis,
        open: Price,
        high: Price,
        low: Price,
        close: Price,
        volume: f64,
    ) -> Self {
        Self {
            instrument_name,
            timeframe,
            start_time,
            end_time,
            open,
            high,
            low,
            close,
            price: close,
            volume,
        }
    }
}
