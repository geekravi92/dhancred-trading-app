pub mod catalog;
pub mod error;
pub mod event;
pub mod historical_candles;
pub mod universe;

pub use catalog::{
    InstrumentCatalog, InstrumentDefinition, InstrumentName, InstrumentType, InstrumentUniverse,
    UNIVERSAL_INSTRUMENT_CSV_HEADER, UniverseFilter, parse_instrument_type,
};
pub use error::FeedError;
pub use event::{Candle, FeedChannel, Price, PriceEvent, PriceTick, Timeframe, UnixMillis};
pub use universe::{RefreshDecision, SubscriptionDiff, UniverseRefreshState};

pub trait Feeder {
    fn subscribe(&mut self, subscription: FeedSubscription) -> Result<(), FeedError>;

    fn next_event(&mut self) -> Result<Option<PriceEvent>, FeedError>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FeedSubscription {
    instruments: Vec<InstrumentName>,
    channels: Vec<FeedChannel>,
}

impl FeedSubscription {
    pub fn new(instruments: Vec<InstrumentName>, channels: Vec<FeedChannel>) -> Self {
        Self {
            instruments,
            channels,
        }
    }

    pub fn instruments(&self) -> &[InstrumentName] {
        &self.instruments
    }

    pub fn channels(&self) -> &[FeedChannel] {
        &self.channels
    }

    pub fn wants(&self, instrument: &InstrumentName, channel: &FeedChannel) -> bool {
        self.instruments.iter().any(|wanted| wanted == instrument)
            && self.channels.iter().any(|wanted| wanted == channel)
    }
}
