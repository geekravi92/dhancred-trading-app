use crate::feeder::{FeedChannel, FeedError, FeedSubscription, Feeder, PriceEvent};

pub mod historical;
pub mod latest_price_file;
pub mod live;
pub mod product_master;
pub mod runtime;

pub struct DeltaFeederAdapter {
    subscription: Option<FeedSubscription>,
}

impl DeltaFeederAdapter {
    pub fn new() -> Self {
        Self { subscription: None }
    }
}

impl Default for DeltaFeederAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl Feeder for DeltaFeederAdapter {
    fn subscribe(&mut self, subscription: FeedSubscription) -> Result<(), FeedError> {
        for channel in subscription.channels() {
            ensure_supported(channel)?;
        }

        self.subscription = Some(subscription);
        Ok(())
    }

    fn next_event(&mut self) -> Result<Option<PriceEvent>, FeedError> {
        if self.subscription.is_none() {
            return Err(FeedError::NotSubscribed);
        }

        Err(FeedError::Disconnected(
            "delta websocket implementation is not wired yet".to_string(),
        ))
    }
}

fn ensure_supported(channel: &FeedChannel) -> Result<(), FeedError> {
    match channel {
        FeedChannel::PriceTick | FeedChannel::PriceCandle(_) => Ok(()),
    }
}
