use std::collections::VecDeque;

use crate::feeder::{FeedError, FeedSubscription, Feeder, PriceEvent};

pub struct HistoricalReplayFeeder {
    events: VecDeque<PriceEvent>,
    subscription: Option<FeedSubscription>,
}

impl HistoricalReplayFeeder {
    pub fn new(events: Vec<PriceEvent>) -> Self {
        Self {
            events: events.into(),
            subscription: None,
        }
    }
}

impl Feeder for HistoricalReplayFeeder {
    fn subscribe(&mut self, subscription: FeedSubscription) -> Result<(), FeedError> {
        self.subscription = Some(subscription);
        Ok(())
    }

    fn next_event(&mut self) -> Result<Option<PriceEvent>, FeedError> {
        let subscription = self.subscription.as_ref().ok_or(FeedError::NotSubscribed)?;

        while let Some(event) = self.events.pop_front() {
            if subscription.wants(event.instrument_name(), &event.channel()) {
                return Ok(Some(event));
            }
        }

        Ok(None)
    }
}
