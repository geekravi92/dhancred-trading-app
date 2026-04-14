pub mod historical;
pub mod latest_price_file;
pub mod live;
pub mod master;
pub mod runtime;
pub mod session;
pub mod token;

use crate::config::{FyersBrokerSection, HistoricalCandlesSection};
use crate::feeder::FeedError;

pub fn run_live(
    config: &FyersBrokerSection,
    historical_candles_config: Option<&HistoricalCandlesSection>,
    max_events: usize,
) -> Result<(), FeedError> {
    runtime::run_live(config, historical_candles_config, max_events)
}
