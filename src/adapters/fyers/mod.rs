pub mod latest_price_file;
pub mod live;
pub mod master;
pub mod runtime;
pub mod session;
pub mod token;

use crate::config::FyersBrokerSection;
use crate::feeder::FeedError;

pub fn run_live(config: &FyersBrokerSection, max_events: usize) -> Result<(), FeedError> {
    runtime::run_live(config, max_events)
}
