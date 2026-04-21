pub mod historical;
pub mod latest_price_file;
pub mod live;
pub mod master;
pub mod runtime;
pub mod session;
pub mod token;

use crate::config::{FyersBrokerSection, HistoricalCandlesSection};
use crate::feeder::FeedError;
use crate::strategy::StrategyRuntimeHandle;
use std::sync::Arc;

pub fn run_live(
    config: &FyersBrokerSection,
    historical_candles_config: Option<&HistoricalCandlesSection>,
    strategy_runtime: Option<Arc<StrategyRuntimeHandle>>,
    max_events: usize,
) -> Result<(), FeedError> {
    runtime::run_live(config, historical_candles_config, strategy_runtime, max_events)
}
