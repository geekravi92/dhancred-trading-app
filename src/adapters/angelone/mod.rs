pub mod auth;
pub mod latest_price_file;
pub mod live;
pub mod master;
pub mod runtime;
pub mod universe;

use std::sync::Arc;

use crate::config::{AngeloneBrokerSection, HistoricalCandlesSection, MarketSessionSection};
use crate::feeder::FeedError;
use crate::strategy::StrategyRuntimeHandle;

pub fn run_live(
    config: &AngeloneBrokerSection,
    market_sessions: &[MarketSessionSection],
    historical_candles_config: Option<&HistoricalCandlesSection>,
    strategy_runtime: Option<Arc<StrategyRuntimeHandle>>,
    max_events: usize,
) -> Result<(), FeedError> {
    runtime::run_live(
        config,
        market_sessions,
        historical_candles_config,
        strategy_runtime,
        max_events,
    )
}
