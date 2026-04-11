pub mod master;
pub mod session;
pub mod token;

use crate::config::FyersBrokerSection;
use crate::feeder::FeedError;

pub fn run_live(config: &FyersBrokerSection, _max_events: usize) -> Result<(), FeedError> {
    if !config.enabled {
        return Ok(());
    }

    let summaries = master::refresh_all(config)?;
    println!("FYERS symbol master refreshed");
    for summary in summaries {
        println!(
            "  {} | {} | {} instruments | {}",
            summary.source,
            if summary.downloaded {
                "downloaded"
            } else {
                "cached"
            },
            summary.instrument_count,
            summary.output_path.display()
        );
    }

    session::wait_for_market_session(config.market_sessions.as_deref())?;

    Err(FeedError::Config(
        "FYERS live WebSocket integration is not implemented yet".to_string(),
    ))
}
