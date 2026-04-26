mod adaptive_supertrend;
mod candle_cycle;
mod dhanrekha;
mod ema_pullback_scalp;
mod exponential_edge;
mod noop;

use std::sync::Arc;

use crate::strategy::{Strategy, StrategyError};

pub(crate) fn strategy_by_key(strategy_key: &str) -> Result<Arc<dyn Strategy>, StrategyError> {
    match strategy_key.trim().to_ascii_lowercase().as_str() {
        "adaptive_supertrend" => Ok(Arc::new(
            adaptive_supertrend::AdaptiveSupertrendStrategy::default(),
        )),
        "candle_cycle" => Ok(Arc::new(candle_cycle::CandleCycleStrategy::default())),
        "dhanrekha" => Ok(Arc::new(dhanrekha::DhanrekhaStrategy::default())),
        "ema_pullback_scalp" => Ok(Arc::new(
            ema_pullback_scalp::EmaPullbackScalpStrategy::default(),
        )),
        "exponential_edge" => Ok(Arc::new(
            exponential_edge::ExponentialEdgeStrategy::default(),
        )),
        "noop" => Ok(Arc::new(noop::NoopStrategy)),
        value => Err(StrategyError::Unsupported(format!(
            "unsupported strategy_key {value}; available builtins: adaptive_supertrend, candle_cycle, dhanrekha, ema_pullback_scalp, exponential_edge, noop"
        ))),
    }
}
