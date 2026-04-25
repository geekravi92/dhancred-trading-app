mod candle_cycle;
mod ema_pullback_scalp;
mod noop;

use std::sync::Arc;

use crate::strategy::{Strategy, StrategyError};

pub(crate) fn strategy_by_key(strategy_key: &str) -> Result<Arc<dyn Strategy>, StrategyError> {
    match strategy_key.trim().to_ascii_lowercase().as_str() {
        "candle_cycle" => Ok(Arc::new(candle_cycle::CandleCycleStrategy::default())),
        "ema_pullback_scalp" => Ok(Arc::new(
            ema_pullback_scalp::EmaPullbackScalpStrategy::default(),
        )),
        "noop" => Ok(Arc::new(noop::NoopStrategy)),
        value => Err(StrategyError::Unsupported(format!(
            "unsupported strategy_key {value}; available builtins: candle_cycle, ema_pullback_scalp, noop"
        ))),
    }
}
