mod adaptive_supertrend;

use std::sync::Arc;

use crate::strategy::{Strategy, StrategyError};

pub(crate) fn strategy_by_key(strategy_key: &str) -> Result<Arc<dyn Strategy>, StrategyError> {
    match strategy_key.trim().to_ascii_lowercase().as_str() {
        "adaptive_supertrend" => Ok(Arc::new(
            adaptive_supertrend::AdaptiveSupertrendStrategy::default(),
        )),
        value => Err(StrategyError::Unsupported(format!(
            "unsupported strategy_key {value}; available builtins: adaptive_supertrend"
        ))),
    }
}
