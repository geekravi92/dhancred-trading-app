use crate::strategy::{
    PriceUpdated, SsuConfig, Strategy, StrategyContext, StrategyError, StrategySignal,
    TimeframeUpdate,
};

#[derive(Debug)]
pub(crate) struct NoopStrategy;

impl Strategy for NoopStrategy {
    fn strategy_key(&self) -> &'static str {
        "noop"
    }

    fn on_price_updated(
        &self,
        _ctx: &StrategyContext,
        _ssu: &SsuConfig,
        _event: &PriceUpdated,
        _tf_update: &TimeframeUpdate,
    ) -> Result<Vec<StrategySignal>, StrategyError> {
        Ok(Vec::new())
    }
}
