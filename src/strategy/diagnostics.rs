use std::sync::atomic::{AtomicBool, Ordering};

use crate::config::StrategyDiagnosticsSection;

static CLOSED_CANDLE_DECISIONS: AtomicBool = AtomicBool::new(false);
static WARMUP_REPLAY: AtomicBool = AtomicBool::new(false);

pub(crate) fn configure(config: &StrategyDiagnosticsSection) {
    CLOSED_CANDLE_DECISIONS.store(config.closed_candle_decisions, Ordering::Relaxed);
    WARMUP_REPLAY.store(config.warmup_replay, Ordering::Relaxed);
}

pub(crate) fn closed_candle_decisions_enabled() -> bool {
    CLOSED_CANDLE_DECISIONS.load(Ordering::Relaxed)
}

pub(crate) fn warmup_replay_enabled() -> bool {
    WARMUP_REPLAY.load(Ordering::Relaxed)
}
