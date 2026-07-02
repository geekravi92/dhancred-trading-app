use std::collections::BTreeMap;

use crate::feeder::catalog::{InstrumentCatalog, InstrumentDefinition};
use crate::feeder::market_session::{MarketSessionPolicy, MarketSessionSchedule};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CandleAlignment {
    pub anchor_offset_seconds: i32,
    pub session_close_offset_seconds: Option<i32>,
}

pub type CandleAlignmentMap = BTreeMap<String, CandleAlignment>;

impl CandleAlignment {
    pub const UTC: Self = Self {
        anchor_offset_seconds: 0,
        session_close_offset_seconds: None,
    };

    pub fn for_instrument(
        instrument: &InstrumentDefinition,
        market_sessions: &MarketSessionSchedule,
    ) -> Self {
        market_sessions
            .policy_for_exchange(&instrument.exchange)
            .map(Self::from_market_session)
            .unwrap_or(Self::UTC)
    }

    fn from_market_session(policy: &MarketSessionPolicy) -> Self {
        Self {
            anchor_offset_seconds: policy.candle_anchor_offset_seconds(),
            session_close_offset_seconds: policy.candle_close_offset_seconds(),
        }
    }
}

pub fn candle_alignments_from_catalog(
    catalog: &InstrumentCatalog,
    market_sessions: &MarketSessionSchedule,
) -> CandleAlignmentMap {
    catalog
        .instruments()
        .map(|instrument| {
            (
                instrument.instrument_name.to_string(),
                CandleAlignment::for_instrument(instrument, market_sessions),
            )
        })
        .collect()
}

pub fn merge_candle_alignments(
    target: &mut CandleAlignmentMap,
    source: impl IntoIterator<Item = (String, CandleAlignment)>,
) {
    for (instrument, alignment) in source {
        target.insert(instrument, alignment);
    }
}
