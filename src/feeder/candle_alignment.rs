use std::collections::BTreeMap;

use crate::feeder::catalog::{InstrumentCatalog, InstrumentDefinition};

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

    pub const INDIAN_MARKET: Self = Self {
        // 09:15 IST is 03:45 UTC.
        anchor_offset_seconds: 3 * 60 * 60 + 45 * 60,
        // 15:30 IST is 10:00 UTC.
        session_close_offset_seconds: Some(10 * 60 * 60),
    };

    pub fn for_instrument(instrument: &InstrumentDefinition) -> Self {
        let exchange = instrument.exchange.trim().to_ascii_uppercase();
        let broker = instrument.broker.trim().to_ascii_uppercase();

        if broker == "FYERS" || matches!(exchange.as_str(), "NSE" | "BSE" | "NFO" | "BFO" | "MCX") {
            Self::INDIAN_MARKET
        } else {
            Self::UTC
        }
    }
}

pub fn candle_alignments_from_catalog(catalog: &InstrumentCatalog) -> CandleAlignmentMap {
    catalog
        .instruments()
        .map(|instrument| {
            (
                instrument.instrument_name.to_string(),
                CandleAlignment::for_instrument(instrument),
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
