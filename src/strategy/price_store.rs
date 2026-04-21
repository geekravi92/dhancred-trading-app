use std::collections::BTreeMap;
use std::sync::RwLock;

use crate::strategy::PriceSnapshot;

pub trait PriceStore: Send + Sync {
    fn put_price(&self, instrument: &str, ltp: f64, updated_at: u64);
    fn get_price(&self, instrument: &str) -> Option<PriceSnapshot>;
}

#[derive(Debug, Default)]
pub struct InMemoryPriceStore {
    prices: RwLock<BTreeMap<String, PriceSnapshot>>,
}

impl InMemoryPriceStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl PriceStore for InMemoryPriceStore {
    fn put_price(&self, instrument: &str, ltp: f64, updated_at: u64) {
        self.prices
            .write()
            .expect("price store lock poisoned")
            .insert(
                instrument.to_string(),
                PriceSnapshot {
                    instrument: instrument.to_string(),
                    ltp,
                    updated_at,
                },
            );
    }

    fn get_price(&self, instrument: &str) -> Option<PriceSnapshot> {
        self.prices
            .read()
            .expect("price store lock poisoned")
            .get(instrument)
            .cloned()
    }
}
