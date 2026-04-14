use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration as StdDuration, SystemTime, UNIX_EPOCH};

use chrono::{Datelike, Duration as ChronoDuration, FixedOffset, TimeZone, Timelike};

use crate::config::HistoricalCandlesSection;
use crate::feeder::{
    Candle, FeedError, InstrumentCatalog, InstrumentName, InstrumentType, Price, PriceTick,
    Timeframe, UnixMillis,
};
use crate::storage::historical_candles::HistoricalCandleStore;

const IST_OFFSET_SECONDS: i32 = 5 * 60 * 60 + 30 * 60;
const FLUSH_POLL_INTERVAL: StdDuration = StdDuration::from_secs(1);

pub struct HistoricalCandleService {
    raw_to_canonical: BTreeMap<InstrumentName, InstrumentName>,
    state: Arc<Mutex<CandleState>>,
    store: Option<HistoricalCandleStore>,
    flusher: Option<HistoricalCandleFlusher>,
}

impl HistoricalCandleService {
    pub fn new(
        config: Option<&HistoricalCandlesSection>,
        catalog: &InstrumentCatalog,
    ) -> Result<Self, FeedError> {
        let raw_to_canonical = catalog
            .instruments()
            .filter(|instrument| instrument.instrument_type == InstrumentType::Spot)
            .filter(|instrument| instrument.tradable)
            .map(|instrument| {
                (
                    InstrumentName::new(instrument.trading_symbol.clone()),
                    instrument.instrument_name.clone(),
                )
            })
            .collect::<BTreeMap<_, _>>();

        let state = Arc::new(Mutex::new(CandleState::default()));
        let store = match config {
            Some(config) if config.enabled => {
                Some(HistoricalCandleStore::open(&config.sqlite_path)?)
            }
            _ => None,
        };
        let flusher = match config {
            Some(config) if config.enabled => Some(HistoricalCandleFlusher::spawn(
                config.sqlite_path.clone(),
                Arc::clone(&state),
            )?),
            _ => None,
        };

        Ok(Self {
            raw_to_canonical,
            state,
            store,
            flusher,
        })
    }

    pub fn on_tick(&mut self, tick: &PriceTick) -> Result<(), FeedError> {
        let Some(canonical_name) = self.raw_to_canonical.get(&tick.instrument_name).cloned() else {
            return Ok(());
        };

        let completed = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| FeedError::Io("historical candle state lock poisoned".to_string()))?;
            state.aggregate_tick(canonical_name, tick)?
        };

        let Some(completed) = completed else {
            return Ok(());
        };

        if let Some(store) = &self.store {
            store.upsert_candle(&completed)?;
        }

        Ok(())
    }

    #[cfg(test)]
    fn flush_due_before_for_test(&mut self, now_millis: u64) -> Result<Vec<Candle>, FeedError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| FeedError::Io("historical candle state lock poisoned".to_string()))?;
        Ok(state.flush_due_before(now_millis))
    }
}

impl Drop for HistoricalCandleService {
    fn drop(&mut self) {
        if let Some(flusher) = self.flusher.take() {
            drop(flusher);
        }
    }
}

#[derive(Default)]
struct CandleState {
    active: BTreeMap<InstrumentName, InProgressCandle>,
    last_closed_end: BTreeMap<InstrumentName, u64>,
}

impl CandleState {
    fn aggregate_tick(
        &mut self,
        instrument_name: InstrumentName,
        tick: &PriceTick,
    ) -> Result<Option<Candle>, FeedError> {
        let (bucket_start, bucket_end) = one_minute_bucket_bounds_ist(tick.time.as_u64())?;
        let already_closed_until = self
            .last_closed_end
            .get(&instrument_name)
            .copied()
            .unwrap_or_default();
        if bucket_end <= already_closed_until {
            return Ok(None);
        }

        let current_bucket_start = self
            .active
            .get(&instrument_name)
            .map(|candle| candle.start_time);
        match current_bucket_start {
            None => {
                self.active.insert(
                    instrument_name,
                    InProgressCandle::new(bucket_start, bucket_end, tick.price),
                );
                Ok(None)
            }
            Some(start_time) if bucket_start == start_time => {
                if let Some(existing) = self.active.get_mut(&instrument_name) {
                    existing.apply_price(tick.price);
                }
                Ok(None)
            }
            Some(start_time) if bucket_start < start_time => Ok(None),
            Some(_) => {
                let completed = self
                    .active
                    .insert(
                        instrument_name.clone(),
                        InProgressCandle::new(bucket_start, bucket_end, tick.price),
                    )
                    .expect("active candle must exist");
                self.last_closed_end
                    .insert(instrument_name.clone(), completed.end_time);
                Ok(Some(completed.to_candle(instrument_name)))
            }
        }
    }

    fn flush_due_before(&mut self, now_millis: u64) -> Vec<Candle> {
        let due_instruments = self
            .active
            .iter()
            .filter(|(_, candle)| candle.end_time <= now_millis)
            .map(|(instrument_name, _)| instrument_name.clone())
            .collect::<Vec<_>>();
        let mut completed = Vec::with_capacity(due_instruments.len());

        for instrument_name in due_instruments {
            if let Some(candle) = self.active.remove(&instrument_name) {
                self.last_closed_end
                    .insert(instrument_name.clone(), candle.end_time);
                completed.push(candle.to_candle(instrument_name));
            }
        }

        completed
    }
}

#[derive(Clone, Copy)]
struct InProgressCandle {
    start_time: u64,
    end_time: u64,
    open: Price,
    high: Price,
    low: Price,
    close: Price,
    volume: f64,
}

impl InProgressCandle {
    fn new(start_time: u64, end_time: u64, price: Price) -> Self {
        Self {
            start_time,
            end_time,
            open: price,
            high: price,
            low: price,
            close: price,
            // The current live tick contract is price-only, so candle volume stays zero
            // until broker adapters start normalizing volume into PriceTick.
            volume: 0.0,
        }
    }

    fn apply_price(&mut self, price: Price) {
        if price.as_f64() > self.high.as_f64() {
            self.high = price;
        }
        if price.as_f64() < self.low.as_f64() {
            self.low = price;
        }
        self.close = price;
    }

    fn to_candle(self, instrument_name: InstrumentName) -> Candle {
        Candle::new(
            instrument_name,
            Timeframe::OneMinute,
            UnixMillis::new(self.start_time),
            UnixMillis::new(self.end_time),
            self.open,
            self.high,
            self.low,
            self.close,
            self.volume,
        )
    }
}

struct HistoricalCandleFlusher {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl HistoricalCandleFlusher {
    fn spawn(sqlite_path: String, state: Arc<Mutex<CandleState>>) -> Result<Self, FeedError> {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_signal = Arc::clone(&stop);
        let handle = thread::Builder::new()
            .name("historical-candle-flusher".to_string())
            .spawn(move || {
                let store = match HistoricalCandleStore::open(&sqlite_path) {
                    Ok(store) => store,
                    Err(error) => {
                        eprintln!("historical candle flusher failed to open sqlite store: {error}");
                        return;
                    }
                };

                while !stop_signal.load(Ordering::Relaxed) {
                    let due_candles = {
                        let mut state = match state.lock() {
                            Ok(state) => state,
                            Err(_) => {
                                eprintln!("historical candle flusher state lock poisoned");
                                return;
                            }
                        };
                        state.flush_due_before(current_unix_millis())
                    };

                    for candle in due_candles {
                        if let Err(error) = store.upsert_candle(&candle) {
                            eprintln!("historical candle flusher upsert failed: {error}");
                        }
                    }

                    thread::sleep(FLUSH_POLL_INTERVAL);
                }
            })
            .map_err(|error| {
                FeedError::Io(format!(
                    "failed to spawn historical candle flusher: {error}"
                ))
            })?;

        Ok(Self {
            stop,
            handle: Some(handle),
        })
    }
}

impl Drop for HistoricalCandleFlusher {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn one_minute_bucket_bounds_ist(unix_millis: u64) -> Result<(u64, u64), FeedError> {
    let ist = FixedOffset::east_opt(IST_OFFSET_SECONDS)
        .ok_or_else(|| FeedError::Config("failed to create IST fixed offset".to_string()))?;
    let utc = chrono::DateTime::from_timestamp_millis(unix_millis as i64)
        .ok_or_else(|| FeedError::Parse(format!("invalid unix millis {unix_millis}")))?;
    let ist_dt = utc.with_timezone(&ist);
    let bucket_start = ist
        .with_ymd_and_hms(
            ist_dt.year(),
            ist_dt.month(),
            ist_dt.day(),
            ist_dt.hour(),
            ist_dt.minute(),
            0,
        )
        .single()
        .ok_or_else(|| FeedError::Parse("failed to compute IST minute bucket".to_string()))?;
    let bucket_end = bucket_start + ChronoDuration::minutes(1);

    Ok((
        bucket_start.timestamp_millis() as u64,
        bucket_end.timestamp_millis() as u64,
    ))
}

fn current_unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feeder::{InstrumentCatalog, InstrumentDefinition};

    #[test]
    fn maps_raw_spot_symbol_to_canonical_name_and_closes_minute() {
        let catalog = InstrumentCatalog::new(vec![InstrumentDefinition {
            instrument_name: InstrumentName::new("NIFTY"),
            instrument_type: InstrumentType::Spot,
            strike: None,
            expiry: None,
            broker: "FYERS".to_string(),
            instrument_token: "101".to_string(),
            trading_symbol: "NSE:NIFTY50-INDEX".to_string(),
            exchange: "NSE".to_string(),
            segment: "SPOT".to_string(),
            underlying: "NIFTY".to_string(),
            lot_size: 1.0,
            tick_size: 0.05,
            tradable: true,
        }]);
        let mut service = HistoricalCandleService::new(None, &catalog).expect("service");

        let first_tick = PriceTick::new(
            InstrumentName::new("NSE:NIFTY50-INDEX"),
            Price::new(100.0).expect("price"),
            UnixMillis::new(1_776_108_301_000),
        );
        service.on_tick(&first_tick).expect("first tick");

        let completed = {
            let mut state = service.state.lock().expect("state lock");
            state
                .aggregate_tick(
                    InstrumentName::new("NIFTY"),
                    &PriceTick::new(
                        InstrumentName::new("NSE:NIFTY50-INDEX"),
                        Price::new(101.0).expect("price"),
                        UnixMillis::new(1_776_108_360_000),
                    ),
                )
                .expect("aggregate")
                .expect("completed candle")
        };

        assert_eq!(completed.instrument_name.as_str(), "NIFTY");
        assert_eq!(completed.timeframe, Timeframe::OneMinute);
        assert_eq!(completed.open.as_f64(), 100.0);
        assert_eq!(completed.close.as_f64(), 100.0);
    }

    #[test]
    fn flushes_completed_minute_without_waiting_for_next_tick_and_ignores_late_ticks() {
        let catalog = InstrumentCatalog::new(vec![InstrumentDefinition {
            instrument_name: InstrumentName::new("NIFTY"),
            instrument_type: InstrumentType::Spot,
            strike: None,
            expiry: None,
            broker: "FYERS".to_string(),
            instrument_token: "101".to_string(),
            trading_symbol: "NSE:NIFTY50-INDEX".to_string(),
            exchange: "NSE".to_string(),
            segment: "SPOT".to_string(),
            underlying: "NIFTY".to_string(),
            lot_size: 1.0,
            tick_size: 0.05,
            tradable: true,
        }]);
        let mut service = HistoricalCandleService::new(None, &catalog).expect("service");

        let first_tick = PriceTick::new(
            InstrumentName::new("NSE:NIFTY50-INDEX"),
            Price::new(100.0).expect("price"),
            UnixMillis::new(1_776_108_301_000),
        );
        service.on_tick(&first_tick).expect("first tick");

        let flushed = service
            .flush_due_before_for_test(1_776_108_360_000)
            .expect("flush due");
        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0].instrument_name.as_str(), "NIFTY");
        assert_eq!(flushed[0].open.as_f64(), 100.0);
        assert_eq!(flushed[0].close.as_f64(), 100.0);

        let late_tick = PriceTick::new(
            InstrumentName::new("NSE:NIFTY50-INDEX"),
            Price::new(101.0).expect("price"),
            UnixMillis::new(1_776_108_340_000),
        );
        service.on_tick(&late_tick).expect("late tick ignored");

        let flushed_after_late_tick = service
            .flush_due_before_for_test(u64::MAX)
            .expect("flush after late tick");
        assert!(flushed_after_late_tick.is_empty());
    }
}
