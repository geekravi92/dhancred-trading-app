use std::collections::VecDeque;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::config::HistoricalCandlesSection;
use crate::feeder::{
    Candle, FeedError, FeedSubscription, Feeder, InstrumentCatalog, InstrumentDefinition,
    InstrumentType, PriceEvent, Timeframe, UnixMillis,
};
use crate::notification::{AlertSeverity, notify_failure, notify_recovery};
use crate::storage::historical_candles::HistoricalCandleStore;

const DAY_SECONDS: u64 = 86_400;
const IST_OFFSET_SECONDS: i64 = 5 * 60 * 60 + 30 * 60;
const MAINTENANCE_SLEEP_SLICE_SECONDS: u64 = 30;

pub struct HistoricalReplayFeeder {
    events: VecDeque<PriceEvent>,
    subscription: Option<FeedSubscription>,
}

impl HistoricalReplayFeeder {
    pub fn new(events: Vec<PriceEvent>) -> Self {
        Self {
            events: events.into(),
            subscription: None,
        }
    }
}

impl Feeder for HistoricalReplayFeeder {
    fn subscribe(&mut self, subscription: FeedSubscription) -> Result<(), FeedError> {
        self.subscription = Some(subscription);
        Ok(())
    }

    fn next_event(&mut self) -> Result<Option<PriceEvent>, FeedError> {
        let subscription = self.subscription.as_ref().ok_or(FeedError::NotSubscribed)?;

        while let Some(event) = self.events.pop_front() {
            if subscription.wants(event.instrument_name(), &event.channel()) {
                return Ok(Some(event));
            }
        }

        Ok(None)
    }
}

pub trait HistoricalCandleSource {
    fn broker_name(&self) -> &'static str;

    fn max_chunk_candles(&self, timeframe: Timeframe) -> Result<u64, FeedError>;

    fn fetch_candles(
        &self,
        instrument: &InstrumentDefinition,
        timeframe: Timeframe,
        start_inclusive: UnixMillis,
        end_inclusive: UnixMillis,
    ) -> Result<Vec<Candle>, FeedError>;
}

pub struct HistoricalMaintenanceHandle {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

pub fn recover_spot_history(
    source: &impl HistoricalCandleSource,
    config: Option<&HistoricalCandlesSection>,
    catalog: &InstrumentCatalog,
    log_to_console: bool,
) -> Result<(), FeedError> {
    let Some(config) = config.filter(|config| config.enabled) else {
        return Ok(());
    };

    let store = HistoricalCandleStore::open(&config.sqlite_path)?;
    let instruments = catalog
        .instruments()
        .filter(|instrument| instrument.instrument_type == InstrumentType::Spot)
        .filter(|instrument| instrument.tradable)
        .cloned()
        .collect::<Vec<_>>();

    for instrument in instruments {
        recover_instrument_timeframe(
            source,
            &store,
            &instrument,
            Timeframe::OneMinute,
            config.one_minute_days,
            log_to_console,
        )?;
        recover_instrument_timeframe(
            source,
            &store,
            &instrument,
            Timeframe::OneDay,
            config.one_day_days,
            log_to_console,
        )?;
    }

    Ok(())
}

pub fn start_spot_history_maintenance<S>(
    source: S,
    config: Option<&HistoricalCandlesSection>,
    catalog: &InstrumentCatalog,
    log_to_console: bool,
) -> Result<Option<HistoricalMaintenanceHandle>, FeedError>
where
    S: HistoricalCandleSource + Send + 'static,
{
    let Some(config) = config.filter(|config| config.enabled).cloned() else {
        return Ok(None);
    };

    let scheduled_second = parse_ist_hh_mm(&config.maintenance_time_ist)?;
    let catalog = catalog.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_signal = Arc::clone(&stop);
    let broker = source.broker_name();
    let thread_name = format!("historical-maintenance-{}", broker.to_ascii_lowercase());
    let handle = thread::Builder::new()
        .name(thread_name)
        .spawn(move || {
            if log_to_console {
                println!(
                    "{} historical maintenance enabled: time={} IST reconcile_1m={}d reconcile_1d={}d",
                    broker,
                    config.maintenance_time_ist,
                    config.reconcile_one_minute_days,
                    config.reconcile_one_day_days
                );
            }

            loop {
                let now = now_unix_seconds();
                let next_run = next_scheduled_utc_epoch(now, scheduled_second);
                let sleep_seconds = next_run.saturating_sub(now).max(1);
                if log_to_console {
                    println!(
                        "{} historical maintenance sleeping {}s until next {} IST run",
                        broker,
                        sleep_seconds,
                        format_hh_mm(scheduled_second)
                    );
                }
                if sleep_until_or_stop(&stop_signal, sleep_seconds) {
                    return;
                }

                let result = run_spot_history_maintenance(&source, &config, &catalog, log_to_console);
                match result {
                    Ok(()) => notify_recovery(
                        format!("historical_maintenance:{broker}"),
                        format!("HISTORICAL_MAINTENANCE:{broker}"),
                        "daily historical maintenance recovered",
                    ),
                    Err(error) => {
                        eprintln!("{broker} historical maintenance failed: {error}");
                        notify_failure(
                            format!("historical_maintenance:{broker}"),
                            format!("HISTORICAL_MAINTENANCE:{broker}"),
                            AlertSeverity::Error,
                            format!("daily historical maintenance failed: {error}"),
                        );
                    }
                }
            }
        })
        .map_err(|error| {
            FeedError::Io(format!(
                "failed to spawn historical maintenance thread for {broker}: {error}"
            ))
        })?;

    Ok(Some(HistoricalMaintenanceHandle {
        stop,
        handle: Some(handle),
    }))
}

fn recover_instrument_timeframe(
    source: &impl HistoricalCandleSource,
    store: &HistoricalCandleStore,
    instrument: &InstrumentDefinition,
    timeframe: Timeframe,
    lookback_days: u32,
    log_to_console: bool,
) -> Result<(), FeedError> {
    if lookback_days == 0 {
        return Ok(());
    }

    let now = current_unix_millis()?;
    let retention_cutoff = now.saturating_sub(days_to_millis(lookback_days));
    let next_start = store
        .latest_end_time(&instrument.instrument_name, timeframe)?
        .map_or(retention_cutoff, |value| {
            value.as_u64().max(retention_cutoff)
        });

    if next_start >= now {
        store.prune_before(&instrument.instrument_name, timeframe, retention_cutoff)?;
        return Ok(());
    }

    let written = sync_candle_range(source, store, instrument, timeframe, next_start, now)?;
    let pruned = store.prune_before(&instrument.instrument_name, timeframe, retention_cutoff)?;
    if log_to_console {
        println!(
            "{} history recovery {} {} | upserted={} pruned={}",
            source.broker_name(),
            instrument.instrument_name,
            timeframe_label(timeframe),
            written,
            pruned
        );
    }

    Ok(())
}

fn run_spot_history_maintenance(
    source: &impl HistoricalCandleSource,
    config: &HistoricalCandlesSection,
    catalog: &InstrumentCatalog,
    log_to_console: bool,
) -> Result<(), FeedError> {
    let store = HistoricalCandleStore::open(&config.sqlite_path)?;
    let instruments = catalog
        .instruments()
        .filter(|instrument| instrument.instrument_type == InstrumentType::Spot)
        .filter(|instrument| instrument.tradable)
        .cloned()
        .collect::<Vec<_>>();

    if log_to_console {
        println!("{} historical maintenance started", source.broker_name());
    }

    for instrument in instruments {
        maintain_instrument_timeframe(
            source,
            &store,
            &instrument,
            Timeframe::OneMinute,
            config.one_minute_days,
            config.reconcile_one_minute_days,
            log_to_console,
        )?;
        maintain_instrument_timeframe(
            source,
            &store,
            &instrument,
            Timeframe::OneDay,
            config.one_day_days,
            config.reconcile_one_day_days,
            log_to_console,
        )?;
    }

    if log_to_console {
        println!("{} historical maintenance finished", source.broker_name());
    }

    Ok(())
}

fn maintain_instrument_timeframe(
    source: &impl HistoricalCandleSource,
    store: &HistoricalCandleStore,
    instrument: &InstrumentDefinition,
    timeframe: Timeframe,
    retention_days: u32,
    reconcile_days: u32,
    log_to_console: bool,
) -> Result<(), FeedError> {
    if retention_days == 0 {
        return Ok(());
    }

    let now = current_unix_millis()?;
    let retention_cutoff = now.saturating_sub(days_to_millis(retention_days));
    let written = if reconcile_days == 0 {
        0
    } else {
        let reconcile_cutoff = now.saturating_sub(days_to_millis(reconcile_days));
        let start = reconcile_cutoff.max(retention_cutoff);
        sync_candle_range(source, store, instrument, timeframe, start, now)?
    };
    let pruned = store.prune_before(&instrument.instrument_name, timeframe, retention_cutoff)?;

    if log_to_console {
        println!(
            "{} historical maintenance {} {} | upserted={} pruned={}",
            source.broker_name(),
            instrument.instrument_name,
            timeframe_label(timeframe),
            written,
            pruned
        );
    }

    Ok(())
}

fn sync_candle_range(
    source: &impl HistoricalCandleSource,
    store: &HistoricalCandleStore,
    instrument: &InstrumentDefinition,
    timeframe: Timeframe,
    start_inclusive: u64,
    end_inclusive: u64,
) -> Result<usize, FeedError> {
    if start_inclusive >= end_inclusive {
        return Ok(0);
    }

    let mut cursor = start_inclusive;
    let mut written = 0usize;
    let chunk_millis = timeframe_millis(timeframe)?
        .checked_mul(source.max_chunk_candles(timeframe)?)
        .ok_or_else(|| FeedError::Config("historical recovery chunk size overflow".to_string()))?;

    while cursor < end_inclusive {
        let chunk_end = (cursor + chunk_millis).min(end_inclusive);
        let candles = source.fetch_candles(
            instrument,
            timeframe,
            UnixMillis::new(cursor),
            UnixMillis::new(chunk_end),
        )?;

        let mut advanced_to = cursor;
        for candle in candles {
            if candle.instrument_name != instrument.instrument_name || candle.timeframe != timeframe
            {
                continue;
            }
            let candle_end = candle.end_time.as_u64();
            if candle_end <= cursor || candle_end > end_inclusive {
                continue;
            }
            store.upsert_candle(&candle)?;
            written += 1;
            advanced_to = advanced_to.max(candle_end);
        }

        cursor = if advanced_to > cursor {
            advanced_to
        } else {
            chunk_end
        };
    }

    Ok(written)
}

fn timeframe_label(timeframe: Timeframe) -> &'static str {
    match timeframe {
        Timeframe::OneMinute => "1m",
        Timeframe::ThreeMinute => "3m",
        Timeframe::FiveMinute => "5m",
        Timeframe::FifteenMinute => "15m",
        Timeframe::OneHour => "1h",
        Timeframe::OneDay => "1d",
    }
}

fn days_to_millis(days: u32) -> u64 {
    u64::from(days) * DAY_SECONDS * 1_000
}

fn timeframe_millis(timeframe: Timeframe) -> Result<u64, FeedError> {
    match timeframe {
        Timeframe::OneMinute => Ok(60_000),
        Timeframe::ThreeMinute => Ok(180_000),
        Timeframe::FiveMinute => Ok(300_000),
        Timeframe::FifteenMinute => Ok(900_000),
        Timeframe::OneHour => Ok(3_600_000),
        Timeframe::OneDay => Ok(86_400_000),
    }
}

fn current_unix_millis() -> Result<u64, FeedError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .map_err(|error| FeedError::Io(format!("system clock is before unix epoch: {error}")))
}

fn parse_ist_hh_mm(value: &str) -> Result<u64, FeedError> {
    let Some((hours, minutes)) = value.trim().split_once(':') else {
        return Err(FeedError::Config(format!(
            "invalid historical_candles.maintenance_time_ist {value}; expected HH:MM"
        )));
    };
    let hours: u64 = hours.parse().map_err(|error| {
        FeedError::Config(format!(
            "invalid historical_candles.maintenance_time_ist hour: {error}"
        ))
    })?;
    let minutes: u64 = minutes.parse().map_err(|error| {
        FeedError::Config(format!(
            "invalid historical_candles.maintenance_time_ist minute: {error}"
        ))
    })?;
    if hours > 23 || minutes > 59 {
        return Err(FeedError::Config(format!(
            "invalid historical_candles.maintenance_time_ist {value}; expected 00:00 through 23:59"
        )));
    }

    Ok(hours * 60 * 60 + minutes * 60)
}

fn next_scheduled_utc_epoch(now_utc: u64, scheduled_second: u64) -> u64 {
    let today_ist_day = ist_day_and_second(now_utc).0;
    for day_offset in 0..3 {
        let candidate_day = today_ist_day + day_offset;
        let candidate_utc = (candidate_day * DAY_SECONDS as i64 + scheduled_second as i64
            - IST_OFFSET_SECONDS) as u64;
        if candidate_utc > now_utc {
            return candidate_utc;
        }
    }

    now_utc + DAY_SECONDS
}

fn ist_day_and_second(now_utc: u64) -> (i64, u64) {
    let ist_seconds = now_utc as i64 + IST_OFFSET_SECONDS;
    (
        ist_seconds.div_euclid(DAY_SECONDS as i64),
        ist_seconds.rem_euclid(DAY_SECONDS as i64) as u64,
    )
}

fn now_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn format_hh_mm(second_of_day: u64) -> String {
    format!(
        "{:02}:{:02}",
        second_of_day / 3_600,
        (second_of_day % 3_600) / 60
    )
}

fn sleep_until_or_stop(stop: &AtomicBool, sleep_seconds: u64) -> bool {
    let mut remaining = sleep_seconds;
    while remaining > 0 {
        if stop.load(Ordering::Relaxed) {
            return true;
        }
        let slice = remaining.min(MAINTENANCE_SLEEP_SLICE_SECONDS);
        thread::sleep(Duration::from_secs(slice));
        remaining -= slice;
    }
    stop.load(Ordering::Relaxed)
}

impl Drop for HistoricalMaintenanceHandle {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::params;

    use super::*;
    use crate::feeder::{InstrumentName, Price};

    struct FakeHistoricalSource {
        candles: Vec<Candle>,
    }

    impl HistoricalCandleSource for FakeHistoricalSource {
        fn broker_name(&self) -> &'static str {
            "FAKE"
        }

        fn max_chunk_candles(&self, _timeframe: Timeframe) -> Result<u64, FeedError> {
            Ok(2_000)
        }

        fn fetch_candles(
            &self,
            _instrument: &InstrumentDefinition,
            timeframe: Timeframe,
            start_inclusive: UnixMillis,
            end_inclusive: UnixMillis,
        ) -> Result<Vec<Candle>, FeedError> {
            Ok(self
                .candles
                .iter()
                .filter(|candle| candle.timeframe == timeframe)
                .filter(|candle| candle.end_time.as_u64() > start_inclusive.as_u64())
                .filter(|candle| candle.end_time.as_u64() <= end_inclusive.as_u64())
                .cloned()
                .collect())
        }
    }

    #[test]
    fn historical_replay_feeder_filters_by_subscription() {
        let instrument = InstrumentName::new("BTC");
        let tick = PriceEvent::Tick(crate::feeder::PriceTick::new(
            instrument.clone(),
            Price::new(100.0).expect("price"),
            UnixMillis::new(1),
        ));
        let mut feeder = HistoricalReplayFeeder::new(vec![tick.clone()]);
        feeder
            .subscribe(FeedSubscription::new(
                vec![instrument],
                vec![crate::feeder::FeedChannel::PriceTick],
            ))
            .expect("subscribe");
        assert_eq!(feeder.next_event().expect("event"), Some(tick));
        assert_eq!(feeder.next_event().expect("end"), None);
    }

    #[test]
    fn recover_spot_history_prunes_old_candles_and_keeps_recent_gapfill() {
        let dir = std::env::temp_dir().join(format!(
            "dhancred-historical-recovery-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("mkdir");
        let sqlite_path = dir.join("historical.sqlite");
        let store = HistoricalCandleStore::open(&sqlite_path).expect("store");
        let instrument = InstrumentDefinition {
            instrument_name: InstrumentName::new("BTC"),
            instrument_type: InstrumentType::Spot,
            strike: None,
            expiry: None,
            broker: "DELTA".to_string(),
            instrument_token: "1".to_string(),
            trading_symbol: "BTCUSD".to_string(),
            exchange: "DELTA".to_string(),
            segment: "SPOT".to_string(),
            underlying: "BTC".to_string(),
            lot_size: 1.0,
            tick_size: 0.1,
            tradable: true,
        };
        let now = current_unix_millis().expect("now");
        let old_end = now - days_to_millis(2);
        let recent_start = now - (2 * 60 * 1_000);
        let recent_end = recent_start + 60 * 1_000;

        store
            .upsert_candle(&Candle::new(
                instrument.instrument_name.clone(),
                Timeframe::OneMinute,
                UnixMillis::new(old_end - 60 * 1_000),
                UnixMillis::new(old_end),
                Price::new(1.0).unwrap(),
                Price::new(1.0).unwrap(),
                Price::new(1.0).unwrap(),
                Price::new(1.0).unwrap(),
                0.0,
            ))
            .expect("seed old");

        let source = FakeHistoricalSource {
            candles: vec![Candle::new(
                instrument.instrument_name.clone(),
                Timeframe::OneMinute,
                UnixMillis::new(recent_start),
                UnixMillis::new(recent_end),
                Price::new(2.0).unwrap(),
                Price::new(2.0).unwrap(),
                Price::new(2.0).unwrap(),
                Price::new(2.0).unwrap(),
                0.0,
            )],
        };
        let config = HistoricalCandlesSection {
            enabled: true,
            sqlite_path: sqlite_path.display().to_string(),
            one_minute_days: 1,
            one_day_days: 1,
            maintenance_time_ist: "00:10".to_string(),
            reconcile_one_minute_days: 2,
            reconcile_one_day_days: 5,
        };
        let catalog = InstrumentCatalog::new(vec![instrument.clone()]);

        recover_spot_history(&source, Some(&config), &catalog, false).expect("recover");

        let connection = rusqlite::Connection::open(&sqlite_path).expect("open sqlite");
        let count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM historical_candles WHERE instrument_name = 'BTC' AND timeframe = '1m'",
                params![],
                |row| row.get(0),
            )
            .expect("count");
        assert_eq!(count, 1);

        std::fs::remove_file(sqlite_path).ok();
        std::fs::remove_dir_all(dir).ok();
    }
}
