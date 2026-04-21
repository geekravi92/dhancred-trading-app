use std::fs;
use std::path::Path;
use std::time::Duration;

use chrono::{FixedOffset, NaiveDateTime};
use rusqlite::{Connection, params};

use crate::feeder::{Candle, FeedError, InstrumentName, Price, Timeframe, UnixMillis};

const IST_OFFSET_SECONDS: i32 = 5 * 60 * 60 + 30 * 60;

pub struct HistoricalCandleStore {
    connection: Connection,
}

impl HistoricalCandleStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, FeedError> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let connection = Connection::open(path).map_err(|error| {
            FeedError::Io(format!(
                "failed to open sqlite db {}: {error}",
                path.display()
            ))
        })?;
        connection
            .busy_timeout(Duration::from_secs(5))
            .map_err(|error| {
                FeedError::Io(format!("failed to set sqlite busy timeout: {error}"))
            })?;
        connection
            .execute_batch(
                "\
                PRAGMA journal_mode = WAL;
                PRAGMA synchronous = NORMAL;
                CREATE TABLE IF NOT EXISTS historical_candles (
                    instrument_name TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    date TEXT NOT NULL,
                    start_time TEXT NOT NULL,
                    end_time TEXT NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume REAL NOT NULL,
                    PRIMARY KEY (instrument_name, timeframe, date, start_time)
                ) WITHOUT ROWID;
                ",
            )
            .map_err(|error| {
                FeedError::Io(format!("failed to initialize sqlite schema: {error}"))
            })?;

        Ok(Self { connection })
    }

    pub fn upsert_candle(&self, candle: &Candle) -> Result<(), FeedError> {
        let (date, start_time) = format_ist_date_and_time(candle.start_time.as_u64())?;
        let (_, end_time) = format_ist_date_and_time(candle.end_time.as_u64())?;
        let timeframe = timeframe_label(candle.timeframe);

        self.connection
            .execute(
                "\
                INSERT INTO historical_candles (
                    instrument_name,
                    timeframe,
                    date,
                    start_time,
                    end_time,
                    open,
                    high,
                    low,
                    close,
                    volume
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(instrument_name, timeframe, date, start_time)
                DO UPDATE SET
                    end_time = excluded.end_time,
                    open = excluded.open,
                    high = excluded.high,
                    low = excluded.low,
                    close = excluded.close,
                    volume = excluded.volume
                ",
                params![
                    candle.instrument_name.as_str(),
                    timeframe,
                    date,
                    start_time,
                    end_time,
                    candle.open.as_f64(),
                    candle.high.as_f64(),
                    candle.low.as_f64(),
                    candle.close.as_f64(),
                    candle.volume,
                ],
            )
            .map_err(|error| {
                FeedError::Io(format!(
                    "failed to upsert historical candle {} {} {} {}: {error}",
                    candle.instrument_name, timeframe, date, start_time
                ))
            })?;

        Ok(())
    }

    pub fn latest_end_time(
        &self,
        instrument_name: &InstrumentName,
        timeframe: Timeframe,
    ) -> Result<Option<UnixMillis>, FeedError> {
        let timeframe = timeframe_label(timeframe);
        let row = self.connection.query_row(
            "\
                SELECT date, end_time
                FROM historical_candles
                WHERE instrument_name = ?1 AND timeframe = ?2
                ORDER BY date DESC, start_time DESC
                LIMIT 1
                ",
            params![instrument_name.as_str(), timeframe],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        );

        match row {
            Ok((date, end_time)) => Ok(Some(UnixMillis::new(
                parse_ist_date_and_time_to_unix_millis(&date, &end_time)?,
            ))),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(error) => Err(FeedError::Io(format!(
                "failed to read latest historical candle for {} {}: {error}",
                instrument_name, timeframe
            ))),
        }
    }

    pub fn prune_before(
        &self,
        instrument_name: &InstrumentName,
        timeframe: Timeframe,
        cutoff_millis: u64,
    ) -> Result<usize, FeedError> {
        let (cutoff_date, cutoff_time) = format_ist_date_and_time(cutoff_millis)?;
        let timeframe = timeframe_label(timeframe);
        self.connection
            .execute(
                "\
                DELETE FROM historical_candles
                WHERE instrument_name = ?1
                  AND timeframe = ?2
                  AND (date < ?3 OR (date = ?3 AND start_time < ?4))
                ",
                params![
                    instrument_name.as_str(),
                    timeframe,
                    cutoff_date,
                    cutoff_time
                ],
            )
            .map_err(|error| {
                FeedError::Io(format!(
                    "failed to prune historical candles for {} {} before {} {}: {error}",
                    instrument_name, timeframe, cutoff_date, cutoff_time
                ))
            })
    }

    pub fn load_recent_candles(
        &self,
        instrument_name: &InstrumentName,
        timeframe: Timeframe,
        limit: usize,
    ) -> Result<Vec<Candle>, FeedError> {
        let timeframe = timeframe_label(timeframe);
        let mut statement = self
            .connection
            .prepare(
                "\
                SELECT date, start_time, end_time, open, high, low, close, volume
                FROM historical_candles
                WHERE instrument_name = ?1 AND timeframe = ?2
                ORDER BY date DESC, start_time DESC
                LIMIT ?3
                ",
            )
            .map_err(|error| {
                FeedError::Io(format!(
                    "failed to prepare recent historical candles query for {} {}: {error}",
                    instrument_name, timeframe
                ))
            })?;
        let rows = statement
            .query_map(params![instrument_name.as_str(), timeframe, limit as i64], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, f64>(3)?,
                    row.get::<_, f64>(4)?,
                    row.get::<_, f64>(5)?,
                    row.get::<_, f64>(6)?,
                    row.get::<_, f64>(7)?,
                ))
            })
            .map_err(|error| {
                FeedError::Io(format!(
                    "failed to query recent historical candles for {} {}: {error}",
                    instrument_name, timeframe
                ))
            })?;

        let mut candles = Vec::new();
        for row in rows {
            let (date, start_time, end_time, open, high, low, close, volume) =
                row.map_err(|error| {
                    FeedError::Io(format!(
                        "failed to read recent historical candles for {} {}: {error}",
                        instrument_name, timeframe
                    ))
                })?;
            let start_at = parse_ist_date_and_time_to_unix_millis(&date, &start_time)?;
            let end_at = parse_ist_date_and_time_to_unix_millis(&date, &end_time)?;
            candles.push(Candle::new(
                instrument_name.clone(),
                parse_timeframe_label(timeframe)?,
                UnixMillis::new(start_at),
                UnixMillis::new(end_at),
                Price::new(open).map_err(|error| {
                    FeedError::Parse(format!(
                        "invalid open price for {} {} {} {}: {error}",
                        instrument_name, timeframe, date, start_time
                    ))
                })?,
                Price::new(high).map_err(|error| {
                    FeedError::Parse(format!(
                        "invalid high price for {} {} {} {}: {error}",
                        instrument_name, timeframe, date, start_time
                    ))
                })?,
                Price::new(low).map_err(|error| {
                    FeedError::Parse(format!(
                        "invalid low price for {} {} {} {}: {error}",
                        instrument_name, timeframe, date, start_time
                    ))
                })?,
                Price::new(close).map_err(|error| {
                    FeedError::Parse(format!(
                        "invalid close price for {} {} {} {}: {error}",
                        instrument_name, timeframe, date, start_time
                    ))
                })?,
                volume,
            ));
        }
        candles.reverse();
        Ok(candles)
    }
}

pub fn timeframe_label(timeframe: Timeframe) -> &'static str {
    match timeframe {
        Timeframe::OneMinute => "1m",
        Timeframe::ThreeMinute => "3m",
        Timeframe::FiveMinute => "5m",
        Timeframe::FifteenMinute => "15m",
        Timeframe::OneHour => "1h",
        Timeframe::OneDay => "1d",
    }
}

fn parse_timeframe_label(value: &str) -> Result<Timeframe, FeedError> {
    match value {
        "1m" => Ok(Timeframe::OneMinute),
        "3m" => Ok(Timeframe::ThreeMinute),
        "5m" => Ok(Timeframe::FiveMinute),
        "15m" => Ok(Timeframe::FifteenMinute),
        "1h" => Ok(Timeframe::OneHour),
        "1d" => Ok(Timeframe::OneDay),
        _ => Err(FeedError::Parse(format!("unsupported timeframe label {value}"))),
    }
}

fn format_ist_date_and_time(unix_millis: u64) -> Result<(String, String), FeedError> {
    let ist = FixedOffset::east_opt(IST_OFFSET_SECONDS)
        .ok_or_else(|| FeedError::Config("failed to create IST fixed offset".to_string()))?;
    let utc = chrono::DateTime::from_timestamp_millis(unix_millis as i64)
        .ok_or_else(|| FeedError::Parse(format!("invalid unix millis {unix_millis}")))?;
    let ist_time = utc.with_timezone(&ist);

    Ok((
        ist_time.format("%Y-%m-%d").to_string(),
        ist_time.format("%H:%M").to_string(),
    ))
}

fn parse_ist_date_and_time_to_unix_millis(date: &str, time: &str) -> Result<u64, FeedError> {
    let ist = FixedOffset::east_opt(IST_OFFSET_SECONDS)
        .ok_or_else(|| FeedError::Config("failed to create IST fixed offset".to_string()))?;
    let value = format!("{date} {time}");
    let naive = NaiveDateTime::parse_from_str(&value, "%Y-%m-%d %H:%M").map_err(|error| {
        FeedError::Parse(format!(
            "invalid IST historical candle time {value}: {error}"
        ))
    })?;
    let parsed = naive.and_local_timezone(ist).single().ok_or_else(|| {
        FeedError::Parse(format!(
            "invalid IST historical candle time {value}: ambiguous local time"
        ))
    })?;
    Ok(parsed.timestamp_millis() as u64)
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    use chrono::TimeZone;
    use rusqlite::params;

    use super::*;
    use crate::feeder::{Candle, InstrumentName, Price, Timeframe, UnixMillis};

    #[test]
    fn upserts_one_minute_candle_in_ist_schema() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        let path = env::temp_dir().join(format!("dhancred-historical-candles-{unique}.sqlite"));
        let store = HistoricalCandleStore::open(&path).expect("store");
        let ist = FixedOffset::east_opt(IST_OFFSET_SECONDS).expect("ist offset");
        let start = ist
            .with_ymd_and_hms(2026, 4, 14, 9, 15, 0)
            .single()
            .expect("ist start");
        let end = ist
            .with_ymd_and_hms(2026, 4, 14, 9, 16, 0)
            .single()
            .expect("ist end");
        let candle = Candle::new(
            InstrumentName::new("NIFTY"),
            Timeframe::OneMinute,
            UnixMillis::new(start.timestamp_millis() as u64),
            UnixMillis::new(end.timestamp_millis() as u64),
            Price::new(100.0).expect("open"),
            Price::new(101.0).expect("high"),
            Price::new(99.5).expect("low"),
            Price::new(100.5).expect("close"),
            0.0,
        );

        store.upsert_candle(&candle).expect("upsert");

        let row = store
            .connection
            .query_row(
                "SELECT instrument_name, timeframe, date, start_time, end_time, open, high, low, close, volume FROM historical_candles",
                params![],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, f64>(5)?,
                        row.get::<_, f64>(6)?,
                        row.get::<_, f64>(7)?,
                        row.get::<_, f64>(8)?,
                        row.get::<_, f64>(9)?,
                    ))
                },
            )
            .expect("row");

        assert_eq!(row.0, "NIFTY");
        assert_eq!(row.1, "1m");
        assert_eq!(row.2, "2026-04-14");
        assert_eq!(row.3, "09:15");
        assert_eq!(row.4, "09:16");
        assert_eq!(row.5, 100.0);
        assert_eq!(row.6, 101.0);
        assert_eq!(row.7, 99.5);
        assert_eq!(row.8, 100.5);
        assert_eq!(row.9, 0.0);

        fs::remove_file(path).ok();
    }
}
