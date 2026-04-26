use std::fs;
use std::path::Path;
use std::time::Duration;

use chrono::{NaiveDateTime, TimeZone, Utc};
use rusqlite::{Connection, params};

use crate::feeder::{Candle, FeedError, InstrumentName, Price, Timeframe, UnixMillis};

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
        let (date, start_time) = format_utc_date_and_time(candle.start_time.as_u64())?;
        let (_, end_time) = format_utc_date_and_time(candle.end_time.as_u64())?;
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
                SELECT date, start_time, end_time
                FROM historical_candles
                WHERE instrument_name = ?1 AND timeframe = ?2
                ORDER BY date DESC, start_time DESC
                LIMIT 1
                ",
            params![instrument_name.as_str(), timeframe],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        );

        match row {
            Ok((date, start_time, end_time)) => Ok(Some(UnixMillis::new(
                parse_utc_candle_end_millis(&date, &start_time, &end_time)?,
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
        let (cutoff_date, cutoff_time) = format_utc_date_and_time(cutoff_millis)?;
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
            .query_map(
                params![instrument_name.as_str(), timeframe, limit as i64],
                |row| {
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
                },
            )
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
            let start_at = parse_utc_date_and_time_to_unix_millis(&date, &start_time)?;
            let end_at = parse_utc_candle_end_millis(&date, &start_time, &end_time)?;
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

    pub fn load_candles_before(
        &self,
        instrument_name: &InstrumentName,
        timeframe: Timeframe,
        before_millis: u64,
        limit: usize,
    ) -> Result<Vec<Candle>, FeedError> {
        let (before_date, before_time) = format_utc_date_and_time(before_millis)?;
        let timeframe = timeframe_label(timeframe);
        let mut statement = self
            .connection
            .prepare(
                "\
                SELECT date, start_time, end_time, open, high, low, close, volume
                FROM historical_candles
                WHERE instrument_name = ?1
                  AND timeframe = ?2
                  AND (date < ?3 OR (date = ?3 AND start_time < ?4))
                ORDER BY date DESC, start_time DESC
                LIMIT ?5
                ",
            )
            .map_err(|error| {
                FeedError::Io(format!(
                    "failed to prepare historical warmup query for {} {}: {error}",
                    instrument_name, timeframe
                ))
            })?;
        let rows = statement
            .query_map(
                params![
                    instrument_name.as_str(),
                    timeframe,
                    before_date,
                    before_time,
                    limit as i64
                ],
                |row| {
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
                },
            )
            .map_err(|error| {
                FeedError::Io(format!(
                    "failed to query historical warmup candles for {} {}: {error}",
                    instrument_name, timeframe
                ))
            })?;

        let mut candles = candles_from_rows(instrument_name, timeframe, rows)?;
        candles.reverse();
        Ok(candles)
    }

    pub fn load_candles_between(
        &self,
        instrument_name: &InstrumentName,
        timeframe: Timeframe,
        from_millis: u64,
        to_millis: u64,
    ) -> Result<Vec<Candle>, FeedError> {
        let (from_date, from_time) = format_utc_date_and_time(from_millis)?;
        let (to_date, to_time) = format_utc_date_and_time(to_millis)?;
        let timeframe = timeframe_label(timeframe);
        let mut statement = self
            .connection
            .prepare(
                "\
                SELECT date, start_time, end_time, open, high, low, close, volume
                FROM historical_candles
                WHERE instrument_name = ?1
                  AND timeframe = ?2
                  AND (date > ?3 OR (date = ?3 AND start_time >= ?4))
                  AND (date < ?5 OR (date = ?5 AND start_time <= ?6))
                ORDER BY date ASC, start_time ASC
                ",
            )
            .map_err(|error| {
                FeedError::Io(format!(
                    "failed to prepare historical range query for {} {}: {error}",
                    instrument_name, timeframe
                ))
            })?;
        let rows = statement
            .query_map(
                params![
                    instrument_name.as_str(),
                    timeframe,
                    from_date,
                    from_time,
                    to_date,
                    to_time
                ],
                |row| {
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
                },
            )
            .map_err(|error| {
                FeedError::Io(format!(
                    "failed to query historical range candles for {} {}: {error}",
                    instrument_name, timeframe
                ))
            })?;

        candles_from_rows(instrument_name, timeframe, rows)
    }
}

type CandleRow = (String, String, String, f64, f64, f64, f64, f64);

fn candles_from_rows<'stmt, F>(
    instrument_name: &InstrumentName,
    timeframe: &str,
    rows: rusqlite::MappedRows<'stmt, F>,
) -> Result<Vec<Candle>, FeedError>
where
    F: FnMut(&rusqlite::Row<'_>) -> Result<CandleRow, rusqlite::Error>,
{
    let mut candles = Vec::new();
    for row in rows {
        let (date, start_time, end_time, open, high, low, close, volume) =
            row.map_err(|error| {
                FeedError::Io(format!(
                    "failed to read historical candles for {} {}: {error}",
                    instrument_name, timeframe
                ))
            })?;
        candles.push(candle_from_parts(
            instrument_name,
            timeframe,
            &date,
            &start_time,
            &end_time,
            open,
            high,
            low,
            close,
            volume,
        )?);
    }
    Ok(candles)
}

#[allow(clippy::too_many_arguments)]
fn candle_from_parts(
    instrument_name: &InstrumentName,
    timeframe: &str,
    date: &str,
    start_time: &str,
    end_time: &str,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
) -> Result<Candle, FeedError> {
    let start_at = parse_utc_date_and_time_to_unix_millis(date, start_time)?;
    let end_at = parse_utc_candle_end_millis(date, start_time, end_time)?;
    Ok(Candle::new(
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
    ))
}

pub fn timeframe_label(timeframe: Timeframe) -> &'static str {
    match timeframe {
        Timeframe::OneMinute => "1m",
        Timeframe::ThreeMinute => "3m",
        Timeframe::FiveMinute => "5m",
        Timeframe::FifteenMinute => "15m",
        Timeframe::ThirtyMinute => "30m",
        Timeframe::SeventyFiveMinute => "75m",
        Timeframe::OneHour => "1h",
        Timeframe::FourHour => "4h",
        Timeframe::OneDay => "1d",
    }
}

fn parse_timeframe_label(value: &str) -> Result<Timeframe, FeedError> {
    match value {
        "1m" => Ok(Timeframe::OneMinute),
        "3m" => Ok(Timeframe::ThreeMinute),
        "5m" => Ok(Timeframe::FiveMinute),
        "15m" => Ok(Timeframe::FifteenMinute),
        "30m" => Ok(Timeframe::ThirtyMinute),
        "75m" => Ok(Timeframe::SeventyFiveMinute),
        "1h" => Ok(Timeframe::OneHour),
        "4h" => Ok(Timeframe::FourHour),
        "1d" => Ok(Timeframe::OneDay),
        _ => Err(FeedError::Parse(format!(
            "unsupported timeframe label {value}"
        ))),
    }
}

fn format_utc_date_and_time(unix_millis: u64) -> Result<(String, String), FeedError> {
    let utc = chrono::DateTime::from_timestamp_millis(unix_millis as i64)
        .ok_or_else(|| FeedError::Parse(format!("invalid unix millis {unix_millis}")))?;

    Ok((
        utc.format("%Y-%m-%d").to_string(),
        utc.format("%H:%M").to_string(),
    ))
}

fn parse_utc_date_and_time_to_unix_millis(date: &str, time: &str) -> Result<u64, FeedError> {
    let value = format!("{date} {time}");
    let naive = NaiveDateTime::parse_from_str(&value, "%Y-%m-%d %H:%M").map_err(|error| {
        FeedError::Parse(format!(
            "invalid UTC historical candle time {value}: {error}"
        ))
    })?;
    let parsed = Utc.from_utc_datetime(&naive);
    Ok(parsed.timestamp_millis() as u64)
}

fn parse_utc_candle_end_millis(
    date: &str,
    start_time: &str,
    end_time: &str,
) -> Result<u64, FeedError> {
    let start_at = parse_utc_date_and_time_to_unix_millis(date, start_time)?;
    let mut end_at = parse_utc_date_and_time_to_unix_millis(date, end_time)?;
    if end_at <= start_at {
        end_at = end_at.saturating_add(24 * 60 * 60 * 1_000);
    }
    Ok(end_at)
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
    fn formats_unix_csv_timestamp_as_utc() {
        let (date, time) = format_utc_date_and_time(1_736_208_060_000).expect("format");

        assert_eq!(date, "2025-01-07");
        assert_eq!(time, "00:01");
    }

    #[test]
    fn upserts_one_minute_candle_in_utc_schema() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        let path = env::temp_dir().join(format!("dhancred-historical-candles-{unique}.sqlite"));
        let store = HistoricalCandleStore::open(&path).expect("store");
        let start = Utc
            .with_ymd_and_hms(2026, 4, 14, 9, 15, 0)
            .single()
            .expect("utc start");
        let end = Utc
            .with_ymd_and_hms(2026, 4, 14, 9, 16, 0)
            .single()
            .expect("utc end");
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

    #[test]
    fn loads_midnight_crossing_candle_end_on_next_utc_day() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        let path = env::temp_dir().join(format!("dhancred-midnight-candle-{unique}.sqlite"));
        let store = HistoricalCandleStore::open(&path).expect("store");
        let start = Utc
            .with_ymd_and_hms(2025, 1, 7, 23, 59, 0)
            .single()
            .expect("utc start");
        let end = Utc
            .with_ymd_and_hms(2025, 1, 8, 0, 0, 0)
            .single()
            .expect("utc end");
        let candle = Candle::new(
            InstrumentName::new("BTC"),
            Timeframe::OneMinute,
            UnixMillis::new(start.timestamp_millis() as u64),
            UnixMillis::new(end.timestamp_millis() as u64),
            Price::new(100.0).expect("open"),
            Price::new(101.0).expect("high"),
            Price::new(99.0).expect("low"),
            Price::new(100.5).expect("close"),
            1.0,
        );

        store.upsert_candle(&candle).expect("upsert");

        let loaded = store
            .load_recent_candles(&InstrumentName::new("BTC"), Timeframe::OneMinute, 1)
            .expect("load")
            .pop()
            .expect("candle");
        assert_eq!(loaded.start_time.as_u64(), start.timestamp_millis() as u64);
        assert_eq!(loaded.end_time.as_u64(), end.timestamp_millis() as u64);
        assert_eq!(
            store
                .latest_end_time(&InstrumentName::new("BTC"), Timeframe::OneMinute)
                .expect("latest")
                .expect("latest")
                .as_u64(),
            end.timestamp_millis() as u64
        );

        fs::remove_file(path).ok();
    }
}
