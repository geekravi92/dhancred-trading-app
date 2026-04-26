use std::collections::VecDeque;
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use reqwest::blocking::Client;
use reqwest::header::{ACCEPT, HeaderMap, HeaderValue, USER_AGENT};
use serde::Deserialize;

use crate::adapters::fyers::token::history_authorization_header;
use crate::adapters::historical::HistoricalCandleSource;
use crate::config::FyersBrokerSection;
use crate::feeder::{Candle, FeedError, InstrumentDefinition, Price, Timeframe, UnixMillis};

const FYERS_RATE_LIMIT_PER_SECOND: usize = 10;
const FYERS_RATE_LIMIT_PER_MINUTE: usize = 200;
const IST_OFFSET_SECONDS: i64 = 5 * 60 * 60 + 30 * 60;
const DAY_SECONDS: i64 = 86_400;
const NSE_BSE_EQUITY_OPEN_SECOND: u64 = 9 * 3_600 + 15 * 60;
const NSE_BSE_EQUITY_CLOSE_SECOND: u64 = 15 * 3_600 + 30 * 60;

pub struct FyersHistoricalClient {
    authorization: String,
    client: Client,
    rate_limiter: HistoricalRateLimiter,
}

impl FyersHistoricalClient {
    pub fn new(config: &FyersBrokerSection, access_token: &str) -> Result<Self, FeedError> {
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
        headers.insert(
            USER_AGENT,
            HeaderValue::from_static("dhancred-trading-app/0.1"),
        );

        let authorization =
            history_authorization_header(access_token, config.app_id_env.as_deref())?;

        Ok(Self {
            authorization,
            client: Client::builder()
                .default_headers(headers)
                .build()
                .expect("valid fyers historical client"),
            rate_limiter: HistoricalRateLimiter::new(
                FYERS_RATE_LIMIT_PER_SECOND,
                FYERS_RATE_LIMIT_PER_MINUTE,
            ),
        })
    }
}

impl HistoricalCandleSource for FyersHistoricalClient {
    fn broker_name(&self) -> &'static str {
        "FYERS"
    }

    fn max_chunk_candles(&self, timeframe: Timeframe) -> Result<u64, FeedError> {
        fyers_history_chunk_candles(timeframe)
    }

    fn fetch_candles(
        &self,
        instrument: &InstrumentDefinition,
        timeframe: Timeframe,
        start_inclusive: UnixMillis,
        end_inclusive: UnixMillis,
    ) -> Result<Vec<Candle>, FeedError> {
        let Some((start_inclusive, end_inclusive)) =
            normalize_request_window(instrument, timeframe, start_inclusive, end_inclusive)?
        else {
            return Ok(Vec::new());
        };
        let resolution = fyers_resolution(timeframe)?;
        self.rate_limiter.wait();
        let request = self
            .client
            .get("https://api-t1.fyers.in/data/history")
            .query(&[
                ("symbol", instrument.trading_symbol.as_str()),
                ("resolution", resolution),
                ("date_format", "0"),
                (
                    "range_from",
                    &(start_inclusive.as_u64() / 1_000).to_string(),
                ),
                ("range_to", &(end_inclusive.as_u64() / 1_000).to_string()),
                ("cont_flag", "0"),
            ])
            .header("Authorization", self.authorization.as_str())
            .header("version", "2.0");
        let response = request.send()?;
        let status = response.status();
        let url = response.url().to_string();
        let body_text = response.text()?;
        if !status.is_success() {
            return Err(FeedError::Http(format!(
                "FYERS history request failed: instrument={} trading_symbol={} timeframe={} resolution={} status={} url={} body={}",
                instrument.instrument_name,
                instrument.trading_symbol,
                timeframe_label(timeframe),
                resolution,
                status,
                url,
                response_snippet(&body_text),
            )));
        }

        let body: FyersHistoricalResponse = serde_json::from_str(&body_text).map_err(|error| {
            FeedError::Parse(format!(
                "FYERS history decode failed: instrument={} trading_symbol={} timeframe={} resolution={} status={} url={} error={} body={}",
                instrument.instrument_name,
                instrument.trading_symbol,
                timeframe_label(timeframe),
                resolution,
                status,
                url,
                error,
                response_snippet(&body_text),
            ))
        })?;
        if body.s.eq_ignore_ascii_case("no_data") {
            return Ok(Vec::new());
        }

        if !body.s.eq_ignore_ascii_case("ok") {
            return Err(FeedError::Http(format!(
                "FYERS history returned api_status={} instrument={} trading_symbol={} timeframe={} resolution={} status={} url={} body={}",
                body.s,
                instrument.instrument_name,
                instrument.trading_symbol,
                timeframe_label(timeframe),
                resolution,
                status,
                url,
                response_snippet(&body_text),
            )));
        }

        body.candles
            .into_iter()
            .map(|row| row.into_candle(&instrument.instrument_name.to_string(), timeframe))
            .collect()
    }
}

fn timeframe_label(timeframe: Timeframe) -> &'static str {
    match timeframe {
        Timeframe::OneMinute => "1m",
        Timeframe::OneDay => "1d",
        Timeframe::ThreeMinute => "3m",
        Timeframe::FiveMinute => "5m",
        Timeframe::FifteenMinute => "15m",
        Timeframe::ThirtyMinute => "30m",
        Timeframe::SeventyFiveMinute => "75m",
        Timeframe::OneHour => "1h",
        Timeframe::FourHour => "4h",
    }
}

fn response_snippet(body: &str) -> String {
    const MAX_CHARS: usize = 300;
    let snippet = body.replace(char::is_whitespace, " ");
    let snippet = snippet.trim();
    if snippet.chars().count() <= MAX_CHARS {
        snippet.to_string()
    } else {
        format!("{}...", snippet.chars().take(MAX_CHARS).collect::<String>())
    }
}

fn normalize_request_window(
    instrument: &InstrumentDefinition,
    timeframe: Timeframe,
    start_inclusive: UnixMillis,
    end_inclusive: UnixMillis,
) -> Result<Option<(UnixMillis, UnixMillis)>, FeedError> {
    if start_inclusive.as_u64() >= end_inclusive.as_u64() {
        return Ok(None);
    }

    if !is_nse_bse_equity_symbol(&instrument.trading_symbol) {
        return Ok(Some((start_inclusive, end_inclusive)));
    }

    let now_secs = now_unix_seconds()?;
    let (today_ist_day, _) = ist_day_and_second(now_secs);
    let start_secs = start_inclusive.as_u64() / 1_000;
    let end_secs = end_inclusive.as_u64() / 1_000;
    let (start_day, _) = ist_day_and_second(start_secs);
    let (end_day, _) = ist_day_and_second(end_secs);

    match timeframe {
        Timeframe::OneMinute if start_day == today_ist_day && end_day == today_ist_day => {
            let session_open_utc = ist_day_second_to_utc(today_ist_day, NSE_BSE_EQUITY_OPEN_SECOND);
            let session_close_utc =
                ist_day_second_to_utc(today_ist_day, NSE_BSE_EQUITY_CLOSE_SECOND);
            let last_completed_minute_utc = (now_secs / 60) * 60;
            let effective_end = end_secs
                .min(session_close_utc)
                .min(last_completed_minute_utc);
            if effective_end <= session_open_utc {
                return Ok(None);
            }

            Ok(Some((
                UnixMillis::new(session_open_utc * 1_000),
                UnixMillis::new(effective_end * 1_000),
            )))
        }
        Timeframe::OneDay if end_day >= today_ist_day => {
            let previous_trading_day = previous_weekday_ist_day(today_ist_day);
            let previous_close_utc =
                ist_day_second_to_utc(previous_trading_day, NSE_BSE_EQUITY_CLOSE_SECOND);
            if previous_close_utc <= start_secs {
                return Ok(None);
            }
            Ok(Some((
                start_inclusive,
                UnixMillis::new(previous_close_utc.min(end_secs) * 1_000),
            )))
        }
        _ => Ok(Some((start_inclusive, end_inclusive))),
    }
}

fn is_nse_bse_equity_symbol(symbol: &str) -> bool {
    symbol.starts_with("NSE:") || symbol.starts_with("BSE:")
}

fn ist_day_and_second(now_utc: u64) -> (i64, u64) {
    let ist_seconds = now_utc as i64 + IST_OFFSET_SECONDS;
    (
        ist_seconds.div_euclid(DAY_SECONDS),
        ist_seconds.rem_euclid(DAY_SECONDS) as u64,
    )
}

fn ist_day_second_to_utc(ist_day: i64, second_of_day: u64) -> u64 {
    (ist_day * DAY_SECONDS + second_of_day as i64 - IST_OFFSET_SECONDS) as u64
}

fn previous_weekday_ist_day(ist_day: i64) -> i64 {
    let mut day = ist_day - 1;
    while !is_weekday_ist_day(day) {
        day -= 1;
    }
    day
}

fn is_weekday_ist_day(ist_day: i64) -> bool {
    let monday_zero_weekday = (ist_day + 3).rem_euclid(7);
    monday_zero_weekday <= 4
}

fn now_unix_seconds() -> Result<u64, FeedError> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| FeedError::Io(format!("system clock is before unix epoch: {error}")))?
        .as_secs())
}

struct HistoricalRateLimiter {
    per_second: usize,
    per_minute: usize,
    req_times: Mutex<VecDeque<Instant>>,
}

impl HistoricalRateLimiter {
    fn new(per_second: usize, per_minute: usize) -> Self {
        Self {
            per_second,
            per_minute,
            req_times: Mutex::new(VecDeque::new()),
        }
    }

    fn wait(&self) {
        loop {
            let now = Instant::now();
            let mut req_times = self.req_times.lock().expect("rate limiter lock");
            while let Some(front) = req_times.front() {
                if now.duration_since(*front) >= Duration::from_secs(60) {
                    req_times.pop_front();
                } else {
                    break;
                }
            }

            let per_second_count = req_times
                .iter()
                .filter(|instant| now.duration_since(**instant) < Duration::from_secs(1))
                .count();
            let per_minute_count = req_times.len();

            if per_second_count < self.per_second && per_minute_count < self.per_minute {
                req_times.push_back(now);
                return;
            }

            let mut wait_for = Duration::from_millis(100);
            if per_second_count >= self.per_second
                && let Some(oldest_in_second) = req_times
                    .iter()
                    .find(|instant| now.duration_since(**instant) < Duration::from_secs(1))
            {
                wait_for = wait_for.max(
                    Duration::from_secs(1).saturating_sub(now.duration_since(*oldest_in_second))
                        + Duration::from_millis(5),
                );
            }
            if per_minute_count >= self.per_minute
                && let Some(oldest_in_minute) = req_times.front()
            {
                wait_for = wait_for.max(
                    Duration::from_secs(60).saturating_sub(now.duration_since(*oldest_in_minute))
                        + Duration::from_millis(5),
                );
            }

            drop(req_times);
            thread::sleep(wait_for);
        }
    }
}

fn fyers_resolution(timeframe: Timeframe) -> Result<&'static str, FeedError> {
    match timeframe {
        Timeframe::OneMinute => Ok("1"),
        Timeframe::OneDay => Ok("1D"),
        _ => Err(FeedError::Config(format!(
            "FYERS historical recovery does not support timeframe {timeframe:?}"
        ))),
    }
}

pub fn fyers_history_chunk_candles(timeframe: Timeframe) -> Result<u64, FeedError> {
    match timeframe {
        Timeframe::OneMinute => Ok(1_000),
        Timeframe::OneDay => Ok(365),
        _ => Err(FeedError::Config(format!(
            "FYERS historical recovery does not support timeframe {timeframe:?}"
        ))),
    }
}

#[derive(Debug, Deserialize)]
struct FyersHistoricalResponse {
    s: String,
    #[serde(default)]
    candles: Vec<[f64; 6]>,
}

trait IntoHistoricalCandle {
    fn into_candle(self, instrument_name: &str, timeframe: Timeframe) -> Result<Candle, FeedError>;
}

impl IntoHistoricalCandle for [f64; 6] {
    fn into_candle(self, instrument_name: &str, timeframe: Timeframe) -> Result<Candle, FeedError> {
        let start = (self[0] as u64) * 1_000;
        let end = start + timeframe_millis(timeframe)?;
        Ok(Candle::new(
            crate::feeder::InstrumentName::new(instrument_name),
            timeframe,
            UnixMillis::new(start),
            UnixMillis::new(end),
            Price::new(self[1]).map_err(|reason| {
                FeedError::Parse(format!("FYERS history invalid open price: {reason}"))
            })?,
            Price::new(self[2]).map_err(|reason| {
                FeedError::Parse(format!("FYERS history invalid high price: {reason}"))
            })?,
            Price::new(self[3]).map_err(|reason| {
                FeedError::Parse(format!("FYERS history invalid low price: {reason}"))
            })?,
            Price::new(self[4]).map_err(|reason| {
                FeedError::Parse(format!("FYERS history invalid close price: {reason}"))
            })?,
            self[5],
        ))
    }
}

fn timeframe_millis(timeframe: Timeframe) -> Result<u64, FeedError> {
    match timeframe {
        Timeframe::OneMinute => Ok(60_000),
        Timeframe::OneDay => Ok(86_400_000),
        _ => Err(FeedError::Config(format!(
            "unsupported FYERS historical timeframe {timeframe:?}"
        ))),
    }
}
