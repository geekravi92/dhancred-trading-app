use reqwest::blocking::Client;
use reqwest::header::{ACCEPT, HeaderMap, HeaderValue, USER_AGENT};
use serde::Deserialize;

use crate::adapters::historical::HistoricalCandleSource;
use crate::feeder::{Candle, FeedError, InstrumentDefinition, Price, Timeframe, UnixMillis};

const DELTA_MAX_RESPONSE_CANDLES: u64 = 2_000;

pub struct DeltaHistoricalClient {
    rest_url: String,
    client: Client,
}

impl DeltaHistoricalClient {
    pub fn new(rest_url: impl Into<String>) -> Self {
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
        headers.insert(
            USER_AGENT,
            HeaderValue::from_static("dhancred-trading-app/0.1"),
        );

        Self {
            rest_url: rest_url.into().trim_end_matches('/').to_string(),
            client: Client::builder()
                .default_headers(headers)
                .build()
                .expect("valid delta historical client"),
        }
    }
}

impl HistoricalCandleSource for DeltaHistoricalClient {
    fn broker_name(&self) -> &'static str {
        "DELTA"
    }

    fn max_chunk_candles(&self, timeframe: Timeframe) -> Result<u64, FeedError> {
        delta_history_chunk_candles(timeframe)
    }

    fn fetch_candles(
        &self,
        instrument: &InstrumentDefinition,
        timeframe: Timeframe,
        start_inclusive: UnixMillis,
        end_inclusive: UnixMillis,
    ) -> Result<Vec<Candle>, FeedError> {
        let resolution = delta_resolution(timeframe)?;
        let start_secs = start_inclusive.as_u64() / 1_000;
        let end_secs = end_inclusive.as_u64() / 1_000;
        let request = self
            .client
            .get(format!("{}/v2/history/candles", self.rest_url))
            .query(&[
                ("symbol", instrument.trading_symbol.as_str()),
                ("resolution", resolution),
                ("start", &start_secs.to_string()),
                ("end", &end_secs.to_string()),
            ]);
        let response = request.send()?;
        let status = response.status();
        let url = response.url().to_string();
        let body_text = response.text()?;
        if !status.is_success() {
            return Err(FeedError::Http(format!(
                "Delta history request failed: instrument={} trading_symbol={} timeframe={} resolution={} status={} url={} body={}",
                instrument.instrument_name,
                instrument.trading_symbol,
                timeframe_label(timeframe),
                resolution,
                status,
                url,
                response_snippet(&body_text),
            )));
        }

        let body: DeltaHistoricalResponse = serde_json::from_str(&body_text).map_err(|error| {
            FeedError::Parse(format!(
                "Delta history decode failed: instrument={} trading_symbol={} timeframe={} resolution={} status={} url={} error={} body={}",
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
        if !body.success {
            return Err(FeedError::Http(format!(
                "Delta history returned success=false: instrument={} trading_symbol={} timeframe={} resolution={} status={} url={} body={}",
                instrument.instrument_name,
                instrument.trading_symbol,
                timeframe_label(timeframe),
                resolution,
                status,
                url,
                response_snippet(&body_text),
            )));
        }

        body.result
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

fn delta_resolution(timeframe: Timeframe) -> Result<&'static str, FeedError> {
    match timeframe {
        Timeframe::OneMinute => Ok("1m"),
        Timeframe::OneDay => Ok("1d"),
        _ => Err(FeedError::Config(format!(
            "Delta historical recovery does not support timeframe {timeframe:?}"
        ))),
    }
}

pub fn delta_history_chunk_candles(timeframe: Timeframe) -> Result<u64, FeedError> {
    match timeframe {
        Timeframe::OneMinute | Timeframe::OneDay => Ok(DELTA_MAX_RESPONSE_CANDLES),
        _ => Err(FeedError::Config(format!(
            "Delta historical recovery does not support timeframe {timeframe:?}"
        ))),
    }
}

#[derive(Debug, Deserialize)]
struct DeltaHistoricalResponse {
    success: bool,
    result: Vec<DeltaHistoricalRow>,
}

#[derive(Debug, Deserialize)]
struct DeltaHistoricalRow {
    time: u64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: Option<f64>,
}

impl DeltaHistoricalRow {
    fn into_candle(self, instrument_name: &str, timeframe: Timeframe) -> Result<Candle, FeedError> {
        let start = self.time * 1_000;
        let end = start + timeframe_millis(timeframe)?;
        Ok(Candle::new(
            crate::feeder::InstrumentName::new(instrument_name),
            timeframe,
            UnixMillis::new(start),
            UnixMillis::new(end),
            Price::new(self.open).map_err(|reason| {
                FeedError::Parse(format!("Delta history invalid open price: {reason}"))
            })?,
            Price::new(self.high).map_err(|reason| {
                FeedError::Parse(format!("Delta history invalid high price: {reason}"))
            })?,
            Price::new(self.low).map_err(|reason| {
                FeedError::Parse(format!("Delta history invalid low price: {reason}"))
            })?,
            Price::new(self.close).map_err(|reason| {
                FeedError::Parse(format!("Delta history invalid close price: {reason}"))
            })?,
            self.volume.unwrap_or(0.0),
        ))
    }
}

fn timeframe_millis(timeframe: Timeframe) -> Result<u64, FeedError> {
    match timeframe {
        Timeframe::OneMinute => Ok(60_000),
        Timeframe::OneDay => Ok(86_400_000),
        _ => Err(FeedError::Config(format!(
            "unsupported delta historical timeframe {timeframe:?}"
        ))),
    }
}
