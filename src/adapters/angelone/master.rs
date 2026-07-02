use std::fs;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{FixedOffset, Utc};
use reqwest::blocking::Client;
use reqwest::header::{ACCEPT, ACCEPT_ENCODING, HeaderMap, HeaderValue, USER_AGENT};
use serde::Deserialize;

use crate::config::AngeloneBrokerSection;
use crate::feeder::{
    FeedError, InstrumentDefinition, InstrumentName, InstrumentType,
    UNIVERSAL_INSTRUMENT_CSV_HEADER,
};

const BROKER: &str = "ANGELONE";
const IST_OFFSET_SECONDS: i32 = 5 * 60 * 60 + 30 * 60;
const DAY_SECONDS: u64 = 86_400;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AngeloneMasterRefreshSummary {
    pub instrument_count: usize,
    pub output_path: String,
    pub refreshed: bool,
}

#[derive(Clone, Debug, Deserialize)]
pub struct AngeloneRawInstrument {
    pub token: String,
    pub symbol: String,
    pub name: String,
    pub expiry: String,
    pub strike: String,
    pub lotsize: String,
    pub instrumenttype: String,
    pub exch_seg: String,
    pub tick_size: String,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct AngeloneInstrumentKey {
    pub exchange_type: u8,
    pub token: String,
}

pub fn refresh_master(
    config: &AngeloneBrokerSection,
) -> Result<AngeloneMasterRefreshSummary, FeedError> {
    let client = build_client()?;
    let response = client.get(&config.master_url).send()?;
    let status = response.status();
    let body = response.text()?;
    if !status.is_success() {
        return Err(FeedError::Http(format!(
            "AngelOne master failed status={} body={}",
            status.as_u16(),
            response_snippet(&body)
        )));
    }

    let count = parse_master_json(&body)?.len();
    write_master_file(&config.master_file, &body)?;

    Ok(AngeloneMasterRefreshSummary {
        instrument_count: count,
        output_path: config.master_file.clone(),
        refreshed: true,
    })
}

pub fn ensure_master_current(
    config: &AngeloneBrokerSection,
) -> Result<AngeloneMasterRefreshSummary, FeedError> {
    if master_file_current_today(&config.master_file)? {
        return Ok(AngeloneMasterRefreshSummary {
            instrument_count: read_master_file(&config.master_file)?.len(),
            output_path: config.master_file.clone(),
            refreshed: false,
        });
    }

    refresh_master(config)
}

pub fn read_master_file(path: &str) -> Result<Vec<InstrumentDefinition>, FeedError> {
    let content = fs::read_to_string(path).map_err(|error| {
        FeedError::Config(format!("failed to read AngelOne master {path}: {error}"))
    })?;
    parse_master_json(&content)
}

pub fn parse_master_json(content: &str) -> Result<Vec<InstrumentDefinition>, FeedError> {
    let rows: Vec<AngeloneRawInstrument> = serde_json::from_str(content)
        .map_err(|error| FeedError::Parse(format!("AngelOne master JSON parse failed: {error}")))?;
    rows.iter()
        .filter_map(|row| match instrument_from_raw(row) {
            Ok(Some(value)) => Some(Ok(value)),
            Ok(None) => None,
            Err(error) => Some(Err(error)),
        })
        .collect()
}

pub fn instrument_from_raw(
    row: &AngeloneRawInstrument,
) -> Result<Option<InstrumentDefinition>, FeedError> {
    let Some(exchange_type) = exchange_type(&row.exch_seg) else {
        return Ok(None);
    };
    let Some(instrument_type) = instrument_type(row) else {
        return Ok(None);
    };

    let token = row.token.trim();
    let trading_symbol = row.symbol.trim();
    let underlying = row.name.trim().to_ascii_uppercase();
    if token.is_empty() || trading_symbol.is_empty() || underlying.is_empty() {
        return Ok(None);
    }

    let strike = if matches!(instrument_type, InstrumentType::Call | InstrumentType::Put) {
        Some(parse_decimal(&row.strike, "strike")? / 100.0)
    } else {
        None
    };
    let expiry = parse_expiry(&row.expiry);
    let lot_size = parse_decimal(&row.lotsize, "lotsize")?;
    let tick_size = parse_decimal(&row.tick_size, "tick_size")? / 100.0;

    let instrument_name = if instrument_type == InstrumentType::Spot {
        InstrumentName::new(underlying.clone())
    } else {
        InstrumentName::new(trading_symbol)
    };

    Ok(Some(InstrumentDefinition {
        instrument_name,
        instrument_type,
        strike,
        expiry,
        broker: BROKER.to_string(),
        instrument_token: format!("{}:{token}", row.exch_seg.trim().to_ascii_uppercase()),
        trading_symbol: trading_symbol.to_string(),
        exchange: exchange_name(&row.exch_seg),
        segment: segment_name(row, instrument_type),
        underlying,
        lot_size,
        tick_size,
        tradable: exchange_type > 0,
    }))
}

pub fn instrument_key_from_token(value: &str) -> Result<AngeloneInstrumentKey, FeedError> {
    let Some((exchange_segment, token)) = value.trim().split_once(':') else {
        return Err(FeedError::InvalidInstrument(format!(
            "AngelOne instrument_token {value} must be exch_seg:token"
        )));
    };
    let exchange_type = exchange_type(exchange_segment).ok_or_else(|| {
        FeedError::InvalidInstrument(format!(
            "unsupported AngelOne exchange segment {exchange_segment}"
        ))
    })?;
    let token = token.trim();
    if token.is_empty() {
        return Err(FeedError::InvalidInstrument(
            "AngelOne instrument token is empty".to_string(),
        ));
    }

    Ok(AngeloneInstrumentKey {
        exchange_type,
        token: token.to_string(),
    })
}

pub fn exchange_type(exchange_segment: &str) -> Option<u8> {
    match exchange_segment.trim().to_ascii_uppercase().as_str() {
        "NSE" => Some(1),
        "NFO" => Some(2),
        "BSE" => Some(3),
        "BFO" => Some(4),
        "MCX" => Some(5),
        "NCDEX" | "NCO" => Some(7),
        "CDS" => Some(13),
        _ => None,
    }
}

pub fn price_divisor(exchange_type: u8) -> f64 {
    if exchange_type == 13 {
        10_000_000.0
    } else {
        100.0
    }
}

pub fn csv_header() -> &'static str {
    UNIVERSAL_INSTRUMENT_CSV_HEADER
}

fn build_client() -> Result<Client, FeedError> {
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("identity"));
    headers.insert(
        USER_AGENT,
        HeaderValue::from_static("dhancred-trading-app/0.1"),
    );

    Ok(Client::builder()
        .default_headers(headers)
        .http1_only()
        .connect_timeout(Duration::from_secs(15))
        .timeout(Duration::from_secs(180))
        .build()?)
}

fn write_master_file(path: &str, content: &str) -> Result<(), FeedError> {
    let path = Path::new(path);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| FeedError::Config("invalid AngelOne master path".to_string()))?;
    let tmp_path = path.with_file_name(format!("{file_name}.tmp"));
    fs::write(&tmp_path, content)?;
    fs::rename(tmp_path, path)?;
    Ok(())
}

fn master_file_current_today(path: &str) -> Result<bool, FeedError> {
    let modified = match fs::metadata(path).and_then(|metadata| metadata.modified()) {
        Ok(value) => value,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    let modified_epoch = modified
        .duration_since(UNIX_EPOCH)
        .map_err(|error| FeedError::Io(format!("AngelOne master mtime before epoch: {error}")))?
        .as_secs();
    Ok(ist_day(modified_epoch) == ist_day(now_unix_seconds()))
}

fn instrument_type(row: &AngeloneRawInstrument) -> Option<InstrumentType> {
    let instrument_type = row.instrumenttype.trim().to_ascii_uppercase();
    if row.expiry.trim().is_empty()
        && (instrument_type.is_empty()
            || instrument_type == "AMXIDX"
            || instrument_type == "INDEX"
            || instrument_type.starts_with("UND"))
    {
        return Some(InstrumentType::Spot);
    }

    if instrument_type.starts_with("FUT") {
        return Some(InstrumentType::Fut);
    }
    if instrument_type.starts_with("OPT") {
        return option_side(&row.symbol);
    }

    None
}

fn option_side(symbol: &str) -> Option<InstrumentType> {
    let symbol = symbol.trim().to_ascii_uppercase();
    if symbol.ends_with("CE") {
        Some(InstrumentType::Call)
    } else if symbol.ends_with("PE") {
        Some(InstrumentType::Put)
    } else {
        None
    }
}

fn segment_name(row: &AngeloneRawInstrument, instrument_type: InstrumentType) -> String {
    let raw = row.instrumenttype.trim();
    if !raw.is_empty() {
        return raw.to_string();
    }
    instrument_type.segment().to_string()
}

fn exchange_name(exchange_segment: &str) -> String {
    match exchange_segment.trim().to_ascii_uppercase().as_str() {
        "NSE" | "NFO" => "NSE".to_string(),
        "BSE" | "BFO" => "BSE".to_string(),
        "MCX" => "MCX".to_string(),
        "NCDEX" | "NCO" => "NCDEX".to_string(),
        "CDS" => "CDS".to_string(),
        other => other.to_string(),
    }
}

fn parse_decimal(value: &str, field: &str) -> Result<f64, FeedError> {
    value.trim().parse::<f64>().map_err(|error| {
        FeedError::Parse(format!("AngelOne master invalid {field} {value}: {error}"))
    })
}

fn parse_expiry(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    if value.len() != 9 {
        return None;
    }
    let day = &value[0..2];
    let month = month_number(&value[2..5])?;
    let year = &value[5..9];
    Some(format!("{year}-{month:02}-{day}"))
}

fn month_number(value: &str) -> Option<u8> {
    match value.to_ascii_uppercase().as_str() {
        "JAN" => Some(1),
        "FEB" => Some(2),
        "MAR" => Some(3),
        "APR" => Some(4),
        "MAY" => Some(5),
        "JUN" => Some(6),
        "JUL" => Some(7),
        "AUG" => Some(8),
        "SEP" => Some(9),
        "OCT" => Some(10),
        "NOV" => Some(11),
        "DEC" => Some(12),
        _ => None,
    }
}

fn ist_day(epoch_seconds: u64) -> u64 {
    (epoch_seconds + IST_OFFSET_SECONDS as u64) / DAY_SECONDS
}

fn now_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

pub fn current_ist_date() -> String {
    let ist = FixedOffset::east_opt(IST_OFFSET_SECONDS).expect("valid IST offset");
    Utc::now()
        .with_timezone(&ist)
        .date_naive()
        .format("%Y-%m-%d")
        .to_string()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_spot_and_derivative_rows() {
        let content = r#"[
          {"token":"3045","symbol":"SBIN-EQ","name":"SBIN","expiry":"","strike":"-1.000000","lotsize":"1","instrumenttype":"","exch_seg":"NSE","tick_size":"10.000000"},
          {"token":"61093","symbol":"NIFTY28JUL26FUT","name":"NIFTY","expiry":"28JUL2026","strike":"-1.000000","lotsize":"65","instrumenttype":"FUTIDX","exch_seg":"NFO","tick_size":"10.000000"},
          {"token":"63915","symbol":"NIFTY28JUL2623500CE","name":"NIFTY","expiry":"28JUL2026","strike":"2350000.000000","lotsize":"65","instrumenttype":"OPTIDX","exch_seg":"NFO","tick_size":"5.000000"}
        ]"#;

        let instruments = parse_master_json(content).expect("master");

        assert_eq!(instruments.len(), 3);
        assert_eq!(instruments[0].instrument_token, "NSE:3045");
        assert_eq!(instruments[1].expiry.as_deref(), Some("2026-07-28"));
        assert_eq!(instruments[2].strike, Some(23_500.0));
        assert_eq!(instruments[2].instrument_type, InstrumentType::Call);
    }

    #[test]
    fn parses_exchange_token_key() {
        let key = instrument_key_from_token("NFO:61093").expect("key");

        assert_eq!(key.exchange_type, 2);
        assert_eq!(key.token, "61093");
    }
}
