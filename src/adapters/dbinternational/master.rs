use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::net::{IpAddr, Ipv4Addr};
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use reqwest::blocking::Client;
use reqwest::header::{ACCEPT, ACCEPT_ENCODING, AUTHORIZATION, HeaderMap, HeaderValue, USER_AGENT};
use serde::Serialize;
use serde_json::Value;

use crate::adapters::dbinternational::auth::read_market_data_session;
use crate::config::DbinternationalBrokerSection;
use crate::feeder::FeedError;

const IST_OFFSET_SECONDS: u64 = 5 * 60 * 60 + 30 * 60;
const DAY_SECONDS: u64 = 86_400;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DbinternationalMasterRefreshSummary {
    pub instrument_count: usize,
    pub output_path: String,
    pub index_count: usize,
    pub index_output_path: String,
    pub refreshed: bool,
}

#[derive(Clone, Debug)]
pub struct DbinternationalMasterClient {
    master_url: String,
    index_list_url: String,
    access_token: String,
    client: Client,
}

impl DbinternationalMasterClient {
    pub fn new(config: &DbinternationalBrokerSection) -> Result<Self, FeedError> {
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
        headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("identity"));
        headers.insert(
            USER_AGENT,
            HeaderValue::from_static("dhancred-trading-app/0.1"),
        );

        let session = read_market_data_session(config)?;
        let master_url = market_data_master_url_from_base_url(&session.base_url)?;
        let index_list_url = market_data_index_list_url_from_base_url(&session.base_url)?;

        Ok(Self {
            master_url,
            index_list_url,
            access_token: session.access_token,
            client: Client::builder()
                .default_headers(headers)
                .http1_only()
                .local_address(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
                .connect_timeout(Duration::from_secs(15))
                .timeout(Duration::from_secs(180))
                .build()?,
        })
    }

    pub fn fetch_master(&self, exchange_segments: &[String]) -> Result<String, FeedError> {
        if exchange_segments.is_empty() {
            return Err(FeedError::Config(
                "DBInternational market_data_exchange_segments cannot be empty".to_string(),
            ));
        }

        let request = MasterRequest {
            exchange_segment_list: exchange_segments,
        };
        let response = self
            .client
            .post(&self.master_url)
            .header(AUTHORIZATION, &self.access_token)
            .json(&request)
            .send()?;
        let status = response.status();
        let body = response.bytes()?;

        if !status.is_success() {
            return Err(FeedError::Http(format!(
                "DBInternational master failed status={} body={}",
                status.as_u16(),
                response_snippet(&String::from_utf8_lossy(&body))
            )));
        }

        let response_json: Value = serde_json::from_slice(&body)?;
        parse_master_response(&response_json)
    }

    fn fetch_index_list(
        &self,
        exchange_segment: &str,
    ) -> Result<Vec<DbinternationalIndex>, FeedError> {
        let exchange_segment = exchange_segment.trim();
        let exchange_segment_code = exchange_segment_code(exchange_segment).ok_or_else(|| {
            FeedError::Config(format!(
                "unsupported DBInternational index exchange segment {exchange_segment}"
            ))
        })?;

        let response = self
            .client
            .get(&self.index_list_url)
            .header(AUTHORIZATION, &self.access_token)
            .query(&[("exchangeSegment", exchange_segment_code.to_string())])
            .send()?;
        let status = response.status();
        let body = response.bytes()?;

        if !status.is_success() {
            return Err(FeedError::Http(format!(
                "DBInternational indexlist failed segment={} status={} body={}",
                exchange_segment,
                status.as_u16(),
                response_snippet(&String::from_utf8_lossy(&body))
            )));
        }

        let response_json: Value = serde_json::from_slice(&body)?;
        parse_index_list_response(exchange_segment, &response_json)
    }
}

#[derive(Serialize)]
struct MasterRequest<'a> {
    #[serde(rename = "exchangeSegmentList")]
    exchange_segment_list: &'a [String],
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DbinternationalIndex {
    exchange_segment: String,
    exchange_instrument_id: u64,
    name: String,
}

pub fn refresh_master(
    config: &DbinternationalBrokerSection,
) -> Result<DbinternationalMasterRefreshSummary, FeedError> {
    let client = DbinternationalMasterClient::new(config)?;
    let content = client.fetch_master(&config.market_data_exchange_segments)?;
    let instrument_count = write_master_file(&config.market_data_master_file, &content)?;
    let index_content =
        client.fetch_index_master_content(&config.market_data_index_exchange_segments)?;
    let index_count = write_master_file(&config.market_data_index_file, &index_content)?;

    Ok(DbinternationalMasterRefreshSummary {
        instrument_count,
        output_path: config.market_data_master_file.clone(),
        index_count,
        index_output_path: config.market_data_index_file.clone(),
        refreshed: true,
    })
}

pub fn ensure_master_current(
    config: &DbinternationalBrokerSection,
) -> Result<DbinternationalMasterRefreshSummary, FeedError> {
    let now = now_epoch_secs();
    if master_file_current_today(&config.market_data_master_file, now)?
        && master_file_current_today(&config.market_data_index_file, now)?
    {
        return Ok(DbinternationalMasterRefreshSummary {
            instrument_count: count_master_file_lines(&config.market_data_master_file)?,
            output_path: config.market_data_master_file.clone(),
            index_count: count_master_file_lines(&config.market_data_index_file)?,
            index_output_path: config.market_data_index_file.clone(),
            refreshed: false,
        });
    }

    refresh_master(config)
}

impl DbinternationalMasterClient {
    fn fetch_index_master_content(
        &self,
        exchange_segments: &[String],
    ) -> Result<String, FeedError> {
        if exchange_segments.is_empty() {
            return Err(FeedError::Config(
                "DBInternational market_data_index_exchange_segments cannot be empty".to_string(),
            ));
        }

        let mut rows = Vec::new();
        for exchange_segment in exchange_segments {
            for index in self.fetch_index_list(exchange_segment)? {
                rows.push(index_master_line(&index));
            }
        }

        if rows.is_empty() {
            return Err(FeedError::Parse(
                "DBInternational indexlist returned no indices".to_string(),
            ));
        }

        Ok(rows.join("\n"))
    }
}

fn parse_master_response(response_json: &Value) -> Result<String, FeedError> {
    let envelope = response_envelope(response_json)
        .ok_or_else(|| FeedError::Parse("DBInternational master response is empty".to_string()))?;

    let response_type = envelope.get("type").and_then(Value::as_str);
    if response_type != Some("success") {
        return Err(FeedError::Http(format!(
            "DBInternational master returned non-success response: {}",
            response_snippet(&envelope.to_string())
        )));
    }

    let result = envelope
        .get("result")
        .ok_or_else(|| FeedError::Parse("DBInternational master missing result".to_string()))?;

    let content = match result {
        Value::String(value) => value.clone(),
        Value::Array(values) => values
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>()
            .join("\n"),
        _ => {
            return Err(FeedError::Parse(format!(
                "DBInternational master result has unsupported shape: {}",
                response_snippet(&result.to_string())
            )));
        }
    };

    if content.trim().is_empty() {
        return Err(FeedError::Parse(
            "DBInternational master result is empty".to_string(),
        ));
    }

    Ok(content)
}

fn parse_index_list_response(
    exchange_segment: &str,
    response_json: &Value,
) -> Result<Vec<DbinternationalIndex>, FeedError> {
    let envelope = response_envelope(response_json).ok_or_else(|| {
        FeedError::Parse("DBInternational indexlist response is empty".to_string())
    })?;

    let response_type = envelope.get("type").and_then(Value::as_str);
    if response_type != Some("success") {
        return Err(FeedError::Http(format!(
            "DBInternational indexlist returned non-success response: {}",
            response_snippet(&envelope.to_string())
        )));
    }

    let result = envelope
        .get("result")
        .ok_or_else(|| FeedError::Parse("DBInternational indexlist missing result".to_string()))?;
    let values = result
        .get("indexList")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            FeedError::Parse("DBInternational indexlist missing indexList".to_string())
        })?;

    let mut indices = Vec::new();
    for value in values {
        let Some(raw_index) = value.as_str() else {
            continue;
        };
        let Some((name, instrument_id)) = raw_index.trim().rsplit_once('_') else {
            return Err(FeedError::Parse(format!(
                "DBInternational index entry has unsupported format: {raw_index}"
            )));
        };
        let exchange_instrument_id = instrument_id.parse::<u64>().map_err(|error| {
            FeedError::Parse(format!(
                "invalid DBInternational index exchangeInstrumentID {instrument_id}: {error}"
            ))
        })?;
        let name = name.trim();
        if name.is_empty() {
            continue;
        }

        indices.push(DbinternationalIndex {
            exchange_segment: exchange_segment.trim().to_ascii_uppercase(),
            exchange_instrument_id,
            name: name.to_string(),
        });
    }

    Ok(indices)
}

fn index_master_line(index: &DbinternationalIndex) -> String {
    let aliases = index_aliases(&index.name);
    let alias_1 = aliases.first().map(String::as_str).unwrap_or("");
    let alias_2 = aliases.get(1).map(String::as_str).unwrap_or("");
    let alias_3 = aliases.get(2).map(String::as_str).unwrap_or("");
    let alias_4 = aliases.get(3).map(String::as_str).unwrap_or("");

    format!(
        "{}|{}|16|{}|{}|INDEX|{}-INDEX|{}|0|0|0|0.05|1|1|{}|{}|1|1|{}|{}|{}|-1|{}",
        index.exchange_segment,
        index.exchange_instrument_id,
        index.name,
        index.name,
        index.name,
        index.exchange_instrument_id,
        index.name,
        alias_1,
        alias_2,
        alias_3,
        alias_4,
        index.name
    )
}

fn index_aliases(name: &str) -> Vec<String> {
    let mut aliases = Vec::new();
    push_unique_alias(&mut aliases, &compact_alias(name));

    match name.trim().to_ascii_uppercase().as_str() {
        "NIFTY 50" => {
            push_unique_alias(&mut aliases, "NIFTY50");
        }
        "NIFTY BANK" => {
            push_unique_alias(&mut aliases, "NIFTYBANK");
        }
        "NIFTY MID SELECT" => {
            push_unique_alias(&mut aliases, "NIFTYMIDSELECT");
            push_unique_alias(&mut aliases, "MIDCPNIFTY");
            push_unique_alias(&mut aliases, "MIDCAPSELECT");
        }
        "SENSEX" => {
            push_unique_alias(&mut aliases, "SENSEX");
        }
        _ => {}
    }

    aliases
}

fn compact_alias(value: &str) -> String {
    value
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect::<String>()
}

fn push_unique_alias(aliases: &mut Vec<String>, value: &str) {
    let value = value.trim();
    if value.is_empty() {
        return;
    }
    if aliases
        .iter()
        .all(|existing| !existing.eq_ignore_ascii_case(value))
    {
        aliases.push(value.to_string());
    }
}

fn response_envelope(response_json: &Value) -> Option<&Value> {
    response_json
        .as_array()
        .and_then(|items| items.first())
        .or_else(|| {
            if response_json.is_object() {
                Some(response_json)
            } else {
                None
            }
        })
}

fn market_data_master_url_from_base_url(base_url: &str) -> Result<String, FeedError> {
    let base_url = normalize_market_data_base_url(base_url);
    if base_url.is_empty() {
        return Err(FeedError::Config(
            "DBInternational market-data session base_url is empty".to_string(),
        ));
    }
    Ok(format!("{base_url}/instruments/master"))
}

fn market_data_index_list_url_from_base_url(base_url: &str) -> Result<String, FeedError> {
    let base_url = normalize_market_data_base_url(base_url);
    if base_url.is_empty() {
        return Err(FeedError::Config(
            "DBInternational market-data session base_url is empty".to_string(),
        ));
    }
    Ok(format!("{base_url}/instruments/indexlist"))
}

fn normalize_market_data_base_url(value: &str) -> String {
    let mut value = value.trim().trim_end_matches('/').to_string();
    for suffix in [
        "/instruments/indexlist",
        "/instruments/master",
        "/auth/login",
    ] {
        let lower = value.to_ascii_lowercase();
        if lower.ends_with(suffix) {
            value.truncate(value.len() - suffix.len());
        }
    }
    value
}

fn exchange_segment_code(value: &str) -> Option<u16> {
    match value.trim().to_ascii_uppercase().as_str() {
        "NSECM" => Some(1),
        "NSEFO" => Some(2),
        "NSECD" => Some(3),
        "NSECO" => Some(4),
        "BSECM" => Some(11),
        "BSEFO" => Some(12),
        "BSECD" => Some(13),
        "NCDEX" => Some(21),
        "MCXFO" => Some(51),
        _ => None,
    }
}

fn write_master_file(path: impl AsRef<Path>, content: &str) -> Result<usize, FeedError> {
    let content = content.trim();
    if content.is_empty() {
        return Err(FeedError::Parse(
            "DBInternational master content is empty".to_string(),
        ));
    }

    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let tmp_path = path.with_extension("tmp");
    let mut writer = BufWriter::new(File::create(&tmp_path)?);
    writer.write_all(content.as_bytes())?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    drop(writer);
    fs::rename(tmp_path, path)?;

    Ok(content
        .lines()
        .filter(|line| !line.trim().is_empty())
        .count())
}

fn master_file_current_today(path: impl AsRef<Path>, now_utc: u64) -> Result<bool, FeedError> {
    let path = path.as_ref();
    if !path.exists() {
        return Ok(false);
    }
    if count_master_file_lines(path)? == 0 {
        return Ok(false);
    }

    let modified = fs::metadata(path)?
        .modified()?
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            FeedError::Config(format!("invalid DBInternational master mtime: {error}"))
        })?
        .as_secs();
    Ok(same_ist_day(modified, now_utc))
}

fn count_master_file_lines(path: impl AsRef<Path>) -> Result<usize, FeedError> {
    let content = fs::read_to_string(path)?;
    Ok(content
        .lines()
        .filter(|line| !line.trim().is_empty())
        .count())
}

fn same_ist_day(left_utc: u64, right_utc: u64) -> bool {
    ist_day(left_utc) == ist_day(right_utc)
}

fn ist_day(epoch_utc: u64) -> u64 {
    epoch_utc.saturating_add(IST_OFFSET_SECONDS) / DAY_SECONDS
}

fn now_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn response_snippet(value: &str) -> String {
    let normalized = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.len() > 500 {
        format!("{}...", &normalized[..500])
    } else {
        normalized
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_string_master_response() {
        let value = serde_json::json!({
            "type": "success",
            "result": "NSECM|2885|8|RELIANCE|RELIANCE-EQ\nNSEFO|49229|1|NIFTY|NIFTY26JANFUT"
        });

        let content = parse_master_response(&value).expect("master response");

        assert!(content.contains("RELIANCE"));
        assert!(content.contains("NIFTY26JANFUT"));
    }

    #[test]
    fn parses_array_wrapped_master_response() {
        let value = serde_json::json!([
            {
                "type": "success",
                "result": [
                    "NSECM|2885|8|RELIANCE|RELIANCE-EQ",
                    "NSEFO|49229|1|NIFTY|NIFTY26JANFUT"
                ]
            }
        ]);

        let content = parse_master_response(&value).expect("master response");

        assert_eq!(content.lines().count(), 2);
    }

    #[test]
    fn rejects_error_master_response() {
        let value = serde_json::json!({
            "type": "error",
            "description": "bad token"
        });

        assert!(parse_master_response(&value).is_err());
    }

    #[test]
    fn builds_master_url_from_saved_session_base_url() {
        let url = market_data_master_url_from_base_url(
            "https://developers.symphonyfintech.in/apibinarymarketdata/auth/login",
        )
        .expect("master url");

        assert_eq!(
            url,
            "https://developers.symphonyfintech.in/apibinarymarketdata/instruments/master"
        );
    }

    #[test]
    fn builds_indexlist_url_from_saved_session_base_url() {
        let url = market_data_index_list_url_from_base_url(
            "https://developers.symphonyfintech.in/apibinarymarketdata/instruments/indexlist",
        )
        .expect("indexlist url");

        assert_eq!(
            url,
            "https://developers.symphonyfintech.in/apibinarymarketdata/instruments/indexlist"
        );
    }

    #[test]
    fn parses_indexlist_response() {
        let value = serde_json::json!({
            "type": "success",
            "result": {
                "exchangeSegment": "1",
                "indexList": [
                    "NIFTY 50_26000",
                    "NIFTY BANK_26001",
                    "NIFTY MID SELECT_26121"
                ]
            }
        });

        let indices = parse_index_list_response("NSECM", &value).expect("indexlist");

        assert_eq!(indices.len(), 3);
        assert_eq!(indices[0].name, "NIFTY 50");
        assert_eq!(indices[0].exchange_instrument_id, 26000);
        assert_eq!(indices[2].exchange_segment, "NSECM");
    }

    #[test]
    fn formats_index_master_line_with_aliases() {
        let index = DbinternationalIndex {
            exchange_segment: "BSECM".to_string(),
            exchange_instrument_id: 26065,
            name: "SENSEX".to_string(),
        };

        let line = index_master_line(&index);

        assert!(line.starts_with("BSECM|26065|16|SENSEX|"));
        assert!(line.contains("|INDEX|"));
        assert_eq!(line.split('|').count(), 23);
    }

    #[test]
    fn detects_current_non_empty_master_file() {
        let path = temp_master_path("current-master");
        fs::write(&path, "NSECM|1|ABC\n\nNSEFO|2|XYZ\n").expect("write master file");

        assert!(master_file_current_today(&path, now_epoch_secs()).expect("current file"));
        assert_eq!(count_master_file_lines(&path).expect("line count"), 2);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn rejects_empty_master_file_as_stale() {
        let path = temp_master_path("empty-master");
        fs::write(&path, "\n\n").expect("write master file");

        assert!(!master_file_current_today(&path, now_epoch_secs()).expect("empty file"));

        let _ = fs::remove_file(path);
    }

    fn temp_master_path(label: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        std::env::temp_dir().join(format!("dhancred-dbinternational-{label}-{nanos}.txt"))
    }
}
