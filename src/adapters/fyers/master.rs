use std::collections::BTreeSet;
use std::fmt;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::{IpAddr, Ipv4Addr};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use reqwest::blocking::Client;
use reqwest::header::{ACCEPT, ACCEPT_ENCODING, HeaderMap, HeaderValue, USER_AGENT};
use serde::Deserialize;
use serde::de::{self, MapAccess, Visitor};
use serde_json::{Map, Value};

use crate::config::FyersBrokerSection;
use crate::config::InstrumentSelection;
use crate::feeder::{
    FeedError, InstrumentDefinition, InstrumentName, InstrumentType,
    UNIVERSAL_INSTRUMENT_CSV_HEADER, parse_instrument_type,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FyersMasterRefreshSummary {
    pub source: String,
    pub url: String,
    pub output_path: PathBuf,
    pub instrument_count: usize,
    pub downloaded: bool,
}

pub fn refresh_all(
    config: &FyersBrokerSection,
) -> Result<Vec<FyersMasterRefreshSummary>, FeedError> {
    if config.master_urls.is_empty() {
        return Err(FeedError::Config(
            "FYERS master_urls cannot be empty".to_string(),
        ));
    }

    let output_dir = Path::new(&config.master_data_dir);
    fs::create_dir_all(output_dir)?;

    let today_epoch_day = current_epoch_day()?;
    let mut client = None;
    let mut summaries = Vec::new();

    for (source, url) in &config.master_urls {
        let normalized_source = normalize_source(source)?;
        let output_path = output_dir.join(format!("{normalized_source}.csv"));
        if is_fresh_for_epoch_day(&output_path, today_epoch_day)? {
            summaries.push(FyersMasterRefreshSummary {
                source: normalized_source,
                url: url.clone(),
                instrument_count: count_csv_instruments(&output_path)?,
                output_path,
                downloaded: false,
            });
            continue;
        }

        let client = client.get_or_insert(build_client()?);
        println!("FYERS {normalized_source} master downloading");
        let instrument_count = fetch_master_csv(client, &normalized_source, url, &output_path)?;

        summaries.push(FyersMasterRefreshSummary {
            source: normalized_source,
            url: url.clone(),
            output_path,
            instrument_count,
            downloaded: true,
        });
    }

    Ok(summaries)
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
        .local_address(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
        .connect_timeout(Duration::from_secs(15))
        .timeout(Duration::from_secs(180))
        .build()?)
}

fn fetch_master_csv(
    client: &Client,
    source: &str,
    url: &str,
    output_path: &Path,
) -> Result<usize, FeedError> {
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file_name = output_path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| FeedError::Config("invalid FYERS master output path".to_string()))?;
    let tmp_path = output_path.with_file_name(format!("{file_name}.tmp"));
    let mut writer = BufWriter::new(File::create(&tmp_path)?);
    writeln!(writer, "{UNIVERSAL_INSTRUMENT_CSV_HEADER}")?;

    let response = client
        .get(url)
        .send()
        .map_err(|error| FeedError::Http(format!("FYERS {source} request failed: {error}")))?
        .error_for_status()
        .map_err(|error| FeedError::Http(format!("FYERS {source} returned HTTP error: {error}")))?;
    let mut deserializer = serde_json::Deserializer::from_reader(response);
    let instrument_count = serde::Deserializer::deserialize_map(
        &mut deserializer,
        FyersMasterCsvVisitor {
            source,
            writer: &mut writer,
        },
    )
    .map_err(|error| FeedError::Parse(format!("FYERS {source} JSON parse failed: {error}")))?;

    writer.flush()?;
    drop(writer);
    fs::rename(tmp_path, output_path)?;

    Ok(instrument_count)
}

struct FyersMasterCsvVisitor<'a, W> {
    source: &'a str,
    writer: &'a mut W,
}

impl<'de, W: Write> Visitor<'de> for FyersMasterCsvVisitor<'_, W> {
    type Value = usize;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("FYERS symbol master object")
    }

    fn visit_map<A>(self, mut access: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut count = 0usize;
        while let Some((symbol_key, raw_instrument)) =
            access.next_entry::<String, FyersRawInstrument>()?
        {
            let fields = raw_instrument.into_fields_map();
            if let Some(instrument) =
                parse_instrument(self.source, &symbol_key, &fields).map_err(de::Error::custom)?
            {
                writeln!(self.writer, "{}", instrument.to_csv_row()).map_err(de::Error::custom)?;
                count += 1;
            }
        }

        Ok(count)
    }
}

#[derive(Debug, Deserialize)]
struct FyersRawInstrument {
    #[serde(rename = "fyToken")]
    fy_token: Option<Value>,
    #[serde(rename = "exInstType")]
    ex_inst_type: Option<Value>,
    #[serde(rename = "tradeStatus")]
    trade_status: Option<Value>,
    #[serde(rename = "underSym")]
    under_sym: Option<Value>,
    #[serde(rename = "expiryDate")]
    expiry_date: Option<Value>,
    #[serde(rename = "optType")]
    opt_type: Option<Value>,
    #[serde(rename = "strikePrice")]
    strike_price: Option<Value>,
    #[serde(rename = "minLotSize")]
    min_lot_size: Option<Value>,
    #[serde(rename = "tickSize")]
    tick_size: Option<Value>,
    #[serde(rename = "symTicker")]
    sym_ticker: Option<Value>,
    #[serde(rename = "exchangeName")]
    exchange_name: Option<Value>,
    exchange: Option<Value>,
}

impl FyersRawInstrument {
    fn into_fields_map(self) -> Map<String, Value> {
        let mut fields = Map::new();
        insert_if_some(&mut fields, "fyToken", self.fy_token);
        insert_if_some(&mut fields, "exInstType", self.ex_inst_type);
        insert_if_some(&mut fields, "tradeStatus", self.trade_status);
        insert_if_some(&mut fields, "underSym", self.under_sym);
        insert_if_some(&mut fields, "expiryDate", self.expiry_date);
        insert_if_some(&mut fields, "optType", self.opt_type);
        insert_if_some(&mut fields, "strikePrice", self.strike_price);
        insert_if_some(&mut fields, "minLotSize", self.min_lot_size);
        insert_if_some(&mut fields, "tickSize", self.tick_size);
        insert_if_some(&mut fields, "symTicker", self.sym_ticker);
        insert_if_some(&mut fields, "exchangeName", self.exchange_name);
        insert_if_some(&mut fields, "exchange", self.exchange);
        fields
    }
}

fn insert_if_some(fields: &mut Map<String, Value>, name: &str, value: Option<Value>) {
    if let Some(value) = value {
        fields.insert(name.to_string(), value);
    }
}

pub fn parse_master_json(
    source: &str,
    content: &str,
) -> Result<Vec<InstrumentDefinition>, FeedError> {
    let value: Value = serde_json::from_str(content)
        .map_err(|error| FeedError::Parse(format!("FYERS {source} JSON parse failed: {error}")))?;

    parse_master_value(source, value)
}

#[derive(Clone, Debug, PartialEq)]
pub struct FyersUniverseSummary {
    pub selected_underlying: String,
    pub reference_symbol: String,
    pub reference_price: f64,
    pub spot_or_index_symbols: Vec<String>,
    pub futures: Vec<InstrumentDefinition>,
    pub atm_options: Vec<InstrumentDefinition>,
}

pub fn build_fyers_universe_summary_from_master_csvs(
    master_data_dir: impl AsRef<Path>,
    selection: &InstrumentSelection,
    underlying: &str,
    reference_symbol: &str,
    reference_price: f64,
) -> Result<FyersUniverseSummary, FeedError> {
    let lower_strike = reference_price * (1.0 - selection.strike_distance_pct / 100.0);
    let upper_strike = reference_price * (1.0 + selection.strike_distance_pct / 100.0);

    let mut matching_instruments = read_matching_fyers_master_instruments(
        master_data_dir,
        selection,
        underlying,
        lower_strike,
        upper_strike,
    )?;
    matching_instruments.sort_by(|left, right| left.trading_symbol.cmp(&right.trading_symbol));

    let mut spot_or_index_symbols = matching_instruments
        .iter()
        .filter(|instrument| instrument.instrument_type == InstrumentType::Spot)
        .map(|instrument| instrument.trading_symbol.clone())
        .collect::<Vec<_>>();
    if spot_or_index_symbols.is_empty() {
        spot_or_index_symbols.push(reference_symbol.to_string());
    }
    spot_or_index_symbols.sort();
    spot_or_index_symbols.dedup();

    let futures = matching_instruments
        .iter()
        .filter(|instrument| instrument.instrument_type == InstrumentType::Fut)
        .cloned()
        .collect::<Vec<_>>();
    let mut atm_options = matching_instruments
        .iter()
        .filter(|instrument| {
            matches!(
                instrument.instrument_type,
                InstrumentType::Call | InstrumentType::Put
            )
        })
        .filter(|instrument| {
            instrument
                .strike
                .is_some_and(|strike| strike >= lower_strike && strike <= upper_strike)
        })
        .cloned()
        .collect::<Vec<_>>();

    atm_options.sort_by(|left, right| {
        let left_distance = (left.strike.unwrap_or(f64::MAX) - reference_price).abs();
        let right_distance = (right.strike.unwrap_or(f64::MAX) - reference_price).abs();
        left_distance
            .total_cmp(&right_distance)
            .then_with(|| left.trading_symbol.cmp(&right.trading_symbol))
    });

    Ok(FyersUniverseSummary {
        selected_underlying: underlying.to_string(),
        reference_symbol: reference_symbol.to_string(),
        reference_price,
        spot_or_index_symbols,
        futures,
        atm_options,
    })
}

pub fn selected_trading_symbols(summary: &FyersUniverseSummary) -> BTreeSet<String> {
    summary
        .futures
        .iter()
        .chain(summary.atm_options.iter())
        .filter(|instrument| instrument.tradable)
        .map(|instrument| instrument.trading_symbol.clone())
        .collect()
}

pub fn write_fyers_derivatives_csv(
    summaries: &[FyersUniverseSummary],
    path: impl AsRef<Path>,
) -> Result<(), FeedError> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| FeedError::Config("invalid FYERS derivatives CSV path".to_string()))?;
    let tmp_path = path.with_file_name(format!("{file_name}.tmp"));
    let mut writer = BufWriter::new(File::create(&tmp_path)?);
    writeln!(writer, "{UNIVERSAL_INSTRUMENT_CSV_HEADER}")?;

    let mut rows = summaries
        .iter()
        .flat_map(|summary| summary.futures.iter().chain(summary.atm_options.iter()))
        .filter(|instrument| instrument.tradable)
        .map(InstrumentDefinition::to_csv_row)
        .collect::<Vec<_>>();
    rows.sort();
    rows.dedup();

    for row in rows {
        writeln!(writer, "{row}")?;
    }

    writer.flush()?;
    drop(writer);
    fs::rename(tmp_path, path)?;

    Ok(())
}

fn read_matching_fyers_master_instruments(
    master_data_dir: impl AsRef<Path>,
    selection: &InstrumentSelection,
    underlying: &str,
    lower_strike: f64,
    upper_strike: f64,
) -> Result<Vec<InstrumentDefinition>, FeedError> {
    let mut paths = fs::read_dir(master_data_dir.as_ref())?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "csv"))
        .collect::<Vec<_>>();
    paths.sort();

    let mut instruments = Vec::new();
    for path in paths {
        read_matching_fyers_master_csv(
            &path,
            selection,
            underlying,
            lower_strike,
            upper_strike,
            &mut instruments,
        )?;
    }

    Ok(instruments)
}

fn read_matching_fyers_master_csv(
    path: &Path,
    selection: &InstrumentSelection,
    underlying: &str,
    lower_strike: f64,
    upper_strike: f64,
    instruments: &mut Vec<InstrumentDefinition>,
) -> Result<(), FeedError> {
    let file = File::open(path)?;
    let mut lines = BufReader::new(file).lines();
    let header_line = lines.next().ok_or_else(|| {
        FeedError::Parse(format!("FYERS master CSV {} is empty", path.display()))
    })??;
    let headers = header_line
        .split(',')
        .map(|value| value.trim().to_string())
        .collect::<Vec<_>>();

    for (index, line) in lines.enumerate() {
        let line_number = index + 2;
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }

        let instrument = parse_universal_csv_row(&headers, &line, line_number, path)?;
        if instrument.underlying != underlying {
            continue;
        }
        if !instrument_type_allowed_definition(&instrument, &selection.instrument_types) {
            continue;
        }
        if matches!(
            instrument.instrument_type,
            InstrumentType::Call | InstrumentType::Put
        ) && !instrument
            .strike
            .is_some_and(|strike| strike >= lower_strike && strike <= upper_strike)
        {
            continue;
        }

        instruments.push(instrument);
    }

    Ok(())
}

fn parse_universal_csv_row(
    headers: &[String],
    line: &str,
    line_number: usize,
    path: &Path,
) -> Result<InstrumentDefinition, FeedError> {
    let values = line.split(',').map(str::trim).collect::<Vec<_>>();
    if values.len() != headers.len() {
        return Err(FeedError::Parse(format!(
            "FYERS master CSV {} line {line_number} has {} fields, expected {}",
            path.display(),
            values.len(),
            headers.len()
        )));
    }

    let get = |name: &str| -> Result<&str, FeedError> {
        headers
            .iter()
            .position(|header| header == name)
            .map(|position| values[position])
            .ok_or_else(|| FeedError::Parse(format!("missing FYERS master CSV column {name}")))
    };

    Ok(InstrumentDefinition {
        instrument_name: InstrumentName::new(get("instrument_name")?),
        instrument_type: parse_instrument_type(get("instrument_type")?)?,
        strike: parse_optional_csv_f64(get("strike")?)?,
        expiry: parse_optional_csv_string(get("expiry")?),
        broker: get("broker")?.to_string(),
        instrument_token: get("instrument_token")?.to_string(),
        trading_symbol: get("trading_symbol")?.to_string(),
        exchange: get("exchange")?.to_string(),
        segment: get("segment")?.to_string(),
        underlying: get("underlying")?.to_string(),
        lot_size: parse_required_csv_f64(get("lot_size")?)?,
        tick_size: parse_required_csv_f64(get("tick_size")?)?,
        tradable: parse_csv_bool(get("tradable")?)?,
    })
}

fn instrument_type_allowed_definition(
    instrument: &InstrumentDefinition,
    allowed_types: &[String],
) -> bool {
    allowed_types.is_empty()
        || allowed_types
            .iter()
            .any(|allowed_type| allowed_type == instrument.instrument_type.as_str())
}

fn parse_optional_csv_string(value: &str) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

fn parse_optional_csv_f64(value: &str) -> Result<Option<f64>, FeedError> {
    if value.is_empty() {
        Ok(None)
    } else {
        parse_required_csv_f64(value).map(Some)
    }
}

fn parse_required_csv_f64(value: &str) -> Result<f64, FeedError> {
    value
        .parse()
        .map_err(|error| FeedError::Parse(format!("invalid f64 {value}: {error}")))
}

fn parse_csv_bool(value: &str) -> Result<bool, FeedError> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(FeedError::Parse(format!("invalid bool {value}"))),
    }
}

fn parse_master_value(source: &str, value: Value) -> Result<Vec<InstrumentDefinition>, FeedError> {
    let object = value.as_object().ok_or_else(|| {
        FeedError::Parse("FYERS symbol master response must be a JSON object".to_string())
    })?;

    let mut instruments = Vec::new();
    for (symbol_key, item) in object {
        let fields = item.as_object().ok_or_else(|| {
            FeedError::Parse(format!(
                "FYERS symbol master item {symbol_key} is not an object"
            ))
        })?;

        if let Some(instrument) = parse_instrument(source, symbol_key, fields)? {
            instruments.push(instrument);
        }
    }

    instruments.sort_by(|left, right| {
        left.trading_symbol
            .cmp(&right.trading_symbol)
            .then_with(|| left.instrument_token.cmp(&right.instrument_token))
    });

    Ok(instruments)
}

fn parse_instrument(
    source: &str,
    symbol_key: &str,
    fields: &Map<String, Value>,
) -> Result<Option<InstrumentDefinition>, FeedError> {
    let trading_symbol = string_field(fields, "symTicker")
        .unwrap_or_else(|| symbol_key.to_string())
        .to_ascii_uppercase();
    let instrument_token = string_field(fields, "fyToken").ok_or_else(|| {
        FeedError::Parse(format!(
            "FYERS symbol master item {trading_symbol} is missing fyToken"
        ))
    })?;
    let instrument_type = fyers_instrument_type(fields, &trading_symbol);
    let Some(instrument_type) = instrument_type else {
        return Ok(None);
    };

    let exchange = string_field(fields, "exchangeName")
        .or_else(|| exchange_from_source(source))
        .or_else(|| i64_field(fields, "exchange").and_then(exchange_code_name))
        .unwrap_or_else(|| "FYERS".to_string())
        .to_ascii_uppercase();
    let underlying = string_field(fields, "underSym")
        .or_else(|| infer_underlying_from_symbol(&trading_symbol))
        .unwrap_or_else(|| trading_symbol.clone())
        .to_ascii_uppercase();
    let lot_size = f64_field(fields, "minLotSize").unwrap_or(1.0);
    let tick_size = f64_field(fields, "tickSize").unwrap_or(0.0);
    let tradable = i64_field(fields, "tradeStatus").unwrap_or(1) == 1;

    Ok(Some(InstrumentDefinition {
        instrument_name: InstrumentName::new(trading_symbol.clone()),
        instrument_type,
        strike: f64_field(fields, "strikePrice").filter(|strike| *strike > 0.0),
        expiry: expiry_field(fields, "expiryDate"),
        broker: "FYERS".to_string(),
        instrument_token,
        trading_symbol,
        exchange,
        segment: instrument_type.segment().to_string(),
        underlying,
        lot_size,
        tick_size,
        tradable,
    }))
}

fn is_fresh_for_epoch_day(path: &Path, today_epoch_day: u64) -> Result<bool, FeedError> {
    if !path.exists() {
        return Ok(false);
    }

    let modified_time = fs::metadata(path)?.modified()?;
    Ok(epoch_day(modified_time)? >= today_epoch_day)
}

fn count_csv_instruments(path: &Path) -> Result<usize, FeedError> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut count = 0usize;

    for line in reader.lines() {
        if !line?.trim().is_empty() {
            count += 1;
        }
    }

    Ok(count.saturating_sub(1))
}

fn current_epoch_day() -> Result<u64, FeedError> {
    epoch_day(SystemTime::now())
}

fn epoch_day(time: SystemTime) -> Result<u64, FeedError> {
    let duration = time.duration_since(UNIX_EPOCH).map_err(|error| {
        FeedError::Config(format!("system clock is before unix epoch: {error}"))
    })?;

    Ok(duration.as_secs() / 86_400)
}

fn normalize_source(source: &str) -> Result<String, FeedError> {
    let normalized = source.trim().to_ascii_uppercase();
    if normalized.is_empty()
        || !normalized
            .chars()
            .all(|value| value.is_ascii_alphanumeric() || value == '_' || value == '-')
    {
        return Err(FeedError::Config(format!(
            "invalid FYERS master source {source}"
        )));
    }

    Ok(normalized)
}

fn fyers_instrument_type(
    fields: &Map<String, Value>,
    trading_symbol: &str,
) -> Option<InstrumentType> {
    match string_field(fields, "optType")
        .unwrap_or_default()
        .to_ascii_uppercase()
        .as_str()
    {
        "CE" => return Some(InstrumentType::Call),
        "PE" => return Some(InstrumentType::Put),
        _ => {}
    }

    if trading_symbol.ends_with("CE") {
        return Some(InstrumentType::Call);
    }
    if trading_symbol.ends_with("PE") {
        return Some(InstrumentType::Put);
    }
    if trading_symbol.ends_with("FUT") {
        return Some(InstrumentType::Fut);
    }

    match i64_field(fields, "exInstType") {
        Some(11) => Some(InstrumentType::Fut),
        Some(0 | 10) | None => Some(InstrumentType::Spot),
        Some(_) => None,
    }
}

fn string_field(fields: &Map<String, Value>, name: &str) -> Option<String> {
    fields
        .get(name)
        .and_then(value_to_string)
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty() && value != "None")
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

fn f64_field(fields: &Map<String, Value>, name: &str) -> Option<f64> {
    fields.get(name).and_then(value_to_f64)
}

fn value_to_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(value) => value.as_f64(),
        Value::String(value) => value.trim().parse().ok(),
        _ => None,
    }
}

fn i64_field(fields: &Map<String, Value>, name: &str) -> Option<i64> {
    fields.get(name).and_then(value_to_i64)
}

fn value_to_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(value) => value
            .as_i64()
            .or_else(|| value.as_u64().map(|value| value as i64)),
        Value::String(value) => value.trim().parse().ok(),
        _ => None,
    }
}

fn expiry_field(fields: &Map<String, Value>, name: &str) -> Option<String> {
    fields.get(name).and_then(|value| match value {
        Value::Number(_) => value_to_i64(value).and_then(unix_seconds_to_yyyy_mm_dd),
        Value::String(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                trimmed
                    .parse()
                    .ok()
                    .and_then(unix_seconds_to_yyyy_mm_dd)
                    .or_else(|| Some(trimmed.to_string()))
            }
        }
        _ => None,
    })
}

fn unix_seconds_to_yyyy_mm_dd(seconds: i64) -> Option<String> {
    if seconds <= 0 {
        return None;
    }

    let days = seconds.div_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    Some(format!("{year:04}-{month:02}-{day:02}"))
}

fn civil_from_days(days_since_unix_epoch: i64) -> (i64, i64, i64) {
    let days = days_since_unix_epoch + 719_468;
    let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
    let day_of_era = days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    let year = year + if month <= 2 { 1 } else { 0 };

    (year, month, day)
}

fn exchange_from_source(source: &str) -> Option<String> {
    let source = source.to_ascii_uppercase();
    if source.starts_with("NSE_") {
        Some("NSE".to_string())
    } else if source.starts_with("BSE_") {
        Some("BSE".to_string())
    } else if source.starts_with("MCX_") {
        Some("MCX".to_string())
    } else {
        None
    }
}

fn exchange_code_name(code: i64) -> Option<String> {
    match code {
        10 => Some("NSE".to_string()),
        12 => Some("BSE".to_string()),
        _ => None,
    }
}

fn infer_underlying_from_symbol(trading_symbol: &str) -> Option<String> {
    let symbol = trading_symbol.split(':').nth(1).unwrap_or(trading_symbol);
    let symbol = symbol
        .split('-')
        .next()
        .unwrap_or(symbol)
        .trim_end_matches("FUT")
        .trim_end_matches("CE")
        .trim_end_matches("PE");

    if symbol.is_empty() {
        None
    } else {
        Some(symbol.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_fyers_master_json_to_universal_instruments() {
        let json = r#"{
            "NSE:NIFTY26MARFUT": {
                "fyToken": "101126033051714",
                "exInstType": 11,
                "tradeStatus": 1,
                "underSym": "NIFTY",
                "expiryDate": 1774864800,
                "optType": "XX",
                "strikePrice": -1.0,
                "minLotSize": 65,
                "tickSize": 0.1,
                "symTicker": "NSE:NIFTY26MARFUT",
                "exchangeName": "NSE"
            },
            "NSE:NIFTY26MAR25000CE": {
                "fyToken": "101126033000001",
                "exInstType": 14,
                "tradeStatus": 1,
                "underSym": "NIFTY",
                "expiryDate": "1774864800",
                "optType": "CE",
                "strikePrice": 25000.0,
                "minLotSize": 65,
                "tickSize": 0.05,
                "symTicker": "NSE:NIFTY26MAR25000CE",
                "exchangeName": "NSE"
            }
        }"#;

        let instruments = parse_master_json("NSE_FO", json).expect("parsed instruments");

        assert_eq!(instruments.len(), 2);
        assert_eq!(instruments[0].instrument_type, InstrumentType::Call);
        assert_eq!(instruments[0].expiry.as_deref(), Some("2026-03-30"));
        assert_eq!(instruments[0].strike, Some(25_000.0));
        assert_eq!(instruments[0].segment, "CALL");
        assert_eq!(instruments[1].instrument_type, InstrumentType::Fut);
        assert_eq!(instruments[1].lot_size, 65.0);
    }

    #[test]
    fn selects_fyers_derivative_universe_from_master_csvs() {
        let temp_dir =
            std::env::temp_dir().join(format!("dhancred-fyers-master-test-{}", std::process::id()));
        let master_dir = temp_dir.join("master");
        fs::create_dir_all(&master_dir).expect("created test master dir");
        let master_csv = master_dir.join("NSE_FO.csv");
        fs::write(
            &master_csv,
            [
                UNIVERSAL_INSTRUMENT_CSV_HEADER,
                "NIFTY,SPOT,,,FYERS,101000000026000,NSE:NIFTY50-INDEX,NSE,SPOT,NIFTY,1,0.05,true",
                "NIFTY26APRFUT,FUT,,2026-04-30,FYERS,101126043051714,NSE:NIFTY26APRFUT,NSE,FUT,NIFTY,65,0.05,true",
                "NIFTY26APR25000CE,CALL,25000,2026-04-30,FYERS,101126043000001,NSE:NIFTY26APR25000CE,NSE,CALL,NIFTY,65,0.05,true",
                "NIFTY26APR25000PE,PUT,25000,2026-04-30,FYERS,101126043000002,NSE:NIFTY26APR25000PE,NSE,PUT,NIFTY,65,0.05,true",
                "NIFTY26APR25600CE,CALL,25600,2026-04-30,FYERS,101126043000003,NSE:NIFTY26APR25600CE,NSE,CALL,NIFTY,65,0.05,true",
                "BANKNIFTY26APR25000CE,CALL,25000,2026-04-30,FYERS,101126043000004,NSE:BANKNIFTY26APR25000CE,NSE,CALL,BANKNIFTY,35,0.05,true",
            ]
            .join("\n"),
        )
        .expect("wrote test master CSV");

        let selection = InstrumentSelection {
            instrument_types: vec!["FUT".to_string(), "CALL".to_string(), "PUT".to_string()],
            strike_distance_pct: 2.0,
            refresh_trigger_pct: 1.5,
        };
        let summary = build_fyers_universe_summary_from_master_csvs(
            &master_dir,
            &selection,
            "NIFTY",
            "NSE:NIFTY50-INDEX",
            25_000.0,
        )
        .expect("built universe summary");
        let selected = selected_trading_symbols(&summary);

        assert_eq!(summary.futures.len(), 1);
        assert_eq!(summary.atm_options.len(), 2);
        assert!(selected.contains("NSE:NIFTY26APRFUT"));
        assert!(selected.contains("NSE:NIFTY26APR25000CE"));
        assert!(selected.contains("NSE:NIFTY26APR25000PE"));
        assert!(!selected.contains("NSE:NIFTY26APR25600CE"));
        assert!(!selected.contains("NSE:BANKNIFTY26APR25000CE"));

        fs::remove_dir_all(temp_dir).expect("removed test master dir");
    }
}
