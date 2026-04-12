use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use reqwest::blocking::Client;
use reqwest::header::{ACCEPT, HeaderMap, HeaderValue, USER_AGENT};
use serde::Deserialize;
use serde_json::Value;

use crate::config::InstrumentSelection;
use crate::feeder::{
    FeedError, InstrumentDefinition, InstrumentName, InstrumentType,
    UNIVERSAL_INSTRUMENT_CSV_HEADER, parse_instrument_type,
};

const PRODUCTS_PAGE_SIZE: usize = 500;
const DAY_SECONDS: u64 = 86_400;
const DELTA_OPTION_SETTLEMENT_UTC_SECONDS: u64 = 12 * 60 * 60;

#[derive(Clone, Debug)]
pub struct DeltaProductClient {
    rest_url: String,
    client: Client,
}

impl DeltaProductClient {
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
                .expect("valid delta http client"),
        }
    }

    pub fn fetch_live_products(&self) -> Result<Vec<DeltaProduct>, FeedError> {
        let mut products = Vec::new();
        let mut after: Option<String> = None;

        loop {
            let mut request = self
                .client
                .get(format!("{}/v2/products", self.rest_url))
                .query(&[
                    ("states", "live".to_string()),
                    ("page_size", PRODUCTS_PAGE_SIZE.to_string()),
                ]);

            if let Some(cursor) = after.as_ref() {
                request = request.query(&[("after", cursor)]);
            }

            let response = request.send()?.error_for_status()?;
            let page: DeltaProductsResponse = response.json()?;

            if !page.success {
                return Err(FeedError::Http(
                    "Delta products API returned success=false".to_string(),
                ));
            }

            products.extend(page.result);

            after = page.meta.and_then(|meta| meta.after);
            if after.as_deref().is_none_or(str::is_empty) {
                break;
            }
        }

        Ok(products)
    }

    pub fn write_live_products_csv(&self, path: impl AsRef<Path>) -> Result<usize, FeedError> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let tmp_path = path.with_extension("csv.tmp");
        let mut writer = BufWriter::new(File::create(&tmp_path)?);
        writeln!(writer, "{UNIVERSAL_INSTRUMENT_CSV_HEADER}")?;

        let mut count = 0usize;
        let mut after: Option<String> = None;
        loop {
            let mut request = self
                .client
                .get(format!("{}/v2/products", self.rest_url))
                .query(&[
                    ("states", "live".to_string()),
                    ("page_size", PRODUCTS_PAGE_SIZE.to_string()),
                ]);

            if let Some(cursor) = after.as_ref() {
                request = request.query(&[("after", cursor)]);
            }

            let response = request.send()?.error_for_status()?;
            let page: DeltaProductsResponse = response.json()?;

            if !page.success {
                return Err(FeedError::Http(
                    "Delta products API returned success=false".to_string(),
                ));
            }

            for product in page.result {
                if let Some(instrument) = delta_product_to_instrument_definition(&product) {
                    writeln!(writer, "{}", instrument.to_csv_row())?;
                    count += 1;
                }
            }

            after = page.meta.and_then(|meta| meta.after);
            if after.as_deref().is_none_or(str::is_empty) {
                break;
            }
        }

        writer.flush()?;
        drop(writer);
        fs::rename(&tmp_path, path)?;

        Ok(count)
    }

    pub fn fetch_ticker(&self, symbol: &str) -> Result<DeltaTicker, FeedError> {
        let response = self
            .client
            .get(format!("{}/v2/tickers/{}", self.rest_url, symbol))
            .send()?
            .error_for_status()?;
        let ticker: DeltaTickerResponse = response.json()?;

        if !ticker.success {
            return Err(FeedError::Http(format!(
                "Delta ticker API returned success=false for {symbol}"
            )));
        }

        Ok(ticker.result)
    }
}

#[derive(Clone, Debug, Deserialize)]
struct DeltaProductsResponse {
    success: bool,
    result: Vec<DeltaProduct>,
    meta: Option<DeltaPaginationMeta>,
}

#[derive(Clone, Debug, Deserialize)]
struct DeltaTickerResponse {
    success: bool,
    result: DeltaTicker,
}

#[derive(Clone, Debug, Deserialize)]
struct DeltaPaginationMeta {
    after: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct DeltaProduct {
    pub id: u64,
    pub symbol: String,
    pub description: Option<String>,
    pub settlement_time: Option<String>,
    pub tick_size: Option<String>,
    pub contract_value: Option<String>,
    pub contract_unit_currency: Option<String>,
    pub state: Option<String>,
    pub trading_status: Option<String>,
    pub contract_type: String,
    pub strike_price: Option<String>,
    pub underlying_asset: Option<DeltaAsset>,
    pub quoting_asset: Option<DeltaAsset>,
    pub settling_asset: Option<DeltaAsset>,
    pub spot_index: Option<DeltaSpotIndex>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

impl DeltaProduct {
    pub fn instrument_type(&self) -> Option<InstrumentType> {
        match self.contract_type.as_str() {
            "spot" => Some(InstrumentType::Spot),
            "futures" => Some(InstrumentType::Fut),
            "perpetual_futures" => Some(InstrumentType::PerpFut),
            "call_options" => Some(InstrumentType::Call),
            "put_options" => Some(InstrumentType::Put),
            _ => None,
        }
    }

    pub fn underlying(&self) -> Option<String> {
        self.contract_unit_currency
            .clone()
            .or_else(|| parse_underlying_from_symbol(&self.symbol))
    }

    pub fn strike(&self) -> Option<f64> {
        self.strike_price
            .as_deref()
            .and_then(parse_decimal)
            .or_else(|| parse_strike_from_symbol(&self.symbol))
    }

    pub fn expiry_ddmmyy(&self) -> Option<String> {
        parse_expiry_from_symbol(&self.symbol)
    }

    pub fn expiry_yyyy_mm_dd(&self) -> Option<String> {
        self.settlement_time
            .as_deref()
            .and_then(|value| value.get(0..10))
            .map(str::to_string)
            .or_else(|| self.expiry_ddmmyy().and_then(ddmmyy_to_yyyy_mm_dd))
    }

    pub fn expiry_display(&self) -> String {
        self.expiry_yyyy_mm_dd()
            .or_else(|| self.settlement_time.clone())
            .unwrap_or_else(|| "-".to_string())
    }

    pub fn is_option(&self) -> bool {
        matches!(
            self.instrument_type(),
            Some(InstrumentType::Call | InstrumentType::Put)
        )
    }

    pub fn is_future_like(&self) -> bool {
        matches!(
            self.instrument_type(),
            Some(InstrumentType::Fut | InstrumentType::PerpFut)
        )
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct DeltaAsset {
    pub id: u64,
    pub symbol: String,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct DeltaSpotIndex {
    pub id: u64,
    pub symbol: String,
    pub tick_size: Option<String>,
    pub index_type: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct DeltaTicker {
    pub symbol: String,
    pub spot_price: Option<String>,
    pub mark_price: Option<String>,
    pub close: Option<Value>,
    pub timestamp: Option<u64>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

impl DeltaTicker {
    pub fn reference_price(&self) -> Option<f64> {
        self.spot_price
            .as_deref()
            .and_then(parse_decimal)
            .or_else(|| self.mark_price.as_deref().and_then(parse_decimal))
            .or_else(|| self.close.as_ref().and_then(parse_json_number))
    }
}

#[derive(Clone, Debug)]
pub struct DeltaUniverseSummary {
    pub selected_underlying: String,
    pub reference_symbol: String,
    pub reference_price: f64,
    pub spot_or_index_symbols: Vec<String>,
    pub futures: Vec<InstrumentDefinition>,
    pub atm_options: Vec<InstrumentDefinition>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeltaSpotReference {
    pub underlying: String,
    pub spot_symbol: String,
}

pub fn write_delta_derivatives_csv(
    summaries: &[DeltaUniverseSummary],
    path: impl AsRef<Path>,
) -> Result<(), FeedError> {
    let mut rows = Vec::new();
    for summary in summaries {
        rows.extend(summary.futures.iter().map(InstrumentDefinition::to_csv_row));
        rows.extend(
            summary
                .atm_options
                .iter()
                .map(InstrumentDefinition::to_csv_row),
        );
    }

    write_instrument_rows_csv(path, rows)
}

pub fn ensure_delta_master_csv(
    client: &DeltaProductClient,
    path: impl AsRef<Path>,
) -> Result<(), FeedError> {
    ensure_delta_master_csv_with_logging(client, path, true)
}

pub fn ensure_delta_master_csv_with_logging(
    client: &DeltaProductClient,
    path: impl AsRef<Path>,
    log_to_console: bool,
) -> Result<(), FeedError> {
    let path = path.as_ref();
    let today_epoch_day = current_epoch_day()?;
    if is_fresh_for_epoch_day(path, today_epoch_day)? {
        if log_to_console {
            println!(
                "Delta product master cached: {} instruments | {}",
                count_csv_instruments(path)?,
                path.display()
            );
        }
        return Ok(());
    }

    if log_to_console {
        println!("Delta product master downloading");
    }
    let instrument_count = client.write_live_products_csv(path)?;
    if log_to_console {
        println!(
            "Delta product master downloaded: {} instruments | {}",
            instrument_count,
            path.display()
        );
    }

    Ok(())
}

pub fn write_delta_master_csv(
    path: impl AsRef<Path>,
    instruments: &[InstrumentDefinition],
) -> Result<(), FeedError> {
    write_instrument_rows_csv(
        path,
        instruments
            .iter()
            .map(InstrumentDefinition::to_csv_row)
            .collect(),
    )
}

fn write_instrument_rows_csv(path: impl AsRef<Path>, rows: Vec<String>) -> Result<(), FeedError> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let tmp_path = path.with_extension("csv.tmp");
    let mut all_rows = Vec::with_capacity(rows.len() + 1);
    all_rows.push(UNIVERSAL_INSTRUMENT_CSV_HEADER.to_string());
    all_rows.extend(rows);

    fs::write(&tmp_path, format!("{}\n", all_rows.join("\n")))?;
    fs::rename(&tmp_path, path)?;

    Ok(())
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

fn is_fresh_for_epoch_day(path: &Path, today_epoch_day: u64) -> Result<bool, FeedError> {
    if !path.exists() {
        return Ok(false);
    }

    let modified_time = fs::metadata(path)?.modified()?;
    Ok(epoch_day(modified_time)? >= today_epoch_day)
}

fn current_epoch_day() -> Result<u64, FeedError> {
    epoch_day(SystemTime::now())
}

fn current_unix_seconds() -> Result<u64, FeedError> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| FeedError::Config(format!("system clock is before unix epoch: {error}")))?
        .as_secs())
}

fn epoch_day(time: SystemTime) -> Result<u64, FeedError> {
    let duration = time.duration_since(UNIX_EPOCH).map_err(|error| {
        FeedError::Config(format!("system clock is before unix epoch: {error}"))
    })?;

    Ok(duration.as_secs() / 86_400)
}

pub fn build_delta_universe_summary_from_master_csv(
    path: impl AsRef<Path>,
    selection: &InstrumentSelection,
    underlying: &str,
    reference_symbol: &str,
    reference_price: f64,
) -> Result<DeltaUniverseSummary, FeedError> {
    let lower_strike = reference_price * (1.0 - selection.strike_distance_pct / 100.0);
    let upper_strike = reference_price * (1.0 + selection.strike_distance_pct / 100.0);
    let now_unix_seconds = current_unix_seconds()?;

    let mut matching_instruments = read_matching_delta_master_instruments(
        path,
        selection,
        underlying,
        lower_strike,
        upper_strike,
        now_unix_seconds,
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
        .filter(|instrument| {
            matches!(
                instrument.instrument_type,
                InstrumentType::Fut | InstrumentType::PerpFut
            )
        })
        .cloned()
        .collect();
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
        let left_distance = left.strike.unwrap_or(f64::MAX).abs() - reference_price;
        let right_distance = right.strike.unwrap_or(f64::MAX).abs() - reference_price;
        left_distance
            .abs()
            .total_cmp(&right_distance.abs())
            .then_with(|| left.trading_symbol.cmp(&right.trading_symbol))
    });

    Ok(DeltaUniverseSummary {
        selected_underlying: underlying.to_string(),
        reference_symbol: reference_symbol.to_string(),
        reference_price,
        spot_or_index_symbols,
        futures,
        atm_options,
    })
}

fn read_matching_delta_master_instruments(
    path: impl AsRef<Path>,
    selection: &InstrumentSelection,
    underlying: &str,
    lower_strike: f64,
    upper_strike: f64,
    now_unix_seconds: u64,
) -> Result<Vec<InstrumentDefinition>, FeedError> {
    let file = File::open(path)?;
    let mut lines = BufReader::new(file).lines();
    let header_line = lines
        .next()
        .ok_or_else(|| FeedError::Parse("Delta master CSV is empty".to_string()))??;
    let headers = header_line
        .split(',')
        .map(|value| value.trim().to_string())
        .collect::<Vec<_>>();

    let mut instruments = Vec::new();
    for (index, line) in lines.enumerate() {
        let line_number = index + 2;
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }

        let instrument = parse_master_csv_row(&headers, &line, line_number)?;
        if instrument.underlying != underlying {
            continue;
        }
        if !delta_instrument_active_at(&instrument, now_unix_seconds) {
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

    Ok(instruments)
}

fn parse_master_csv_row(
    headers: &[String],
    line: &str,
    line_number: usize,
) -> Result<InstrumentDefinition, FeedError> {
    let values = line.split(',').map(str::trim).collect::<Vec<_>>();
    if values.len() != headers.len() {
        return Err(FeedError::Parse(format!(
            "Delta master CSV line {line_number} has {} fields, expected {}",
            values.len(),
            headers.len()
        )));
    }

    let get = |name: &str| -> Result<&str, FeedError> {
        headers
            .iter()
            .position(|header| header == name)
            .map(|position| values[position])
            .ok_or_else(|| FeedError::Parse(format!("missing Delta master CSV column {name}")))
    };

    Ok(InstrumentDefinition {
        instrument_name: InstrumentName::new(get("instrument_name")?),
        instrument_type: parse_instrument_type(get("instrument_type")?)?,
        strike: parse_optional_f64(get("strike")?)?,
        expiry: parse_optional_string(get("expiry")?),
        broker: get("broker")?.to_string(),
        instrument_token: get("instrument_token")?.to_string(),
        trading_symbol: get("trading_symbol")?.to_string(),
        exchange: get("exchange")?.to_string(),
        segment: get("segment")?.to_string(),
        underlying: get("underlying")?.to_string(),
        lot_size: parse_required_f64(get("lot_size")?)?,
        tick_size: parse_required_f64(get("tick_size")?)?,
        tradable: parse_bool(get("tradable")?)?,
    })
}

fn parse_optional_string(value: &str) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

fn parse_optional_f64(value: &str) -> Result<Option<f64>, FeedError> {
    if value.is_empty() {
        Ok(None)
    } else {
        parse_required_f64(value).map(Some)
    }
}

fn parse_required_f64(value: &str) -> Result<f64, FeedError> {
    value
        .parse()
        .map_err(|error| FeedError::Parse(format!("invalid f64 {value}: {error}")))
}

fn parse_bool(value: &str) -> Result<bool, FeedError> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(FeedError::Parse(format!("invalid bool {value}"))),
    }
}

fn delta_instrument_active_at(instrument: &InstrumentDefinition, now_unix_seconds: u64) -> bool {
    if !matches!(
        instrument.instrument_type,
        InstrumentType::Call | InstrumentType::Put
    ) {
        return true;
    }

    delta_expiry_active_at(instrument.expiry.as_deref(), now_unix_seconds)
}

fn delta_product_active_at(product: &DeltaProduct, now_unix_seconds: u64) -> bool {
    if !product.is_option() {
        return true;
    }

    delta_expiry_active_at(product.expiry_yyyy_mm_dd().as_deref(), now_unix_seconds)
}

fn delta_expiry_active_at(expiry: Option<&str>, now_unix_seconds: u64) -> bool {
    let Some(expiry_day) = expiry.and_then(yyyy_mm_dd_to_epoch_day) else {
        return true;
    };

    now_unix_seconds < expiry_day * DAY_SECONDS + DELTA_OPTION_SETTLEMENT_UTC_SECONDS
}

pub fn selected_trading_symbols(summary: &DeltaUniverseSummary) -> BTreeSet<String> {
    summary
        .futures
        .iter()
        .chain(summary.atm_options.iter())
        .filter(|instrument| instrument.tradable)
        .map(|instrument| instrument.trading_symbol.clone())
        .collect()
}

pub fn resolve_delta_spot_references(
    products: &[DeltaProduct],
    underlyings: &[String],
) -> Result<Vec<DeltaSpotReference>, FeedError> {
    let mut references = Vec::new();

    for underlying in underlyings {
        let mut spot_symbols: Vec<String> = products
            .iter()
            .filter(|product| product.underlying().as_deref() == Some(underlying.as_str()))
            .filter_map(|product| product.spot_index.as_ref().map(|spot| spot.symbol.clone()))
            .collect();

        spot_symbols.sort();
        spot_symbols.dedup();

        let spot_symbol = preferred_spot_index_symbol(&spot_symbols).ok_or_else(|| {
            FeedError::Config(format!(
                "could not resolve Delta spot/index symbol for underlying {underlying}"
            ))
        })?;

        references.push(DeltaSpotReference {
            underlying: underlying.clone(),
            spot_symbol,
        });
    }

    Ok(references)
}

fn preferred_spot_index_symbol(symbols: &[String]) -> Option<String> {
    symbols
        .iter()
        .find(|symbol| symbol.ends_with("USD"))
        .or_else(|| symbols.iter().find(|symbol| symbol.ends_with("USDT")))
        .or_else(|| symbols.first())
        .cloned()
}

pub fn build_delta_universe_summary(
    products: &[DeltaProduct],
    selection: &InstrumentSelection,
    underlying: &str,
    reference_symbol: &str,
    reference_price: f64,
) -> Result<DeltaUniverseSummary, FeedError> {
    let lower_strike = reference_price * (1.0 - selection.strike_distance_pct / 100.0);
    let upper_strike = reference_price * (1.0 + selection.strike_distance_pct / 100.0);
    let now_unix_seconds = current_unix_seconds()?;

    let mut matching_products: Vec<DeltaProduct> = products
        .iter()
        .filter(|product| product.underlying().as_deref() == Some(underlying))
        .filter(|product| delta_product_active_at(product, now_unix_seconds))
        .filter(|product| instrument_type_allowed(product, &selection.instrument_types))
        .cloned()
        .collect();

    matching_products.sort_by(|left, right| left.symbol.cmp(&right.symbol));

    let mut spot_or_index_symbols: Vec<String> = matching_products
        .iter()
        .filter_map(|product| product.spot_index.as_ref().map(|spot| spot.symbol.clone()))
        .collect();
    spot_or_index_symbols.sort();
    spot_or_index_symbols.dedup();

    let futures = matching_products
        .iter()
        .filter(|product| product.is_future_like())
        .filter_map(delta_product_to_instrument_definition)
        .collect();
    let mut atm_options: Vec<DeltaProduct> = matching_products
        .iter()
        .filter(|product| product.is_option())
        .filter(|product| {
            product
                .strike()
                .is_some_and(|strike| strike >= lower_strike && strike <= upper_strike)
        })
        .cloned()
        .collect();

    atm_options.sort_by(|left, right| {
        let left_distance = left.strike().unwrap_or(f64::MAX).abs() - reference_price;
        let right_distance = right.strike().unwrap_or(f64::MAX).abs() - reference_price;
        left_distance
            .abs()
            .total_cmp(&right_distance.abs())
            .then_with(|| left.symbol.cmp(&right.symbol))
    });

    Ok(DeltaUniverseSummary {
        selected_underlying: underlying.to_string(),
        reference_symbol: reference_symbol.to_string(),
        reference_price,
        spot_or_index_symbols,
        futures,
        atm_options: atm_options
            .iter()
            .filter_map(delta_product_to_instrument_definition)
            .collect(),
    })
}

pub fn print_delta_universe_summary(summary: &DeltaUniverseSummary) {
    println!("Delta product master loaded");
    println!(
        "Underlying: {} | reference: {} @ {:.4}",
        summary.selected_underlying, summary.reference_symbol, summary.reference_price
    );
    println!();

    println!("Spot/index references:");
    if summary.spot_or_index_symbols.is_empty() {
        println!("  none found in product master");
    } else {
        for symbol in &summary.spot_or_index_symbols {
            println!("  {symbol}");
        }
    }
    println!();

    println!("Futures/perpetual instruments:");
    if summary.futures.is_empty() {
        println!("  none found");
    } else {
        for product in &summary.futures {
            println!(
                "  {} | type={} | tradable={} | tick_size={} | expiry={}",
                product.trading_symbol,
                product.instrument_type.as_str(),
                product.tradable,
                product.tick_size,
                product.expiry.as_deref().unwrap_or("-")
            );
        }
    }
    println!();

    println!(
        "ATM option instruments: {} within configured strike distance; showing nearest strike by expiry",
        summary.atm_options.len()
    );
    if summary.atm_options.is_empty() {
        println!("  none found within configured strike_distance_pct");
    } else {
        for (expiry, products) in nearest_options_by_expiry(summary) {
            println!("  expiry={expiry}");
            for product in products {
                println!(
                    "    {} | type={} | strike={} | status={}",
                    product.trading_symbol,
                    product.instrument_type.as_str(),
                    product
                        .strike
                        .map(|strike| format!("{strike:.4}"))
                        .unwrap_or_else(|| "-".to_string()),
                    if product.tradable {
                        "tradable"
                    } else {
                        "not_tradable"
                    },
                );
            }
        }
    }
}

fn nearest_options_by_expiry(
    summary: &DeltaUniverseSummary,
) -> BTreeMap<String, Vec<&InstrumentDefinition>> {
    let mut by_expiry: BTreeMap<String, Vec<&InstrumentDefinition>> = BTreeMap::new();

    for product in &summary.atm_options {
        by_expiry
            .entry(product.expiry.clone().unwrap_or_else(|| "-".to_string()))
            .or_default()
            .push(product);
    }

    by_expiry
        .into_iter()
        .filter_map(|(expiry, products)| {
            let nearest_strike = products
                .iter()
                .filter_map(|product| product.strike)
                .min_by(|left, right| {
                    (left - summary.reference_price)
                        .abs()
                        .total_cmp(&(right - summary.reference_price).abs())
                })?;
            let mut nearest_products: Vec<&InstrumentDefinition> = products
                .into_iter()
                .filter(|product| product.strike == Some(nearest_strike))
                .collect();

            nearest_products.sort_by(|left, right| left.trading_symbol.cmp(&right.trading_symbol));
            Some((expiry, nearest_products))
        })
        .collect()
}

fn delta_product_to_instrument_definition(product: &DeltaProduct) -> Option<InstrumentDefinition> {
    let instrument_type = product.instrument_type()?;
    let underlying = product.underlying()?;
    let tick_size = product
        .tick_size
        .as_deref()
        .and_then(parse_decimal)
        .unwrap_or(0.0);
    let lot_size = product
        .contract_value
        .as_deref()
        .and_then(parse_decimal)
        .unwrap_or(1.0);

    Some(InstrumentDefinition {
        instrument_name: InstrumentName::new(product.symbol.clone()),
        instrument_type,
        strike: product.strike(),
        expiry: product.expiry_yyyy_mm_dd(),
        broker: "DELTA".to_string(),
        instrument_token: product.id.to_string(),
        trading_symbol: product.symbol.clone(),
        exchange: "DELTA".to_string(),
        segment: instrument_type.segment().to_string(),
        underlying,
        lot_size,
        tick_size,
        tradable: product.trading_status.as_deref() == Some("operational")
            && product.state.as_deref() == Some("live"),
    })
}

fn instrument_type_allowed(product: &DeltaProduct, allowed_types: &[String]) -> bool {
    if allowed_types.is_empty() {
        return true;
    }

    let Some(instrument_type) = product.instrument_type() else {
        return false;
    };

    allowed_types
        .iter()
        .any(|allowed| allowed.eq_ignore_ascii_case(instrument_type.as_str()))
}

fn instrument_type_allowed_definition(
    instrument: &InstrumentDefinition,
    allowed_types: &[String],
) -> bool {
    allowed_types.is_empty()
        || allowed_types
            .iter()
            .any(|allowed| allowed.eq_ignore_ascii_case(instrument.instrument_type.as_str()))
}

fn parse_underlying_from_symbol(symbol: &str) -> Option<String> {
    if symbol.starts_with("C-") || symbol.starts_with("P-") {
        return symbol.split('-').nth(1).map(str::to_string);
    }

    ["BTC", "ETH", "SOL", "XRP", "BNB", "USDT"]
        .iter()
        .find(|underlying| symbol.starts_with(**underlying))
        .map(|underlying| (*underlying).to_string())
}

fn parse_strike_from_symbol(symbol: &str) -> Option<f64> {
    if !(symbol.starts_with("C-") || symbol.starts_with("P-")) {
        return None;
    }

    symbol.split('-').nth(2).and_then(parse_decimal)
}

fn parse_expiry_from_symbol(symbol: &str) -> Option<String> {
    if !(symbol.starts_with("C-") || symbol.starts_with("P-")) {
        return None;
    }

    symbol.split('-').nth(3).map(str::to_string)
}

fn ddmmyy_to_yyyy_mm_dd(value: String) -> Option<String> {
    if value.len() != 6 {
        return None;
    }

    let day = value.get(0..2)?;
    let month = value.get(2..4)?;
    let year = value.get(4..6)?;

    Some(format!("20{year}-{month}-{day}"))
}

fn yyyy_mm_dd_to_epoch_day(value: &str) -> Option<u64> {
    if value.len() != 10 {
        return None;
    }

    let year = value.get(0..4)?.parse().ok()?;
    let month = value.get(5..7)?.parse().ok()?;
    let day = value.get(8..10)?.parse().ok()?;
    days_from_civil(year, month, day).try_into().ok()
}

fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let month_prime = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * month_prime + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn parse_decimal(value: &str) -> Option<f64> {
    value.trim().parse().ok()
}

fn parse_json_number(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(parse_decimal))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_delta_option_symbol_parts() {
        let product = DeltaProduct {
            id: 1,
            symbol: "C-BTC-90000-260426".to_string(),
            description: None,
            settlement_time: None,
            tick_size: None,
            contract_value: None,
            contract_unit_currency: None,
            state: Some("live".to_string()),
            trading_status: Some("operational".to_string()),
            contract_type: "call_options".to_string(),
            strike_price: None,
            underlying_asset: None,
            quoting_asset: None,
            settling_asset: None,
            spot_index: None,
            extra: BTreeMap::new(),
        };

        assert_eq!(product.underlying().as_deref(), Some("BTC"));
        assert_eq!(product.strike(), Some(90_000.0));
        assert_eq!(product.expiry_ddmmyy().as_deref(), Some("260426"));
        assert_eq!(product.expiry_yyyy_mm_dd().as_deref(), Some("2026-04-26"));
    }

    #[test]
    fn prefers_usd_spot_index_when_multiple_indices_exist() {
        let products = vec![
            test_product_with_spot_index(1, "BTCINR", ".DEXBTINR"),
            test_product_with_spot_index(2, "BTCUSD", ".DEXBTUSD"),
        ];
        let references = resolve_delta_spot_references(&products, &["BTC".to_string()]).unwrap();

        assert_eq!(
            references,
            vec![DeltaSpotReference {
                underlying: "BTC".to_string(),
                spot_symbol: ".DEXBTUSD".to_string(),
            }]
        );
    }

    #[test]
    fn delta_options_expire_at_12_utc_on_expiry_date() {
        let option = InstrumentDefinition {
            instrument_name: InstrumentName::new("C-BTC-70800-120426"),
            instrument_type: InstrumentType::Call,
            strike: Some(70_800.0),
            expiry: Some("2026-04-12".to_string()),
            broker: "DELTA".to_string(),
            instrument_token: "130262".to_string(),
            trading_symbol: "C-BTC-70800-120426".to_string(),
            exchange: "DELTA".to_string(),
            segment: "CALL".to_string(),
            underlying: "BTC".to_string(),
            lot_size: 0.001,
            tick_size: 0.1,
            tradable: true,
        };
        let expiry_day = yyyy_mm_dd_to_epoch_day("2026-04-12").expect("expiry day");
        let before_settlement = expiry_day * DAY_SECONDS + DELTA_OPTION_SETTLEMENT_UTC_SECONDS - 1;
        let at_settlement = expiry_day * DAY_SECONDS + DELTA_OPTION_SETTLEMENT_UTC_SECONDS;

        assert!(delta_instrument_active_at(&option, before_settlement));
        assert!(!delta_instrument_active_at(&option, at_settlement));
    }

    fn test_product_with_spot_index(id: u64, symbol: &str, spot_symbol: &str) -> DeltaProduct {
        DeltaProduct {
            id,
            symbol: symbol.to_string(),
            description: None,
            settlement_time: None,
            tick_size: None,
            contract_value: None,
            contract_unit_currency: Some("BTC".to_string()),
            state: Some("live".to_string()),
            trading_status: Some("operational".to_string()),
            contract_type: "perpetual_futures".to_string(),
            strike_price: None,
            underlying_asset: None,
            quoting_asset: None,
            settling_asset: None,
            spot_index: Some(DeltaSpotIndex {
                id,
                symbol: spot_symbol.to_string(),
                tick_size: None,
                index_type: Some("spot_pair".to_string()),
                extra: BTreeMap::new(),
            }),
            extra: BTreeMap::new(),
        }
    }
}
