use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::Path;

use chrono::{FixedOffset, NaiveDate, Utc};

use crate::config::{DbinternationalBrokerSection, InstrumentSelection};
use crate::feeder::{
    FeedError, InstrumentDefinition, InstrumentName, InstrumentType,
    UNIVERSAL_INSTRUMENT_CSV_HEADER, parse_instrument_type,
};

const BROKER: &str = "DBINTERNATIONAL";
const IST_OFFSET_SECONDS: i32 = 5 * 60 * 60 + 30 * 60;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DbinternationalSpotReference {
    pub underlying: String,
    pub spot_symbol: String,
}

#[derive(Clone, Debug, Default)]
pub struct DbinternationalUniverseCatalog {
    spot_references: Vec<DbinternationalSpotReference>,
    derivatives_by_underlying: BTreeMap<String, Vec<InstrumentDefinition>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DbinternationalUniverseSummary {
    pub selected_underlying: String,
    pub reference_symbol: String,
    pub reference_price: f64,
    pub futures: Vec<InstrumentDefinition>,
    pub atm_options: Vec<InstrumentDefinition>,
}

impl DbinternationalUniverseCatalog {
    pub fn load(config: &DbinternationalBrokerSection) -> Result<Self, FeedError> {
        let master_content =
            fs::read_to_string(&config.market_data_master_file).map_err(|error| {
                FeedError::Config(format!(
                    "failed to read DBInternational master {}: {error}",
                    config.market_data_master_file
                ))
            })?;
        let index_content =
            fs::read_to_string(&config.market_data_index_file).map_err(|error| {
                FeedError::Config(format!(
                    "failed to read DBInternational index master {}: {error}",
                    config.market_data_index_file
                ))
            })?;

        parse_catalog_contents(&master_content, &index_content)
    }

    pub fn spot_references(&self) -> &[DbinternationalSpotReference] {
        &self.spot_references
    }

    pub fn build_summary(
        &self,
        selection: &InstrumentSelection,
        underlying: &str,
        reference_symbol: &str,
        reference_price: f64,
    ) -> Result<DbinternationalUniverseSummary, FeedError> {
        self.build_summary_for_date(
            selection,
            underlying,
            reference_symbol,
            reference_price,
            &current_ist_date(),
        )
    }

    fn build_summary_for_date(
        &self,
        selection: &InstrumentSelection,
        underlying: &str,
        reference_symbol: &str,
        reference_price: f64,
        today: &str,
    ) -> Result<DbinternationalUniverseSummary, FeedError> {
        let Some(instruments) = self.derivatives_by_underlying.get(underlying) else {
            return Ok(DbinternationalUniverseSummary {
                selected_underlying: underlying.to_string(),
                reference_symbol: reference_symbol.to_string(),
                reference_price,
                futures: Vec::new(),
                atm_options: Vec::new(),
            });
        };

        let allowed_types = selection_types(selection)?;
        let lower_strike = reference_price * (1.0 - selection.strike_distance_pct / 100.0);
        let upper_strike = reference_price * (1.0 + selection.strike_distance_pct / 100.0);
        let future_expiries = current_and_next_expiries(
            instruments
                .iter()
                .filter(|instrument| instrument.instrument_type == InstrumentType::Fut),
            today,
        );
        let option_expiries = current_and_next_expiries(
            instruments.iter().filter(|instrument| {
                matches!(
                    instrument.instrument_type,
                    InstrumentType::Call | InstrumentType::Put
                )
            }),
            today,
        );

        let mut futures = instruments
            .iter()
            .filter(|instrument| instrument.instrument_type == InstrumentType::Fut)
            .filter(|instrument| allowed_types.contains(&instrument.instrument_type))
            .filter(|instrument| {
                instrument
                    .expiry
                    .as_ref()
                    .is_some_and(|expiry| future_expiries.contains(expiry))
            })
            .cloned()
            .collect::<Vec<_>>();

        let mut atm_options = instruments
            .iter()
            .filter(|instrument| {
                matches!(
                    instrument.instrument_type,
                    InstrumentType::Call | InstrumentType::Put
                )
            })
            .filter(|instrument| allowed_types.contains(&instrument.instrument_type))
            .filter(|instrument| {
                instrument
                    .expiry
                    .as_ref()
                    .is_some_and(|expiry| option_expiries.contains(expiry))
            })
            .filter(|instrument| {
                instrument
                    .strike
                    .is_some_and(|strike| strike >= lower_strike && strike <= upper_strike)
            })
            .cloned()
            .collect::<Vec<_>>();

        futures.sort_by(|left, right| {
            left.expiry
                .cmp(&right.expiry)
                .then_with(|| left.trading_symbol.cmp(&right.trading_symbol))
        });
        atm_options.sort_by(|left, right| {
            left.expiry
                .cmp(&right.expiry)
                .then_with(|| {
                    let left_distance = (left.strike.unwrap_or(f64::MAX) - reference_price).abs();
                    let right_distance = (right.strike.unwrap_or(f64::MAX) - reference_price).abs();
                    left_distance.total_cmp(&right_distance)
                })
                .then_with(|| left.trading_symbol.cmp(&right.trading_symbol))
        });

        Ok(DbinternationalUniverseSummary {
            selected_underlying: underlying.to_string(),
            reference_symbol: reference_symbol.to_string(),
            reference_price,
            futures,
            atm_options,
        })
    }
}

pub fn selected_trading_symbols(summary: &DbinternationalUniverseSummary) -> BTreeSet<String> {
    summary
        .futures
        .iter()
        .chain(summary.atm_options.iter())
        .filter(|instrument| instrument.tradable)
        .map(|instrument| instrument.trading_symbol.clone())
        .collect()
}

pub fn write_dbinternational_derivatives_csv(
    summaries: &[DbinternationalUniverseSummary],
    path: impl AsRef<Path>,
) -> Result<(), FeedError> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| {
            FeedError::Config("invalid DBInternational derivatives CSV path".to_string())
        })?;
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

fn parse_catalog_contents(
    master_content: &str,
    index_content: &str,
) -> Result<DbinternationalUniverseCatalog, FeedError> {
    let mut builder = CatalogBuilder::default();
    for (index, line) in master_content.lines().enumerate() {
        parse_master_line(line, index + 1, &mut builder)?;
    }
    for (index, line) in index_content.lines().enumerate() {
        parse_index_line(line, index + 1, &mut builder)?;
    }
    builder.build()
}

#[derive(Default)]
struct CatalogBuilder {
    nse_eq_spots: BTreeMap<String, String>,
    nse_fno_stock_roots: BTreeSet<String>,
    mcx_commodity_spots: BTreeMap<String, String>,
    index_spots: Vec<DbinternationalSpotReference>,
    derivatives: Vec<InstrumentDefinition>,
}

impl CatalogBuilder {
    fn build(self) -> Result<DbinternationalUniverseCatalog, FeedError> {
        let mut spots = BTreeMap::new();
        for root in &self.nse_fno_stock_roots {
            if invalid_nse_stock_root(root) {
                continue;
            }
            if let Some(symbol) = self.nse_eq_spots.get(root) {
                spots.insert(
                    symbol.clone(),
                    DbinternationalSpotReference {
                        underlying: root.clone(),
                        spot_symbol: symbol.clone(),
                    },
                );
            }
        }
        for reference in self.index_spots {
            spots.insert(reference.spot_symbol.clone(), reference);
        }
        for (root, symbol) in self.mcx_commodity_spots {
            spots.insert(
                symbol.clone(),
                DbinternationalSpotReference {
                    underlying: root,
                    spot_symbol: symbol,
                },
            );
        }

        let valid_stock_roots = spots
            .values()
            .filter(|spot| self.nse_fno_stock_roots.contains(&spot.underlying))
            .map(|spot| spot.underlying.clone())
            .collect::<BTreeSet<_>>();

        let mut derivatives_by_underlying: BTreeMap<String, Vec<InstrumentDefinition>> =
            BTreeMap::new();
        for instrument in self.derivatives {
            if is_nse_stock_derivative(&instrument)
                && !valid_stock_roots.contains(&instrument.underlying)
            {
                continue;
            }
            derivatives_by_underlying
                .entry(instrument.underlying.clone())
                .or_default()
                .push(instrument);
        }

        Ok(DbinternationalUniverseCatalog {
            spot_references: spots.into_values().collect(),
            derivatives_by_underlying,
        })
    }
}

fn parse_master_line(
    line: &str,
    line_number: usize,
    builder: &mut CatalogBuilder,
) -> Result<(), FeedError> {
    let line = line.trim();
    if line.is_empty() {
        return Ok(());
    }
    let fields = line.split('|').map(str::trim).collect::<Vec<_>>();
    if fields.len() < 7 {
        return Ok(());
    }

    let exchange_segment = fields[0];
    let type_code = fields[2];
    let root = fields[3].to_ascii_uppercase();
    let trading_symbol = fields[4];
    let product = fields[5];

    if exchange_segment == "NSECM" && type_code == "8" && product == "EQ" {
        builder
            .nse_eq_spots
            .entry(root)
            .or_insert_with(|| trading_symbol.to_string());
        return Ok(());
    }

    if exchange_segment == "MCXFO"
        && type_code == "16"
        && product == "COMDTY"
        && allowed_mcx_root(&root)
    {
        builder
            .mcx_commodity_spots
            .entry(root)
            .or_insert_with(|| trading_symbol.to_string());
        return Ok(());
    }

    if let Some(instrument) = parse_derivative_line(&fields, line_number)? {
        if instrument.exchange == "NSE" && instrument.segment == "FUTSTK"
            || instrument.exchange == "NSE" && instrument.segment == "OPTSTK"
        {
            builder
                .nse_fno_stock_roots
                .insert(instrument.underlying.clone());
        }
        builder.derivatives.push(instrument);
    }

    Ok(())
}

fn parse_index_line(
    line: &str,
    line_number: usize,
    builder: &mut CatalogBuilder,
) -> Result<(), FeedError> {
    let line = line.trim();
    if line.is_empty() {
        return Ok(());
    }
    let fields = line.split('|').map(str::trim).collect::<Vec<_>>();
    if fields.len() < 5 {
        return Err(FeedError::Parse(format!(
            "DBInternational index line {line_number} has too few fields"
        )));
    }

    let Some((underlying, spot_symbol)) = index_reference_symbol(fields[3]) else {
        return Ok(());
    };
    builder.index_spots.push(DbinternationalSpotReference {
        underlying,
        spot_symbol,
    });

    Ok(())
}

fn parse_derivative_line(
    fields: &[&str],
    line_number: usize,
) -> Result<Option<InstrumentDefinition>, FeedError> {
    if fields.len() < 21 {
        return Ok(None);
    }

    let exchange_segment = fields[0];
    let instrument_id = fields[1];
    let type_code = fields[2];
    let root = fields[3].to_ascii_uppercase();
    let product = fields[5];

    if type_code == "4" {
        return Ok(None);
    }

    let Some(instrument_type) =
        derivative_instrument_type(exchange_segment, product, type_code, fields)
    else {
        return Ok(None);
    };
    if !derivative_root_allowed(exchange_segment, product, &root, instrument_type) {
        return Ok(None);
    }

    let expiry = expiry_date(fields.get(16).copied()).ok_or_else(|| {
        FeedError::Parse(format!(
            "DBInternational derivative line {line_number} missing expiry"
        ))
    })?;
    let strike = if matches!(instrument_type, InstrumentType::Call | InstrumentType::Put) {
        Some(parse_f64_field(
            fields.get(17).copied(),
            "strike",
            line_number,
        )?)
    } else {
        None
    };
    let trading_symbol = fields
        .get(22)
        .copied()
        .filter(|value| usable_text(value))
        .or_else(|| fields.get(4).copied())
        .unwrap_or("")
        .to_string();
    if trading_symbol.is_empty() {
        return Ok(None);
    }

    let tick_size = parse_f64_field(fields.get(11).copied(), "tick size", line_number)?;
    let lot_size = parse_f64_field(fields.get(12).copied(), "lot size", line_number)?;
    let exchange = exchange_name(exchange_segment);

    Ok(Some(InstrumentDefinition {
        instrument_name: InstrumentName::new(trading_symbol.clone()),
        instrument_type,
        strike,
        expiry: Some(expiry),
        broker: BROKER.to_string(),
        instrument_token: format!("{exchange_segment}:{instrument_id}"),
        trading_symbol,
        exchange,
        segment: product.to_string(),
        underlying: root,
        lot_size,
        tick_size,
        tradable: true,
    }))
}

fn derivative_instrument_type(
    exchange_segment: &str,
    product: &str,
    type_code: &str,
    fields: &[&str],
) -> Option<InstrumentType> {
    match (exchange_segment, product, type_code) {
        ("NSEFO", "FUTIDX" | "FUTSTK", "1") | ("BSEFO", "IF", "1") | ("MCXFO", "FUTCOM", "1") => {
            Some(InstrumentType::Fut)
        }
        ("NSEFO", "OPTIDX" | "OPTSTK", "2") | ("BSEFO", "IO", "2") | ("MCXFO", "OPTFUT", "2") => {
            option_side(fields.get(18).copied())
        }
        _ => None,
    }
}

fn option_side(value: Option<&str>) -> Option<InstrumentType> {
    match value.map(str::trim) {
        Some("3") => Some(InstrumentType::Call),
        Some("4") => Some(InstrumentType::Put),
        _ => None,
    }
}

fn derivative_root_allowed(
    exchange_segment: &str,
    product: &str,
    root: &str,
    instrument_type: InstrumentType,
) -> bool {
    match (exchange_segment, product, instrument_type) {
        ("NSEFO", "FUTIDX" | "OPTIDX", _) => {
            matches!(root, "NIFTY" | "BANKNIFTY" | "MIDCPNIFTY")
        }
        ("NSEFO", "FUTSTK" | "OPTSTK", _) => !invalid_nse_stock_root(root),
        ("BSEFO", "IF" | "IO", _) => root == "SENSEX",
        ("MCXFO", "FUTCOM" | "OPTFUT", _) => allowed_mcx_root(root),
        _ => false,
    }
}

fn is_nse_stock_derivative(instrument: &InstrumentDefinition) -> bool {
    instrument.exchange == "NSE" && matches!(instrument.segment.as_str(), "FUTSTK" | "OPTSTK")
}

fn index_reference_symbol(name: &str) -> Option<(String, String)> {
    match name.trim().to_ascii_uppercase().as_str() {
        "NIFTY 50" => Some(("NIFTY".to_string(), "NIFTY50".to_string())),
        "NIFTY BANK" => Some(("BANKNIFTY".to_string(), "NIFTYBANK".to_string())),
        "NIFTY MID SELECT" => Some(("MIDCPNIFTY".to_string(), "MIDCPNIFTY".to_string())),
        "SENSEX" => Some(("SENSEX".to_string(), "SENSEX".to_string())),
        _ => None,
    }
}

fn allowed_mcx_root(root: &str) -> bool {
    root.starts_with("GOLD")
        || root.starts_with("SILVER")
        || root.starts_with("NATURALGAS")
        || root.starts_with("NATGAS")
        || root.starts_with("CRUDEOIL")
}

fn invalid_nse_stock_root(root: &str) -> bool {
    root.contains("NSETEST") || root.starts_with("DUMMY")
}

fn current_and_next_expiries<'a>(
    instruments: impl Iterator<Item = &'a InstrumentDefinition>,
    today: &str,
) -> BTreeSet<String> {
    instruments
        .filter_map(|instrument| instrument.expiry.as_deref())
        .filter(|expiry| *expiry >= today)
        .map(str::to_string)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .take(2)
        .collect()
}

fn selection_types(selection: &InstrumentSelection) -> Result<Vec<InstrumentType>, FeedError> {
    selection
        .instrument_types
        .iter()
        .map(|value| parse_instrument_type(value))
        .collect()
}

fn expiry_date(value: Option<&str>) -> Option<String> {
    let value = value?.trim();
    if value.len() < 10 {
        return None;
    }
    let date = &value[..10];
    if NaiveDate::parse_from_str(date, "%Y-%m-%d").is_ok() {
        Some(date.to_string())
    } else {
        None
    }
}

fn parse_f64_field(
    value: Option<&str>,
    field_name: &str,
    line_number: usize,
) -> Result<f64, FeedError> {
    let value = value.ok_or_else(|| {
        FeedError::Parse(format!(
            "DBInternational derivative line {line_number} missing {field_name}"
        ))
    })?;
    value.parse::<f64>().map_err(|error| {
        FeedError::Parse(format!(
            "DBInternational derivative line {line_number} invalid {field_name} {value}: {error}"
        ))
    })
}

fn exchange_name(exchange_segment: &str) -> String {
    match exchange_segment {
        "NSECM" | "NSEFO" => "NSE".to_string(),
        "BSECM" | "BSEFO" => "BSE".to_string(),
        "MCXFO" => "MCX".to_string(),
        _ => exchange_segment.to_string(),
    }
}

fn usable_text(value: &str) -> bool {
    let value = value.trim();
    !value.is_empty() && value != "-1" && value != "0"
}

fn current_ist_date() -> String {
    let ist = FixedOffset::east_opt(IST_OFFSET_SECONDS).expect("valid IST offset");
    Utc::now()
        .with_timezone(&ist)
        .date_naive()
        .format("%Y-%m-%d")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_spot_anchors_from_master_and_index_cache() {
        let catalog = parse_catalog_contents(sample_master(), sample_indices()).expect("catalog");
        let symbols = catalog
            .spot_references()
            .iter()
            .map(|reference| format!("{}={}", reference.underlying, reference.spot_symbol))
            .collect::<BTreeSet<_>>();

        assert!(symbols.contains("RELIANCE=RELIANCE-EQ"));
        assert!(symbols.contains("NIFTY=NIFTY50"));
        assert!(symbols.contains("BANKNIFTY=NIFTYBANK"));
        assert!(symbols.contains("MIDCPNIFTY=MIDCPNIFTY"));
        assert!(symbols.contains("SENSEX=SENSEX"));
        assert!(symbols.contains("GOLD=GOLD"));
    }

    #[test]
    fn selects_current_next_expiry_and_strike_window() {
        let catalog = parse_catalog_contents(sample_master(), sample_indices()).expect("catalog");
        let selection = InstrumentSelection {
            instrument_types: vec!["FUT".to_string(), "CALL".to_string(), "PUT".to_string()],
            strike_distance_pct: 3.0,
            refresh_trigger_pct: 3.0,
        };

        let summary = catalog
            .build_summary_for_date(&selection, "SENSEX", "SENSEX", 80_000.0, "2026-06-25")
            .expect("summary");
        let symbols = selected_trading_symbols(&summary);

        assert!(symbols.contains("SENSEX26JUNFUT"));
        assert!(symbols.contains("SENSEX26JULFUT"));
        assert!(!symbols.contains("SENSEX26AUGFUT"));
        assert!(symbols.contains("SENSEX26JUN80000CE"));
        assert!(symbols.contains("SENSEX2670279000PE"));
        assert!(!symbols.contains("SENSEX2670285000CE"));
        assert!(!symbols.contains("SENSEX26JUN26JULFUT"));
    }

    #[test]
    fn keeps_all_allowed_mcx_variations_with_derivatives() {
        let catalog = parse_catalog_contents(sample_master(), sample_indices()).expect("catalog");
        let selection = InstrumentSelection {
            instrument_types: vec!["FUT".to_string(), "CALL".to_string(), "PUT".to_string()],
            strike_distance_pct: 3.0,
            refresh_trigger_pct: 3.0,
        };

        let summary = catalog
            .build_summary_for_date(&selection, "GOLDPETAL", "GOLDPETAL", 9_000.0, "2026-06-25")
            .expect("summary");

        assert_eq!(summary.futures.len(), 2);
        assert_eq!(summary.atm_options.len(), 0);
    }

    fn sample_indices() -> &'static str {
        "\
NSECM|26000|16|NIFTY 50|NIFTY 50|INDEX|NIFTY 50-INDEX|26000|0|0|0|0.05|1|1|NIFTY 50|NIFTY50|1|1||||-1|NIFTY 50
NSECM|26001|16|NIFTY BANK|NIFTY BANK|INDEX|NIFTY BANK-INDEX|26001|0|0|0|0.05|1|1|NIFTY BANK|NIFTYBANK|1|1||||-1|NIFTY BANK
NSECM|26121|16|NIFTY MID SELECT|NIFTY MID SELECT|INDEX|NIFTY MID SELECT-INDEX|26121|0|0|0|0.05|1|1|NIFTY MID SELECT|NIFTYMIDSELECT|1|1|MIDCPNIFTY|MIDCAPSELECT||-1|NIFTY MID SELECT
BSECM|26065|16|SENSEX|SENSEX|INDEX|SENSEX-INDEX|26065|0|0|0|0.05|1|1|SENSEX|SENSEX|1|1||||-1|SENSEX
"
    }

    fn sample_master() -> &'static str {
        "\
NSECM|2885|8|RELIANCE|RELIANCE-EQ|EQ|RELIANCE-EQ|1100100002885|1440.4|1178.6|67662|0.1|1|1|RELIANCE|INE002A01018|1|1|RELIANCE INDUSTRIES LTD-EQ|0|-1|-1
NSEFO|50001|1|RELIANCE|RELIANCE26JUNFUT|FUTSTK|RELIANCE-FUTSTK|x|0|0|0|0.05|500|1|-1|RELIANCE|2026-06-30T14:30:00|RELIANCE 30JUN2026|1|1|RELIANCE26JUNFUT
NSEFO|50002|2|RELIANCE|RELIANCE26JUN1500CE|OPTSTK|RELIANCE-OPTSTK|x|0|0|0|0.05|500|1|-1|RELIANCE|2026-06-30T14:30:00|1500|3|RELIANCE 30JUN2026 CE 1500|1|1|RELIANCE26JUN1500CE
BSEFO|1105863|1|SENSEX|SENSEX26JUNFUT|IF|SENSEX-IF|x|84787.5|69371.55|500|0.05|20|1|-1|SENSEX|2026-06-25T00:00:00|SENSEX 25JUN2026|1|1|SENSEX26JUNFUT
BSEFO|1144507|1|SENSEX|SENSEX26JULFUT|IF|SENSEX-IF|x|85401.1|69873.65|500|0.05|20|1|-1|SENSEX|2026-07-30T00:00:00|SENSEX 30JUL2026|1|1|SENSEX26JULFUT
BSEFO|825622|1|SENSEX|SENSEX26AUGFUT|IF|SENSEX-IF|x|85889.8|70273.45|500|0.05|20|1|-1|SENSEX|2026-08-27T00:00:00|SENSEX 27AUG2026|1|1|SENSEX26AUGFUT
BSEFO|16016544|4|SENSEX|SENSEX26JUN26JULFUT|IF|SENSEX-IF|x|1145|-1145|901|0.2|30|1|-1||2026-06-25T00:00:00|SENSEX 25JUN30JUL SPD|1|1|SENSEX26JUN26JULFUT
BSEFO|887600|2|SENSEX|SENSEX26JUN80000CE|IO|SENSEX-IO|x|100|0.05|1000|0.05|20|1|-1|SENSEX|2026-06-25T00:00:00|80000|3|SENSEX 25JUN2026 CE 80000|1|1|SENSEX26JUN80000CE
BSEFO|820380|2|SENSEX|SENSEX2670279000PE|IO|SENSEX-IO|x|100|0.05|1000|0.05|20|1|-1|SENSEX|2026-07-02T00:00:00|79000|4|SENSEX 02JUL2026 PE 79000|1|1|SENSEX2670279000PE
BSEFO|820381|2|SENSEX|SENSEX2670285000CE|IO|SENSEX-IO|x|100|0.05|1000|0.05|20|1|-1|SENSEX|2026-07-02T00:00:00|85000|3|SENSEX 02JUL2026 CE 85000|1|1|SENSEX2670285000CE
MCXFO|1001|16|GOLD|GOLD|COMDTY|GOLD-COMDTY|x|0|0|0|1|1|1|GOLD|GOLD|1|1
MCXFO|1002|16|GOLDPETAL|GOLDPETAL|COMDTY|GOLDPETAL-COMDTY|x|0|0|0|1|1|1|GOLDPETAL|GOLDPETAL|1|1
MCXFO|2001|1|GOLDPETAL|GOLDPETAL26JUNFUT|FUTCOM|GOLDPETAL-FUTCOM|x|0|0|0|1|1|1|-1|GOLDPETAL|2026-06-30T00:00:00|GOLDPETAL 30JUN2026|1|1|GOLDPETAL26JUNFUT
MCXFO|2002|1|GOLDPETAL|GOLDPETAL26JULFUT|FUTCOM|GOLDPETAL-FUTCOM|x|0|0|0|1|1|1|-1|GOLDPETAL|2026-07-31T00:00:00|GOLDPETAL 31JUL2026|1|1|GOLDPETAL26JULFUT
MCXFO|2003|1|GOLDPETAL|GOLDPETAL26AUGFUT|FUTCOM|GOLDPETAL-FUTCOM|x|0|0|0|1|1|1|-1|GOLDPETAL|2026-08-31T00:00:00|GOLDPETAL 31AUG2026|1|1|GOLDPETAL26AUGFUT
"
    }
}
