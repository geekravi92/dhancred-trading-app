use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use crate::feeder::FeedError;
pub use crate::feeder::event::InstrumentName;

pub const UNIVERSAL_INSTRUMENT_CSV_HEADER: &str = "instrument_name,instrument_type,strike,expiry,broker,instrument_token,trading_symbol,exchange,segment,underlying,lot_size,tick_size,tradable";

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum InstrumentType {
    Spot,
    Fut,
    PerpFut,
    Call,
    Put,
}

impl InstrumentType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Spot => "SPOT",
            Self::Fut => "FUT",
            Self::PerpFut => "PERP_FUT",
            Self::Call => "CALL",
            Self::Put => "PUT",
        }
    }

    pub fn segment(self) -> &'static str {
        match self {
            Self::Spot => "SPOT",
            Self::Fut => "FUT",
            Self::PerpFut => "PERP_FUT",
            Self::Call => "CALL",
            Self::Put => "PUT",
        }
    }

    pub fn is_derivative(self) -> bool {
        matches!(self, Self::Fut | Self::PerpFut | Self::Call | Self::Put)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct InstrumentDefinition {
    pub instrument_name: InstrumentName,
    pub instrument_type: InstrumentType,
    pub strike: Option<f64>,
    pub expiry: Option<String>,
    pub broker: String,
    pub instrument_token: String,
    pub trading_symbol: String,
    pub exchange: String,
    pub segment: String,
    pub underlying: String,
    pub lot_size: f64,
    pub tick_size: f64,
    pub tradable: bool,
}

impl InstrumentDefinition {
    pub fn is_derivative(&self) -> bool {
        self.instrument_type.is_derivative()
    }

    pub fn to_csv_row(&self) -> String {
        [
            self.instrument_name.to_string(),
            self.instrument_type.as_str().to_string(),
            optional_f64(self.strike),
            self.expiry.clone().unwrap_or_default(),
            self.broker.clone(),
            self.instrument_token.clone(),
            self.trading_symbol.clone(),
            self.exchange.clone(),
            self.segment.clone(),
            self.underlying.clone(),
            self.lot_size.to_string(),
            self.tick_size.to_string(),
            self.tradable.to_string(),
        ]
        .join(",")
    }
}

#[derive(Clone, Debug, Default)]
pub struct InstrumentCatalog {
    by_name: BTreeMap<InstrumentName, InstrumentDefinition>,
}

impl InstrumentCatalog {
    pub fn new(instruments: Vec<InstrumentDefinition>) -> Self {
        let by_name = instruments
            .into_iter()
            .map(|instrument| (instrument.instrument_name.clone(), instrument))
            .collect();

        Self { by_name }
    }

    pub fn get(&self, name: &InstrumentName) -> Option<&InstrumentDefinition> {
        self.by_name.get(name)
    }

    pub fn instruments(&self) -> impl Iterator<Item = &InstrumentDefinition> {
        self.by_name.values()
    }

    pub fn select(&self, filter: &UniverseFilter) -> InstrumentUniverse {
        let instruments = self
            .by_name
            .values()
            .filter(|instrument| filter.matches(instrument))
            .cloned()
            .collect();

        InstrumentUniverse::new(instruments)
    }

    pub fn load_csv(path: impl AsRef<Path>) -> Result<Self, FeedError> {
        let content = fs::read_to_string(path)?;
        parse_catalog_csv(&content)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UniverseFilter {
    pub broker: Option<String>,
    pub underlyings: Vec<String>,
    pub instrument_types: Vec<InstrumentType>,
    pub tradable_only: bool,
}

impl UniverseFilter {
    pub fn matches(&self, instrument: &InstrumentDefinition) -> bool {
        let broker_matches = self
            .broker
            .as_ref()
            .is_none_or(|broker| broker == &instrument.broker);
        let underlying_matches = self.underlyings.is_empty()
            || self
                .underlyings
                .iter()
                .any(|underlying| underlying == &instrument.underlying);
        let type_matches = self.instrument_types.is_empty()
            || self
                .instrument_types
                .iter()
                .any(|instrument_type| instrument_type == &instrument.instrument_type);
        let tradable_matches = !self.tradable_only || instrument.tradable;

        broker_matches && underlying_matches && type_matches && tradable_matches
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstrumentUniverse {
    instruments: Vec<InstrumentName>,
}

impl InstrumentUniverse {
    pub fn new(instruments: Vec<InstrumentDefinition>) -> Self {
        Self {
            instruments: instruments
                .into_iter()
                .map(|instrument| instrument.instrument_name)
                .collect(),
        }
    }

    pub fn names(&self) -> &[InstrumentName] {
        &self.instruments
    }
}

fn parse_catalog_csv(content: &str) -> Result<InstrumentCatalog, FeedError> {
    let mut lines = content.lines().filter(|line| !line.trim().is_empty());
    let header = lines
        .next()
        .ok_or_else(|| FeedError::Parse("instrument csv is empty".to_string()))?;
    let headers: Vec<&str> = header.split(',').map(str::trim).collect();

    let mut instruments = Vec::new();
    for (index, line) in lines.enumerate() {
        let values: Vec<&str> = line.split(',').map(str::trim).collect();
        if values.len() != headers.len() {
            return Err(FeedError::Parse(format!(
                "line {} has {} fields, expected {}",
                index + 2,
                values.len(),
                headers.len()
            )));
        }

        let get = |name: &str| -> Result<&str, FeedError> {
            headers
                .iter()
                .position(|header| *header == name)
                .map(|position| values[position])
                .ok_or_else(|| FeedError::Parse(format!("missing csv column {name}")))
        };

        let instrument_name = InstrumentName::new(get("instrument_name")?);
        let instrument_type = parse_instrument_type(get("instrument_type")?)?;
        let strike = parse_optional_f64(get("strike")?)?;
        let expiry = parse_optional_string(get("expiry")?);
        let broker = get("broker")?.to_string();
        let instrument_token = get("instrument_token")?.to_string();
        let trading_symbol = get("trading_symbol")?.to_string();
        let exchange = get("exchange")?.to_string();
        let segment = get("segment")?.to_string();
        let underlying = get("underlying")?.to_string();
        let lot_size = parse_required_f64(get("lot_size")?)?;
        let tick_size = parse_required_f64(get("tick_size")?)?;
        let tradable = parse_bool(get("tradable")?)?;

        instruments.push(InstrumentDefinition {
            instrument_name,
            instrument_type,
            strike,
            expiry,
            broker,
            instrument_token,
            trading_symbol,
            exchange,
            segment,
            underlying,
            lot_size,
            tick_size,
            tradable,
        });
    }

    Ok(InstrumentCatalog::new(instruments))
}

pub fn parse_instrument_type(value: &str) -> Result<InstrumentType, FeedError> {
    match value.trim().to_ascii_uppercase().as_str() {
        "SPOT" => Ok(InstrumentType::Spot),
        "FUT" => Ok(InstrumentType::Fut),
        "PERP_FUT" => Ok(InstrumentType::PerpFut),
        "CALL" => Ok(InstrumentType::Call),
        "PUT" => Ok(InstrumentType::Put),
        _ => Err(FeedError::Parse(format!(
            "unsupported instrument_type {value}"
        ))),
    }
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

fn optional_f64(value: Option<f64>) -> String {
    value.map(|value| value.to_string()).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loads_catalog_from_universal_csv() {
        let csv = "\
instrument_name,instrument_type,strike,expiry,broker,instrument_token,trading_symbol,exchange,segment,underlying,lot_size,tick_size,tradable
BTCUSD,PERP_FUT,,,DELTA,27,BTCUSD,DELTA,PERP_FUT,BTC,1,0.5,true
C-BTC-90000-260426,CALL,90000,2026-04-26,DELTA,130000,C-BTC-90000-260426,DELTA,CALL,BTC,1,0.1,true
";

        let catalog = parse_catalog_csv(csv).expect("catalog");
        let universe = catalog.select(&UniverseFilter {
            broker: Some("DELTA".to_string()),
            underlyings: vec!["BTC".to_string()],
            instrument_types: vec![InstrumentType::Call],
            tradable_only: true,
        });

        assert_eq!(
            universe.names(),
            &[InstrumentName::new("C-BTC-90000-260426")]
        );
    }
}
