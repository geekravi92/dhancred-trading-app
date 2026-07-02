use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::Path;

use crate::adapters::angelone::master::{current_ist_date, read_master_file};
use crate::config::{AngeloneBrokerSection, InstrumentSelection};
use crate::feeder::{
    FeedError, InstrumentCatalog, InstrumentDefinition, InstrumentType,
    UNIVERSAL_INSTRUMENT_CSV_HEADER, exchange_key, parse_instrument_type,
};

#[derive(Clone, Debug)]
pub struct AngeloneSpotReference {
    pub underlying: String,
    pub spot_symbol: String,
    pub instrument: InstrumentDefinition,
}

#[derive(Clone, Debug, Default)]
pub struct AngeloneUniverseCatalog {
    derivatives_by_underlying: BTreeMap<String, Vec<InstrumentDefinition>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AngeloneUniverseSummary {
    pub selected_underlying: String,
    pub reference_symbol: String,
    pub reference_price: f64,
    pub futures: Vec<InstrumentDefinition>,
    pub atm_options: Vec<InstrumentDefinition>,
}

impl AngeloneUniverseCatalog {
    pub fn load(config: &AngeloneBrokerSection) -> Result<Self, FeedError> {
        let instruments = read_master_file(&config.master_file)?;
        Ok(Self::from_instruments(instruments))
    }

    fn from_instruments(instruments: Vec<InstrumentDefinition>) -> Self {
        let mut derivatives_by_underlying: BTreeMap<String, Vec<InstrumentDefinition>> =
            BTreeMap::new();

        for instrument in instruments
            .into_iter()
            .filter(|instrument| instrument.instrument_type.is_derivative())
            .filter(|instrument| instrument.tradable)
        {
            derivatives_by_underlying
                .entry(instrument.underlying.clone())
                .or_default()
                .push(instrument);
        }

        Self {
            derivatives_by_underlying,
        }
    }

    pub fn build_summary(
        &self,
        selection: &InstrumentSelection,
        underlying: &str,
        reference_symbol: &str,
        reference_price: f64,
    ) -> Result<AngeloneUniverseSummary, FeedError> {
        self.build_summary_for_exchanges(
            selection,
            underlying,
            reference_symbol,
            reference_price,
            None,
        )
    }

    pub fn build_summary_for_exchanges(
        &self,
        selection: &InstrumentSelection,
        underlying: &str,
        reference_symbol: &str,
        reference_price: f64,
        allowed_exchanges: Option<&BTreeSet<String>>,
    ) -> Result<AngeloneUniverseSummary, FeedError> {
        self.build_summary_for_date(
            selection,
            underlying,
            reference_symbol,
            reference_price,
            &current_ist_date(),
            allowed_exchanges,
        )
    }

    fn build_summary_for_date(
        &self,
        selection: &InstrumentSelection,
        underlying: &str,
        reference_symbol: &str,
        reference_price: f64,
        today: &str,
        allowed_exchanges: Option<&BTreeSet<String>>,
    ) -> Result<AngeloneUniverseSummary, FeedError> {
        let Some(instruments) = self.derivatives_by_underlying.get(underlying) else {
            return Ok(AngeloneUniverseSummary {
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
                .filter(|instrument| exchange_allowed(instrument, allowed_exchanges))
                .filter(|instrument| instrument.instrument_type == InstrumentType::Fut),
            today,
        );
        let option_expiries = current_and_next_expiries(
            instruments
                .iter()
                .filter(|instrument| exchange_allowed(instrument, allowed_exchanges))
                .filter(|instrument| {
                    matches!(
                        instrument.instrument_type,
                        InstrumentType::Call | InstrumentType::Put
                    )
                }),
            today,
        );

        let mut futures = instruments
            .iter()
            .filter(|instrument| exchange_allowed(instrument, allowed_exchanges))
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
            .filter(|instrument| exchange_allowed(instrument, allowed_exchanges))
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

        Ok(AngeloneUniverseSummary {
            selected_underlying: underlying.to_string(),
            reference_symbol: reference_symbol.to_string(),
            reference_price,
            futures,
            atm_options,
        })
    }
}

pub fn spot_references_from_base_catalog(
    catalog: &InstrumentCatalog,
) -> Result<Vec<AngeloneSpotReference>, FeedError> {
    let references = catalog
        .instruments()
        .filter(|instrument| instrument.tradable)
        .map(|instrument| AngeloneSpotReference {
            underlying: instrument.underlying.clone(),
            spot_symbol: instrument.trading_symbol.clone(),
            instrument: instrument.clone(),
        })
        .collect::<Vec<_>>();

    if references.is_empty() {
        return Err(FeedError::Config(
            "AngelOne base_instruments_csv must contain at least one tradable row".to_string(),
        ));
    }

    Ok(references)
}

pub fn selected_trading_symbols(summary: &AngeloneUniverseSummary) -> BTreeSet<String> {
    summary
        .futures
        .iter()
        .chain(summary.atm_options.iter())
        .filter(|instrument| instrument.tradable)
        .map(|instrument| instrument.trading_symbol.clone())
        .collect()
}

pub fn selected_instruments_by_symbol(
    summary: &AngeloneUniverseSummary,
) -> BTreeMap<String, InstrumentDefinition> {
    summary
        .futures
        .iter()
        .chain(summary.atm_options.iter())
        .filter(|instrument| instrument.tradable)
        .map(|instrument| (instrument.trading_symbol.clone(), instrument.clone()))
        .collect()
}

pub fn write_angelone_derivatives_csv(
    summaries: &[AngeloneUniverseSummary],
    path: impl AsRef<Path>,
) -> Result<(), FeedError> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| FeedError::Config("invalid AngelOne derivatives CSV path".to_string()))?;
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

fn exchange_allowed(
    instrument: &InstrumentDefinition,
    allowed_exchanges: Option<&BTreeSet<String>>,
) -> bool {
    allowed_exchanges
        .is_none_or(|exchanges| exchanges.contains(&exchange_key(&instrument.exchange)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::angelone::master::parse_master_json;
    use crate::feeder::InstrumentName;

    #[test]
    fn selects_current_next_expiry_and_strike_window() {
        let instruments = parse_master_json(sample_master()).expect("master");
        let catalog = AngeloneUniverseCatalog::from_instruments(instruments);
        let selection = InstrumentSelection {
            instrument_types: vec!["FUT".to_string(), "CALL".to_string(), "PUT".to_string()],
            strike_distance_pct: 3.0,
            refresh_trigger_pct: 3.0,
        };

        let summary = catalog
            .build_summary_for_date(&selection, "NIFTY", "NIFTY", 23_500.0, "2026-07-02", None)
            .expect("summary");
        let symbols = selected_trading_symbols(&summary);

        assert!(symbols.contains("NIFTY28JUL26FUT"));
        assert!(symbols.contains("NIFTY25AUG26FUT"));
        assert!(!symbols.contains("NIFTY29SEP26FUT"));
        assert!(symbols.contains("NIFTY07JUL2623500CE"));
        assert!(symbols.contains("NIFTY07JUL2623500PE"));
        assert!(!symbols.contains("NIFTY07JUL2626000CE"));
    }

    #[test]
    fn filters_derivatives_by_allowed_exchanges() {
        let instruments = parse_master_json(sample_master()).expect("master");
        let catalog = AngeloneUniverseCatalog::from_instruments(instruments);
        let selection = InstrumentSelection {
            instrument_types: vec!["FUT".to_string(), "CALL".to_string(), "PUT".to_string()],
            strike_distance_pct: 3.0,
            refresh_trigger_pct: 3.0,
        };

        let summary = catalog
            .build_summary_for_date(
                &selection,
                "NIFTY",
                "NIFTY",
                23_500.0,
                "2026-07-02",
                Some(&BTreeSet::from(["MCX".to_string()])),
            )
            .expect("summary");

        assert!(selected_trading_symbols(&summary).is_empty());
    }

    #[test]
    fn base_references_can_use_futures() {
        let catalog = InstrumentCatalog::new(vec![InstrumentDefinition {
            instrument_name: InstrumentName::new("GOLD05AUG26FUT"),
            instrument_type: InstrumentType::Fut,
            strike: None,
            expiry: Some("2026-08-05".to_string()),
            broker: "ANGELONE".to_string(),
            instrument_token: "MCX:466583".to_string(),
            trading_symbol: "GOLD05AUG26FUT".to_string(),
            exchange: "MCX".to_string(),
            segment: "FUTCOM".to_string(),
            underlying: "GOLD".to_string(),
            lot_size: 1.0,
            tick_size: 1.0,
            tradable: true,
        }]);

        let references = spot_references_from_base_catalog(&catalog).expect("references");

        assert_eq!(references.len(), 1);
        assert_eq!(references[0].underlying, "GOLD");
        assert_eq!(references[0].spot_symbol, "GOLD05AUG26FUT");
    }

    fn sample_master() -> &'static str {
        r#"[
          {"token":"61093","symbol":"NIFTY28JUL26FUT","name":"NIFTY","expiry":"28JUL2026","strike":"-1.000000","lotsize":"65","instrumenttype":"FUTIDX","exch_seg":"NFO","tick_size":"10.000000"},
          {"token":"58072","symbol":"NIFTY25AUG26FUT","name":"NIFTY","expiry":"25AUG2026","strike":"-1.000000","lotsize":"65","instrumenttype":"FUTIDX","exch_seg":"NFO","tick_size":"10.000000"},
          {"token":"68392","symbol":"NIFTY29SEP26FUT","name":"NIFTY","expiry":"29SEP2026","strike":"-1.000000","lotsize":"65","instrumenttype":"FUTIDX","exch_seg":"NFO","tick_size":"10.000000"},
          {"token":"1","symbol":"NIFTY07JUL2623500CE","name":"NIFTY","expiry":"07JUL2026","strike":"2350000.000000","lotsize":"65","instrumenttype":"OPTIDX","exch_seg":"NFO","tick_size":"5.000000"},
          {"token":"2","symbol":"NIFTY07JUL2623500PE","name":"NIFTY","expiry":"07JUL2026","strike":"2350000.000000","lotsize":"65","instrumenttype":"OPTIDX","exch_seg":"NFO","tick_size":"5.000000"},
          {"token":"3","symbol":"NIFTY07JUL2626000CE","name":"NIFTY","expiry":"07JUL2026","strike":"2600000.000000","lotsize":"65","instrumenttype":"OPTIDX","exch_seg":"NFO","tick_size":"5.000000"}
        ]"#
    }
}
