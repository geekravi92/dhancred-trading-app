use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::adapters::fyers::master::FyersUniverseSummary;
use crate::feeder::{FeedError, InstrumentDefinition, InstrumentName, InstrumentType};

#[derive(Debug)]
pub struct FyersLatestPriceFile {
    path: PathBuf,
    underlyings: BTreeMap<String, UnderlyingLatestPrices>,
}

impl FyersLatestPriceFile {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            underlyings: BTreeMap::new(),
        }
    }

    pub fn set_spot_symbol(&mut self, underlying: &str, symbol: &str) -> Result<(), FeedError> {
        let state = self
            .underlyings
            .entry(underlying.to_string())
            .or_insert_with(|| UnderlyingLatestPrices::new(underlying));

        if state.spot_symbol.as_deref() != Some(symbol) {
            state.spot_symbol = Some(symbol.to_string());
            state.spot = None;
            self.write()?;
        }

        Ok(())
    }

    pub fn update_targets_from_summary(
        &mut self,
        summary: &FyersUniverseSummary,
    ) -> Result<(), FeedError> {
        let state = self
            .underlyings
            .entry(summary.selected_underlying.clone())
            .or_insert_with(|| UnderlyingLatestPrices::new(&summary.selected_underlying));

        let fut_symbol = nearest_future_symbol(summary);
        let atm_call = nearest_option_target(summary, InstrumentType::Call);
        let atm_put = nearest_option_target(summary, InstrumentType::Put);

        let mut changed = update_target(&mut state.fut_symbol, &mut state.fut, fut_symbol);
        changed |= update_option_target(
            &mut state.atm_call_symbol,
            &mut state.atm_call_strike,
            &mut state.atm_call,
            atm_call,
        );
        changed |= update_option_target(
            &mut state.atm_put_symbol,
            &mut state.atm_put_strike,
            &mut state.atm_put,
            atm_put,
        );

        if changed {
            self.write()?;
        }

        Ok(())
    }

    pub fn update_tick(
        &mut self,
        instrument_name: &InstrumentName,
        price: f64,
    ) -> Result<(), FeedError> {
        let symbol = instrument_name.as_str();
        let mut changed = false;

        for state in self.underlyings.values_mut() {
            if state.spot_symbol.as_deref() == Some(symbol) {
                changed |= update_price(&mut state.spot, price);
            }
            if state.fut_symbol.as_deref() == Some(symbol) {
                changed |= update_price(&mut state.fut, price);
            }
            if state.atm_call_symbol.as_deref() == Some(symbol) {
                changed |= update_price(&mut state.atm_call, price);
            }
            if state.atm_put_symbol.as_deref() == Some(symbol) {
                changed |= update_price(&mut state.atm_put, price);
            }
        }

        if changed {
            self.write()?;
        }

        Ok(())
    }

    pub fn write(&self) -> Result<(), FeedError> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }

        let tmp_path = tmp_path(&self.path)?;
        let content = self
            .underlyings
            .values()
            .map(UnderlyingLatestPrices::to_file_block)
            .collect::<Vec<_>>()
            .join("");

        fs::write(&tmp_path, content)?;
        fs::rename(tmp_path, &self.path)?;

        Ok(())
    }
}

#[derive(Debug)]
struct UnderlyingLatestPrices {
    underlying: String,
    spot_symbol: Option<String>,
    fut_symbol: Option<String>,
    atm_call_symbol: Option<String>,
    atm_put_symbol: Option<String>,
    atm_call_strike: Option<f64>,
    atm_put_strike: Option<f64>,
    spot: Option<f64>,
    fut: Option<f64>,
    atm_call: Option<f64>,
    atm_put: Option<f64>,
}

impl UnderlyingLatestPrices {
    fn new(underlying: &str) -> Self {
        Self {
            underlying: underlying.to_string(),
            spot_symbol: None,
            fut_symbol: None,
            atm_call_symbol: None,
            atm_put_symbol: None,
            atm_call_strike: None,
            atm_put_strike: None,
            spot: None,
            fut: None,
            atm_call: None,
            atm_put: None,
        }
    }

    fn to_file_block(&self) -> String {
        format!(
            "{} Spot: {}\n{} FUT: {}\n{} ATM CALL: {} -> {}\n{} ATM PUT: {} -> {}\n",
            self.underlying,
            format_optional_price(self.spot),
            self.underlying,
            format_optional_price(self.fut),
            self.underlying,
            format_optional_strike(self.atm_call_strike),
            format_optional_price(self.atm_call),
            self.underlying,
            format_optional_strike(self.atm_put_strike),
            format_optional_price(self.atm_put),
        )
    }
}

fn update_price(current: &mut Option<f64>, next: f64) -> bool {
    if current.is_some_and(|current| current == next) {
        return false;
    }

    *current = Some(next);
    true
}

fn update_target(
    target: &mut Option<String>,
    price: &mut Option<f64>,
    next: Option<String>,
) -> bool {
    if *target == next {
        return false;
    }

    *target = next;
    *price = None;
    true
}

#[derive(Debug)]
struct OptionTarget {
    symbol: String,
    strike: Option<f64>,
}

fn update_option_target(
    target: &mut Option<String>,
    strike: &mut Option<f64>,
    price: &mut Option<f64>,
    next: Option<OptionTarget>,
) -> bool {
    let next_symbol = next.as_ref().map(|target| target.symbol.clone());
    let next_strike = next.and_then(|target| target.strike);
    if *target == next_symbol && *strike == next_strike {
        return false;
    }

    *target = next_symbol;
    *strike = next_strike;
    *price = None;
    true
}

fn nearest_future_symbol(summary: &FyersUniverseSummary) -> Option<String> {
    summary
        .futures
        .iter()
        .filter(|instrument| instrument.instrument_type == InstrumentType::Fut)
        .filter(|instrument| instrument.tradable)
        .min_by(|left, right| {
            left.expiry
                .cmp(&right.expiry)
                .then_with(|| left.trading_symbol.cmp(&right.trading_symbol))
        })
        .map(|instrument| instrument.trading_symbol.clone())
}

fn nearest_option_target(
    summary: &FyersUniverseSummary,
    instrument_type: InstrumentType,
) -> Option<OptionTarget> {
    nearest_instrument(
        summary.atm_options.iter().filter(|instrument| {
            instrument.instrument_type == instrument_type && instrument.tradable
        }),
        summary.reference_price,
    )
}

fn nearest_instrument<'a>(
    instruments: impl Iterator<Item = &'a InstrumentDefinition>,
    reference_price: f64,
) -> Option<OptionTarget> {
    instruments
        .min_by(|left, right| {
            let left_distance = left.strike.unwrap_or(f64::MAX) - reference_price;
            let right_distance = right.strike.unwrap_or(f64::MAX) - reference_price;
            left_distance
                .abs()
                .total_cmp(&right_distance.abs())
                .then_with(|| left.expiry.cmp(&right.expiry))
                .then_with(|| left.trading_symbol.cmp(&right.trading_symbol))
        })
        .map(|instrument| OptionTarget {
            symbol: instrument.trading_symbol.clone(),
            strike: instrument.strike,
        })
}

fn format_optional_price(price: Option<f64>) -> String {
    price
        .map(|price| format!("{price:.4}"))
        .unwrap_or_else(|| "-".to_string())
}

fn format_optional_strike(strike: Option<f64>) -> String {
    strike.map(format_strike).unwrap_or_else(|| "-".to_string())
}

fn format_strike(strike: f64) -> String {
    let mut value = format!("{strike:.4}");
    while value.contains('.') && value.ends_with('0') {
        value.pop();
    }
    if value.ends_with('.') {
        value.pop();
    }
    value
}

fn tmp_path(path: &Path) -> Result<PathBuf, FeedError> {
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| FeedError::Config("invalid FYERS latest prices file path".to_string()))?;
    Ok(path.with_file_name(format!("{file_name}.tmp")))
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::feeder::InstrumentName;

    #[test]
    fn writes_latest_prices_for_multiple_underlyings() {
        let path = std::env::temp_dir().join(format!(
            "dhancred-fyers-latest-prices-test-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        ));
        let mut file = FyersLatestPriceFile::new(&path);
        let nifty = test_summary("NIFTY", "NSE:NIFTY50-INDEX", 24_050.0, 24_000.0);
        let banknifty = test_summary("BANKNIFTY", "NSE:NIFTYBANK-INDEX", 55_900.0, 56_000.0);

        file.set_spot_symbol("NIFTY", "NSE:NIFTY50-INDEX")
            .expect("nifty spot");
        file.set_spot_symbol("BANKNIFTY", "NSE:NIFTYBANK-INDEX")
            .expect("banknifty spot");
        file.update_targets_from_summary(&nifty)
            .expect("nifty targets");
        file.update_targets_from_summary(&banknifty)
            .expect("banknifty targets");
        file.update_tick(&InstrumentName::new("NSE:NIFTY50-INDEX"), 24_050.6)
            .expect("nifty spot tick");
        file.update_tick(&InstrumentName::new("NSE:NIFTY26APRFUT"), 24_070.25)
            .expect("nifty fut tick");
        file.update_tick(&InstrumentName::new("NSE:NIFTY26APR24000CE"), 210.5)
            .expect("nifty call tick");
        file.update_tick(&InstrumentName::new("NSE:NIFTY26APR24000PE"), 188.25)
            .expect("nifty put tick");
        file.update_tick(&InstrumentName::new("NSE:NIFTYBANK-INDEX"), 55_912.75)
            .expect("banknifty spot tick");

        assert_eq!(
            fs::read_to_string(path).expect("latest prices"),
            "BANKNIFTY Spot: 55912.7500\nBANKNIFTY FUT: -\nBANKNIFTY ATM CALL: 56000 -> -\nBANKNIFTY ATM PUT: 56000 -> -\nNIFTY Spot: 24050.6000\nNIFTY FUT: 24070.2500\nNIFTY ATM CALL: 24000 -> 210.5000\nNIFTY ATM PUT: 24000 -> 188.2500\n"
        );
    }

    fn test_summary(
        underlying: &str,
        reference_symbol: &str,
        reference_price: f64,
        strike: f64,
    ) -> FyersUniverseSummary {
        FyersUniverseSummary {
            selected_underlying: underlying.to_string(),
            reference_symbol: reference_symbol.to_string(),
            reference_price,
            spot_or_index_symbols: vec![reference_symbol.to_string()],
            futures: if underlying == "NIFTY" {
                vec![test_instrument(
                    "NSE:NIFTY26APRFUT",
                    underlying,
                    InstrumentType::Fut,
                    None,
                    Some("2026-04-30"),
                )]
            } else {
                Vec::new()
            },
            atm_options: vec![
                test_instrument(
                    &format!("NSE:{underlying}26APR{}CE", strike as i64),
                    underlying,
                    InstrumentType::Call,
                    Some(strike),
                    Some("2026-04-30"),
                ),
                test_instrument(
                    &format!("NSE:{underlying}26APR{}PE", strike as i64),
                    underlying,
                    InstrumentType::Put,
                    Some(strike),
                    Some("2026-04-30"),
                ),
            ],
        }
    }

    fn test_instrument(
        symbol: &str,
        underlying: &str,
        instrument_type: InstrumentType,
        strike: Option<f64>,
        expiry: Option<&str>,
    ) -> InstrumentDefinition {
        InstrumentDefinition {
            instrument_name: InstrumentName::new(symbol),
            instrument_type,
            strike,
            expiry: expiry.map(str::to_string),
            broker: "FYERS".to_string(),
            instrument_token: symbol.to_string(),
            trading_symbol: symbol.to_string(),
            exchange: "NSE".to_string(),
            segment: instrument_type.segment().to_string(),
            underlying: underlying.to_string(),
            lot_size: 1.0,
            tick_size: 0.05,
            tradable: true,
        }
    }
}
