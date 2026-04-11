use std::fs;
use std::path::{Path, PathBuf};

use crate::adapters::delta::product_master::DeltaUniverseSummary;
use crate::feeder::{FeedError, InstrumentDefinition, InstrumentName, InstrumentType};

#[derive(Debug)]
pub struct DeltaLatestPriceFile {
    path: PathBuf,
    underlying: String,
    spot_symbol: Option<String>,
    perp_fut_symbol: Option<String>,
    atm_call_symbol: Option<String>,
    atm_put_symbol: Option<String>,
    atm_call_strike: Option<f64>,
    atm_put_strike: Option<f64>,
    btc_spot: Option<f64>,
    btc_perp_fut: Option<f64>,
    btc_atm_call: Option<f64>,
    btc_atm_put: Option<f64>,
}

impl DeltaLatestPriceFile {
    pub fn new(path: impl Into<PathBuf>, underlying: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            underlying: underlying.into(),
            spot_symbol: None,
            perp_fut_symbol: None,
            atm_call_symbol: None,
            atm_put_symbol: None,
            atm_call_strike: None,
            atm_put_strike: None,
            btc_spot: None,
            btc_perp_fut: None,
            btc_atm_call: None,
            btc_atm_put: None,
        }
    }

    pub fn set_spot_symbol(&mut self, symbol: &str) -> Result<(), FeedError> {
        if self.spot_symbol.as_deref() != Some(symbol) {
            self.spot_symbol = Some(symbol.to_string());
            self.btc_spot = None;
            self.write()?;
        }

        Ok(())
    }

    pub fn update_targets_from_summary(
        &mut self,
        summary: &DeltaUniverseSummary,
    ) -> Result<(), FeedError> {
        if summary.selected_underlying != self.underlying {
            return Ok(());
        }

        let perp_fut_symbol = nearest_future_symbol(summary, InstrumentType::PerpFut);
        let atm_call = nearest_option_target(summary, InstrumentType::Call);
        let atm_put = nearest_option_target(summary, InstrumentType::Put);

        let mut changed = update_target(
            &mut self.perp_fut_symbol,
            &mut self.btc_perp_fut,
            perp_fut_symbol,
        );
        changed |= update_option_target(
            &mut self.atm_call_symbol,
            &mut self.atm_call_strike,
            &mut self.btc_atm_call,
            atm_call,
        );
        changed |= update_option_target(
            &mut self.atm_put_symbol,
            &mut self.atm_put_strike,
            &mut self.btc_atm_put,
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

        if self.spot_symbol.as_deref() == Some(symbol) {
            changed |= update_price(&mut self.btc_spot, price);
        }
        if self.perp_fut_symbol.as_deref() == Some(symbol) {
            changed |= update_price(&mut self.btc_perp_fut, price);
        }
        if self.atm_call_symbol.as_deref() == Some(symbol) {
            changed |= update_price(&mut self.btc_atm_call, price);
        }
        if self.atm_put_symbol.as_deref() == Some(symbol) {
            changed |= update_price(&mut self.btc_atm_put, price);
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
        fs::write(
            &tmp_path,
            format!(
                "{} Spot: {}\n{} Perp FUT: {}\n{} ATM CALL: {} -> {}\n{} ATM PUT: {} -> {}\n",
                self.underlying,
                format_optional_price(self.btc_spot),
                self.underlying,
                format_optional_price(self.btc_perp_fut),
                self.underlying,
                format_optional_strike(self.atm_call_strike),
                format_optional_price(self.btc_atm_call),
                self.underlying,
                format_optional_strike(self.atm_put_strike),
                format_optional_price(self.btc_atm_put),
            ),
        )?;
        fs::rename(tmp_path, &self.path)?;

        Ok(())
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

fn nearest_future_symbol(
    summary: &DeltaUniverseSummary,
    instrument_type: InstrumentType,
) -> Option<String> {
    summary
        .futures
        .iter()
        .find(|instrument| instrument.instrument_type == instrument_type && instrument.tradable)
        .map(|instrument| instrument.trading_symbol.clone())
}

fn nearest_option_target(
    summary: &DeltaUniverseSummary,
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
        .ok_or_else(|| FeedError::Config("invalid Delta latest prices file path".to_string()))?;
    Ok(path.with_file_name(format!("{file_name}.tmp")))
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::feeder::{InstrumentDefinition, InstrumentName};

    use super::*;

    #[test]
    fn writes_exactly_four_latest_price_lines() {
        let path = std::env::temp_dir().join(format!(
            "dhancred-delta-latest-prices-test-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        ));
        let mut file = DeltaLatestPriceFile::new(&path, "BTC");
        let summary = test_summary();

        file.set_spot_symbol(".DEXBTUSD").expect("spot symbol");
        file.update_targets_from_summary(&summary).expect("targets");
        file.update_tick(&InstrumentName::new(".DEXBTUSD"), 72_500.25)
            .expect("spot tick");
        file.update_tick(&InstrumentName::new("BTCUSD"), 72_505.75)
            .expect("perp tick");
        file.update_tick(&InstrumentName::new("C-BTC-72500-120426"), 120.5)
            .expect("call tick");
        file.update_tick(&InstrumentName::new("P-BTC-72500-120426"), 101.25)
            .expect("put tick");

        assert_eq!(
            fs::read_to_string(path).expect("latest prices"),
            "BTC Spot: 72500.2500\nBTC Perp FUT: 72505.7500\nBTC ATM CALL: 72500 -> 120.5000\nBTC ATM PUT: 72500 -> 101.2500\n"
        );
    }

    fn test_summary() -> DeltaUniverseSummary {
        DeltaUniverseSummary {
            selected_underlying: "BTC".to_string(),
            reference_symbol: ".DEXBTUSD".to_string(),
            reference_price: 72_520.0,
            spot_or_index_symbols: vec![".DEXBTUSD".to_string()],
            futures: vec![test_instrument(
                "BTCUSD",
                InstrumentType::PerpFut,
                None,
                None,
            )],
            atm_options: vec![
                test_instrument(
                    "C-BTC-73000-120426",
                    InstrumentType::Call,
                    Some(73_000.0),
                    Some("2026-04-12"),
                ),
                test_instrument(
                    "C-BTC-72500-120426",
                    InstrumentType::Call,
                    Some(72_500.0),
                    Some("2026-04-12"),
                ),
                test_instrument(
                    "P-BTC-72500-120426",
                    InstrumentType::Put,
                    Some(72_500.0),
                    Some("2026-04-12"),
                ),
            ],
        }
    }

    fn test_instrument(
        symbol: &str,
        instrument_type: InstrumentType,
        strike: Option<f64>,
        expiry: Option<&str>,
    ) -> InstrumentDefinition {
        InstrumentDefinition {
            instrument_name: InstrumentName::new(symbol),
            instrument_type,
            strike,
            expiry: expiry.map(str::to_string),
            broker: "DELTA".to_string(),
            instrument_token: symbol.to_string(),
            trading_symbol: symbol.to_string(),
            exchange: "DELTA".to_string(),
            segment: instrument_type.segment().to_string(),
            underlying: "BTC".to_string(),
            lot_size: 1.0,
            tick_size: 0.1,
            tradable: true,
        }
    }
}
