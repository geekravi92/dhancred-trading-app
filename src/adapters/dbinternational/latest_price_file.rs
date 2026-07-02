use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::feeder::{FeedError, InstrumentName};

#[derive(Debug)]
pub struct DbinternationalLatestPriceFile {
    path: PathBuf,
    instruments: Vec<TrackedInstrumentPrice>,
}

impl DbinternationalLatestPriceFile {
    pub fn new(path: impl Into<PathBuf>, symbols: &[String]) -> Result<Self, FeedError> {
        let mut file = Self {
            path: path.into(),
            instruments: Vec::new(),
        };
        file.set_symbols(symbols)?;

        Ok(file)
    }

    pub fn set_symbols(&mut self, symbols: &[String]) -> Result<(), FeedError> {
        let existing_prices = self
            .instruments
            .iter()
            .map(|instrument| (instrument.symbol.clone(), instrument.price))
            .collect::<BTreeMap<_, _>>();
        let mut symbols = symbols
            .iter()
            .map(|symbol| symbol.trim())
            .filter(|symbol| !symbol.is_empty())
            .collect::<Vec<_>>();
        symbols.sort();
        symbols.dedup();

        let next_instruments = symbols
            .into_iter()
            .map(|symbol| TrackedInstrumentPrice {
                symbol: symbol.to_string(),
                price: existing_prices.get(symbol).copied().flatten(),
            })
            .collect::<Vec<_>>();

        if self.instruments != next_instruments {
            self.instruments = next_instruments;
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

        for instrument in &mut self.instruments {
            if instrument.symbol == symbol {
                changed |= update_price(&mut instrument.price, price);
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
            .instruments
            .iter()
            .map(TrackedInstrumentPrice::to_file_line)
            .collect::<Vec<_>>()
            .join("");
        fs::write(&tmp_path, content)?;
        fs::rename(tmp_path, &self.path)?;

        Ok(())
    }
}

#[derive(Debug, PartialEq)]
struct TrackedInstrumentPrice {
    symbol: String,
    price: Option<f64>,
}

impl TrackedInstrumentPrice {
    fn to_file_line(&self) -> String {
        format!("{}: {}\n", self.symbol, format_optional_price(self.price))
    }
}

fn update_price(current: &mut Option<f64>, next: f64) -> bool {
    if current.is_some_and(|current| current == next) {
        return false;
    }

    *current = Some(next);
    true
}

fn format_optional_price(price: Option<f64>) -> String {
    price
        .map(|price| format!("{price:.4}"))
        .unwrap_or_else(|| "-".to_string())
}

fn tmp_path(path: &Path) -> Result<PathBuf, FeedError> {
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| {
            FeedError::Config("invalid DBInternational latest prices file path".to_string())
        })?;
    Ok(path.with_file_name(format!("{file_name}.tmp")))
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    #[test]
    fn writes_latest_prices_for_configured_symbols() {
        let path = test_path("configured-symbols");
        let symbols = vec![
            "GOLD05AUG2026FUT".to_string(),
            "GOLDM03JUL2026FUT".to_string(),
            "GOLDTEN30JUN2026FUT".to_string(),
        ];
        let mut file = DbinternationalLatestPriceFile::new(&path, &symbols).expect("file");

        assert_eq!(
            fs::read_to_string(&path).expect("initial prices"),
            "GOLD05AUG2026FUT: -\nGOLDM03JUL2026FUT: -\nGOLDTEN30JUN2026FUT: -\n"
        );

        file.update_tick(&InstrumentName::new("GOLDM03JUL2026FUT"), 140_317.0)
            .expect("goldm tick");
        file.update_tick(&InstrumentName::new("GOLDTEN30JUN2026FUT"), 141_000.5)
            .expect("goldten tick");
        file.update_tick(&InstrumentName::new("SILVER05SEP2026FUT"), 150_000.0)
            .expect("ignored tick");

        assert_eq!(
            fs::read_to_string(path).expect("latest prices"),
            "GOLD05AUG2026FUT: -\nGOLDM03JUL2026FUT: 140317.0000\nGOLDTEN30JUN2026FUT: 141000.5000\n"
        );
    }

    #[test]
    fn deduplicates_configured_symbols() {
        let path = test_path("duplicate-symbols");
        let symbols = vec![
            "GOLD05AUG2026FUT".to_string(),
            "GOLD05AUG2026FUT".to_string(),
        ];

        DbinternationalLatestPriceFile::new(&path, &symbols).expect("file");

        assert_eq!(
            fs::read_to_string(path).expect("latest prices"),
            "GOLD05AUG2026FUT: -\n"
        );
    }

    #[test]
    fn replaces_symbols_and_preserves_existing_prices() {
        let path = test_path("replace-symbols");
        let symbols = vec!["GOLD".to_string(), "GOLD26JUNFUT".to_string()];
        let mut file = DbinternationalLatestPriceFile::new(&path, &symbols).expect("file");

        file.update_tick(&InstrumentName::new("GOLD"), 90_000.0)
            .expect("gold tick");
        file.set_symbols(&[
            "GOLD".to_string(),
            "SENSEX".to_string(),
            "SENSEX26JUNFUT".to_string(),
        ])
        .expect("set symbols");

        assert_eq!(
            fs::read_to_string(path).expect("latest prices"),
            "GOLD: 90000.0000\nSENSEX: -\nSENSEX26JUNFUT: -\n"
        );
    }

    fn test_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "dhancred-dbinternational-latest-prices-{name}-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        ))
    }
}
