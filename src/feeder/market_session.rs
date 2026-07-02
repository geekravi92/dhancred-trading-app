use std::collections::{BTreeMap, BTreeSet};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::config::MarketSessionSection;
use crate::feeder::FeedError;

const DAY_SECONDS: i64 = 86_400;
const IST_OFFSET_SECONDS: i32 = 5 * 60 * 60 + 30 * 60;

#[derive(Clone, Debug, Default)]
pub struct MarketSessionSchedule {
    policies: Vec<MarketSessionPolicy>,
    exchange_to_policy: BTreeMap<String, usize>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketSessionPolicy {
    id: String,
    exchanges: BTreeSet<String>,
    timezone: MarketTimezone,
    open_second: Option<u32>,
    close_second: Option<u32>,
    always_open: bool,
    connect_before_open_secs: u32,
    weekdays_only: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MarketTimezone {
    Utc,
    AsiaKolkata,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExchangeSessionStatus {
    Active,
    Inactive {
        next_open_utc: Option<u64>,
        sleep_seconds: u64,
    },
}

impl MarketSessionSchedule {
    pub fn from_configs(configs: &[MarketSessionSection]) -> Result<Self, FeedError> {
        let mut policies: Vec<MarketSessionPolicy> = Vec::new();
        let mut exchange_to_policy = BTreeMap::new();

        for config in configs {
            let policy = MarketSessionPolicy::from_config(config)?;
            let policy_index = policies.len();
            for exchange in &policy.exchanges {
                if let Some(existing_index) =
                    exchange_to_policy.insert(exchange.clone(), policy_index)
                {
                    return Err(FeedError::Config(format!(
                        "market session exchange {exchange} is configured in both {} and {}",
                        policies[existing_index].id, policy.id
                    )));
                }
            }
            policies.push(policy);
        }

        Ok(Self {
            policies,
            exchange_to_policy,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.policies.is_empty()
    }

    pub fn is_exchange_active(&self, exchange: &str, now_utc: u64) -> bool {
        self.policy_for_exchange(exchange)
            .is_none_or(|policy| policy.is_active_at(now_utc))
    }

    pub fn status_for_exchange(&self, exchange: &str, now_utc: u64) -> ExchangeSessionStatus {
        self.policy_for_exchange(exchange)
            .map(|policy| policy.status_at(now_utc))
            .unwrap_or(ExchangeSessionStatus::Active)
    }

    pub fn session_exchanges_for_exchange(&self, exchange: &str) -> Option<BTreeSet<String>> {
        self.policy_for_exchange(exchange)
            .map(|policy| policy.exchanges.clone())
    }

    pub fn policy_for_exchange(&self, exchange: &str) -> Option<&MarketSessionPolicy> {
        let exchange = exchange_key(exchange);
        self.exchange_to_policy
            .get(&exchange)
            .and_then(|index| self.policies.get(*index))
    }

    pub fn combined_status_for_exchanges(
        &self,
        exchanges: &BTreeSet<String>,
        now_utc: u64,
    ) -> ExchangeSessionStatus {
        if exchanges.is_empty() {
            return ExchangeSessionStatus::Active;
        }

        let mut next_open_utc = None;
        for exchange in exchanges {
            match self.status_for_exchange(exchange, now_utc) {
                ExchangeSessionStatus::Active => return ExchangeSessionStatus::Active,
                ExchangeSessionStatus::Inactive {
                    next_open_utc: Some(candidate),
                    ..
                } => {
                    if next_open_utc.is_none_or(|existing| candidate < existing) {
                        next_open_utc = Some(candidate);
                    }
                }
                ExchangeSessionStatus::Inactive { .. } => {}
            }
        }

        let sleep_seconds = next_open_utc
            .map(|value| value.saturating_sub(now_utc))
            .unwrap_or(DAY_SECONDS as u64);
        ExchangeSessionStatus::Inactive {
            next_open_utc,
            sleep_seconds,
        }
    }
}

impl MarketSessionPolicy {
    fn from_config(config: &MarketSessionSection) -> Result<Self, FeedError> {
        let id = config.id.trim();
        if id.is_empty() {
            return Err(FeedError::Config(
                "market_sessions.id cannot be empty".to_string(),
            ));
        }

        let exchanges = config
            .exchanges
            .iter()
            .map(|exchange| exchange_key(exchange))
            .filter(|exchange| !exchange.is_empty())
            .collect::<BTreeSet<_>>();
        if exchanges.is_empty() {
            return Err(FeedError::Config(format!(
                "market_sessions.{id}.exchanges cannot be empty"
            )));
        }

        let timezone = MarketTimezone::parse(&config.timezone, id)?;
        let (open_second, close_second) = if config.always_open {
            (None, None)
        } else {
            let open = config.open.as_deref().ok_or_else(|| {
                FeedError::Config(format!("market_sessions.{id}.open is required"))
            })?;
            let close = config.close.as_deref().ok_or_else(|| {
                FeedError::Config(format!("market_sessions.{id}.close is required"))
            })?;
            let open_second = parse_hh_mm(open, &format!("market_sessions.{id}.open"))?;
            let close_second = parse_hh_mm(close, &format!("market_sessions.{id}.close"))?;
            if close_second <= open_second {
                return Err(FeedError::Config(format!(
                    "market_sessions.{id}.close must be after open"
                )));
            }
            if config.connect_before_open_secs > u64::from(open_second) {
                return Err(FeedError::Config(format!(
                    "market_sessions.{id}.connect_before_open_secs is too large"
                )));
            }
            (Some(open_second), Some(close_second))
        };

        Ok(Self {
            id: id.to_string(),
            exchanges,
            timezone,
            open_second,
            close_second,
            always_open: config.always_open,
            connect_before_open_secs: config.connect_before_open_secs as u32,
            weekdays_only: config.weekdays_only,
        })
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn exchanges(&self) -> &BTreeSet<String> {
        &self.exchanges
    }

    pub fn candle_anchor_offset_seconds(&self) -> i32 {
        self.open_second
            .map(|second| second as i32 - self.timezone.offset_seconds())
            .unwrap_or_else(|| -self.timezone.offset_seconds())
    }

    pub fn candle_close_offset_seconds(&self) -> Option<i32> {
        self.close_second
            .map(|second| second as i32 - self.timezone.offset_seconds())
    }

    fn status_at(&self, now_utc: u64) -> ExchangeSessionStatus {
        if self.is_active_at(now_utc) {
            return ExchangeSessionStatus::Active;
        }

        let next_open_utc = self.next_open_utc(now_utc);
        ExchangeSessionStatus::Inactive {
            next_open_utc,
            sleep_seconds: next_open_utc
                .map(|value| value.saturating_sub(now_utc))
                .unwrap_or(DAY_SECONDS as u64),
        }
    }

    fn is_active_at(&self, now_utc: u64) -> bool {
        if self.always_open {
            return !self.weekdays_only || is_weekday_local_day(self.local_day(now_utc));
        }

        let (local_day, second_of_day) = self.local_day_and_second(now_utc);
        if self.weekdays_only && !is_weekday_local_day(local_day) {
            return false;
        }

        let Some(open_second) = self.open_second else {
            return true;
        };
        let Some(close_second) = self.close_second else {
            return true;
        };
        let connect_second = open_second - self.connect_before_open_secs;

        second_of_day >= connect_second && second_of_day < close_second
    }

    fn next_open_utc(&self, now_utc: u64) -> Option<u64> {
        if self.always_open {
            return Some(now_utc);
        }

        let (today_local_day, _) = self.local_day_and_second(now_utc);
        let open_second = self.open_second?;
        let connect_second = open_second - self.connect_before_open_secs;

        for day_offset in 0..14 {
            let candidate_day = today_local_day + day_offset;
            if self.weekdays_only && !is_weekday_local_day(candidate_day) {
                continue;
            }

            let start_utc = self.local_day_second_to_utc(candidate_day, connect_second);
            let close_utc = self.local_day_second_to_utc(candidate_day, self.close_second?);
            if now_utc >= start_utc && now_utc < close_utc {
                return Some(now_utc);
            }
            if now_utc < start_utc {
                return Some(start_utc);
            }
        }

        None
    }

    fn local_day(&self, now_utc: u64) -> i64 {
        self.local_day_and_second(now_utc).0
    }

    fn local_day_and_second(&self, now_utc: u64) -> (i64, u32) {
        let local_seconds = now_utc as i64 + i64::from(self.timezone.offset_seconds());
        (
            local_seconds.div_euclid(DAY_SECONDS),
            local_seconds.rem_euclid(DAY_SECONDS) as u32,
        )
    }

    fn local_day_second_to_utc(&self, local_day: i64, second_of_day: u32) -> u64 {
        (local_day * DAY_SECONDS + i64::from(second_of_day)
            - i64::from(self.timezone.offset_seconds())) as u64
    }
}

impl MarketTimezone {
    fn parse(value: &str, session_id: &str) -> Result<Self, FeedError> {
        match value.trim().to_ascii_uppercase().as_str() {
            "UTC" => Ok(Self::Utc),
            "ASIA/KOLKATA" | "IST" => Ok(Self::AsiaKolkata),
            other => Err(FeedError::Config(format!(
                "unsupported market_sessions.{session_id}.timezone {other}; expected UTC or Asia/Kolkata"
            ))),
        }
    }

    fn offset_seconds(self) -> i32 {
        match self {
            Self::Utc => 0,
            Self::AsiaKolkata => IST_OFFSET_SECONDS,
        }
    }
}

pub fn wait_for_any_exchange_session(
    configs: &[MarketSessionSection],
    exchanges: impl IntoIterator<Item = String>,
) -> Result<(), FeedError> {
    let schedule = MarketSessionSchedule::from_configs(configs)?;
    if schedule.is_empty() {
        return Ok(());
    }

    let exchanges = exchanges
        .into_iter()
        .map(|exchange| exchange_key(&exchange))
        .filter(|exchange| !exchange.is_empty())
        .collect::<BTreeSet<_>>();
    let now_utc = now_unix_seconds()?;

    match schedule.combined_status_for_exchanges(&exchanges, now_utc) {
        ExchangeSessionStatus::Active => Ok(()),
        ExchangeSessionStatus::Inactive {
            next_open_utc,
            sleep_seconds,
        } => {
            let next_open = next_open_utc
                .map(format_utc_epoch)
                .unwrap_or_else(|| "unknown".to_string());
            println!(
                "market sessions closed for {}; next connect window is {next_open}; sleeping {}s",
                exchanges.into_iter().collect::<Vec<_>>().join(","),
                sleep_seconds
            );
            thread::sleep(Duration::from_secs(sleep_seconds.max(1)));
            Ok(())
        }
    }
}

pub fn exchange_key(value: &str) -> String {
    value.trim().to_ascii_uppercase()
}

pub fn now_unix_seconds() -> Result<u64, FeedError> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| FeedError::Config(format!("system clock is before unix epoch: {error}")))?
        .as_secs())
}

fn parse_hh_mm(value: &str, field: &str) -> Result<u32, FeedError> {
    let Some((hours, minutes)) = value.trim().split_once(':') else {
        return Err(FeedError::Config(format!(
            "invalid {field} {value}; expected HH:MM"
        )));
    };
    let hours: u32 = hours
        .parse()
        .map_err(|error| FeedError::Config(format!("invalid {field} hour: {error}")))?;
    let minutes: u32 = minutes
        .parse()
        .map_err(|error| FeedError::Config(format!("invalid {field} minute: {error}")))?;
    if hours > 23 || minutes > 59 {
        return Err(FeedError::Config(format!(
            "invalid {field} {value}; expected 00:00 through 23:59"
        )));
    }

    Ok(hours * 3_600 + minutes * 60)
}

fn is_weekday_local_day(local_day: i64) -> bool {
    let monday_zero_weekday = (local_day + 3).rem_euclid(7);
    monday_zero_weekday <= 4
}

fn format_utc_epoch(utc_epoch: u64) -> String {
    let days = (utc_epoch / DAY_SECONDS as u64) as i64;
    let second_of_day = utc_epoch % DAY_SECONDS as u64;
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02} {:02}:{:02} UTC",
        second_of_day / 3_600,
        (second_of_day % 3_600) / 60
    )
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

#[cfg(test)]
mod tests {
    use super::*;

    fn section(id: &str, exchanges: &[&str], open: &str, close: &str) -> MarketSessionSection {
        MarketSessionSection {
            id: id.to_string(),
            exchanges: exchanges.iter().map(|value| value.to_string()).collect(),
            timezone: "Asia/Kolkata".to_string(),
            open: Some(open.to_string()),
            close: Some(close.to_string()),
            always_open: false,
            connect_before_open_secs: 300,
            weekdays_only: true,
        }
    }

    #[test]
    fn gates_nse_session_with_connect_window() {
        let schedule = MarketSessionSchedule::from_configs(&[section(
            "NSE_BSE",
            &["NSE", "NFO"],
            "09:15",
            "15:30",
        )])
        .expect("schedule");

        // 2026-07-02 09:10 IST, connect window open.
        assert!(schedule.is_exchange_active("NSE", 1_782_963_600));
        // 2026-07-02 15:31 IST, closed.
        assert!(!schedule.is_exchange_active("NFO", 1_782_986_460));
    }

    #[test]
    fn supports_mcx_late_close_and_crypto_always_open() {
        let crypto = MarketSessionSection {
            id: "CRYPTO".to_string(),
            exchanges: vec!["DELTA".to_string()],
            timezone: "UTC".to_string(),
            open: None,
            close: None,
            always_open: true,
            connect_before_open_secs: 0,
            weekdays_only: false,
        };
        let schedule = MarketSessionSchedule::from_configs(&[
            section("MCX", &["MCX"], "09:00", "23:30"),
            crypto,
        ])
        .expect("schedule");

        // 2026-07-02 23:00 IST.
        assert!(schedule.is_exchange_active("MCX", 1_783_013_400));
        // Saturday 2026-07-04 12:00 IST.
        assert!(!schedule.is_exchange_active("MCX", 1_783_148_400));
        assert!(schedule.is_exchange_active("DELTA", 1_783_148_400));
    }

    #[test]
    fn returns_exchange_group_for_derivative_filtering() {
        let schedule = MarketSessionSchedule::from_configs(&[section(
            "NSE_BSE",
            &["NSE", "NFO"],
            "09:15",
            "15:30",
        )])
        .expect("schedule");

        assert_eq!(
            schedule
                .session_exchanges_for_exchange("nse")
                .expect("group"),
            BTreeSet::from(["NFO".to_string(), "NSE".to_string()])
        );
    }
}
