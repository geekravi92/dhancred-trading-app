use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::config::FyersMarketSessionSection;
use crate::feeder::FeedError;

// India has no DST, so a fixed offset is enough for FYERS market-session gates.
const IST_OFFSET_SECONDS: i64 = 5 * 60 * 60 + 30 * 60;
const DAY_SECONDS: i64 = 86_400;

#[derive(Clone, Debug, Eq, PartialEq)]
struct MarketSessionPolicy {
    name: String,
    close_second: u64,
    connect_second: u64,
    weekdays_only: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ConnectWindowStatus {
    Open {
        session_name: String,
    },
    Closed {
        session_name: String,
        next_connect_utc: u64,
        sleep_seconds: u64,
    },
}

pub fn wait_for_market_session(
    config: Option<&[FyersMarketSessionSection]>,
) -> Result<(), FeedError> {
    let Some(configs) = config else {
        return Ok(());
    };

    let policies = MarketSessionPolicy::from_configs(configs)?;
    if policies.is_empty() {
        return Ok(());
    }

    match combined_status_at(&policies, now_unix_seconds()?) {
        ConnectWindowStatus::Open { .. } => Ok(()),
        ConnectWindowStatus::Closed {
            session_name,
            next_connect_utc,
            sleep_seconds,
        } => {
            println!(
                "FYERS market sessions closed; next connect window is {session_name} at {}; sleeping {}s without reconnect spam",
                format_ist_epoch(next_connect_utc),
                sleep_seconds
            );
            thread::sleep(Duration::from_secs(sleep_seconds.max(1)));
            Ok(())
        }
    }
}

impl MarketSessionPolicy {
    fn from_configs(configs: &[FyersMarketSessionSection]) -> Result<Vec<Self>, FeedError> {
        let mut policies = Vec::new();
        for config in configs {
            if config.enabled {
                policies.push(Self::from_config(config)?);
            }
        }
        Ok(policies)
    }

    fn from_config(config: &FyersMarketSessionSection) -> Result<Self, FeedError> {
        let name = config.name.trim();
        if name.is_empty() {
            return Err(FeedError::Config(
                "brokers.fyers.market_sessions.name cannot be empty".to_string(),
            ));
        }

        let timezone = config.timezone.trim();
        if timezone != "Asia/Kolkata" && timezone != "IST" {
            return Err(FeedError::Config(format!(
                "unsupported FYERS market_sessions.{name}.timezone {timezone}; expected Asia/Kolkata"
            )));
        }

        let open_second = parse_hh_mm(&config.open_ist, "brokers.fyers.market_sessions.open_ist")?;
        let close_second =
            parse_hh_mm(&config.close_ist, "brokers.fyers.market_sessions.close_ist")?;
        if close_second <= open_second {
            return Err(FeedError::Config(format!(
                "brokers.fyers.market_sessions.{name}.close_ist must be after open_ist"
            )));
        }
        if config.connect_before_open_secs > open_second {
            return Err(FeedError::Config(format!(
                "brokers.fyers.market_sessions.{name}.connect_before_open_secs is too large"
            )));
        }

        Ok(Self {
            name: name.to_string(),
            close_second,
            connect_second: open_second - config.connect_before_open_secs,
            weekdays_only: config.weekdays_only,
        })
    }

    fn status_at(&self, now_utc: u64) -> ConnectWindowStatus {
        let next_connect_utc = self.next_connect_utc(now_utc);
        if next_connect_utc == now_utc {
            ConnectWindowStatus::Open {
                session_name: self.name.clone(),
            }
        } else {
            ConnectWindowStatus::Closed {
                session_name: self.name.clone(),
                next_connect_utc,
                sleep_seconds: next_connect_utc.saturating_sub(now_utc),
            }
        }
    }

    fn next_connect_utc(&self, now_utc: u64) -> u64 {
        let (today_ist_day, _) = ist_day_and_second(now_utc);
        for day_offset in 0..14 {
            let candidate_day = today_ist_day + day_offset;
            if self.weekdays_only && !is_weekday_ist_day(candidate_day) {
                continue;
            }

            let start_utc = ist_day_second_to_utc(candidate_day, self.connect_second);
            let close_utc = ist_day_second_to_utc(candidate_day, self.close_second);
            if now_utc >= start_utc && now_utc < close_utc {
                return now_utc;
            }
            if now_utc < start_utc {
                return start_utc;
            }
        }

        now_utc + DAY_SECONDS as u64
    }
}

fn combined_status_at(policies: &[MarketSessionPolicy], now_utc: u64) -> ConnectWindowStatus {
    let mut next_window = None;

    for policy in policies {
        match policy.status_at(now_utc) {
            open @ ConnectWindowStatus::Open { .. } => return open,
            closed @ ConnectWindowStatus::Closed {
                next_connect_utc, ..
            } => {
                let replace = next_window
                    .as_ref()
                    .is_none_or(|existing: &ConnectWindowStatus| match existing {
                        ConnectWindowStatus::Open { .. } => false,
                        ConnectWindowStatus::Closed {
                            next_connect_utc: existing_utc,
                            ..
                        } => next_connect_utc < *existing_utc,
                    });
                if replace {
                    next_window = Some(closed);
                }
            }
        }
    }

    next_window.unwrap_or_else(|| ConnectWindowStatus::Closed {
        session_name: "UNKNOWN".to_string(),
        next_connect_utc: now_utc + DAY_SECONDS as u64,
        sleep_seconds: DAY_SECONDS as u64,
    })
}

fn parse_hh_mm(value: &str, field: &str) -> Result<u64, FeedError> {
    let Some((hours, minutes)) = value.trim().split_once(':') else {
        return Err(FeedError::Config(format!(
            "invalid {field} {value}; expected HH:MM"
        )));
    };
    let hours: u64 = hours
        .parse()
        .map_err(|error| FeedError::Config(format!("invalid {field} hour: {error}")))?;
    let minutes: u64 = minutes
        .parse()
        .map_err(|error| FeedError::Config(format!("invalid {field} minute: {error}")))?;
    if hours > 23 || minutes > 59 {
        return Err(FeedError::Config(format!(
            "invalid {field} {value}; expected 00:00 through 23:59"
        )));
    }

    Ok(hours * 3_600 + minutes * 60)
}

fn ist_day_second_to_utc(ist_day: i64, second_of_day: u64) -> u64 {
    (ist_day * DAY_SECONDS + second_of_day as i64 - IST_OFFSET_SECONDS) as u64
}

fn ist_day_and_second(now_utc: u64) -> (i64, u64) {
    let ist_seconds = now_utc as i64 + IST_OFFSET_SECONDS;
    (
        ist_seconds.div_euclid(DAY_SECONDS),
        ist_seconds.rem_euclid(DAY_SECONDS) as u64,
    )
}

fn is_weekday_ist_day(ist_day: i64) -> bool {
    let monday_zero_weekday = (ist_day + 3).rem_euclid(7);
    monday_zero_weekday <= 4
}

fn now_unix_seconds() -> Result<u64, FeedError> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| FeedError::Config(format!("system clock is before unix epoch: {error}")))?
        .as_secs())
}

fn format_ist_epoch(utc_epoch: u64) -> String {
    let (ist_day, second_of_day) = ist_day_and_second(utc_epoch);
    let (year, month, day) = civil_from_days(ist_day);
    format!(
        "{year:04}-{month:02}-{day:02} {:02}:{:02} IST",
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
    use crate::config::FyersMarketSessionSection;

    fn policy() -> MarketSessionPolicy {
        MarketSessionPolicy::from_config(&FyersMarketSessionSection {
            enabled: true,
            name: "NSE_BSE_EQUITY".to_string(),
            timezone: "Asia/Kolkata".to_string(),
            open_ist: "09:15".to_string(),
            close_ist: "15:30".to_string(),
            connect_before_open_secs: 300,
            weekdays_only: true,
        })
        .expect("policy")
    }

    fn utc_from_ist_day_second(day: i64, second: u64) -> u64 {
        ist_day_second_to_utc(day, second)
    }

    #[test]
    fn allows_connection_from_pre_open_until_close() {
        let policy = policy();
        let monday = 4;
        assert_eq!(
            policy.status_at(utc_from_ist_day_second(monday, 9 * 3_600 + 10 * 60)),
            ConnectWindowStatus::Open {
                session_name: "NSE_BSE_EQUITY".to_string()
            }
        );
        assert_eq!(
            policy.status_at(utc_from_ist_day_second(monday, 15 * 3_600 + 29 * 60)),
            ConnectWindowStatus::Open {
                session_name: "NSE_BSE_EQUITY".to_string()
            }
        );
    }

    #[test]
    fn waits_until_pre_open_before_market() {
        let policy = policy();
        let monday = 4;
        let now = utc_from_ist_day_second(monday, 9 * 3_600);
        let next = utc_from_ist_day_second(monday, 9 * 3_600 + 10 * 60);

        assert_eq!(
            policy.status_at(now),
            ConnectWindowStatus::Closed {
                session_name: "NSE_BSE_EQUITY".to_string(),
                next_connect_utc: next,
                sleep_seconds: 10 * 60
            }
        );
    }

    #[test]
    fn skips_weekend_when_weekdays_only() {
        let policy = policy();
        let sunday = 3;
        let monday = 4;
        let now = utc_from_ist_day_second(sunday, 12 * 3_600);
        let next = utc_from_ist_day_second(monday, 9 * 3_600 + 10 * 60);

        assert_eq!(
            policy.status_at(now),
            ConnectWindowStatus::Closed {
                session_name: "NSE_BSE_EQUITY".to_string(),
                next_connect_utc: next,
                sleep_seconds: next - now
            }
        );
    }

    #[test]
    fn moves_to_next_day_after_close() {
        let policy = policy();
        let monday = 4;
        let tuesday = 5;
        let now = utc_from_ist_day_second(monday, 15 * 3_600 + 30 * 60);
        let next = utc_from_ist_day_second(tuesday, 9 * 3_600 + 10 * 60);

        assert_eq!(
            policy.status_at(now),
            ConnectWindowStatus::Closed {
                session_name: "NSE_BSE_EQUITY".to_string(),
                next_connect_utc: next,
                sleep_seconds: next - now
            }
        );
    }

    #[test]
    fn combined_sessions_allow_connection_until_latest_open_session_close() {
        let equity = policy();
        let commodity = MarketSessionPolicy::from_config(&FyersMarketSessionSection {
            enabled: true,
            name: "MCX_NON_AGRI".to_string(),
            timezone: "Asia/Kolkata".to_string(),
            open_ist: "09:00".to_string(),
            close_ist: "23:30".to_string(),
            connect_before_open_secs: 300,
            weekdays_only: true,
        })
        .expect("commodity policy");
        let monday = 4;

        assert_eq!(
            combined_status_at(
                &[equity, commodity],
                utc_from_ist_day_second(monday, 20 * 3_600)
            ),
            ConnectWindowStatus::Open {
                session_name: "MCX_NON_AGRI".to_string()
            }
        );
    }

    #[test]
    fn combined_sessions_sleep_until_earliest_next_window() {
        let equity = policy();
        let commodity = MarketSessionPolicy::from_config(&FyersMarketSessionSection {
            enabled: true,
            name: "MCX_NON_AGRI".to_string(),
            timezone: "Asia/Kolkata".to_string(),
            open_ist: "09:00".to_string(),
            close_ist: "23:30".to_string(),
            connect_before_open_secs: 300,
            weekdays_only: true,
        })
        .expect("commodity policy");
        let sunday = 3;
        let monday = 4;
        let now = utc_from_ist_day_second(sunday, 12 * 3_600);
        let next = utc_from_ist_day_second(monday, 8 * 3_600 + 55 * 60);

        assert_eq!(
            combined_status_at(&[equity, commodity], now),
            ConnectWindowStatus::Closed {
                session_name: "MCX_NON_AGRI".to_string(),
                next_connect_utc: next,
                sleep_seconds: next - now
            }
        );
    }
}
