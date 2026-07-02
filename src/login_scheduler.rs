use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::adapters::angelone::auth::{
    current_session as current_angelone_session, login as login_angelone,
};
use crate::adapters::dbinternational::auth::{
    current_interactive_session, current_market_data_session, login_interactive, login_market_data,
};
use crate::config::{AppConfig, BrokersSection};
use crate::feeder::FeedError;
use crate::notification::{AlertSeverity, notify_failure, notify_recovery};

const IST_OFFSET_SECONDS: i64 = 5 * 60 * 60 + 30 * 60;
const DAY_SECONDS: i64 = 86_400;

pub struct LoginSchedulerHandle {
    _handle: JoinHandle<()>,
}

pub fn start_login_scheduler(
    config: &AppConfig,
) -> Result<Option<LoginSchedulerHandle>, FeedError> {
    let Some(scheduler_config) = &config.login_scheduler else {
        return Ok(None);
    };
    if !scheduler_config.enabled {
        return Ok(None);
    }

    let scheduled_second = parse_ist_hh_mm(&scheduler_config.time_ist)?;
    let weekdays_only = scheduler_config.weekdays_only;
    let brokers = config.feeder.feed_brokers.clone();
    let broker_configs = config.brokers.clone();

    println!(
        "Login scheduler enabled: time={} IST weekdays_only={} brokers={}",
        scheduler_config.time_ist,
        weekdays_only,
        brokers.join(",")
    );

    if should_run_catchup(now_unix_seconds(), scheduled_second, weekdays_only) {
        run_login_job(&broker_configs, &brokers);
    }

    let handle = thread::spawn(move || {
        loop {
            let now = now_unix_seconds();
            let next_run = next_scheduled_utc_epoch(now, scheduled_second, weekdays_only);
            let sleep_seconds = next_run.saturating_sub(now).max(1);
            println!(
                "Login scheduler sleeping {}s until next {} IST run",
                sleep_seconds,
                format_hh_mm(scheduled_second)
            );
            thread::sleep(Duration::from_secs(sleep_seconds));
            run_login_job(&broker_configs, &brokers);
        }
    });

    Ok(Some(LoginSchedulerHandle { _handle: handle }))
}

fn run_login_job(broker_configs: &BrokersSection, brokers: &[String]) {
    println!("Login scheduler started");
    for broker in brokers {
        if let Err(error) = login_broker(broker_configs, broker) {
            eprintln!("Login scheduler {broker} login failed: {error}");
            notify_failure(
                format!("login_scheduler:{broker}"),
                format!("LOGIN_SCHEDULER:{broker}"),
                AlertSeverity::Error,
                format!("daily login failed: {error}"),
            );
        } else {
            notify_recovery(
                format!("login_scheduler:{broker}"),
                format!("LOGIN_SCHEDULER:{broker}"),
                "daily login recovered",
            );
        }
    }
    println!("Login scheduler finished");
}

fn login_broker(broker_configs: &BrokersSection, broker: &str) -> Result<(), FeedError> {
    match broker.trim().to_ascii_uppercase().as_str() {
        "DBINTERNATIONAL" | "DB" => {
            let Some(config) = &broker_configs.dbinternational else {
                return Ok(());
            };

            let now = now_unix_seconds();
            if let Some(session) = current_market_data_session(config, now)? {
                println!(
                    "DBInternational market_data login skipped: current session user_id={}",
                    session.user_id.as_deref().unwrap_or("-")
                );
            } else {
                let summary = login_market_data(config)?;
                println!(
                    "DBInternational {} login ok user_id={} token_file={} session_file={}",
                    summary.kind.as_str(),
                    summary.user_id.as_deref().unwrap_or("-"),
                    summary.token_file,
                    summary.session_file.as_deref().unwrap_or("-")
                );
            }

            if let Some(session) = current_interactive_session(config, now)? {
                println!(
                    "DBInternational interactive login skipped: current session user_id={}",
                    session.user_id.as_deref().unwrap_or("-")
                );
            } else {
                let summary = login_interactive(config)?;
                println!(
                    "DBInternational {} login ok user_id={} token_file={} session_file={}",
                    summary.kind.as_str(),
                    summary.user_id.as_deref().unwrap_or("-"),
                    summary.token_file,
                    summary.session_file.as_deref().unwrap_or("-")
                );
            }
            Ok(())
        }
        "ANGELONE" | "ANGEL" | "ANGEL_ONE" => {
            let Some(config) = &broker_configs.angelone else {
                return Ok(());
            };

            let now = now_unix_seconds();
            if let Some(session) = current_angelone_session(config, now)? {
                println!(
                    "AngelOne login skipped: current session client_code={}",
                    session.client_code
                );
            } else {
                let summary = login_angelone(config, None)?;
                println!(
                    "AngelOne login ok client_code={} session_file={}",
                    summary.client_code, summary.session_file
                );
            }
            Ok(())
        }
        value => Err(FeedError::Config(format!(
            "unsupported login scheduler broker {value}"
        ))),
    }
}

fn parse_ist_hh_mm(value: &str) -> Result<u64, FeedError> {
    let Some((hours, minutes)) = value.trim().split_once(':') else {
        return Err(FeedError::Config(format!(
            "invalid login_scheduler.time_ist {value}; expected HH:MM"
        )));
    };
    let hours: u64 = hours.parse().map_err(|error| {
        FeedError::Config(format!("invalid login_scheduler.time_ist hour: {error}"))
    })?;
    let minutes: u64 = minutes.parse().map_err(|error| {
        FeedError::Config(format!("invalid login_scheduler.time_ist minute: {error}"))
    })?;
    if hours > 23 || minutes > 59 {
        return Err(FeedError::Config(format!(
            "invalid login_scheduler.time_ist {value}; expected 00:00 through 23:59"
        )));
    }

    Ok(hours * 60 * 60 + minutes * 60)
}

fn should_run_catchup(now_utc: u64, scheduled_second: u64, weekdays_only: bool) -> bool {
    let (ist_day, ist_second) = ist_day_and_second(now_utc);
    (!weekdays_only || is_weekday_ist_day(ist_day)) && ist_second >= scheduled_second
}

fn next_scheduled_utc_epoch(now_utc: u64, scheduled_second: u64, weekdays_only: bool) -> u64 {
    let (today_ist_day, _) = ist_day_and_second(now_utc);
    for day_offset in 0..14 {
        let candidate_day = today_ist_day + day_offset;
        if weekdays_only && !is_weekday_ist_day(candidate_day) {
            continue;
        }

        let candidate_utc =
            (candidate_day * DAY_SECONDS + scheduled_second as i64 - IST_OFFSET_SECONDS) as u64;
        if candidate_utc > now_utc {
            return candidate_utc;
        }
    }

    now_utc + DAY_SECONDS as u64
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

fn now_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn format_hh_mm(second_of_day: u64) -> String {
    format!(
        "{:02}:{:02}",
        second_of_day / 3_600,
        (second_of_day % 3_600) / 60
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_ist_hh_mm() {
        assert_eq!(parse_ist_hh_mm("08:05").unwrap(), 29_100);
        assert!(parse_ist_hh_mm("24:00").is_err());
    }

    #[test]
    fn schedules_next_weekday_login() {
        let friday_7_30_ist = 86_400 + (2 * 60 * 60);
        let friday_8_05_utc = 86_400 + (2 * 60 * 60 + 35 * 60);
        assert_eq!(
            next_scheduled_utc_epoch(friday_7_30_ist, 29_100, true),
            friday_8_05_utc
        );
    }
}
