use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::adapters::angelone::master as angelone_master;
use crate::adapters::dbinternational::master as dbinternational_master;
use crate::adapters::delta::product_master::{DeltaProductClient, ensure_delta_master_csv};
use crate::adapters::fyers::master as fyers_master;
use crate::config::{AppConfig, BrokersSection};
use crate::feeder::FeedError;
use crate::notification::{AlertSeverity, notify_failure, notify_recovery};

// IST is a fixed UTC+05:30 offset and has no daylight-saving shift, so this
// scheduler does not need a timezone database just to run at 08:00 IST.
const IST_OFFSET_SECONDS: i64 = 5 * 60 * 60 + 30 * 60;
const DAY_SECONDS: i64 = 86_400;

pub struct MasterSchedulerHandle {
    _handle: JoinHandle<()>,
}

pub fn start_master_scheduler(
    config: &AppConfig,
) -> Result<Option<MasterSchedulerHandle>, FeedError> {
    let Some(scheduler_config) = &config.master_scheduler else {
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
        "Master scheduler enabled: time={} IST weekdays_only={} brokers={}",
        scheduler_config.time_ist,
        weekdays_only,
        brokers.join(",")
    );

    if should_run_catchup(now_unix_seconds(), scheduled_second, weekdays_only) {
        run_master_refresh_job(&broker_configs, &brokers);
    }

    let handle = thread::spawn(move || {
        loop {
            let now = now_unix_seconds();
            let next_run = next_scheduled_utc_epoch(now, scheduled_second, weekdays_only);
            let sleep_seconds = next_run.saturating_sub(now).max(1);
            println!(
                "Master scheduler sleeping {}s until next {} IST run",
                sleep_seconds,
                format_hh_mm(scheduled_second)
            );
            thread::sleep(Duration::from_secs(sleep_seconds));
            run_master_refresh_job(&broker_configs, &brokers);
        }
    });

    Ok(Some(MasterSchedulerHandle { _handle: handle }))
}

fn run_master_refresh_job(broker_configs: &BrokersSection, brokers: &[String]) {
    println!("Master scheduler refresh started");
    for broker in brokers {
        if let Err(error) = refresh_broker_master(broker_configs, broker) {
            eprintln!("Master scheduler {broker} refresh failed: {error}");
            notify_failure(
                format!("master_scheduler:{broker}"),
                format!("MASTER_SCHEDULER:{broker}"),
                AlertSeverity::Error,
                format!("master refresh failed: {error}"),
            );
        } else {
            notify_recovery(
                format!("master_scheduler:{broker}"),
                format!("MASTER_SCHEDULER:{broker}"),
                "master refresh recovered",
            );
        }
    }
    println!("Master scheduler refresh finished");
}

fn refresh_broker_master(broker_configs: &BrokersSection, broker: &str) -> Result<(), FeedError> {
    match broker.trim().to_ascii_uppercase().as_str() {
        "DELTA" => {
            let Some(config) = &broker_configs.delta else {
                return Ok(());
            };

            let client = DeltaProductClient::new(config.rest_url()?);
            ensure_delta_master_csv(&client, &config.master_csv)
        }
        "FYERS" => {
            let Some(config) = &broker_configs.fyers else {
                return Ok(());
            };

            fyers_master::refresh_all(config).map(|summaries| {
                for summary in summaries {
                    println!(
                        "FYERS {} | {} | {} instruments | {}",
                        summary.source,
                        if summary.downloaded {
                            "downloaded"
                        } else {
                            "cached"
                        },
                        summary.instrument_count,
                        summary.output_path.display()
                    );
                }
            })
        }
        "DBINTERNATIONAL" | "DB" => {
            let Some(config) = &broker_configs.dbinternational else {
                return Ok(());
            };

            dbinternational_master::ensure_master_current(config).map(|summary| {
                if summary.refreshed {
                    println!(
                        "DBInternational master refreshed: {} instruments | {} | {} indices | {}",
                        summary.instrument_count,
                        summary.output_path,
                        summary.index_count,
                        summary.index_output_path
                    );
                } else {
                    println!(
                        "DBInternational master skipped: current files have {} instruments | {} | {} indices | {}",
                        summary.instrument_count,
                        summary.output_path,
                        summary.index_count,
                        summary.index_output_path
                    );
                }
            })
        }
        "ANGELONE" | "ANGEL" | "ANGEL_ONE" => {
            let Some(config) = &broker_configs.angelone else {
                return Ok(());
            };

            angelone_master::ensure_master_current(config).map(|summary| {
                if summary.refreshed {
                    println!(
                        "AngelOne master refreshed: {} instruments | {}",
                        summary.instrument_count, summary.output_path
                    );
                } else {
                    println!(
                        "AngelOne master skipped: current file has {} instruments | {}",
                        summary.instrument_count, summary.output_path
                    );
                }
            })
        }
        value => Err(FeedError::Config(format!(
            "unsupported master scheduler broker {value}"
        ))),
    }
}

fn parse_ist_hh_mm(value: &str) -> Result<u64, FeedError> {
    let Some((hours, minutes)) = value.trim().split_once(':') else {
        return Err(FeedError::Config(format!(
            "invalid master_scheduler.time_ist {value}; expected HH:MM"
        )));
    };
    let hours: u64 = hours.parse().map_err(|error| {
        FeedError::Config(format!("invalid master_scheduler.time_ist hour: {error}"))
    })?;
    let minutes: u64 = minutes.parse().map_err(|error| {
        FeedError::Config(format!("invalid master_scheduler.time_ist minute: {error}"))
    })?;
    if hours > 23 || minutes > 59 {
        return Err(FeedError::Config(format!(
            "invalid master_scheduler.time_ist {value}; expected 00:00 through 23:59"
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
        assert_eq!(parse_ist_hh_mm("08:00").unwrap(), 28_800);
        assert!(parse_ist_hh_mm("24:00").is_err());
    }

    #[test]
    fn identifies_weekdays_in_ist() {
        let thursday_1970_ist_day = 0;
        assert!(is_weekday_ist_day(thursday_1970_ist_day));
        assert!(is_weekday_ist_day(1));
        assert!(!is_weekday_ist_day(2));
        assert!(!is_weekday_ist_day(3));
        assert!(is_weekday_ist_day(4));
    }

    #[test]
    fn schedules_next_weekday_8am_ist() {
        let friday_7_30_ist = 86_400 + (2 * 60 * 60);
        let friday_8am_utc = 86_400 + (2 * 60 * 60 + 30 * 60);
        assert_eq!(
            next_scheduled_utc_epoch(friday_7_30_ist, 28_800, true),
            friday_8am_utc
        );

        let friday_8_30_ist = 86_400 + (3 * 60 * 60);
        let monday_8am_utc = 4 * 86_400 + (2 * 60 * 60 + 30 * 60);
        assert_eq!(
            next_scheduled_utc_epoch(friday_8_30_ist, 28_800, true),
            monday_8am_utc
        );
    }
}
