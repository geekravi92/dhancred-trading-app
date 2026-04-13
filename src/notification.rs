use std::collections::BTreeMap;
use std::env;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use reqwest::blocking::Client;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum AlertSeverity {
    Info,
    Warn,
    Error,
    Critical,
}

impl AlertSeverity {
    fn as_str(self) -> &'static str {
        match self {
            Self::Info => "INFO",
            Self::Warn => "WARN",
            Self::Error => "ERROR",
            Self::Critical => "CRITICAL",
        }
    }

    fn from_env(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "info" => Self::Info,
            "warn" => Self::Warn,
            "critical" => Self::Critical,
            _ => Self::Error,
        }
    }
}

#[derive(Debug)]
struct FailureState {
    active: bool,
    last_message: String,
    last_sent_at: Instant,
}

#[derive(Debug)]
struct TelegramSink {
    bot_token: String,
    chat_id: String,
    client: Client,
}

#[derive(Debug)]
pub struct NotificationService {
    sink: Option<TelegramSink>,
    minimum_severity: AlertSeverity,
    dedupe_window: Duration,
    host: String,
    failure_states: Mutex<BTreeMap<String, FailureState>>,
}

static NOTIFICATION_SERVICE: OnceLock<NotificationService> = OnceLock::new();

pub fn init_notification_service() {
    let _ = notification_service();
}

pub fn notify_failure(
    key: impl AsRef<str>,
    component: impl AsRef<str>,
    severity: AlertSeverity,
    message: impl Into<String>,
) {
    notification_service().notify_failure(key.as_ref(), component.as_ref(), severity, message);
}

pub fn notify_recovery(
    key: impl AsRef<str>,
    component: impl AsRef<str>,
    message: impl Into<String>,
) {
    notification_service().notify_recovery(key.as_ref(), component.as_ref(), message);
}

fn notification_service() -> &'static NotificationService {
    NOTIFICATION_SERVICE.get_or_init(NotificationService::from_env)
}

impl NotificationService {
    fn from_env() -> Self {
        let bot_token = env::var("TELEGRAM_BOT_TOKEN").ok();
        let chat_id = env::var("TELEGRAM_CHAT_ID").ok();
        let sink = match (bot_token, chat_id) {
            (Some(bot_token), Some(chat_id))
                if !bot_token.trim().is_empty() && !chat_id.trim().is_empty() =>
            {
                let client = Client::builder()
                    .timeout(Duration::from_secs(5))
                    .build()
                    .unwrap_or_else(|error| {
                        eprintln!("Telegram notifier client build failed: {error}");
                        Client::new()
                    });
                Some(TelegramSink {
                    bot_token,
                    chat_id,
                    client,
                })
            }
            _ => None,
        };

        let minimum_severity = env::var("TELEGRAM_MIN_SEVERITY")
            .map(|value| AlertSeverity::from_env(&value))
            .unwrap_or(AlertSeverity::Error);
        let dedupe_window = env::var("TELEGRAM_DEDUPE_WINDOW_SECS")
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or_else(|| Duration::from_secs(15 * 60));
        let host = env::var("HOSTNAME").unwrap_or_else(|_| "unknown-host".to_string());

        Self {
            sink,
            minimum_severity,
            dedupe_window,
            host,
            failure_states: Mutex::new(BTreeMap::new()),
        }
    }

    fn notify_failure(
        &self,
        key: &str,
        component: &str,
        severity: AlertSeverity,
        message: impl Into<String>,
    ) {
        if severity < self.minimum_severity {
            return;
        }
        let message = sanitize_message(message.into());
        let now = Instant::now();
        let should_send = {
            let mut states = self
                .failure_states
                .lock()
                .expect("notification failure state mutex poisoned");
            match states.get_mut(key) {
                Some(state)
                    if state.active
                        && state.last_message == message
                        && now.duration_since(state.last_sent_at) < self.dedupe_window =>
                {
                    false
                }
                Some(state) => {
                    state.active = true;
                    state.last_message = message.clone();
                    state.last_sent_at = now;
                    true
                }
                None => {
                    states.insert(
                        key.to_string(),
                        FailureState {
                            active: true,
                            last_message: message.clone(),
                            last_sent_at: now,
                        },
                    );
                    true
                }
            }
        };

        if !should_send {
            return;
        }

        let text = format!(
            "DHANCRED ALERT\nHost: {}\nComponent: {}\nSeverity: {}\nMessage: {}",
            self.host,
            component,
            severity.as_str(),
            message
        );
        self.send(text);
    }

    fn notify_recovery(&self, key: &str, component: &str, message: impl Into<String>) {
        let message = sanitize_message(message.into());
        let should_send = {
            let mut states = self
                .failure_states
                .lock()
                .expect("notification failure state mutex poisoned");
            let Some(state) = states.get_mut(key) else {
                return;
            };
            if !state.active {
                return;
            }
            state.active = false;
            true
        };

        if !should_send {
            return;
        }

        let text = format!(
            "DHANCRED RECOVERY\nHost: {}\nComponent: {}\nMessage: {}",
            self.host, component, message
        );
        self.send(text);
    }

    fn send(&self, text: String) {
        let Some(sink) = &self.sink else {
            return;
        };

        #[derive(serde::Serialize)]
        struct TelegramRequest<'a> {
            chat_id: &'a str,
            text: &'a str,
            disable_web_page_preview: bool,
        }

        let request = TelegramRequest {
            chat_id: &sink.chat_id,
            text: &text,
            disable_web_page_preview: true,
        };
        let url = format!("https://api.telegram.org/bot{}/sendMessage", sink.bot_token);

        if let Err(error) = sink
            .client
            .post(url)
            .json(&request)
            .send()
            .and_then(|response| response.error_for_status())
        {
            eprintln!("Telegram notifier send failed: {error}");
        }
    }
}

fn sanitize_message(message: String) -> String {
    let trimmed = message.trim();
    let mut sanitized = trimmed
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join(" | ");
    if sanitized.len() > 3_000 {
        sanitized.truncate(3_000);
        sanitized.push_str("...");
    }
    sanitized
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitizes_multiline_messages() {
        let value = sanitize_message("  line 1 \n\n line 2  ".to_string());
        assert_eq!(value, "line 1 | line 2");
    }
}
