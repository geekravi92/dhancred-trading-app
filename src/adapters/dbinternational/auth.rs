use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::{URL_SAFE, URL_SAFE_NO_PAD};
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::DbinternationalBrokerSection;
use crate::feeder::FeedError;

const MAX_TOKEN_BYTES: usize = 16 * 1024;
const MAX_SESSION_BYTES: usize = 2 * 1024 * 1024;
const IST_OFFSET_SECONDS: u64 = 5 * 60 * 60 + 30 * 60;
const DAY_SECONDS: u64 = 86_400;
const TOKEN_EXPIRY_GRACE_SECONDS: u64 = 60;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DbinternationalLoginKind {
    MarketData,
    Interactive,
}

impl DbinternationalLoginKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MarketData => "market_data",
            Self::Interactive => "interactive",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::MarketData => "DBInternational market data",
            Self::Interactive => "DBInternational interactive",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DbinternationalLoginSummary {
    pub kind: DbinternationalLoginKind,
    pub user_id: Option<String>,
    pub token_file: String,
    pub session_file: Option<String>,
}

#[derive(Serialize)]
struct LoginRequest {
    #[serde(rename = "secretKey")]
    secret_key: String,
    #[serde(rename = "appKey")]
    app_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    source: Option<String>,
    #[serde(rename = "uniqueKey", skip_serializing_if = "Option::is_none")]
    unique_key: Option<String>,
    #[serde(rename = "accessToken", skip_serializing_if = "Option::is_none")]
    access_token: Option<String>,
}

pub fn login_all(
    config: &DbinternationalBrokerSection,
) -> Result<Vec<DbinternationalLoginSummary>, FeedError> {
    Ok(vec![login_market_data(config)?, login_interactive(config)?])
}

pub fn current_market_data_session(
    config: &DbinternationalBrokerSection,
    now_utc: u64,
) -> Result<Option<DbinternationalSavedSession>, FeedError> {
    let Some(path) = config.market_data_session_file.as_deref() else {
        return Ok(None);
    };
    current_saved_session(path, DbinternationalLoginKind::MarketData, now_utc)
}

pub fn current_interactive_session(
    config: &DbinternationalBrokerSection,
    now_utc: u64,
) -> Result<Option<DbinternationalSavedSession>, FeedError> {
    let Some(path) = config.interactive_session_file.as_deref() else {
        return Ok(None);
    };
    current_saved_session(path, DbinternationalLoginKind::Interactive, now_utc)
}

pub fn login_market_data(
    config: &DbinternationalBrokerSection,
) -> Result<DbinternationalLoginSummary, FeedError> {
    let request = LoginRequest {
        secret_key: required_env(&config.market_data_secret_key_env)?,
        app_key: required_env(&config.market_data_app_key_env)?,
        source: None,
        unique_key: None,
        access_token: None,
    };
    let url = config.market_data_login_url();
    login_and_store(
        DbinternationalLoginKind::MarketData,
        &url,
        request,
        &config.market_data_token_file,
        config.market_data_session_file.as_deref(),
    )
}

pub fn login_interactive(
    config: &DbinternationalBrokerSection,
) -> Result<DbinternationalLoginSummary, FeedError> {
    let request = LoginRequest {
        secret_key: required_env(&config.interactive_secret_key_env)?,
        app_key: required_env(&config.interactive_app_key_env)?,
        source: Some("WebApi".to_string()),
        unique_key: optional_secret(
            config.interactive_unique_key_env.as_deref(),
            config.interactive_unique_key_file.as_deref(),
        )?,
        access_token: optional_secret(
            config.interactive_access_token_env.as_deref(),
            config.interactive_access_token_file.as_deref(),
        )?,
    };
    let url = config.interactive_login_url();
    login_and_store(
        DbinternationalLoginKind::Interactive,
        &url,
        request,
        &config.interactive_token_file,
        config.interactive_session_file.as_deref(),
    )
}

fn login_and_store(
    kind: DbinternationalLoginKind,
    url: &str,
    request: LoginRequest,
    token_file: &str,
    session_file: Option<&str>,
) -> Result<DbinternationalLoginSummary, FeedError> {
    let client = Client::builder()
        .user_agent("dhancred-trading-app/0.1")
        .build()?;
    let response = client.post(url).json(&request).send()?;
    let status = response.status();
    let body = response.text()?;

    if !status.is_success() {
        return Err(FeedError::Http(format!(
            "{} login failed url={} status={} body={}",
            kind.label(),
            url,
            status.as_u16(),
            sanitized_error_snippet(&body)
        )));
    }

    let response_json: Value = serde_json::from_str(&body)?;
    let parsed_session = parse_login_response(kind, &response_json)?;
    let session = build_saved_session(kind, url, parsed_session, &response_json)?;
    write_limited_file(
        token_file,
        &session.access_token,
        MAX_TOKEN_BYTES,
        "DBInternational token",
    )?;
    if let Some(session_file) = session_file {
        write_session_file(session_file, &session)?;
    }

    Ok(DbinternationalLoginSummary {
        kind,
        user_id: session.user_id,
        token_file: token_file.to_string(),
        session_file: session_file.map(str::to_string),
    })
}

#[derive(Debug, Eq, PartialEq)]
struct ParsedLoginSession {
    access_token: String,
    user_id: Option<String>,
    base_url: Option<String>,
}

fn parse_login_response(
    kind: DbinternationalLoginKind,
    response_json: &Value,
) -> Result<ParsedLoginSession, FeedError> {
    let envelope = response_envelope(response_json)
        .ok_or_else(|| FeedError::Parse(format!("{} login response is empty", kind.label())))?;

    let response_type = envelope.get("type").and_then(Value::as_str);
    if response_type != Some("success") {
        return Err(FeedError::Http(format!(
            "{} login returned non-success response: {}",
            kind.label(),
            response_snippet(&envelope.to_string())
        )));
    }

    let result = envelope
        .get("result")
        .ok_or_else(|| FeedError::Parse(format!("{} login missing result", kind.label())))?;
    let access_token =
        first_string_field(result, &["token", "accessToken", "access_token", "Token"])
            .or_else(|| {
                first_string_field(envelope, &["token", "accessToken", "access_token", "Token"])
            })
            .ok_or_else(|| FeedError::Parse(format!("{} login missing token", kind.label())))?
            .to_string();

    let user_id = first_string_field(
        result,
        &[
            "userID", "userId", "user_id", "ClientID", "clientID", "clientId",
        ],
    )
    .or_else(|| {
        first_string_field(
            envelope,
            &[
                "userID", "userId", "user_id", "ClientID", "clientID", "clientId",
            ],
        )
    })
    .map(str::to_string);
    let base_url = first_string_field(
        result,
        &[
            "base_url",
            "baseUrl",
            "baseURL",
            "hostUrl",
            "host_url",
            "connectionString",
            "ConnectionString",
        ],
    )
    .or_else(|| {
        first_string_field(
            envelope,
            &[
                "base_url",
                "baseUrl",
                "baseURL",
                "hostUrl",
                "host_url",
                "connectionString",
                "ConnectionString",
            ],
        )
    })
    .map(str::to_string);

    Ok(ParsedLoginSession {
        access_token,
        user_id,
        base_url,
    })
}

fn response_envelope(response_json: &Value) -> Option<&Value> {
    response_json
        .as_array()
        .and_then(|items| items.first())
        .or_else(|| {
            if response_json.is_object() {
                Some(response_json)
            } else {
                None
            }
        })
}

fn first_string_field<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a str> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_str))
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct DbinternationalSavedSession {
    pub broker: String,
    pub login_kind: String,
    logged_in_at_epoch_secs: u64,
    pub access_token: String,
    pub user_id: Option<String>,
    pub base_url: String,
    pub auth_url: String,
    pub response: Value,
}

fn build_saved_session(
    kind: DbinternationalLoginKind,
    auth_url: &str,
    parsed_session: ParsedLoginSession,
    response_json: &Value,
) -> Result<DbinternationalSavedSession, FeedError> {
    let base_url = session_base_url(kind, auth_url, parsed_session.base_url.as_deref())?;

    Ok(DbinternationalSavedSession {
        broker: "DBINTERNATIONAL".to_string(),
        login_kind: kind.as_str().to_string(),
        logged_in_at_epoch_secs: now_epoch_secs(),
        access_token: parsed_session.access_token,
        user_id: parsed_session.user_id,
        base_url,
        auth_url: auth_url.to_string(),
        response: response_json.clone(),
    })
}

fn write_session_file(
    path: impl AsRef<Path>,
    session: &DbinternationalSavedSession,
) -> Result<(), FeedError> {
    let content = serde_json::to_string_pretty(session)?;
    write_limited_file(path, &content, MAX_SESSION_BYTES, "DBInternational session")
}

pub fn read_market_data_session(
    config: &DbinternationalBrokerSection,
) -> Result<DbinternationalSavedSession, FeedError> {
    let Some(path) = config.market_data_session_file.as_deref() else {
        return Err(FeedError::Config(
            "DBInternational market_data_session_file is required for market-data APIs".to_string(),
        ));
    };
    read_saved_session(path, DbinternationalLoginKind::MarketData)
}

pub fn read_interactive_session(
    config: &DbinternationalBrokerSection,
) -> Result<DbinternationalSavedSession, FeedError> {
    let Some(path) = config.interactive_session_file.as_deref() else {
        return Err(FeedError::Config(
            "DBInternational interactive_session_file is required for interactive APIs".to_string(),
        ));
    };
    read_saved_session(path, DbinternationalLoginKind::Interactive)
}

fn read_saved_session(
    path: &str,
    expected_kind: DbinternationalLoginKind,
) -> Result<DbinternationalSavedSession, FeedError> {
    let content = fs::read_to_string(path)
        .map_err(|error| FeedError::Config(format!("failed to read {path}: {error}")))?;
    let session: DbinternationalSavedSession = serde_json::from_str(&content)?;
    if session.login_kind != expected_kind.as_str() {
        return Err(FeedError::Config(format!(
            "{path} contains {} session, expected {}",
            session.login_kind,
            expected_kind.as_str()
        )));
    }
    if session.access_token.trim().is_empty() {
        return Err(FeedError::Config(format!("{path} missing access_token")));
    }
    if session.base_url.trim().is_empty() {
        return Err(FeedError::Config(format!("{path} missing base_url")));
    }
    Ok(session)
}

fn current_saved_session(
    path: &str,
    expected_kind: DbinternationalLoginKind,
    now_utc: u64,
) -> Result<Option<DbinternationalSavedSession>, FeedError> {
    let session = match read_saved_session(path, expected_kind) {
        Ok(session) => session,
        Err(FeedError::Config(_)) | Err(FeedError::Parse(_)) => return Ok(None),
        Err(error) => return Err(error),
    };

    if !same_ist_day(session.logged_in_at_epoch_secs, now_utc) {
        return Ok(None);
    }
    if !token_valid_at(&session.access_token, now_utc) {
        return Ok(None);
    }

    Ok(Some(session))
}

fn token_valid_at(token: &str, now_utc: u64) -> bool {
    jwt_exp_epoch(token)
        .map(|exp| exp > now_utc.saturating_add(TOKEN_EXPIRY_GRACE_SECONDS))
        .unwrap_or(false)
}

fn jwt_exp_epoch(token: &str) -> Option<u64> {
    jwt_payload(token).and_then(|value| value.get("exp").and_then(Value::as_u64))
}

fn jwt_payload(token: &str) -> Option<Value> {
    let payload = token.split('.').nth(1)?;
    let decoded = URL_SAFE_NO_PAD
        .decode(payload)
        .or_else(|_| URL_SAFE.decode(payload))
        .ok()?;
    serde_json::from_slice(&decoded).ok()
}

fn same_ist_day(left_utc: u64, right_utc: u64) -> bool {
    ist_day(left_utc) == ist_day(right_utc)
}

fn ist_day(epoch_utc: u64) -> u64 {
    epoch_utc.saturating_add(IST_OFFSET_SECONDS) / DAY_SECONDS
}

fn session_base_url(
    kind: DbinternationalLoginKind,
    auth_url: &str,
    response_base_url: Option<&str>,
) -> Result<String, FeedError> {
    let raw_base_url = response_base_url.unwrap_or(auth_url);
    let base_url = normalize_login_url(kind, raw_base_url);
    if base_url.is_empty() {
        return Err(FeedError::Parse(format!(
            "{} login produced empty base_url",
            kind.label()
        )));
    }
    Ok(base_url)
}

fn normalize_login_url(kind: DbinternationalLoginKind, value: &str) -> String {
    let mut value = value.trim().trim_end_matches('/').to_string();
    let lower = value.to_ascii_lowercase();
    let suffix = match kind {
        DbinternationalLoginKind::MarketData => "/auth/login",
        DbinternationalLoginKind::Interactive => "/user/session",
    };
    if lower.ends_with(suffix) {
        value.truncate(value.len() - suffix.len());
    }
    value
}

fn required_env(name: &str) -> Result<String, FeedError> {
    let value = env::var(name)
        .map_err(|_| FeedError::Config(format!("missing environment variable {name}")))?;
    let value = value.trim();
    if value.is_empty() {
        return Err(FeedError::Config(format!(
            "environment variable {name} is empty"
        )));
    }
    Ok(value.to_string())
}

fn optional_secret(
    env_name: Option<&str>,
    file_path: Option<&str>,
) -> Result<Option<String>, FeedError> {
    if let Some(env_name) = env_name {
        if let Ok(value) = env::var(env_name) {
            let value = value.trim();
            if !value.is_empty() {
                return Ok(Some(value.to_string()));
            }
        }
    }

    let Some(file_path) = file_path else {
        return Ok(None);
    };
    let value = match fs::read_to_string(file_path) {
        Ok(value) => value,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(FeedError::Config(format!(
                "failed to read {file_path}: {error}"
            )));
        }
    };
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    Ok(Some(value.to_string()))
}

fn write_limited_file(
    path: impl AsRef<Path>,
    content: &str,
    max_bytes: usize,
    label: &str,
) -> Result<(), FeedError> {
    let content = content.trim();
    if content.is_empty() {
        return Err(FeedError::Config(format!(
            "{label} content cannot be empty"
        )));
    }
    if content.len() > max_bytes {
        return Err(FeedError::Config(format!(
            "{label} content is too large: {} bytes > {} bytes",
            content.len(),
            max_bytes
        )));
    }

    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let tmp_path = tmp_path_for(path)?;
    fs::write(&tmp_path, format!("{content}\n"))?;
    lock_down_file_permissions(&tmp_path)?;
    fs::rename(tmp_path, path)?;
    Ok(())
}

fn tmp_path_for(path: &Path) -> Result<PathBuf, FeedError> {
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| FeedError::Config("invalid DBInternational token path".to_string()))?;
    Ok(path.with_file_name(format!("{file_name}.tmp")))
}

fn response_snippet(value: &str) -> String {
    let normalized = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.len() > 500 {
        format!("{}...", &normalized[..500])
    } else {
        normalized
    }
}

fn sanitized_error_snippet(value: &str) -> String {
    let mut sanitized = value.to_string();
    for field in ["secretKey", "appKey", "accessToken", "uniqueKey", "token"] {
        sanitized = redact_after_pattern(&sanitized, &format!("\"{field}\":\""));
        sanitized = redact_after_pattern(&sanitized, &format!("\"{field}\" with value \""));
        sanitized = redact_after_pattern(&sanitized, &format!("\\\"{field}\\\" with value \\\""));
    }
    response_snippet(&sanitized)
}

fn redact_after_pattern(value: &str, pattern: &str) -> String {
    let mut output = String::with_capacity(value.len());
    let mut remaining = value;

    while let Some(index) = remaining.find(pattern) {
        output.push_str(&remaining[..index]);
        output.push_str(pattern);
        output.push_str("<redacted>");
        remaining = &remaining[index + pattern.len()..];

        if pattern.contains("\\\"") {
            let Some(end) = remaining.find("\\\"") else {
                return output;
            };
            remaining = &remaining[end..];
        } else {
            let Some(end) = remaining.find('"') else {
                return output;
            };
            remaining = &remaining[end..];
        }
    }

    output.push_str(remaining);
    output
}

fn now_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

#[cfg(unix)]
fn lock_down_file_permissions(path: &Path) -> Result<(), FeedError> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(not(unix))]
fn lock_down_file_permissions(_path: &Path) -> Result<(), FeedError> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_interactive_object_login_response() {
        let value = serde_json::json!({
            "type": "success",
            "result": {
                "token": "interactive-token",
                "userID": "SYMP"
            }
        });

        let session = parse_login_response(DbinternationalLoginKind::Interactive, &value).unwrap();

        assert_eq!(
            session,
            ParsedLoginSession {
                access_token: "interactive-token".to_string(),
                user_id: Some("SYMP".to_string()),
                base_url: None
            }
        );
    }

    #[test]
    fn parses_market_data_array_login_response() {
        let value = serde_json::json!([
            {
                "type": "success",
                "result": {
                    "token": "market-data-token",
                    "userID": "ABC",
                    "baseURL": "https://developers.symphonyfintech.in/apibinarymarketdata/auth/login"
                }
            }
        ]);

        let session = parse_login_response(DbinternationalLoginKind::MarketData, &value).unwrap();

        assert_eq!(
            session,
            ParsedLoginSession {
                access_token: "market-data-token".to_string(),
                user_id: Some("ABC".to_string()),
                base_url: Some(
                    "https://developers.symphonyfintech.in/apibinarymarketdata/auth/login"
                        .to_string()
                )
            }
        );
    }

    #[test]
    fn builds_saved_session_with_normalized_base_url() {
        let parsed = ParsedLoginSession {
            access_token: "token".to_string(),
            user_id: Some("ABC".to_string()),
            base_url: None,
        };
        let response = serde_json::json!({
            "type": "success",
            "result": { "token": "token", "userID": "ABC" }
        });

        let session = build_saved_session(
            DbinternationalLoginKind::MarketData,
            "https://developers.symphonyfintech.in/apibinarymarketdata/auth/login",
            parsed,
            &response,
        )
        .expect("saved session");

        assert_eq!(
            session.base_url,
            "https://developers.symphonyfintech.in/apibinarymarketdata"
        );
        assert_eq!(
            session.auth_url,
            "https://developers.symphonyfintech.in/apibinarymarketdata/auth/login"
        );
        assert_eq!(session.access_token, "token");
    }

    #[test]
    fn parses_access_token_and_connection_string_aliases() {
        let value = serde_json::json!({
            "type": "success",
            "result": {
                "accessToken": "alias-token",
                "clientID": "CLIENT1",
                "connectionString": "https://developers.symphonyfintech.in/1interactive/user/session"
            }
        });

        let parsed = parse_login_response(DbinternationalLoginKind::Interactive, &value).unwrap();
        let session = build_saved_session(
            DbinternationalLoginKind::Interactive,
            "https://fallback.example/1interactive/user/session",
            parsed,
            &value,
        )
        .expect("saved session");

        assert_eq!(session.access_token, "alias-token");
        assert_eq!(session.user_id, Some("CLIENT1".to_string()));
        assert_eq!(
            session.base_url,
            "https://developers.symphonyfintech.in/1interactive"
        );
    }

    #[test]
    fn rejects_non_success_login_response() {
        let value = serde_json::json!({
            "type": "error",
            "description": "bad credentials"
        });

        assert!(parse_login_response(DbinternationalLoginKind::MarketData, &value).is_err());
    }

    #[test]
    fn redacts_secret_values_from_error_snippet() {
        let body =
            r#"{"errors":[{"messages":["\"secretKey\" with value \"Secret123$Xy\" failed"]}]}"#;

        let snippet = sanitized_error_snippet(body);

        assert!(snippet.contains("<redacted>"));
        assert!(!snippet.contains("Secret123"));
    }

    #[test]
    fn detects_current_session_by_ist_day_and_jwt_expiry() {
        let session = DbinternationalSavedSession {
            broker: "DBINTERNATIONAL".to_string(),
            login_kind: DbinternationalLoginKind::MarketData.as_str().to_string(),
            logged_in_at_epoch_secs: 1_777_100_000,
            access_token: test_jwt_with_exp(1_777_186_400),
            user_id: Some("ABC".to_string()),
            base_url: "https://xts3.dbonlinetrade.com/apibinarymarketdata".to_string(),
            auth_url: "https://xts3.dbonlinetrade.com/apibinarymarketdata/auth/login".to_string(),
            response: serde_json::json!({}),
        };

        assert!(same_ist_day(session.logged_in_at_epoch_secs, 1_777_100_500));
        assert!(token_valid_at(&session.access_token, 1_777_100_500));
        assert!(!token_valid_at(&session.access_token, 1_777_186_400));
    }

    fn test_jwt_with_exp(exp: u64) -> String {
        test_jwt_with_payload(&format!(r#"{{"exp":{exp}}}"#))
    }

    fn test_jwt_with_payload(payload_json: &str) -> String {
        use base64::engine::general_purpose::URL_SAFE_NO_PAD;

        let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"none"}"#);
        let payload = URL_SAFE_NO_PAD.encode(payload_json);
        format!("{header}.{payload}.signature")
    }
}
