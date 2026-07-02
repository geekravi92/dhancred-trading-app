use std::env;
use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use data_encoding::{BASE32, BASE32_NOPAD};
use reqwest::blocking::Client;
use reqwest::header::{ACCEPT, CONTENT_TYPE, HeaderMap, HeaderValue, USER_AGENT};
use ring::hmac;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::AngeloneBrokerSection;
use crate::feeder::FeedError;

const MAX_SESSION_BYTES: usize = 2 * 1024 * 1024;
const IST_OFFSET_SECONDS: u64 = 5 * 60 * 60 + 30 * 60;
const DAY_SECONDS: u64 = 86_400;
const TOTP_STEP_SECONDS: u64 = 30;
const TOTP_MODULUS: u32 = 1_000_000;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AngeloneLoginSummary {
    pub client_code: String,
    pub session_file: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AngeloneSavedSession {
    pub jwt_token: String,
    pub refresh_token: String,
    pub feed_token: String,
    pub client_code: String,
    pub api_key: String,
    pub login_url: String,
    pub created_at_utc: u64,
    pub response: Value,
}

#[derive(Serialize)]
struct LoginRequest<'a> {
    clientcode: &'a str,
    password: &'a str,
    totp: &'a str,
    state: &'a str,
}

pub fn login(
    config: &AngeloneBrokerSection,
    totp_override: Option<&str>,
) -> Result<AngeloneLoginSummary, FeedError> {
    let api_key = required_env(&config.api_key_env)?;
    let client_code = required_env(&config.client_code_env)?;
    let password = required_env(&config.password_env)?;
    let totp = totp_override
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| optional_env(config.totp_code_env.as_deref()))
        .map(Ok)
        .or_else(|| generate_totp_from_env(config).transpose())
        .transpose()?
        .ok_or_else(|| {
            FeedError::Config(
                "AngelOne login requires a current TOTP code via admin body/configured env or a configured TOTP secret"
                    .to_string(),
            )
        })?;

    let request = LoginRequest {
        clientcode: &client_code,
        password: &password,
        totp: &totp,
        state: &config.state,
    };
    let login_url = config.login_url();
    let client = Client::builder()
        .default_headers(default_headers()?)
        .user_agent("dhancred-trading-app/0.1")
        .build()?;
    let response = client
        .post(&login_url)
        .headers(angelone_headers(config, &api_key)?)
        .json(&request)
        .send()?;
    let status = response.status();
    let body = response.text()?;
    if !status.is_success() {
        return Err(FeedError::Http(format!(
            "AngelOne login failed status={} body={}",
            status.as_u16(),
            response_snippet(&body)
        )));
    }

    let response_json: Value = serde_json::from_str(&body)?;
    let session = parse_login_response(
        &response_json,
        client_code.clone(),
        api_key,
        login_url,
        now_unix_seconds(),
    )?;
    write_session_file(&config.session_file, &session)?;

    Ok(AngeloneLoginSummary {
        client_code,
        session_file: config.session_file.clone(),
    })
}

pub fn current_session(
    config: &AngeloneBrokerSection,
    now_utc: u64,
) -> Result<Option<AngeloneSavedSession>, FeedError> {
    let session = match read_session_file(&config.session_file) {
        Ok(session) => session,
        Err(FeedError::Io(message)) if message.contains("No such file") => return Ok(None),
        Err(error) => return Err(error),
    };

    if session_current_today(&session, now_utc) {
        Ok(Some(session))
    } else {
        Ok(None)
    }
}

pub fn read_session(config: &AngeloneBrokerSection) -> Result<AngeloneSavedSession, FeedError> {
    read_session_file(&config.session_file)
}

fn parse_login_response(
    response_json: &Value,
    client_code: String,
    api_key: String,
    login_url: String,
    created_at_utc: u64,
) -> Result<AngeloneSavedSession, FeedError> {
    if response_json.get("status").and_then(Value::as_bool) != Some(true) {
        return Err(FeedError::Http(format!(
            "AngelOne login returned non-success response: {}",
            response_snippet(&response_json.to_string())
        )));
    }

    let data = response_json
        .get("data")
        .ok_or_else(|| FeedError::Parse("AngelOne login missing data".to_string()))?;
    let jwt_token = required_string(data, "jwtToken")?;
    let refresh_token = required_string(data, "refreshToken")?;
    let feed_token = required_string(data, "feedToken")?;

    Ok(AngeloneSavedSession {
        jwt_token,
        refresh_token,
        feed_token,
        client_code,
        api_key,
        login_url,
        created_at_utc,
        response: response_json.clone(),
    })
}

fn default_headers() -> Result<HeaderMap, FeedError> {
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(
        USER_AGENT,
        HeaderValue::from_static("dhancred-trading-app/0.1"),
    );
    Ok(headers)
}

fn angelone_headers(config: &AngeloneBrokerSection, api_key: &str) -> Result<HeaderMap, FeedError> {
    let mut headers = default_headers()?;
    headers.insert("X-UserType", HeaderValue::from_static("USER"));
    headers.insert("X-SourceID", HeaderValue::from_static("WEB"));
    headers.insert(
        "X-ClientLocalIP",
        HeaderValue::from_str(&config.client_local_ip).map_err(|error| {
            FeedError::Config(format!("invalid AngelOne client_local_ip header: {error}"))
        })?,
    );
    headers.insert(
        "X-ClientPublicIP",
        HeaderValue::from_str(&config.client_public_ip).map_err(|error| {
            FeedError::Config(format!("invalid AngelOne client_public_ip header: {error}"))
        })?,
    );
    headers.insert(
        "X-MACAddress",
        HeaderValue::from_str(&config.mac_address).map_err(|error| {
            FeedError::Config(format!("invalid AngelOne mac_address header: {error}"))
        })?,
    );
    headers.insert(
        "X-PrivateKey",
        HeaderValue::from_str(api_key).map_err(|error| {
            FeedError::Config(format!("invalid AngelOne api key header: {error}"))
        })?,
    );
    Ok(headers)
}

fn write_session_file(path: &str, session: &AngeloneSavedSession) -> Result<(), FeedError> {
    if let Some(parent) = Path::new(path).parent() {
        fs::create_dir_all(parent)?;
    }

    let content = serde_json::to_string_pretty(session)?;
    if content.len() > MAX_SESSION_BYTES {
        return Err(FeedError::Config(
            "AngelOne session file exceeds size limit".to_string(),
        ));
    }
    fs::write(path, content)?;
    Ok(())
}

fn read_session_file(path: &str) -> Result<AngeloneSavedSession, FeedError> {
    let content = fs::read_to_string(path)
        .map_err(|error| FeedError::Io(format!("failed to read {path}: {error}")))?;
    if content.len() > MAX_SESSION_BYTES {
        return Err(FeedError::Config(format!("{path} exceeds size limit")));
    }
    let session: AngeloneSavedSession = serde_json::from_str(&content)?;
    validate_session(path, &session)?;
    Ok(session)
}

fn validate_session(path: &str, session: &AngeloneSavedSession) -> Result<(), FeedError> {
    if session.jwt_token.trim().is_empty() {
        return Err(FeedError::Config(format!("{path} missing jwt_token")));
    }
    if session.feed_token.trim().is_empty() {
        return Err(FeedError::Config(format!("{path} missing feed_token")));
    }
    if session.client_code.trim().is_empty() {
        return Err(FeedError::Config(format!("{path} missing client_code")));
    }
    if session.api_key.trim().is_empty() {
        return Err(FeedError::Config(format!("{path} missing api_key")));
    }
    Ok(())
}

fn session_current_today(session: &AngeloneSavedSession, now_utc: u64) -> bool {
    ist_day(session.created_at_utc) == ist_day(now_utc)
}

fn ist_day(epoch_seconds: u64) -> u64 {
    (epoch_seconds + IST_OFFSET_SECONDS) / DAY_SECONDS
}

fn required_string(value: &Value, key: &str) -> Result<String, FeedError> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| FeedError::Parse(format!("AngelOne login missing {key}")))
}

fn required_env(name: &str) -> Result<String, FeedError> {
    env::var(name).map_err(|_| FeedError::Config(format!("missing environment variable {name}")))
}

fn optional_env(name: Option<&str>) -> Option<String> {
    let name = name?;
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn generate_totp_from_env(config: &AngeloneBrokerSection) -> Result<Option<String>, FeedError> {
    let Some(secret) = optional_env(config.totp_secret_env.as_deref()) else {
        return Ok(None);
    };

    generate_totp(&secret, now_unix_seconds()).map(Some)
}

fn generate_totp(secret_value: &str, epoch_seconds: u64) -> Result<String, FeedError> {
    let secret = decode_totp_secret(secret_value)?;
    let counter = epoch_seconds / TOTP_STEP_SECONDS;
    let key = hmac::Key::new(hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY, &secret);
    let tag = hmac::sign(&key, &counter.to_be_bytes());
    let digest = tag.as_ref();
    let offset = (digest[digest.len() - 1] & 0x0f) as usize;
    let binary = ((u32::from(digest[offset]) & 0x7f) << 24)
        | (u32::from(digest[offset + 1]) << 16)
        | (u32::from(digest[offset + 2]) << 8)
        | u32::from(digest[offset + 3]);
    let code = binary % TOTP_MODULUS;

    Ok(format!("{code:06}"))
}

fn decode_totp_secret(secret_value: &str) -> Result<Vec<u8>, FeedError> {
    let secret = extract_totp_secret(secret_value);
    if secret.is_empty() {
        return Err(FeedError::Config(
            "invalid AngelOne TOTP secret: empty secret".to_string(),
        ));
    }

    let normalized = secret
        .chars()
        .filter(|ch| !ch.is_whitespace() && *ch != '-')
        .map(|ch| ch.to_ascii_uppercase())
        .collect::<String>();
    let decoded = if normalized.contains('=') {
        BASE32.decode(normalized.as_bytes())
    } else {
        BASE32_NOPAD.decode(normalized.as_bytes())
    };

    decoded.map_err(|error| {
        FeedError::Config(format!(
            "invalid AngelOne TOTP secret: expected base32 QR/setup secret ({error})"
        ))
    })
}

fn extract_totp_secret(secret_value: &str) -> String {
    let trimmed = secret_value.trim();
    if !trimmed.to_ascii_lowercase().starts_with("otpauth://") {
        return trimmed.to_string();
    }

    let Some((_, query)) = trimmed.split_once('?') else {
        return trimmed.to_string();
    };

    for part in query.split('&') {
        let Some((key, value)) = part.split_once('=') else {
            continue;
        };
        if key.eq_ignore_ascii_case("secret") {
            return percent_decode(value);
        }
    }

    trimmed.to_string()
}

fn percent_decode(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    let mut bytes = value.as_bytes().iter().copied().peekable();
    while let Some(byte) = bytes.next() {
        if byte == b'%' {
            let Some(high) = bytes.next() else {
                output.push('%');
                break;
            };
            let Some(low) = bytes.next() else {
                output.push('%');
                output.push(high as char);
                break;
            };
            if let (Some(high), Some(low)) = (hex_value(high), hex_value(low)) {
                output.push(char::from((high << 4) | low));
            } else {
                output.push('%');
                output.push(high as char);
                output.push(low as char);
            }
        } else if byte == b'+' {
            output.push(' ');
        } else {
            output.push(byte as char);
        }
    }
    output
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn now_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn response_snippet(body: &str) -> String {
    const MAX_CHARS: usize = 300;
    let snippet = body.replace(char::is_whitespace, " ");
    let snippet = snippet.trim();
    if snippet.chars().count() <= MAX_CHARS {
        snippet.to_string()
    } else {
        format!("{}...", snippet.chars().take(MAX_CHARS).collect::<String>())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_successful_login_response() {
        let response = serde_json::json!({
            "status": true,
            "message": "SUCCESS",
            "data": {
                "jwtToken": "jwt",
                "refreshToken": "refresh",
                "feedToken": "feed"
            }
        });

        let session = parse_login_response(
            &response,
            "CLIENT".to_string(),
            "API".to_string(),
            "https://example.test/login".to_string(),
            100,
        )
        .expect("session");

        assert_eq!(session.jwt_token, "jwt");
        assert_eq!(session.feed_token, "feed");
        assert_eq!(session.client_code, "CLIENT");
    }

    #[test]
    fn expires_session_on_next_ist_day() {
        let session = AngeloneSavedSession {
            jwt_token: "jwt".to_string(),
            refresh_token: "refresh".to_string(),
            feed_token: "feed".to_string(),
            client_code: "CLIENT".to_string(),
            api_key: "API".to_string(),
            login_url: "login".to_string(),
            created_at_utc: 18 * 3_600,
            response: serde_json::json!({}),
        };

        assert!(session_current_today(&session, 18 * 3_600 + 60));
        assert!(!session_current_today(&session, 19 * 3_600));
    }

    #[test]
    fn generates_totp_from_base32_secret() {
        let code = generate_totp("GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ", 59).expect("totp");

        assert_eq!(code, "287082");
    }

    #[test]
    fn generates_totp_from_otpauth_uri() {
        let code = generate_totp(
            "otpauth://totp/AngelOne:CLIENT?secret=GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ&issuer=AngelOne",
            59,
        )
        .expect("totp");

        assert_eq!(code, "287082");
    }
}
