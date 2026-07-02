use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::str::FromStr;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::adapters::angelone::auth::{AngeloneLoginSummary, login as login_angelone};
use crate::adapters::dbinternational::auth::{
    DbinternationalLoginSummary, login_all, login_interactive, login_market_data,
};
use crate::adapters::fyers::token::write_access_token;
use crate::config::{AngeloneBrokerSection, AppConfig, DbinternationalBrokerSection};
use crate::feeder::FeedError;
use crate::strategy::StrategyRuntimeHandle;

const DBINTERNATIONAL_LOGIN_PATH: &str = "/admin/dbinternational/login";
const DBINTERNATIONAL_MARKET_DATA_LOGIN_PATH: &str = "/admin/dbinternational/login/market-data";
const DBINTERNATIONAL_INTERACTIVE_LOGIN_PATH: &str = "/admin/dbinternational/login/interactive";
const ANGELONE_LOGIN_PATH: &str = "/admin/angelone/login";
const FYERS_ACCESS_TOKEN_PATH: &str = "/admin/fyers/access-token";
const STRATEGY_RELOAD_PATH: &str = "/admin/strategy/reload";
const MAX_REQUEST_BYTES: usize = 16 * 1024;

pub struct AdminServerHandle {
    _handle: JoinHandle<()>,
}

struct AdminState {
    fyers_access_token_file: Option<String>,
    dbinternational: Option<DbinternationalBrokerSection>,
    angelone: Option<AngeloneBrokerSection>,
    strategy_runtime: Option<Arc<StrategyRuntimeHandle>>,
}

pub fn start_admin_server(
    config: &AppConfig,
    strategy_runtime: Option<Arc<StrategyRuntimeHandle>>,
) -> Result<Option<AdminServerHandle>, FeedError> {
    let Some(admin_config) = &config.admin else {
        return Ok(None);
    };
    if !admin_config.enabled {
        return Ok(None);
    }

    let bind_addr = SocketAddr::from_str(&admin_config.bind_addr).map_err(|error| {
        FeedError::Config(format!(
            "invalid admin bind_addr {}: {error}",
            admin_config.bind_addr
        ))
    })?;
    if !bind_addr.ip().is_loopback() {
        return Err(FeedError::Config(format!(
            "admin bind_addr must be loopback only, got {bind_addr}"
        )));
    }

    let state = AdminState {
        fyers_access_token_file: config
            .brokers
            .fyers
            .as_ref()
            .map(|config| config.access_token_file.clone()),
        dbinternational: config.brokers.dbinternational.clone(),
        angelone: config.brokers.angelone.clone(),
        strategy_runtime,
    };
    let listener = TcpListener::bind(bind_addr)?;

    println!("Admin API listening on http://{bind_addr}");
    let handle = thread::spawn(move || {
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    if let Err(error) = handle_connection(stream, &state) {
                        eprintln!("admin API request failed: {error}");
                    }
                }
                Err(error) => eprintln!("admin API accept failed: {error}"),
            }
        }
    });

    Ok(Some(AdminServerHandle { _handle: handle }))
}

fn handle_connection(mut stream: TcpStream, state: &AdminState) -> Result<(), FeedError> {
    stream.set_read_timeout(Some(Duration::from_secs(3)))?;
    let request = read_http_request(&mut stream)?;

    match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/health") => write_response(&mut stream, 200, "ok\n"),
        ("POST", FYERS_ACCESS_TOKEN_PATH) => {
            let Some(access_token_file) = state.fyers_access_token_file.as_deref() else {
                return write_response(&mut stream, 404, "FYERS config disabled\n");
            };
            write_access_token(access_token_file, &request.body)?;
            write_response(&mut stream, 200, "FYERS access token updated\n")
        }
        ("POST", DBINTERNATIONAL_LOGIN_PATH) => {
            let Some(config) = dbinternational_config(state) else {
                return write_response(&mut stream, 404, "DBInternational config disabled\n");
            };
            write_login_result(&mut stream, login_all(config))
        }
        ("POST", DBINTERNATIONAL_MARKET_DATA_LOGIN_PATH) => {
            let Some(config) = dbinternational_config(state) else {
                return write_response(&mut stream, 404, "DBInternational config disabled\n");
            };
            write_login_result(
                &mut stream,
                login_market_data(config).map(|summary| vec![summary]),
            )
        }
        ("POST", DBINTERNATIONAL_INTERACTIVE_LOGIN_PATH) => {
            let Some(config) = dbinternational_config(state) else {
                return write_response(&mut stream, 404, "DBInternational config disabled\n");
            };
            write_login_result(
                &mut stream,
                login_interactive(config).map(|summary| vec![summary]),
            )
        }
        ("POST", ANGELONE_LOGIN_PATH) => {
            let Some(config) = angelone_config(state) else {
                return write_response(&mut stream, 404, "AngelOne config disabled\n");
            };
            let totp = request
                .body
                .trim()
                .split_whitespace()
                .next()
                .filter(|value| !value.is_empty());
            write_angelone_login_result(&mut stream, login_angelone(config, totp))
        }
        ("POST", STRATEGY_RELOAD_PATH) => {
            let Some(strategy_runtime) = state.strategy_runtime.as_ref() else {
                return write_response(&mut stream, 404, "strategy runtime disabled\n");
            };
            let count = strategy_runtime.reload_ssus()?;
            write_response(&mut stream, 200, &format!("reloaded {count} active SSUs\n"))
        }
        _ => write_response(&mut stream, 404, "not found\n"),
    }
}

fn dbinternational_config(state: &AdminState) -> Option<&DbinternationalBrokerSection> {
    state.dbinternational.as_ref()
}

fn angelone_config(state: &AdminState) -> Option<&AngeloneBrokerSection> {
    state.angelone.as_ref()
}

fn write_login_result(
    stream: &mut TcpStream,
    result: Result<Vec<DbinternationalLoginSummary>, FeedError>,
) -> Result<(), FeedError> {
    match result {
        Ok(summaries) => write_response(stream, 200, &format_login_summaries(&summaries)),
        Err(error) => write_response(stream, 500, &format!("login failed: {error}\n")),
    }
}

fn write_angelone_login_result(
    stream: &mut TcpStream,
    result: Result<AngeloneLoginSummary, FeedError>,
) -> Result<(), FeedError> {
    match result {
        Ok(summary) => write_response(stream, 200, &format_angelone_login_summary(&summary)),
        Err(error) => write_response(stream, 500, &format!("login failed: {error}\n")),
    }
}

fn format_login_summaries(summaries: &[DbinternationalLoginSummary]) -> String {
    if summaries.is_empty() {
        return "no logins executed\n".to_string();
    }

    let mut lines = Vec::with_capacity(summaries.len() + 1);
    lines.push("DBInternational login completed".to_string());
    for summary in summaries {
        lines.push(format!(
            "{} user_id={} token_file={} session_file={}",
            summary.kind.as_str(),
            summary.user_id.as_deref().unwrap_or("-"),
            summary.token_file,
            summary.session_file.as_deref().unwrap_or("-")
        ));
    }
    lines.push(String::new());
    lines.join("\n")
}

fn format_angelone_login_summary(summary: &AngeloneLoginSummary) -> String {
    format!(
        "AngelOne login completed\nclient_code={} session_file={}\n",
        summary.client_code, summary.session_file
    )
}

struct HttpRequest {
    method: String,
    path: String,
    body: String,
}

fn read_http_request(stream: &mut TcpStream) -> Result<HttpRequest, FeedError> {
    let mut data = Vec::new();
    let mut buffer = [0_u8; 1024];

    loop {
        let read = stream.read(&mut buffer)?;
        if read == 0 {
            break;
        }

        data.extend_from_slice(&buffer[..read]);
        if data.len() > MAX_REQUEST_BYTES {
            return Err(FeedError::Config("admin API request too large".to_string()));
        }

        if request_complete(&data)? {
            break;
        }
    }

    parse_http_request(&data)
}

fn request_complete(data: &[u8]) -> Result<bool, FeedError> {
    let Some(header_end) = find_header_end(data) else {
        return Ok(false);
    };

    let headers = std::str::from_utf8(&data[..header_end])
        .map_err(|error| FeedError::Parse(format!("invalid admin API request headers: {error}")))?;
    let content_length = content_length(headers)?;

    Ok(data.len() >= header_end + 4 + content_length)
}

fn parse_http_request(data: &[u8]) -> Result<HttpRequest, FeedError> {
    let header_end = find_header_end(data)
        .ok_or_else(|| FeedError::Parse("admin API request missing headers".to_string()))?;
    let headers = std::str::from_utf8(&data[..header_end])
        .map_err(|error| FeedError::Parse(format!("invalid admin API request headers: {error}")))?;
    let body_length = content_length(headers)?;
    let mut lines = headers.lines();
    let request_line = lines
        .next()
        .ok_or_else(|| FeedError::Parse("admin API request line missing".to_string()))?;
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts
        .next()
        .ok_or_else(|| FeedError::Parse("admin API request method missing".to_string()))?
        .to_string();
    let path = request_parts
        .next()
        .ok_or_else(|| FeedError::Parse("admin API request path missing".to_string()))?
        .to_string();
    let body_start = header_end + 4;
    let body_end = body_start + body_length;

    if data.len() < body_end {
        return Err(FeedError::Parse(
            "admin API request body incomplete".to_string(),
        ));
    }

    let body = std::str::from_utf8(&data[body_start..body_end])
        .map_err(|error| FeedError::Parse(format!("invalid admin API request body: {error}")))?
        .to_string();

    Ok(HttpRequest { method, path, body })
}

fn find_header_end(data: &[u8]) -> Option<usize> {
    data.windows(4).position(|window| window == b"\r\n\r\n")
}

fn content_length(headers: &str) -> Result<usize, FeedError> {
    for line in headers.lines().skip(1) {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.eq_ignore_ascii_case("content-length") {
            return value.trim().parse().map_err(|error| {
                FeedError::Parse(format!("invalid admin API content-length: {error}"))
            });
        }
    }

    Ok(0)
}

fn write_response(stream: &mut TcpStream, status: u16, body: &str) -> Result<(), FeedError> {
    let reason = match status {
        200 => "OK",
        404 => "Not Found",
        500 => "Internal Server Error",
        _ => "Error",
    };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\ncontent-type: text/plain\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );

    stream.write_all(response.as_bytes())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_plain_text_token_request() {
        let request = b"POST /admin/fyers/access-token HTTP/1.1\r\nhost: 127.0.0.1\r\ncontent-length: 11\r\n\r\ntoken-value";

        let parsed = parse_http_request(request).expect("request");

        assert_eq!(parsed.method, "POST");
        assert_eq!(parsed.path, FYERS_ACCESS_TOKEN_PATH);
        assert_eq!(parsed.body, "token-value");
    }

    #[test]
    fn formats_dbinternational_login_summary_without_token_value() {
        let body = format_login_summaries(&[DbinternationalLoginSummary {
            kind: crate::adapters::dbinternational::auth::DbinternationalLoginKind::MarketData,
            user_id: Some("ABC".to_string()),
            token_file: "runtime/secrets/token".to_string(),
            session_file: None,
        }]);

        assert!(body.contains("market_data user_id=ABC"));
        assert!(body.contains("token_file=runtime/secrets/token"));
        assert!(!body.contains("token-value"));
    }

    #[test]
    fn formats_angelone_login_summary_without_token_value() {
        let body = format_angelone_login_summary(&AngeloneLoginSummary {
            client_code: "ABC".to_string(),
            session_file: "runtime/secrets/angelone_session.json".to_string(),
        });

        assert!(body.contains("client_code=ABC"));
        assert!(body.contains("session_file=runtime/secrets/angelone_session.json"));
        assert!(!body.contains("jwt"));
    }
}
