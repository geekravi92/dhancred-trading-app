use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::str::FromStr;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::adapters::fyers::token::write_access_token;
use crate::config::AppConfig;
use crate::feeder::FeedError;

const FYERS_ACCESS_TOKEN_PATH: &str = "/admin/fyers/access-token";
const MAX_REQUEST_BYTES: usize = 16 * 1024;

pub struct AdminServerHandle {
    _handle: JoinHandle<()>,
}

pub fn start_admin_server(config: &AppConfig) -> Result<Option<AdminServerHandle>, FeedError> {
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

    let fyers_config =
        config.brokers.fyers.clone().ok_or_else(|| {
            FeedError::Config("admin API requires brokers.fyers config".to_string())
        })?;
    let access_token_file = fyers_config.access_token_file;
    let listener = TcpListener::bind(bind_addr)?;

    println!("Admin API listening on http://{bind_addr}");
    let handle = thread::spawn(move || {
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    if let Err(error) = handle_connection(stream, &access_token_file) {
                        eprintln!("admin API request failed: {error}");
                    }
                }
                Err(error) => eprintln!("admin API accept failed: {error}"),
            }
        }
    });

    Ok(Some(AdminServerHandle { _handle: handle }))
}

fn handle_connection(mut stream: TcpStream, access_token_file: &str) -> Result<(), FeedError> {
    stream.set_read_timeout(Some(Duration::from_secs(3)))?;
    let request = read_http_request(&mut stream)?;

    match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/health") => write_response(&mut stream, 200, "ok\n"),
        ("POST", FYERS_ACCESS_TOKEN_PATH) => {
            write_access_token(access_token_file, &request.body)?;
            write_response(&mut stream, 200, "FYERS access token updated\n")
        }
        _ => write_response(&mut stream, 404, "not found\n"),
    }
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
}
