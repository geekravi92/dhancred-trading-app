use crate::errors::{Result, SocketError};
use url::Url;

#[derive(Debug, Clone)]
pub struct ParsedUrl {
    pub protocol: String,
    pub host: String,
    pub port: u16,
    pub path: String,
    pub id: String,
    pub href: String,
}

pub fn parse(uri: Option<&str>) -> Result<ParsedUrl> {
    let uri = uri.unwrap_or("http://localhost");

    let url = if uri.starts_with('/') {
        // Relative path
        if uri.starts_with("//") {
            Url::parse(&format!("http:{}", uri))?
        } else {
            Url::parse(&format!("http://localhost{}", uri))?
        }
    } else if !uri.contains("://") {
        // Protocol-less URL
        Url::parse(&format!("https://{}", uri))?
    } else {
        Url::parse(uri)?
    };

    let protocol = url.scheme().to_string();
    let host = url
        .host_str()
        .ok_or_else(|| SocketError::Url("Invalid host".to_string()))?
        .to_string();

    let port = url.port().unwrap_or_else(|| match protocol.as_str() {
        "http" | "ws" => 80,
        "https" | "wss" => 443,
        _ => 80,
    });

    let mut path_and_query = url.path().to_string();
    if let Some(query) = url.query() {
        path_and_query.push('?');
        path_and_query.push_str(query);
    }
    if path_and_query.is_empty() {
        path_and_query = "/".to_string();
    }
    let path = url.path().to_string();
    let path = if path.is_empty() {
        "/".to_string()
    } else {
        path
    };
    // Handle IPv6
    let host_display = if host.contains(':') {
        format!("[{}]", host)
    } else {
        host.clone()
    };

    let id = format!("{}://{}:{}", protocol, host_display, port);
    let href = format!("{}://{}:{}{}", protocol, host_display, port, path_and_query);

    Ok(ParsedUrl {
        protocol,
        host,
        port,
        path,
        id,
        href,
    })
}

