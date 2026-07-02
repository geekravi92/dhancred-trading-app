use std::collections::{BTreeMap, BTreeSet};
use std::io::ErrorKind;
use std::net::TcpStream;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tungstenite::client::IntoClientRequest;
use tungstenite::http::HeaderValue;
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Message, WebSocket, connect};

use crate::adapters::angelone::auth::read_session;
use crate::adapters::angelone::master::{
    AngeloneInstrumentKey, instrument_key_from_token, price_divisor,
};
use crate::config::AngeloneBrokerSection;
use crate::feeder::{
    FeedError, InstrumentDefinition, InstrumentName, Price, PriceEvent, PriceTick, UnixMillis,
};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(25);
const READ_TIMEOUT: Duration = Duration::from_secs(1);
const LTP_PACKET_SIZE: usize = 51;

type AngeloneWebSocket = WebSocket<MaybeTlsStream<TcpStream>>;

pub struct AngeloneLiveFeeder {
    socket: AngeloneWebSocket,
    mode: u8,
    subscription_limit: usize,
    subscription_batch_size: usize,
    active_keys: BTreeSet<AngeloneInstrumentKey>,
    key_to_name: BTreeMap<AngeloneInstrumentKey, InstrumentName>,
    symbol_to_key: BTreeMap<String, AngeloneInstrumentKey>,
    last_heartbeat: Instant,
    log_to_console: bool,
}

impl AngeloneLiveFeeder {
    pub fn connect(
        config: &AngeloneBrokerSection,
        log_to_console: bool,
    ) -> Result<Self, FeedError> {
        let session = read_session(config)?;
        let mut request = config
            .websocket_url
            .as_str()
            .into_client_request()
            .map_err(|error| {
                FeedError::Config(format!("invalid AngelOne websocket url: {error}"))
            })?;
        let headers = request.headers_mut();
        headers.insert(
            "Authorization",
            header_value("Authorization", &session.jwt_token)?,
        );
        headers.insert("x-api-key", header_value("x-api-key", &session.api_key)?);
        headers.insert(
            "x-client-code",
            header_value("x-client-code", &session.client_code)?,
        );
        headers.insert(
            "x-feed-token",
            header_value("x-feed-token", &session.feed_token)?,
        );

        let (mut socket, _response) = connect(request)?;
        set_read_timeout(&mut socket, Some(READ_TIMEOUT))?;
        let now = Instant::now();

        Ok(Self {
            socket,
            mode: config.websocket_mode,
            subscription_limit: config.websocket_subscription_limit,
            subscription_batch_size: config.websocket_subscription_batch_size.max(1),
            active_keys: BTreeSet::new(),
            key_to_name: BTreeMap::new(),
            symbol_to_key: BTreeMap::new(),
            last_heartbeat: now.checked_sub(HEARTBEAT_INTERVAL).unwrap_or(now),
            log_to_console,
        })
    }

    pub fn subscribe_instruments(
        &mut self,
        instruments: &[InstrumentDefinition],
    ) -> Result<(), FeedError> {
        let mut resolved = Vec::new();
        let mut new_keys = BTreeSet::new();

        for instrument in instruments {
            if !instrument.tradable {
                continue;
            }
            let key = instrument_key_from_token(&instrument.instrument_token)?;
            if self.active_keys.contains(&key) {
                continue;
            }
            new_keys.insert(key.clone());
            resolved.push((
                key,
                instrument.trading_symbol.clone(),
                instrument.instrument_name.clone(),
            ));
        }

        if resolved.is_empty() {
            return Ok(());
        }
        if self.active_keys.len() + new_keys.len() > self.subscription_limit {
            return Err(FeedError::Config(format!(
                "AngelOne websocket subscription limit exceeded: active={} new={} limit={}",
                self.active_keys.len(),
                new_keys.len(),
                self.subscription_limit
            )));
        }

        self.send_subscription(1, &resolved)?;
        for (key, symbol, name) in resolved {
            self.active_keys.insert(key.clone());
            self.key_to_name.insert(key.clone(), name);
            self.symbol_to_key.insert(symbol, key);
        }

        Ok(())
    }

    pub fn unsubscribe_symbols(&mut self, symbols: &[String]) -> Result<(), FeedError> {
        let resolved = symbols
            .iter()
            .filter_map(|symbol| {
                self.symbol_to_key
                    .get(symbol)
                    .cloned()
                    .map(|key| (key, symbol.clone(), InstrumentName::new(symbol)))
            })
            .collect::<Vec<_>>();

        if resolved.is_empty() {
            return Ok(());
        }

        self.send_subscription(0, &resolved)?;
        for (key, symbol, _) in resolved {
            self.active_keys.remove(&key);
            self.key_to_name.remove(&key);
            self.symbol_to_key.remove(&symbol);
        }

        Ok(())
    }

    pub fn next_price_event(&mut self) -> Result<Option<PriceEvent>, FeedError> {
        loop {
            self.send_heartbeat_if_due()?;
            match self.socket.read() {
                Ok(Message::Binary(payload)) => {
                    if let Some(event) = parse_binary_tick(&payload, &self.key_to_name)? {
                        return Ok(Some(event));
                    }
                }
                Ok(Message::Text(text)) => {
                    if text.as_str() != "pong" && self.log_to_console {
                        println!("AngelOne websocket text: {text}");
                    }
                }
                Ok(Message::Ping(payload)) => {
                    self.socket.send(Message::Pong(payload))?;
                }
                Ok(Message::Pong(_)) | Ok(Message::Frame(_)) => {}
                Ok(Message::Close(frame)) => {
                    return Err(FeedError::Disconnected(format!(
                        "AngelOne websocket closed: {frame:?}"
                    )));
                }
                Err(tungstenite::Error::Io(error))
                    if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) =>
                {
                    return Ok(None);
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    fn send_subscription(
        &mut self,
        action: u8,
        resolved: &[(AngeloneInstrumentKey, String, InstrumentName)],
    ) -> Result<(), FeedError> {
        let mut grouped: BTreeMap<u8, Vec<String>> = BTreeMap::new();
        for (key, _, _) in resolved {
            grouped
                .entry(key.exchange_type)
                .or_default()
                .push(key.token.clone());
        }

        for (exchange_type, tokens) in grouped {
            for chunk in tokens.chunks(self.subscription_batch_size) {
                let request = SubscriptionRequest {
                    correlation_id: "dhancred01",
                    action,
                    params: SubscriptionParams {
                        mode: self.mode,
                        token_list: vec![SubscriptionTokenList {
                            exchange_type,
                            tokens: chunk.to_vec(),
                        }],
                    },
                };
                let payload = serde_json::to_string(&request)?;
                self.socket.send(Message::Text(payload.into()))?;
            }
        }

        Ok(())
    }

    fn send_heartbeat_if_due(&mut self) -> Result<(), FeedError> {
        if self.last_heartbeat.elapsed() < HEARTBEAT_INTERVAL {
            return Ok(());
        }
        self.socket.send(Message::Text("ping".into()))?;
        self.last_heartbeat = Instant::now();
        Ok(())
    }
}

#[derive(Serialize)]
struct SubscriptionRequest<'a> {
    #[serde(rename = "correlationID")]
    correlation_id: &'a str,
    action: u8,
    params: SubscriptionParams,
}

#[derive(Serialize)]
struct SubscriptionParams {
    mode: u8,
    #[serde(rename = "tokenList")]
    token_list: Vec<SubscriptionTokenList>,
}

#[derive(Serialize)]
struct SubscriptionTokenList {
    #[serde(rename = "exchangeType")]
    exchange_type: u8,
    tokens: Vec<String>,
}

fn parse_binary_tick(
    payload: &[u8],
    key_to_name: &BTreeMap<AngeloneInstrumentKey, InstrumentName>,
) -> Result<Option<PriceEvent>, FeedError> {
    if payload.len() < LTP_PACKET_SIZE {
        return Ok(None);
    }

    let mode = payload[0];
    if !matches!(mode, 1..=4) {
        return Ok(None);
    }
    let exchange_type = payload[1];
    let token = parse_token(&payload[2..27]);
    if token.is_empty() {
        return Ok(None);
    }
    let key = AngeloneInstrumentKey {
        exchange_type,
        token,
    };
    let instrument_name = key_to_name
        .get(&key)
        .cloned()
        .unwrap_or_else(|| InstrumentName::new(format!("{}:{}", key.exchange_type, key.token)));
    let exchange_timestamp = read_i64_le(payload, 35)?;
    let last_traded_price = read_i64_le(payload, 43)? as f64 / price_divisor(exchange_type);
    let price = Price::new(last_traded_price).map_err(|error| {
        FeedError::Parse(format!(
            "AngelOne tick invalid price {} for {}: {}",
            last_traded_price, instrument_name, error
        ))
    })?;
    let time = if exchange_timestamp > 0 {
        exchange_timestamp as u64
    } else {
        current_unix_millis()
    };

    Ok(Some(PriceEvent::Tick(PriceTick::new(
        instrument_name,
        price,
        UnixMillis::new(time),
    ))))
}

fn parse_token(bytes: &[u8]) -> String {
    let end = bytes
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..end]).trim().to_string()
}

fn read_i64_le(data: &[u8], offset: usize) -> Result<i64, FeedError> {
    let end = offset + 8;
    let bytes = data.get(offset..end).ok_or_else(|| {
        FeedError::Parse(format!(
            "AngelOne binary packet too short for i64 at offset {offset}"
        ))
    })?;
    Ok(i64::from_le_bytes(bytes.try_into().expect("8-byte slice")))
}

fn header_value(name: &str, value: &str) -> Result<HeaderValue, FeedError> {
    HeaderValue::from_str(value)
        .map_err(|error| FeedError::Config(format!("invalid AngelOne {name} header: {error}")))
}

fn set_read_timeout(
    socket: &mut AngeloneWebSocket,
    timeout: Option<Duration>,
) -> Result<(), FeedError> {
    match socket.get_mut() {
        MaybeTlsStream::Plain(stream) => stream.set_read_timeout(timeout)?,
        MaybeTlsStream::Rustls(stream) => stream.sock.set_read_timeout(timeout)?,
        _ => {}
    }
    Ok(())
}

fn current_unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_ltp_packet() {
        let mut payload = vec![0_u8; LTP_PACKET_SIZE];
        payload[0] = 1;
        payload[1] = 2;
        payload[2..7].copy_from_slice(b"61093");
        payload[27..35].copy_from_slice(&7_i64.to_le_bytes());
        payload[35..43].copy_from_slice(&1_756_800_000_000_i64.to_le_bytes());
        payload[43..51].copy_from_slice(&23_512_50_i64.to_le_bytes());
        let key = AngeloneInstrumentKey {
            exchange_type: 2,
            token: "61093".to_string(),
        };
        let names = BTreeMap::from([(key, InstrumentName::new("NIFTY28JUL26FUT"))]);

        let event = parse_binary_tick(&payload, &names)
            .expect("packet")
            .expect("event");

        let PriceEvent::Tick(tick) = event else {
            panic!("expected tick");
        };
        assert_eq!(tick.instrument_name.as_str(), "NIFTY28JUL26FUT");
        assert_eq!(tick.price.as_f64(), 23_512.5);
        assert_eq!(tick.time.as_u64(), 1_756_800_000_000);
    }
}
