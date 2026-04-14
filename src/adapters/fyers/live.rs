use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::net::TcpStream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::{URL_SAFE, URL_SAFE_NO_PAD};
use serde_json::Value;
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Message, WebSocket, connect};

use crate::adapters::fyers::token::jwt_access_token_only;
use crate::feeder::{
    FeedError, InstrumentDefinition, InstrumentName, Price, PriceEvent, PriceTick, UnixMillis,
};

const FYERS_SOURCE: &str = "dhancred-rust-0.1";
const FYERS_DEFAULT_CHANNEL: u8 = 11;
const FYERS_SENTINEL_I32: i32 = i32::MIN;
const FYERS_AUTH_RESP: u8 = 1;
const FYERS_SUBSCRIBE_RESP: u8 = 4;
const FYERS_UNSUBSCRIBE_RESP: u8 = 5;
const FYERS_DATAFEED_RESP: u8 = 6;
const FYERS_MODE_RESP: u8 = 12;
const FYERS_SNAPSHOT_FEED: u8 = 83;
const FYERS_FULL_FEED: u8 = 85;
const FYERS_LITE_FEED: u8 = 76;
const FYERS_LITE_MODE: u8 = 76;
const MAX_INVALID_FYERS_PRICE_LOGS: usize = 20;
static INVALID_FYERS_PRICE_LOGS: AtomicUsize = AtomicUsize::new(0);

type FyersWebSocket = WebSocket<MaybeTlsStream<TcpStream>>;

pub struct FyersLiveFeeder {
    socket: FyersWebSocket,
    access_token: String,
    active_symbols: BTreeSet<String>,
    symbol_to_hsm: BTreeMap<String, String>,
    parser: FyersHsmParser,
}

impl FyersLiveFeeder {
    pub fn connect(
        ws_url: &str,
        access_token: &str,
        log_control_messages: bool,
    ) -> Result<Self, FeedError> {
        let token_input = access_token.trim();
        let access_token = jwt_access_token_only(token_input)?.to_string();
        let hsm_key = decode_hsm_key(&access_token)?;
        let (mut socket, _response) = connect(ws_url)?;

        socket.send(Message::Binary(access_token_msg(&hsm_key).into()))?;
        socket.send(Message::Binary(lite_mode_msg().into()))?;
        std::thread::sleep(Duration::from_secs(2));

        Ok(Self {
            socket,
            access_token,
            active_symbols: BTreeSet::new(),
            symbol_to_hsm: BTreeMap::new(),
            parser: FyersHsmParser {
                log_control_messages,
                ..FyersHsmParser::default()
            },
        })
    }

    pub fn subscribe_instruments(
        &mut self,
        instruments: &[InstrumentDefinition],
    ) -> Result<(), FeedError> {
        let mut converted = BTreeMap::new();
        for instrument in instruments {
            if !instrument.tradable || self.active_symbols.contains(&instrument.trading_symbol) {
                continue;
            }
            let hsm_symbol = hsm_symbol_for_symbol_update(
                &instrument.trading_symbol,
                &instrument.instrument_token,
            )
            .ok_or_else(|| {
                FeedError::InvalidInstrument(format!(
                    "FYERS cannot build HSM symbol for {} token {}",
                    instrument.trading_symbol, instrument.instrument_token
                ))
            })?;
            converted.insert(hsm_symbol, instrument.trading_symbol.clone());
        }

        self.subscribe_converted(converted)
    }

    fn subscribe_converted(
        &mut self,
        converted: BTreeMap<String, String>,
    ) -> Result<(), FeedError> {
        if converted.is_empty() {
            return Ok(());
        }

        let hsm_symbols = converted.keys().cloned().collect::<Vec<_>>();
        for (hsm_symbol, symbol) in converted {
            self.parser
                .hsm_to_symbol
                .insert(hsm_symbol.clone(), symbol.clone());
            self.symbol_to_hsm.insert(symbol.clone(), hsm_symbol);
            self.active_symbols.insert(symbol);
        }

        for chunk in hsm_symbols.chunks(1_500) {
            self.socket.send(Message::Binary(
                subscription_msg(chunk, &self.access_token).into(),
            ))?;
            std::thread::sleep(Duration::from_millis(500));
        }

        Ok(())
    }

    pub fn unsubscribe_symbols(&mut self, symbols: &[String]) -> Result<(), FeedError> {
        let hsm_symbols = symbols
            .iter()
            .filter(|symbol| self.active_symbols.contains(*symbol))
            .filter_map(|symbol| self.symbol_to_hsm.get(symbol).cloned())
            .collect::<Vec<_>>();

        if hsm_symbols.is_empty() {
            return Ok(());
        }

        for chunk in hsm_symbols.chunks(1_500) {
            self.socket.send(Message::Binary(
                unsubscription_msg(chunk, &self.access_token).into(),
            ))?;
        }

        for symbol in symbols {
            self.active_symbols.remove(symbol);
            if let Some(hsm_symbol) = self.symbol_to_hsm.remove(symbol) {
                self.parser.hsm_to_symbol.remove(&hsm_symbol);
                self.parser.topics.remove(&hsm_symbol);
            }
        }

        Ok(())
    }

    pub fn next_price_event(&mut self) -> Result<Option<PriceEvent>, FeedError> {
        loop {
            if let Some(event) = self.parser.pending_events.pop_front() {
                return Ok(Some(event));
            }

            let message = self.socket.read()?;
            match message {
                Message::Binary(payload) => self.parser.handle_binary_message(&payload)?,
                Message::Text(_) => {}
                Message::Ping(payload) => {
                    self.socket.send(Message::Pong(payload))?;
                }
                Message::Pong(_) | Message::Frame(_) => {}
                Message::Close(frame) => {
                    return Err(FeedError::Disconnected(format!(
                        "FYERS websocket closed: {frame:?}"
                    )));
                }
            }
        }
    }
}

#[derive(Default)]
struct FyersHsmParser {
    hsm_to_symbol: BTreeMap<String, String>,
    topic_ids: HashMap<u16, String>,
    topics: HashMap<String, TopicState>,
    pending_events: VecDeque<PriceEvent>,
    log_control_messages: bool,
}

impl FyersHsmParser {
    fn handle_binary_message(&mut self, data: &[u8]) -> Result<(), FeedError> {
        let resp_type = *data
            .get(2)
            .ok_or_else(|| FeedError::Parse("FYERS binary frame too short".to_string()))?;

        match resp_type {
            FYERS_AUTH_RESP => self.handle_control_response("auth", data),
            FYERS_SUBSCRIBE_RESP => self.handle_control_response("subscribe", data),
            FYERS_UNSUBSCRIBE_RESP => self.handle_control_response("unsubscribe", data),
            FYERS_MODE_RESP => self.handle_control_response("mode", data),
            FYERS_DATAFEED_RESP => self.handle_datafeed_resp(data),
            _ => Ok(()),
        }
    }

    fn handle_control_response(&self, label: &str, data: &[u8]) -> Result<(), FeedError> {
        let status = control_response_status(data)?;
        if status == "K" {
            if self.log_control_messages {
                println!("FYERS websocket {label} response: OK");
            }
            Ok(())
        } else {
            Err(FeedError::Parse(format!(
                "FYERS websocket {label} response failed: {status}"
            )))
        }
    }

    fn handle_datafeed_resp(&mut self, data: &[u8]) -> Result<(), FeedError> {
        let mut offset = 7usize;
        let scrip_count = read_u16_be(data, &mut offset)?;

        for _ in 0..scrip_count {
            let feed_type = read_u8(data, &mut offset)?;
            match feed_type {
                FYERS_SNAPSHOT_FEED => self.parse_snapshot(data, &mut offset)?,
                FYERS_FULL_FEED => self.parse_full_update(data, &mut offset)?,
                FYERS_LITE_FEED => self.parse_lite_update(data, &mut offset)?,
                _ => {
                    return Err(FeedError::Parse(format!(
                        "unsupported FYERS datafeed type {feed_type}"
                    )));
                }
            }
        }

        Ok(())
    }

    fn parse_snapshot(&mut self, data: &[u8], offset: &mut usize) -> Result<(), FeedError> {
        let topic_id = read_u16_le(data, offset)?;
        let topic_name = read_string_u8(data, offset)?;
        let Some(kind) = TopicKind::from_hsm_symbol(&topic_name) else {
            return Err(FeedError::Parse(format!(
                "unsupported FYERS topic {topic_name}"
            )));
        };

        let field_count = read_u8(data, offset)?;
        let mut ltp_raw = None;
        let mut exch_feed_time = None;
        for index in 0..field_count {
            let value = read_i32_be(data, offset)?;
            if value == FYERS_SENTINEL_I32 {
                continue;
            }
            if kind.ltp_index() == index {
                ltp_raw = Some(value);
            }
            if kind.exch_feed_time_index() == Some(index) {
                exch_feed_time = Some(value as u64);
            }
        }

        skip_bytes(data, offset, 2)?;
        let multiplier = read_u16_be(data, offset)?;
        let precision = read_u8(data, offset)?;
        for _ in 0..3 {
            let _ = read_string_u8(data, offset)?;
        }

        self.topic_ids.insert(topic_id, topic_name.clone());
        let symbol = self
            .hsm_to_symbol
            .get(&topic_name)
            .cloned()
            .unwrap_or_else(|| topic_name.clone());
        let state = TopicState {
            symbol,
            kind,
            ltp_raw,
            multiplier,
            precision,
            exch_feed_time,
        };
        if let Some(event) = state.to_price_event()? {
            self.pending_events.push_back(event);
        }
        self.topics.insert(topic_name, state);

        Ok(())
    }

    fn parse_full_update(&mut self, data: &[u8], offset: &mut usize) -> Result<(), FeedError> {
        let topic_id = read_u16_le(data, offset)?;
        let field_count = read_u8(data, offset)?;
        let Some(topic_name) = self.topic_ids.get(&topic_id).cloned() else {
            skip_bytes(data, offset, field_count as usize * 4)?;
            return Ok(());
        };
        let Some(state) = self.topics.get_mut(&topic_name) else {
            skip_bytes(data, offset, field_count as usize * 4)?;
            return Ok(());
        };

        let mut updated = false;
        for index in 0..field_count {
            let value = read_i32_be(data, offset)?;
            if value == FYERS_SENTINEL_I32 {
                continue;
            }
            if state.kind.ltp_index() == index && state.ltp_raw != Some(value) {
                state.ltp_raw = Some(value);
                updated = true;
            }
            if state.kind.exch_feed_time_index() == Some(index) {
                state.exch_feed_time = Some(value as u64);
            }
        }

        if updated && let Some(event) = state.to_price_event()? {
            self.pending_events.push_back(event);
        }

        Ok(())
    }

    fn parse_lite_update(&mut self, data: &[u8], offset: &mut usize) -> Result<(), FeedError> {
        let topic_id = read_u16_le(data, offset)?;
        let value = read_i32_be(data, offset)?;
        let Some(topic_name) = self.topic_ids.get(&topic_id).cloned() else {
            return Ok(());
        };
        let Some(state) = self.topics.get_mut(&topic_name) else {
            return Ok(());
        };

        if value != FYERS_SENTINEL_I32 && state.ltp_raw != Some(value) {
            state.ltp_raw = Some(value);
            if let Some(event) = state.to_price_event()? {
                self.pending_events.push_back(event);
            }
        }

        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TopicKind {
    Scrip,
    Index,
}

impl TopicKind {
    fn from_hsm_symbol(value: &str) -> Option<Self> {
        if value.starts_with("sf|") {
            Some(Self::Scrip)
        } else if value.starts_with("if|") {
            Some(Self::Index)
        } else {
            None
        }
    }

    fn ltp_index(self) -> u8 {
        0
    }

    fn exch_feed_time_index(self) -> Option<u8> {
        match self {
            Self::Scrip => Some(3),
            Self::Index => Some(2),
        }
    }
}

#[derive(Clone, Debug)]
struct TopicState {
    symbol: String,
    kind: TopicKind,
    ltp_raw: Option<i32>,
    multiplier: u16,
    precision: u8,
    exch_feed_time: Option<u64>,
}

impl TopicState {
    fn to_price_event(&self) -> Result<Option<PriceEvent>, FeedError> {
        let Some(ltp_raw) = self.ltp_raw else {
            return Ok(None);
        };

        let divisor = 10_f64.powi(self.precision as i32) * f64::from(self.multiplier.max(1));
        let price_value = f64::from(ltp_raw) / divisor;
        let Some(price) = valid_fyers_price(&self.symbol, price_value) else {
            return Ok(None);
        };
        let time = self
            .exch_feed_time
            .map_or_else(current_unix_millis, |time| {
                if time > 1_000_000_000_000 {
                    time
                } else {
                    time * 1_000
                }
            });

        Ok(Some(PriceEvent::Tick(PriceTick::new(
            InstrumentName::new(&self.symbol),
            price,
            UnixMillis::new(time),
        ))))
    }
}

fn valid_fyers_price(symbol: &str, price: f64) -> Option<Price> {
    match Price::new(price) {
        Ok(price) => Some(price),
        Err(reason) => {
            log_ignored_fyers_price(symbol, price, reason);
            None
        }
    }
}

fn log_ignored_fyers_price(symbol: &str, price: f64, reason: &str) {
    let count = INVALID_FYERS_PRICE_LOGS.fetch_add(1, Ordering::Relaxed);
    if count >= MAX_INVALID_FYERS_PRICE_LOGS {
        return;
    }

    eprintln!("FYERS ignored invalid price: symbol={symbol} price={price} reason={reason}");

    if count + 1 == MAX_INVALID_FYERS_PRICE_LOGS {
        eprintln!("FYERS invalid price log limit reached; suppressing further invalid price logs");
    }
}

fn control_response_status(data: &[u8]) -> Result<String, FeedError> {
    if data.len() < 8 {
        return Err(FeedError::Parse(
            "FYERS control response too short".to_string(),
        ));
    }

    let length_bytes = [data[5], data[6]];
    for field_length in [
        u16::from_be_bytes(length_bytes) as usize,
        u16::from_le_bytes(length_bytes) as usize,
    ] {
        let end = 7 + field_length;
        if field_length > 0 && end <= data.len() {
            let value = String::from_utf8_lossy(&data[7..end])
                .trim_matches(char::from(0))
                .to_string();
            if !value.is_empty() {
                return Ok(value);
            }
        }
    }

    Err(FeedError::Parse(
        "FYERS control response status missing".to_string(),
    ))
}

fn decode_hsm_key(access_token: &str) -> Result<String, FeedError> {
    let jwt = jwt_access_token_only(access_token)?;
    let mut parts = jwt.split('.');
    let _header = parts.next();
    let payload = parts
        .next()
        .ok_or_else(|| FeedError::Config("FYERS access token is not a JWT".to_string()))?;

    let decoded = URL_SAFE_NO_PAD
        .decode(payload)
        .or_else(|_| URL_SAFE.decode(payload))
        .map_err(|error| FeedError::Config(format!("invalid FYERS JWT payload: {error}")))?;
    let value: Value = serde_json::from_slice(&decoded)?;
    let exp = value.get("exp").and_then(Value::as_u64).unwrap_or_default();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| FeedError::Config(format!("system clock is before unix epoch: {error}")))?
        .as_secs();
    if exp <= now {
        return Err(FeedError::Config(
            "FYERS access token is expired".to_string(),
        ));
    }

    value
        .get("hsm_key")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
        .ok_or_else(|| FeedError::Config("FYERS access token missing hsm_key".to_string()))
}

fn access_token_msg(hsm_key: &str) -> Vec<u8> {
    let mut data = Vec::new();
    let payload_len = 16 + hsm_key.len() + FYERS_SOURCE.len();
    data.extend_from_slice(&(payload_len as u16).to_be_bytes());
    data.push(1);
    data.push(4);
    append_bytes_field(&mut data, 1, hsm_key.as_bytes());
    append_bytes_field(&mut data, 2, b"P");
    append_bytes_field(&mut data, 3, &[1]);
    append_bytes_field(&mut data, 4, FYERS_SOURCE.as_bytes());
    data
}

fn lite_mode_msg() -> Vec<u8> {
    let mut data = Vec::new();
    data.extend_from_slice(&0u16.to_be_bytes());
    data.push(12);
    data.push(2);
    let channel_bits = 1u64 << FYERS_DEFAULT_CHANNEL;
    append_bytes_field(&mut data, 1, &channel_bits.to_be_bytes());
    append_bytes_field(&mut data, 2, &[FYERS_LITE_MODE]);
    data
}

fn subscription_msg(symbols: &[String], access_token: &str) -> Vec<u8> {
    hsm_symbols_msg(4, symbols, access_token)
}

fn unsubscription_msg(symbols: &[String], access_token: &str) -> Vec<u8> {
    hsm_symbols_msg(5, symbols, access_token)
}

fn hsm_symbols_msg(request_type: u8, symbols: &[String], access_token: &str) -> Vec<u8> {
    let scrips_data = scrips_data(symbols);
    let data_len = 18 + scrips_data.len() + access_token.len() + FYERS_SOURCE.len();
    let mut data = Vec::new();
    data.extend_from_slice(&(data_len as u16).to_be_bytes());
    data.push(request_type);
    data.push(2);
    append_bytes_field(&mut data, 1, &scrips_data);
    append_bytes_field(&mut data, 2, &[FYERS_DEFAULT_CHANNEL]);
    data
}

fn scrips_data(symbols: &[String]) -> Vec<u8> {
    let mut data = Vec::new();
    data.extend_from_slice(&(symbols.len() as u16).to_be_bytes());
    for symbol in symbols {
        let symbol_bytes = symbol.as_bytes();
        data.push(symbol_bytes.len() as u8);
        data.extend_from_slice(symbol_bytes);
    }
    data
}

fn append_bytes_field(data: &mut Vec<u8>, field_id: u8, value: &[u8]) {
    data.push(field_id);
    data.extend_from_slice(&(value.len() as u16).to_be_bytes());
    data.extend_from_slice(value);
}

fn hsm_symbol_for_symbol_update(symbol: &str, fy_token: &str) -> Option<String> {
    let exchange_segment = fy_token.get(..4)?;
    let segment = hsm_segment(exchange_segment)?;
    let upper_symbol = symbol.to_ascii_uppercase();
    let symbol_parts = upper_symbol.split('-').collect::<Vec<_>>();
    if symbol_parts.len() > 1 && symbol_parts.last() == Some(&"INDEX") {
        let index_name = fyers_index_name(&upper_symbol)
            .map(str::to_string)
            .or_else(|| {
                upper_symbol
                    .split(':')
                    .nth(1)
                    .and_then(|value| value.split('-').next())
                    .map(str::to_string)
            })?;
        Some(format!("if|{segment}|{index_name}"))
    } else {
        let exchange_token = fy_token.get(10..)?;
        Some(format!("sf|{segment}|{exchange_token}"))
    }
}

fn hsm_segment(exchange_segment: &str) -> Option<&'static str> {
    match exchange_segment {
        "1010" => Some("nse_cm"),
        "1011" => Some("nse_fo"),
        "1120" => Some("mcx_fo"),
        "1210" => Some("bse_cm"),
        "1012" => Some("cde_fo"),
        "1211" => Some("bse_fo"),
        "1212" => Some("bcs_fo"),
        "1020" => Some("nse_com"),
        _ => None,
    }
}

fn fyers_index_name(symbol: &str) -> Option<&'static str> {
    match symbol {
        "NSE:NIFTY50-INDEX" => Some("Nifty 50"),
        "NSE:NIFTYBANK-INDEX" => Some("Nifty Bank"),
        "BSE:SENSEX-INDEX" => Some("SENSEX"),
        "NSE:FINNIFTY-INDEX" => Some("Nifty Fin Service"),
        "NSE:MIDCPNIFTY-INDEX" | "NSE:NIFTYMIDSELECT-INDEX" => Some("NIFTY MID SELECT"),
        _ => None,
    }
}

fn read_u8(data: &[u8], offset: &mut usize) -> Result<u8, FeedError> {
    let value = *data
        .get(*offset)
        .ok_or_else(|| FeedError::Parse("FYERS frame ended while reading u8".to_string()))?;
    *offset += 1;
    Ok(value)
}

fn read_u16_be(data: &[u8], offset: &mut usize) -> Result<u16, FeedError> {
    let bytes = read_array::<2>(data, offset, "u16")?;
    Ok(u16::from_be_bytes(bytes))
}

fn read_u16_le(data: &[u8], offset: &mut usize) -> Result<u16, FeedError> {
    let bytes = read_array::<2>(data, offset, "u16")?;
    Ok(u16::from_le_bytes(bytes))
}

fn read_i32_be(data: &[u8], offset: &mut usize) -> Result<i32, FeedError> {
    let bytes = read_array::<4>(data, offset, "i32")?;
    Ok(i32::from_be_bytes(bytes))
}

fn read_array<const N: usize>(
    data: &[u8],
    offset: &mut usize,
    type_name: &str,
) -> Result<[u8; N], FeedError> {
    let end = *offset + N;
    let slice = data
        .get(*offset..end)
        .ok_or_else(|| FeedError::Parse(format!("FYERS frame ended while reading {type_name}")))?;
    *offset = end;
    slice
        .try_into()
        .map_err(|_| FeedError::Parse(format!("FYERS invalid {type_name} bytes")))
}

fn read_string_u8(data: &[u8], offset: &mut usize) -> Result<String, FeedError> {
    let len = read_u8(data, offset)? as usize;
    let end = *offset + len;
    let value = data
        .get(*offset..end)
        .ok_or_else(|| FeedError::Parse("FYERS frame ended while reading string".to_string()))?;
    *offset = end;
    String::from_utf8(value.to_vec())
        .map_err(|error| FeedError::Parse(format!("FYERS invalid string: {error}")))
}

fn skip_bytes(data: &[u8], offset: &mut usize, bytes: usize) -> Result<(), FeedError> {
    let end = *offset + bytes;
    if end > data.len() {
        return Err(FeedError::Parse(
            "FYERS frame ended while skipping bytes".to_string(),
        ));
    }
    *offset = end;
    Ok(())
}

fn current_unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_hsm_symbol_for_index() {
        assert_eq!(
            hsm_symbol_for_symbol_update("NSE:NIFTY50-INDEX", "101000000026000").unwrap(),
            "if|nse_cm|Nifty 50"
        );
    }

    #[test]
    fn builds_hsm_symbol_for_scrip() {
        assert_eq!(
            hsm_symbol_for_symbol_update("NSE:SBIN-EQ", "10100000003045").unwrap(),
            "sf|nse_cm|3045"
        );
    }

    #[test]
    fn fyers_data_socket_uses_raw_jwt_even_when_app_id_prefix_is_present() {
        assert_eq!(
            jwt_access_token_only("APPID:header.payload.signature").unwrap(),
            "header.payload.signature"
        );
        assert_eq!(
            jwt_access_token_only("Bearer header.payload.signature").unwrap(),
            "header.payload.signature"
        );
    }

    #[test]
    fn parses_fyers_control_response_status() {
        assert_eq!(
            control_response_status(&[0, 0, 1, 1, 1, 0, 1, b'K']).unwrap(),
            "K"
        );
        assert_eq!(
            control_response_status(&[0, 0, 4, 1, 1, 1, 0, b'K']).unwrap(),
            "K"
        );
    }

    #[test]
    fn snapshot_and_lite_update_emit_price_ticks() {
        let mut parser = FyersHsmParser {
            hsm_to_symbol: BTreeMap::from([(
                "sf|nse_cm|3045".to_string(),
                "NSE:SBIN-EQ".to_string(),
            )]),
            topic_ids: HashMap::new(),
            topics: HashMap::new(),
            pending_events: VecDeque::new(),
            log_control_messages: false,
        };

        let snapshot = sample_snapshot_frame("sf|nse_cm|3045", 7, 12_345, 2, 1);
        parser.handle_binary_message(&snapshot).unwrap();
        let first = parser.pending_events.pop_front().unwrap();
        assert_eq!(first.instrument_name(), &InstrumentName::new("NSE:SBIN-EQ"));
        assert_eq!(first.price().as_f64(), 123.45);

        let lite = sample_lite_frame(7, 12_400);
        parser.handle_binary_message(&lite).unwrap();
        let second = parser.pending_events.pop_front().unwrap();
        assert_eq!(second.price().as_f64(), 124.0);
    }

    #[test]
    fn ignores_invalid_fyers_price_without_failing_feed() {
        let mut parser = FyersHsmParser {
            hsm_to_symbol: BTreeMap::from([(
                "sf|nse_cm|3045".to_string(),
                "NSE:SBIN-EQ".to_string(),
            )]),
            topic_ids: HashMap::new(),
            topics: HashMap::new(),
            pending_events: VecDeque::new(),
            log_control_messages: false,
        };

        let snapshot = sample_snapshot_frame("sf|nse_cm|3045", 7, 0, 2, 1);

        parser.handle_binary_message(&snapshot).unwrap();

        assert!(parser.pending_events.is_empty());
    }

    fn sample_snapshot_frame(
        topic: &str,
        topic_id: u16,
        ltp_raw: i32,
        precision: u8,
        multiplier: u16,
    ) -> Vec<u8> {
        let mut data = vec![0, 0, FYERS_DATAFEED_RESP];
        data.extend_from_slice(&1u32.to_be_bytes());
        data.extend_from_slice(&1u16.to_be_bytes());
        data.push(FYERS_SNAPSHOT_FEED);
        data.extend_from_slice(&topic_id.to_le_bytes());
        data.push(topic.len() as u8);
        data.extend_from_slice(topic.as_bytes());
        data.push(4);
        data.extend_from_slice(&ltp_raw.to_be_bytes());
        data.extend_from_slice(&FYERS_SENTINEL_I32.to_be_bytes());
        data.extend_from_slice(&FYERS_SENTINEL_I32.to_be_bytes());
        data.extend_from_slice(&1_775_000_000i32.to_be_bytes());
        data.extend_from_slice(&0u16.to_be_bytes());
        data.extend_from_slice(&multiplier.to_be_bytes());
        data.push(precision);
        for value in ["NSE", "3045", "SBIN"] {
            data.push(value.len() as u8);
            data.extend_from_slice(value.as_bytes());
        }
        data
    }

    fn sample_lite_frame(topic_id: u16, ltp_raw: i32) -> Vec<u8> {
        let mut data = vec![0, 0, FYERS_DATAFEED_RESP];
        data.extend_from_slice(&2u32.to_be_bytes());
        data.extend_from_slice(&1u16.to_be_bytes());
        data.push(FYERS_LITE_FEED);
        data.extend_from_slice(&topic_id.to_le_bytes());
        data.extend_from_slice(&ltp_raw.to_be_bytes());
        data
    }
}
