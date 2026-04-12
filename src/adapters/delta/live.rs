use std::collections::BTreeSet;
use std::net::TcpStream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Message, WebSocket, connect};

use crate::feeder::{
    FeedChannel, FeedError, FeedSubscription, Feeder, InstrumentName, Price, PriceEvent, PriceTick,
    UnixMillis,
};

const DEFAULT_DELTA_TICKER_CHANNEL: &str = "v2/ticker";
const DELTA_SPOT_PRICE_CHANNEL: &str = "spot_price";
const MAX_INVALID_DELTA_PRICE_LOGS: usize = 20;
static INVALID_DELTA_PRICE_LOGS: AtomicUsize = AtomicUsize::new(0);

type DeltaWebSocket = WebSocket<MaybeTlsStream<TcpStream>>;

pub struct DeltaLiveFeeder {
    socket: DeltaWebSocket,
    ticker_channel: String,
    subscribed_ticker_symbols: BTreeSet<String>,
    subscribed_spot_symbols: BTreeSet<String>,
}

impl DeltaLiveFeeder {
    pub fn connect(ws_url: &str, ticker_channel: Option<&str>) -> Result<Self, FeedError> {
        let (mut socket, _response) = connect(ws_url)?;
        socket.send(Message::Text(
            json!({ "type": "enable_heartbeat" }).to_string().into(),
        ))?;

        Ok(Self {
            socket,
            ticker_channel: ticker_channel
                .filter(|channel| !channel.trim().is_empty())
                .unwrap_or(DEFAULT_DELTA_TICKER_CHANNEL)
                .to_string(),
            subscribed_ticker_symbols: BTreeSet::new(),
            subscribed_spot_symbols: BTreeSet::new(),
        })
    }

    pub fn subscribe_symbols(&mut self, symbols: &[String]) -> Result<(), FeedError> {
        let symbols = symbols
            .iter()
            .filter(|symbol| !symbol.trim().is_empty())
            .filter(|symbol| !self.subscribed_ticker_symbols.contains(*symbol))
            .cloned()
            .collect::<Vec<_>>();

        if symbols.is_empty() {
            return Ok(());
        }

        let channel = self.ticker_channel.clone();
        self.send_channel_message("subscribe", &channel, &symbols)?;
        self.subscribed_ticker_symbols.extend(symbols);
        Ok(())
    }

    pub fn unsubscribe_symbols(&mut self, symbols: &[String]) -> Result<(), FeedError> {
        let symbols = symbols
            .iter()
            .filter(|symbol| self.subscribed_ticker_symbols.contains(*symbol))
            .cloned()
            .collect::<Vec<_>>();

        if symbols.is_empty() {
            return Ok(());
        }

        let channel = self.ticker_channel.clone();
        self.send_channel_message("unsubscribe", &channel, &symbols)?;
        for symbol in symbols {
            self.subscribed_ticker_symbols.remove(&symbol);
        }

        Ok(())
    }

    pub fn subscribe_spot_symbols(&mut self, symbols: &[String]) -> Result<(), FeedError> {
        let symbols = symbols
            .iter()
            .filter(|symbol| !symbol.trim().is_empty())
            .filter(|symbol| !self.subscribed_spot_symbols.contains(*symbol))
            .cloned()
            .collect::<Vec<_>>();

        if symbols.is_empty() {
            return Ok(());
        }

        self.send_channel_message("subscribe", DELTA_SPOT_PRICE_CHANNEL, &symbols)?;
        self.subscribed_spot_symbols.extend(symbols);
        Ok(())
    }

    fn send_channel_message(
        &mut self,
        message_type: &str,
        channel: &str,
        symbols: &[String],
    ) -> Result<(), FeedError> {
        let payload = json!({
            "type": message_type,
            "payload": {
                "channels": [
                    {
                        "name": channel,
                        "symbols": symbols,
                    }
                ]
            }
        });

        self.socket
            .send(Message::Text(payload.to_string().into()))
            .map_err(FeedError::from)
    }

    pub fn next_price_event(&mut self) -> Result<Option<PriceEvent>, FeedError> {
        loop {
            let message = self.socket.read()?;

            match message {
                Message::Text(text) => {
                    if let Some(event) = parse_delta_text_message(&text)? {
                        return Ok(Some(event));
                    }
                }
                Message::Ping(payload) => {
                    self.socket.send(Message::Pong(payload))?;
                }
                Message::Pong(_) | Message::Binary(_) | Message::Frame(_) => {}
                Message::Close(frame) => {
                    return Err(FeedError::Disconnected(format!(
                        "delta websocket closed: {frame:?}"
                    )));
                }
            }
        }
    }
}

impl Feeder for DeltaLiveFeeder {
    fn subscribe(&mut self, subscription: FeedSubscription) -> Result<(), FeedError> {
        for channel in subscription.channels() {
            match channel {
                FeedChannel::PriceTick => {}
                FeedChannel::PriceCandle(timeframe) => {
                    return Err(FeedError::UnsupportedChannel {
                        broker: "delta".to_string(),
                        channel: format!("PriceCandle({timeframe:?})"),
                    });
                }
            }
        }

        let symbols = subscription
            .instruments()
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();

        self.subscribe_symbols(&symbols)
    }

    fn next_event(&mut self) -> Result<Option<PriceEvent>, FeedError> {
        self.next_price_event()
    }
}

pub fn parse_delta_text_message(text: &str) -> Result<Option<PriceEvent>, FeedError> {
    let value: Value = serde_json::from_str(text)?;
    if value.get("success").and_then(Value::as_bool) == Some(false) {
        return Err(FeedError::Parse(format!("Delta websocket error: {value}")));
    }
    let message_type = value.get("type").and_then(Value::as_str);

    match message_type {
        Some("v2/ticker") | Some("ticker") => parse_ticker_tick(&value),
        Some("trades") => parse_trade_tick(&value),
        Some("v2/spot_price") | Some("spot_price") => parse_spot_price_tick(&value),
        Some("heartbeat") | Some("subscriptions") => Ok(None),
        Some("error") => Err(FeedError::Parse(format!("Delta websocket error: {value}"))),
        _ => Ok(None),
    }
}

fn parse_ticker_tick(value: &Value) -> Result<Option<PriceEvent>, FeedError> {
    let symbol = ticker_symbol(value)
        .ok_or_else(|| FeedError::Parse(format!("Delta ticker missing symbol: {value}")))?;
    let Some(price) = ticker_price(value) else {
        log_ignored_delta_price("ticker", symbol, None, "missing price");
        return Ok(None);
    };
    let time_micros = value
        .get("timestamp")
        .or_else(|| value.get("t"))
        .or_else(|| value.get("ts"))
        .and_then(Value::as_u64)
        .unwrap_or_else(current_unix_micros);

    let Some(price) = valid_delta_price("ticker", symbol, price) else {
        return Ok(None);
    };

    Ok(Some(PriceEvent::Tick(PriceTick::new(
        InstrumentName::new(symbol),
        price,
        UnixMillis::new(time_micros / 1000),
    ))))
}

fn ticker_symbol(value: &Value) -> Option<&str> {
    value
        .get("symbol")
        .or_else(|| value.get("sy"))
        .or_else(|| value.get("s"))
        .and_then(Value::as_str)
        .or_else(|| {
            compact_ticker_payload(value).and_then(|payload| {
                payload
                    .get("symbol")
                    .or_else(|| payload.get("sy"))
                    .or_else(|| payload.get("s"))
                    .and_then(Value::as_str)
            })
        })
}

fn ticker_price(value: &Value) -> Option<f64> {
    parse_json_number(
        value
            .get("close")
            .or_else(|| value.get("price"))
            .or_else(|| value.get("last_price"))
            .or_else(|| value.get("last_traded_price"))
            .or_else(|| value.get("mark_price"))
            .or_else(|| value.get("spot_price")),
    )
    .or_else(|| compact_ticker_payload(value).and_then(compact_ticker_price))
}

fn compact_ticker_payload(value: &Value) -> Option<&Value> {
    value.get("d").and_then(Value::as_array)?.first()
}

fn compact_ticker_price(value: &Value) -> Option<f64> {
    parse_json_number(
        value
            .get("close")
            .or_else(|| value.get("price"))
            .or_else(|| value.get("last_price"))
            .or_else(|| value.get("last_traded_price")),
    )
    .or_else(|| {
        value
            .get("ohlc")
            .and_then(Value::as_array)
            .and_then(|ohlc| ohlc.get(3))
            .and_then(|close| parse_json_number(Some(close)))
    })
    .or_else(|| parse_json_number(value.get("m").or_else(|| value.get("mark_price"))))
}

fn parse_trade_tick(value: &Value) -> Result<Option<PriceEvent>, FeedError> {
    let symbol = value
        .get("sy")
        .or_else(|| value.get("symbol"))
        .and_then(Value::as_str)
        .ok_or_else(|| FeedError::Parse(format!("Delta trade missing symbol: {value}")))?;
    let Some(price) = parse_json_number(value.get("p").or_else(|| value.get("price"))) else {
        log_ignored_delta_price("trade", symbol, None, "missing price");
        return Ok(None);
    };
    let time_micros = value
        .get("t")
        .or_else(|| value.get("timestamp"))
        .and_then(Value::as_u64)
        .ok_or_else(|| FeedError::Parse(format!("Delta trade missing timestamp: {value}")))?;

    let Some(price) = valid_delta_price("trade", symbol, price) else {
        return Ok(None);
    };

    Ok(Some(PriceEvent::Tick(PriceTick::new(
        InstrumentName::new(symbol),
        price,
        UnixMillis::new(time_micros / 1000),
    ))))
}

fn parse_spot_price_tick(value: &Value) -> Result<Option<PriceEvent>, FeedError> {
    let symbol = value
        .get("s")
        .or_else(|| value.get("sy"))
        .or_else(|| value.get("symbol"))
        .and_then(Value::as_str)
        .ok_or_else(|| FeedError::Parse(format!("Delta spot price missing symbol: {value}")))?;
    let Some(price) = parse_json_number(value.get("p").or_else(|| value.get("price"))) else {
        log_ignored_delta_price("spot_price", symbol, None, "missing price");
        return Ok(None);
    };
    let time_micros = value
        .get("t")
        .or_else(|| value.get("timestamp"))
        .or_else(|| value.get("ts"))
        .and_then(Value::as_u64)
        .unwrap_or_else(current_unix_micros);

    let Some(price) = valid_delta_price("spot_price", symbol, price) else {
        return Ok(None);
    };

    Ok(Some(PriceEvent::Tick(PriceTick::new(
        InstrumentName::new(symbol),
        price,
        UnixMillis::new(time_micros / 1000),
    ))))
}

fn valid_delta_price(message_type: &str, symbol: &str, price: f64) -> Option<Price> {
    match Price::new(price) {
        Ok(price) => Some(price),
        Err(reason) => {
            log_ignored_delta_price(message_type, symbol, Some(price), reason);
            None
        }
    }
}

fn log_ignored_delta_price(message_type: &str, symbol: &str, price: Option<f64>, reason: &str) {
    let count = INVALID_DELTA_PRICE_LOGS.fetch_add(1, Ordering::Relaxed);
    if count >= MAX_INVALID_DELTA_PRICE_LOGS {
        return;
    }

    match price {
        Some(price) => eprintln!(
            "Delta ignored invalid {message_type} price: symbol={symbol} price={price} reason={reason}"
        ),
        None => {
            eprintln!("Delta ignored invalid {message_type} price: symbol={symbol} reason={reason}")
        }
    }

    if count + 1 == MAX_INVALID_DELTA_PRICE_LOGS {
        eprintln!("Delta invalid price log limit reached; suppressing further invalid price logs");
    }
}

fn parse_json_number(value: Option<&Value>) -> Option<f64> {
    value.and_then(|value| {
        value
            .as_f64()
            .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
    })
}

fn current_unix_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_micros() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_compact_delta_trade_into_price_tick() {
        let message = r#"{
            "p": "72141.5",
            "r": "m",
            "s": 1.0,
            "sy": "BTCUSD",
            "t": 1775800366578410,
            "ts": 1775800367003029,
            "type": "trades"
        }"#;

        let event = parse_delta_text_message(message).unwrap().unwrap();

        assert!(matches!(event, PriceEvent::Tick(_)));
        assert_eq!(event.instrument_name(), &InstrumentName::new("BTCUSD"));
        assert_eq!(event.price().as_f64(), 72_141.5);
    }

    #[test]
    fn parses_v2_ticker_into_price_tick() {
        let message = r#"{
            "close": 71810.0,
            "mark_price": "71805.1",
            "symbol": "BTCUSD",
            "timestamp": 1775800366578410,
            "type": "v2/ticker"
        }"#;

        let event = parse_delta_text_message(message).unwrap().unwrap();

        assert!(matches!(event, PriceEvent::Tick(_)));
        assert_eq!(event.instrument_name(), &InstrumentName::new("BTCUSD"));
        assert_eq!(event.price().as_f64(), 71_810.0);
    }

    #[test]
    fn parses_v2_ticker_mark_price_fallback_into_price_tick() {
        let message = r#"{
            "mark_price": "294.0",
            "symbol": "C-BTC-71800-120426",
            "timestamp": 1775800366578410,
            "type": "v2/ticker"
        }"#;

        let event = parse_delta_text_message(message).unwrap().unwrap();

        assert!(matches!(event, PriceEvent::Tick(_)));
        assert_eq!(
            event.instrument_name(),
            &InstrumentName::new("C-BTC-71800-120426")
        );
        assert_eq!(event.price().as_f64(), 294.0);
    }

    #[test]
    fn parses_compact_delta_india_ticker_into_price_tick() {
        let message = r#"{
            "d": [
                {
                    "g": ["-0.98795059", "0.00004200", "-0.72992386", "-31.84982882", "0.71285309"],
                    "i": 130334,
                    "m": "1715.50081572",
                    "m24hc": "218.6193",
                    "ohlc": [575.0, 2000.0, 161.0, 1714.3],
                    "s": "P-BTC-73400-120426"
                }
            ],
            "sp": "71687.1",
            "sy": "P-BTC-73400-120426",
            "ts": 1775963473692921,
            "type": "ticker"
        }"#;

        let event = parse_delta_text_message(message).unwrap().unwrap();

        assert!(matches!(event, PriceEvent::Tick(_)));
        assert_eq!(
            event.instrument_name(),
            &InstrumentName::new("P-BTC-73400-120426")
        );
        assert_eq!(event.price().as_f64(), 1_714.3);
    }

    #[test]
    fn ignores_heartbeat_and_subscription_ack() {
        assert_eq!(
            parse_delta_text_message(r#"{ "type": "heartbeat" }"#).unwrap(),
            None
        );
        assert_eq!(
            parse_delta_text_message(r#"{ "type": "subscriptions", "channels": [] }"#).unwrap(),
            None
        );
    }

    #[test]
    fn parses_v2_spot_price_into_price_tick() {
        let message = r#"{
            "s": ".DEXBTUSD",
            "p": 72612.42,
            "type": "v2/spot_price"
        }"#;

        let event = parse_delta_text_message(message).unwrap().unwrap();

        assert!(matches!(event, PriceEvent::Tick(_)));
        assert_eq!(event.instrument_name(), &InstrumentName::new(".DEXBTUSD"));
        assert_eq!(event.price().as_f64(), 72_612.42);
    }

    #[test]
    fn ignores_invalid_delta_price_without_failing_feed() {
        let invalid_price = r#"{
            "close": 0,
            "symbol": "BTCUSD",
            "timestamp": 1775800366578410,
            "type": "v2/ticker"
        }"#;
        let missing_price = r#"{
            "symbol": "BTCUSD",
            "timestamp": 1775800366578410,
            "type": "v2/ticker"
        }"#;

        assert_eq!(parse_delta_text_message(invalid_price).unwrap(), None);
        assert_eq!(parse_delta_text_message(missing_price).unwrap(), None);
    }
}
