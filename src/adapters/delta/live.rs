use std::collections::BTreeSet;
use std::net::TcpStream;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Message, WebSocket, connect};

use crate::feeder::{
    FeedChannel, FeedError, FeedSubscription, Feeder, InstrumentName, Price, PriceEvent, PriceTick,
    UnixMillis,
};

const DELTA_TRADES_CHANNEL: &str = "trades";
const DELTA_SPOT_PRICE_CHANNEL: &str = "spot_price";

type DeltaWebSocket = WebSocket<MaybeTlsStream<TcpStream>>;

pub struct DeltaLiveFeeder {
    socket: DeltaWebSocket,
    subscribed_trade_symbols: BTreeSet<String>,
    subscribed_spot_symbols: BTreeSet<String>,
}

impl DeltaLiveFeeder {
    pub fn connect(ws_url: &str) -> Result<Self, FeedError> {
        let (mut socket, _response) = connect(ws_url)?;
        socket.send(Message::Text(
            json!({ "type": "enable_heartbeat" }).to_string().into(),
        ))?;

        Ok(Self {
            socket,
            subscribed_trade_symbols: BTreeSet::new(),
            subscribed_spot_symbols: BTreeSet::new(),
        })
    }

    pub fn subscribe_symbols(&mut self, symbols: &[String]) -> Result<(), FeedError> {
        let symbols = symbols
            .iter()
            .filter(|symbol| !symbol.trim().is_empty())
            .filter(|symbol| !self.subscribed_trade_symbols.contains(*symbol))
            .cloned()
            .collect::<Vec<_>>();

        if symbols.is_empty() {
            return Ok(());
        }

        self.send_channel_message("subscribe", DELTA_TRADES_CHANNEL, &symbols)?;
        self.subscribed_trade_symbols.extend(symbols);
        Ok(())
    }

    pub fn unsubscribe_symbols(&mut self, symbols: &[String]) -> Result<(), FeedError> {
        let symbols = symbols
            .iter()
            .filter(|symbol| self.subscribed_trade_symbols.contains(*symbol))
            .cloned()
            .collect::<Vec<_>>();

        if symbols.is_empty() {
            return Ok(());
        }

        self.send_channel_message("unsubscribe", DELTA_TRADES_CHANNEL, &symbols)?;
        for symbol in symbols {
            self.subscribed_trade_symbols.remove(&symbol);
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
    let message_type = value.get("type").and_then(Value::as_str);

    match message_type {
        Some("trades") => parse_trade_tick(&value).map(Some),
        Some("v2/spot_price") | Some("spot_price") => parse_spot_price_tick(&value).map(Some),
        Some("heartbeat") | Some("subscriptions") => Ok(None),
        _ => Ok(None),
    }
}

fn parse_trade_tick(value: &Value) -> Result<PriceEvent, FeedError> {
    let symbol = value
        .get("sy")
        .or_else(|| value.get("symbol"))
        .and_then(Value::as_str)
        .ok_or_else(|| FeedError::Parse(format!("Delta trade missing symbol: {value}")))?;
    let price = parse_json_number(value.get("p").or_else(|| value.get("price")))
        .ok_or_else(|| FeedError::Parse(format!("Delta trade missing price: {value}")))?;
    let time_micros = value
        .get("t")
        .or_else(|| value.get("timestamp"))
        .and_then(Value::as_u64)
        .ok_or_else(|| FeedError::Parse(format!("Delta trade missing timestamp: {value}")))?;

    let price = Price::new(price).map_err(|error| FeedError::Parse(error.to_string()))?;

    Ok(PriceEvent::Tick(PriceTick::new(
        InstrumentName::new(symbol),
        price,
        UnixMillis::new(time_micros / 1000),
    )))
}

fn parse_spot_price_tick(value: &Value) -> Result<PriceEvent, FeedError> {
    let symbol = value
        .get("s")
        .or_else(|| value.get("sy"))
        .or_else(|| value.get("symbol"))
        .and_then(Value::as_str)
        .ok_or_else(|| FeedError::Parse(format!("Delta spot price missing symbol: {value}")))?;
    let price = parse_json_number(value.get("p").or_else(|| value.get("price")))
        .ok_or_else(|| FeedError::Parse(format!("Delta spot price missing price: {value}")))?;
    let time_micros = value
        .get("t")
        .or_else(|| value.get("timestamp"))
        .or_else(|| value.get("ts"))
        .and_then(Value::as_u64)
        .unwrap_or_else(current_unix_micros);

    let price = Price::new(price).map_err(|error| FeedError::Parse(error.to_string()))?;

    Ok(PriceEvent::Tick(PriceTick::new(
        InstrumentName::new(symbol),
        price,
        UnixMillis::new(time_micros / 1000),
    )))
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
}
