use std::collections::BTreeMap;
use std::fs;
use std::io::ErrorKind;
use std::io::Read;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, RwLock};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use reqwest::blocking::Client as HttpClient;
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, USER_AGENT};
use serde::Serialize;
use serde_json::Value;
use socketio_client::manager::ManagerOptions;

use crate::adapters::dbinternational::auth::read_market_data_session;
use crate::config::DbinternationalBrokerSection;
use crate::feeder::{
    FeedError, InstrumentDefinition, InstrumentName, Price, PriceEvent, PriceTick, UnixMillis,
};

const TOUCHLINE_MESSAGE_CODE: u16 = 1501;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DbinternationalInstrumentKey {
    exchange_segment: u16,
    exchange_instrument_id: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DbinternationalMasterInstrument {
    key: DbinternationalInstrumentKey,
    aliases: Vec<String>,
}

#[derive(Debug, Default)]
struct DbinternationalMasterCatalog {
    by_alias: BTreeMap<String, Vec<DbinternationalMasterInstrument>>,
}

impl DbinternationalMasterCatalog {
    fn load(config: &DbinternationalBrokerSection) -> Result<Self, FeedError> {
        let content = fs::read_to_string(&config.market_data_master_file).map_err(|error| {
            FeedError::Config(format!(
                "failed to read DBInternational master {}: {error}",
                config.market_data_master_file
            ))
        })?;
        let mut catalog = parse_master_catalog(&content)?;

        match fs::read_to_string(&config.market_data_index_file) {
            Ok(index_content) => catalog.merge(parse_master_catalog(&index_content)?),
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => {
                return Err(FeedError::Config(format!(
                    "failed to read DBInternational index master {}: {error}",
                    config.market_data_index_file
                )));
            }
        }

        Ok(catalog)
    }

    fn resolve_symbol(&self, symbol: &str) -> Result<DbinternationalMasterInstrument, FeedError> {
        let symbol = symbol.trim();
        if symbol.is_empty() {
            return Err(FeedError::InvalidInstrument(
                "DBInternational subscription symbol is empty".to_string(),
            ));
        }

        let key = normalize_alias(symbol);
        let Some(matches) = self.by_alias.get(&key) else {
            return Err(FeedError::InvalidInstrument(format!(
                "DBInternational symbol {symbol} was not found in master"
            )));
        };
        if matches.len() > 1 {
            let samples = matches
                .iter()
                .take(5)
                .map(|instrument| instrument.aliases.first().cloned().unwrap_or_default())
                .collect::<Vec<_>>()
                .join(", ");
            return Err(FeedError::InvalidInstrument(format!(
                "DBInternational symbol {symbol} is ambiguous in master: {} matches ({samples})",
                matches.len()
            )));
        }

        Ok(matches[0].clone())
    }

    fn merge(&mut self, other: DbinternationalMasterCatalog) {
        for (alias, mut instruments) in other.by_alias {
            self.by_alias
                .entry(alias)
                .or_default()
                .append(&mut instruments);
        }
    }
}

#[derive(Debug)]
enum DbinternationalLiveMessage {
    Event(PriceEvent),
    Error(String),
}

pub struct DbinternationalLiveFeeder {
    _socket_thread: JoinHandle<()>,
    receiver: Receiver<DbinternationalLiveMessage>,
    sender: Sender<DbinternationalLiveMessage>,
    catalog: DbinternationalMasterCatalog,
    instrument_names: Arc<RwLock<BTreeMap<DbinternationalInstrumentKey, InstrumentName>>>,
    subscription_url: String,
    access_token: String,
    http: HttpClient,
}

impl DbinternationalLiveFeeder {
    pub fn connect(
        config: &DbinternationalBrokerSection,
        log_to_console: bool,
    ) -> Result<Self, FeedError> {
        let session = read_market_data_session(config)?;
        let user_id = session.user_id.clone().ok_or_else(|| {
            FeedError::Config(
                "DBInternational market-data session missing user_id for socket connection"
                    .to_string(),
            )
        })?;
        let catalog = DbinternationalMasterCatalog::load(config)?;
        let socket_url = market_data_socket_url(
            &session.base_url,
            &session.access_token,
            &user_id,
            &config.market_data_publish_format,
            &config.market_data_broadcast_mode,
        )?;
        let socket_path = socketio_manager_path(&config.market_data_socket_path);
        let subscription_url = market_data_subscription_url_from_base_url(&session.base_url)?;
        let (sender, receiver) = mpsc::channel();
        let instrument_names = Arc::new(RwLock::new(BTreeMap::new()));

        let socket_thread = start_socket_thread(
            socket_url,
            socket_path,
            Arc::clone(&instrument_names),
            sender.clone(),
            log_to_console,
        )?;

        Ok(Self {
            _socket_thread: socket_thread,
            receiver,
            sender,
            catalog,
            instrument_names,
            subscription_url,
            access_token: session.access_token,
            http: HttpClient::builder()
                .user_agent("dhancred-trading-app/0.1")
                .connect_timeout(Duration::from_secs(15))
                .timeout(Duration::from_secs(60))
                .build()?,
        })
    }

    pub fn subscribe_symbols(&self, symbols: &[String]) -> Result<(), FeedError> {
        if symbols.is_empty() {
            return Err(FeedError::Config(
                "DBInternational subscription symbols cannot be empty".to_string(),
            ));
        }

        let resolved = self.resolve_subscription_symbols(symbols)?;
        let subscriptions = resolved
            .iter()
            .map(|(_, instrument)| SubscriptionInstrument {
                exchange_segment: instrument.key.exchange_segment,
                exchange_instrument_id: instrument.key.exchange_instrument_id,
            })
            .collect::<Vec<_>>();
        for (symbol, instrument) in &resolved {
            self.instrument_names
                .write()
                .expect("DBInternational instrument map lock poisoned")
                .insert(instrument.key, InstrumentName::new(symbol.trim()));
        }

        for chunk in subscriptions.chunks(100) {
            let response_json = self.subscribe_chunk(chunk)?;
            queue_subscription_response_ticks(
                &response_json,
                &self.instrument_names,
                &self.sender,
            )?;
        }

        Ok(())
    }

    pub fn unsubscribe_symbols(&self, symbols: &[String]) -> Result<(), FeedError> {
        if symbols.is_empty() {
            return Ok(());
        }

        let resolved = self.resolve_subscription_symbols(symbols)?;
        let subscriptions = resolved
            .iter()
            .map(|(_, instrument)| SubscriptionInstrument {
                exchange_segment: instrument.key.exchange_segment,
                exchange_instrument_id: instrument.key.exchange_instrument_id,
            })
            .collect::<Vec<_>>();

        for chunk in subscriptions.chunks(100) {
            self.unsubscribe_chunk(chunk)?;
        }
        for (_, instrument) in resolved {
            self.instrument_names
                .write()
                .expect("DBInternational instrument map lock poisoned")
                .remove(&instrument.key);
        }

        Ok(())
    }

    pub fn subscribe_instruments(
        &self,
        instruments: &[InstrumentDefinition],
    ) -> Result<(), FeedError> {
        if instruments.is_empty() {
            return Ok(());
        }
        let resolved = resolve_subscription_instruments(instruments)?;
        self.subscribe_resolved(resolved)
    }

    pub fn unsubscribe_instruments(
        &self,
        instruments: &[InstrumentDefinition],
    ) -> Result<(), FeedError> {
        if instruments.is_empty() {
            return Ok(());
        }
        let resolved = resolve_subscription_instruments(instruments)?;
        self.unsubscribe_resolved(resolved)
    }

    pub fn next_price_event(&self) -> Result<Option<PriceEvent>, FeedError> {
        match self.receiver.recv() {
            Ok(DbinternationalLiveMessage::Event(event)) => Ok(Some(event)),
            Ok(DbinternationalLiveMessage::Error(error)) => Err(FeedError::Disconnected(error)),
            Err(error) => Err(FeedError::Disconnected(format!(
                "DBInternational event channel closed: {error}"
            ))),
        }
    }

    fn resolve_subscription_symbols(
        &self,
        symbols: &[String],
    ) -> Result<Vec<(String, DbinternationalMasterInstrument)>, FeedError> {
        symbols
            .iter()
            .map(|symbol| {
                self.catalog
                    .resolve_symbol(symbol)
                    .map(|instrument| (symbol.trim().to_string(), instrument))
            })
            .collect()
    }

    fn subscribe_resolved(
        &self,
        resolved: Vec<(String, DbinternationalMasterInstrument)>,
    ) -> Result<(), FeedError> {
        let subscriptions = resolved
            .iter()
            .map(|(_, instrument)| SubscriptionInstrument {
                exchange_segment: instrument.key.exchange_segment,
                exchange_instrument_id: instrument.key.exchange_instrument_id,
            })
            .collect::<Vec<_>>();
        for (symbol, instrument) in &resolved {
            self.instrument_names
                .write()
                .expect("DBInternational instrument map lock poisoned")
                .insert(instrument.key, InstrumentName::new(symbol.trim()));
        }

        for chunk in subscriptions.chunks(100) {
            let response_json = self.subscribe_chunk(chunk)?;
            queue_subscription_response_ticks(
                &response_json,
                &self.instrument_names,
                &self.sender,
            )?;
        }

        Ok(())
    }

    fn unsubscribe_resolved(
        &self,
        resolved: Vec<(String, DbinternationalMasterInstrument)>,
    ) -> Result<(), FeedError> {
        let subscriptions = resolved
            .iter()
            .map(|(_, instrument)| SubscriptionInstrument {
                exchange_segment: instrument.key.exchange_segment,
                exchange_instrument_id: instrument.key.exchange_instrument_id,
            })
            .collect::<Vec<_>>();

        for chunk in subscriptions.chunks(100) {
            self.unsubscribe_chunk(chunk)?;
        }
        for (_, instrument) in resolved {
            self.instrument_names
                .write()
                .expect("DBInternational instrument map lock poisoned")
                .remove(&instrument.key);
        }

        Ok(())
    }

    fn subscribe_chunk(&self, instruments: &[SubscriptionInstrument]) -> Result<Value, FeedError> {
        let request = SubscriptionRequest {
            instruments,
            xts_message_code: TOUCHLINE_MESSAGE_CODE,
        };
        let response = self
            .http
            .post(&self.subscription_url)
            .header(ACCEPT, "application/json")
            .header(CONTENT_TYPE, "application/json")
            .header(USER_AGENT, "dhancred-trading-app/0.1")
            .header(AUTHORIZATION, &self.access_token)
            .json(&request)
            .send()?;
        let status = response.status();
        let body = response.text()?;
        let response_json: Value = serde_json::from_str(&body)?;

        if !status.is_success() {
            if subscription_already_exists(&response_json) {
                return Ok(response_json);
            }
            return Err(FeedError::Http(format!(
                "DBInternational subscription failed url={} status={} body={}",
                self.subscription_url,
                status.as_u16(),
                response_snippet(&body)
            )));
        }

        let envelope = response_envelope(&response_json).ok_or_else(|| {
            FeedError::Parse("DBInternational subscription response envelope is empty".to_string())
        })?;
        if envelope.get("type").and_then(Value::as_str) != Some("success") {
            return Err(FeedError::Http(format!(
                "DBInternational subscription returned non-success response: {}",
                response_snippet(&envelope.to_string())
            )));
        }

        Ok(response_json)
    }

    fn unsubscribe_chunk(&self, instruments: &[SubscriptionInstrument]) -> Result<(), FeedError> {
        let request = SubscriptionRequest {
            instruments,
            xts_message_code: TOUCHLINE_MESSAGE_CODE,
        };
        let response = self
            .http
            .put(&self.subscription_url)
            .header(ACCEPT, "application/json")
            .header(CONTENT_TYPE, "application/json")
            .header(USER_AGENT, "dhancred-trading-app/0.1")
            .header(AUTHORIZATION, &self.access_token)
            .json(&request)
            .send()?;
        let status = response.status();
        let body = response.text()?;
        let response_json: Value = serde_json::from_str(&body)?;

        if !status.is_success() {
            return Err(FeedError::Http(format!(
                "DBInternational unsubscription failed url={} status={} body={}",
                self.subscription_url,
                status.as_u16(),
                response_snippet(&body)
            )));
        }

        let envelope = response_envelope(&response_json).ok_or_else(|| {
            FeedError::Parse(
                "DBInternational unsubscription response envelope is empty".to_string(),
            )
        })?;
        if envelope.get("type").and_then(Value::as_str) != Some("success") {
            return Err(FeedError::Http(format!(
                "DBInternational unsubscription returned non-success response: {}",
                response_snippet(&envelope.to_string())
            )));
        }

        Ok(())
    }
}

fn resolve_subscription_instruments(
    instruments: &[InstrumentDefinition],
) -> Result<Vec<(String, DbinternationalMasterInstrument)>, FeedError> {
    instruments
        .iter()
        .map(|instrument| {
            let key = instrument_key_from_token(&instrument.instrument_token)?;
            Ok((
                instrument.trading_symbol.clone(),
                DbinternationalMasterInstrument {
                    key,
                    aliases: vec![instrument.trading_symbol.clone()],
                },
            ))
        })
        .collect()
}

fn instrument_key_from_token(value: &str) -> Result<DbinternationalInstrumentKey, FeedError> {
    let Some((exchange_segment, exchange_instrument_id)) = value.trim().split_once(':') else {
        return Err(FeedError::InvalidInstrument(format!(
            "DBInternational instrument_token {value} must be exchangeSegment:exchangeInstrumentID"
        )));
    };
    let exchange_segment_code = exchange_segment_code(exchange_segment).ok_or_else(|| {
        FeedError::InvalidInstrument(format!(
            "unsupported DBInternational exchange segment {exchange_segment}"
        ))
    })?;
    let exchange_instrument_id = exchange_instrument_id.parse::<u64>().map_err(|error| {
        FeedError::InvalidInstrument(format!(
            "invalid DBInternational exchangeInstrumentID {exchange_instrument_id}: {error}"
        ))
    })?;

    Ok(DbinternationalInstrumentKey {
        exchange_segment: exchange_segment_code,
        exchange_instrument_id,
    })
}

fn subscription_already_exists(response_json: &Value) -> bool {
    response_envelope(response_json)
        .map(|envelope| {
            envelope.get("code").and_then(Value::as_str) == Some("e-session-0002")
                || envelope
                    .get("description")
                    .and_then(Value::as_str)
                    .map(|description| {
                        description
                            .to_ascii_lowercase()
                            .contains("already subscribed")
                    })
                    .unwrap_or(false)
        })
        .unwrap_or(false)
}

#[derive(Serialize)]
struct SubscriptionRequest<'a> {
    instruments: &'a [SubscriptionInstrument],
    #[serde(rename = "xtsMessageCode")]
    xts_message_code: u16,
}

#[derive(Clone, Debug, Serialize)]
struct SubscriptionInstrument {
    #[serde(rename = "exchangeSegment")]
    exchange_segment: u16,
    #[serde(rename = "exchangeInstrumentID")]
    exchange_instrument_id: u64,
}

fn start_socket_thread(
    socket_url: String,
    socket_path: String,
    instrument_names: Arc<RwLock<BTreeMap<DbinternationalInstrumentKey, InstrumentName>>>,
    sender: Sender<DbinternationalLiveMessage>,
    log_to_console: bool,
) -> Result<JoinHandle<()>, FeedError> {
    let (startup_sender, startup_receiver) = mpsc::channel();
    let thread = thread::spawn(move || {
        let runtime = match tokio::runtime::Builder::new_multi_thread()
            .thread_name("dbinternational-socketio")
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(error) => {
                let _ = startup_sender.send(Err(format!(
                    "DBInternational Socket.IO runtime failed: {error}"
                )));
                return;
            }
        };

        runtime.block_on(async move {
            let options = ManagerOptions {
                path: socket_path,
                reconnection: true,
                reconnection_attempts: None,
                reconnection_delay: 1_000,
                reconnection_delay_max: 5_000,
                randomization_factor: 0.5,
                timeout: Some(20_000),
                auto_connect: false,
                query: None,
            };
            let manager = match socketio_client::connect_with_opts(&socket_url, options) {
                Ok(manager) => manager,
                Err(error) => {
                    let _ = startup_sender.send(Err(format!(
                        "DBInternational Socket.IO manager failed: {error}"
                    )));
                    return;
                }
            };
            let socket = manager.socket("/").await;

            let connect_log = log_to_console;
            socket.on("connect", move |_| {
                if connect_log {
                    println!("DBInternational Socket.IO connected");
                }
            });

            let joined_log = log_to_console;
            socket.on("joined", move |values| {
                if joined_log {
                    println!(
                        "DBInternational socket joined: {}",
                        values_to_log_string(&values)
                    );
                }
            });

            let disconnect_sender = sender.clone();
            socket.on("disconnect", move |values| {
                let _ = disconnect_sender.send(DbinternationalLiveMessage::Error(format!(
                    "DBInternational socket disconnected: {}",
                    values_to_log_string(&values)
                )));
            });

            let error_sender = sender.clone();
            socket.on("error", move |values| {
                let _ = error_sender.send(DbinternationalLiveMessage::Error(format!(
                    "DBInternational socket error: {}",
                    values_to_log_string(&values)
                )));
            });

            let socket_error_sender = sender.clone();
            socket.on("socketError", move |values| {
                let _ = socket_error_sender.send(DbinternationalLiveMessage::Error(format!(
                    "DBInternational socket error: {}",
                    socket_error_message(&values)
                )));
            });

            let event_sender = sender.clone();
            let parser_names = Arc::clone(&instrument_names);
            socket.on("*", move |values| {
                let _ = queue_socketio_values(values, &parser_names, &event_sender, log_to_console);
            });

            if let Err(error) = socket.connect().await {
                let _ = startup_sender.send(Err(format!(
                    "DBInternational Socket.IO socket setup failed: {error}"
                )));
                return;
            }

            if let Err(error) = manager.open().await {
                let _ = startup_sender.send(Err(format!(
                    "DBInternational Socket.IO handshake failed: {error}"
                )));
                return;
            }

            let _ = startup_sender.send(Ok(()));
            loop {
                tokio::time::sleep(Duration::from_secs(60 * 60)).await;
            }
        });
    });

    match startup_receiver.recv_timeout(Duration::from_secs(25)) {
        Ok(Ok(())) => Ok(thread),
        Ok(Err(error)) => Err(FeedError::Disconnected(error)),
        Err(error) => Err(FeedError::Disconnected(format!(
            "DBInternational Socket.IO startup timed out: {error}"
        ))),
    }
}

fn queue_subscription_response_ticks(
    response_json: &Value,
    instrument_names: &Arc<RwLock<BTreeMap<DbinternationalInstrumentKey, InstrumentName>>>,
    sender: &Sender<DbinternationalLiveMessage>,
) -> Result<(), FeedError> {
    let Some(envelope) = response_envelope(response_json) else {
        return Ok(());
    };
    let Some(result) = envelope.get("result") else {
        return Ok(());
    };
    if let Some(list_quotes) = result
        .get("listQuotes")
        .or_else(|| result.get("listquotes"))
    {
        queue_value_ticks(list_quotes, instrument_names, sender)?;
    }
    Ok(())
}

fn socket_error_message(values: &[Value]) -> String {
    let mut parts = Vec::new();
    for value in values {
        match value {
            Value::String(text) => {
                if let Ok(parsed) = serde_json::from_str::<Value>(text) {
                    parts.push(socket_error_value_message(&parsed));
                } else {
                    parts.push(text.clone());
                }
            }
            _ => parts.push(socket_error_value_message(value)),
        }
    }

    if parts.is_empty() {
        "-".to_string()
    } else {
        parts.join(" | ")
    }
}

fn socket_error_value_message(value: &Value) -> String {
    let description = value
        .get("description")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let code = value
        .get("code")
        .and_then(Value::as_str)
        .unwrap_or_default();
    match (code.is_empty(), description.is_empty()) {
        (false, false) => format!("{code}: {description}"),
        (false, true) => code.to_string(),
        (true, false) => description.to_string(),
        (true, true) => value.to_string(),
    }
}

fn queue_socketio_values(
    values: Vec<Value>,
    instrument_names: &Arc<RwLock<BTreeMap<DbinternationalInstrumentKey, InstrumentName>>>,
    sender: &Sender<DbinternationalLiveMessage>,
    log_to_console: bool,
) -> Result<(), FeedError> {
    let mut values = values.into_iter();
    let event_name = values
        .next()
        .and_then(|value| value.as_str().map(str::to_string))
        .unwrap_or_default();
    if log_to_console && !event_name.is_empty() && event_name != "joined" {
        println!("DBInternational socket event: {event_name}");
    }

    let payloads = values.collect::<Vec<_>>();

    if event_name == "xts-binary-packet" {
        for value in payloads {
            queue_binary_value_ticks(&value, instrument_names, sender)?;
        }
        return Ok(());
    }

    for value in payloads {
        queue_value_ticks(&value, instrument_names, sender)?;
    }
    Ok(())
}

fn queue_binary_value_ticks(
    value: &Value,
    instrument_names: &Arc<RwLock<BTreeMap<DbinternationalInstrumentKey, InstrumentName>>>,
    sender: &Sender<DbinternationalLiveMessage>,
) -> Result<(), FeedError> {
    let Some(bytes) = binary_bytes_from_value(value) else {
        return Ok(());
    };
    queue_binary_packet_ticks(&bytes, instrument_names, sender)
}

fn binary_bytes_from_value(value: &Value) -> Option<Vec<u8>> {
    let values = value.as_array()?;
    let mut bytes = Vec::with_capacity(values.len());
    for value in values {
        let byte = value.as_u64().and_then(|value| u8::try_from(value).ok())?;
        bytes.push(byte);
    }
    Some(bytes)
}

fn queue_binary_packet_ticks(
    bytes: &[u8],
    instrument_names: &Arc<RwLock<BTreeMap<DbinternationalInstrumentKey, InstrumentName>>>,
    sender: &Sender<DbinternationalLiveMessage>,
) -> Result<(), FeedError> {
    let mut offset = 0usize;
    while offset < bytes.len() {
        let Some(is_compressed) = read_u8(bytes, &mut offset) else {
            return Ok(());
        };
        let Some(message_code) = read_u16_le(bytes, &mut offset) else {
            return Ok(());
        };
        let _header_exchange_segment = read_i16_le(bytes, &mut offset);
        let _header_exchange_instrument_id = read_i32_le(bytes, &mut offset);
        let _book_type = read_i16_le(bytes, &mut offset);
        let _market_type = read_i16_le(bytes, &mut offset);
        let Some(uncompressed_packet_size) = read_u16_le(bytes, &mut offset) else {
            return Ok(());
        };
        let Some(compressed_packet_size) = read_u16_le(bytes, &mut offset) else {
            return Ok(());
        };

        let packet_size = if is_compressed == 1 {
            compressed_packet_size as usize
        } else {
            uncompressed_packet_size as usize
        };
        let Some(packet_end) = offset.checked_add(packet_size) else {
            return Ok(());
        };
        if packet_end > bytes.len() {
            return Ok(());
        }

        if message_code == TOUCHLINE_MESSAGE_CODE {
            if is_compressed == 1 {
                let mut decoder = flate2::read::DeflateDecoder::new(&bytes[offset..packet_end]);
                let mut decompressed = Vec::with_capacity(uncompressed_packet_size as usize);
                decoder.read_to_end(&mut decompressed)?;
                let touchline_payload = if decompressed.len() >= 2
                    && u16::from_le_bytes([decompressed[0], decompressed[1]])
                        == TOUCHLINE_MESSAGE_CODE
                {
                    &decompressed[2..]
                } else {
                    decompressed.as_slice()
                };
                if let Some(event) =
                    parse_binary_touchline_event(touchline_payload, instrument_names)?
                {
                    let _ = sender.send(DbinternationalLiveMessage::Event(event));
                }
            } else if let Some(event) =
                parse_binary_touchline_event(&bytes[offset..packet_end], instrument_names)?
            {
                let _ = sender.send(DbinternationalLiveMessage::Event(event));
            }
        }

        offset = packet_end;
    }

    Ok(())
}

fn parse_binary_touchline_event(
    payload: &[u8],
    instrument_names: &Arc<RwLock<BTreeMap<DbinternationalInstrumentKey, InstrumentName>>>,
) -> Result<Option<PriceEvent>, FeedError> {
    let mut offset = 0usize;
    let Some(message_version) = read_u16_le(payload, &mut offset) else {
        return Ok(None);
    };
    let _application_type = read_u16_le(payload, &mut offset);
    let _token_id = read_u64_le(payload, &mut offset);

    if message_version >= 4 {
        let _sequence_number = read_u64_le(payload, &mut offset);
        let _skip_bytes = read_i32_le(payload, &mut offset);
    }

    let Some(exchange_segment) = read_i16_le(payload, &mut offset) else {
        return Ok(None);
    };
    let Some(exchange_instrument_id) = read_i32_le(payload, &mut offset) else {
        return Ok(None);
    };
    let exchange_timestamp = read_u64_le(payload, &mut offset).unwrap_or(0);

    skip_bytes(payload, &mut offset, 22);
    skip_bytes(payload, &mut offset, 22);

    let last_update_time = read_u64_le(payload, &mut offset).unwrap_or(exchange_timestamp);
    let Some(last_traded_price) = read_f64_le(payload, &mut offset) else {
        return Ok(None);
    };

    skip_bytes(payload, &mut offset, 4 + 4 + 4 + 4 + 8);
    let last_traded_time = read_i64_le(payload, &mut offset).unwrap_or(0);

    let price = Price::new(last_traded_price).map_err(|error| {
        FeedError::Parse(format!(
            "invalid DBInternational binary tick price {last_traded_price}: {error}"
        ))
    })?;
    let key = DbinternationalInstrumentKey {
        exchange_segment: exchange_segment as u16,
        exchange_instrument_id: exchange_instrument_id as u64,
    };
    let instrument_name = instrument_names
        .read()
        .expect("DBInternational instrument map lock poisoned")
        .get(&key)
        .cloned()
        .unwrap_or_else(|| {
            InstrumentName::new(format!(
                "DBINTERNATIONAL:{}:{}",
                key.exchange_segment, key.exchange_instrument_id
            ))
        });
    let event_time = if last_update_time > 0 {
        last_update_time
    } else if last_traded_time > 0 {
        last_traded_time as u64
    } else {
        exchange_timestamp
    };

    Ok(Some(PriceEvent::Tick(PriceTick::new(
        instrument_name,
        price,
        UnixMillis::new(seconds_or_millis_to_millis(event_time)),
    ))))
}

fn read_u8(bytes: &[u8], offset: &mut usize) -> Option<u8> {
    let value = *bytes.get(*offset)?;
    *offset += 1;
    Some(value)
}

fn read_u16_le(bytes: &[u8], offset: &mut usize) -> Option<u16> {
    let value = u16::from_le_bytes(bytes.get(*offset..*offset + 2)?.try_into().ok()?);
    *offset += 2;
    Some(value)
}

fn read_i16_le(bytes: &[u8], offset: &mut usize) -> Option<i16> {
    let value = i16::from_le_bytes(bytes.get(*offset..*offset + 2)?.try_into().ok()?);
    *offset += 2;
    Some(value)
}

fn read_i32_le(bytes: &[u8], offset: &mut usize) -> Option<i32> {
    let value = i32::from_le_bytes(bytes.get(*offset..*offset + 4)?.try_into().ok()?);
    *offset += 4;
    Some(value)
}

fn read_u64_le(bytes: &[u8], offset: &mut usize) -> Option<u64> {
    let value = u64::from_le_bytes(bytes.get(*offset..*offset + 8)?.try_into().ok()?);
    *offset += 8;
    Some(value)
}

fn read_i64_le(bytes: &[u8], offset: &mut usize) -> Option<i64> {
    let value = i64::from_le_bytes(bytes.get(*offset..*offset + 8)?.try_into().ok()?);
    *offset += 8;
    Some(value)
}

fn read_f64_le(bytes: &[u8], offset: &mut usize) -> Option<f64> {
    let value = f64::from_le_bytes(bytes.get(*offset..*offset + 8)?.try_into().ok()?);
    *offset += 8;
    Some(value)
}

fn skip_bytes(bytes: &[u8], offset: &mut usize, count: usize) -> Option<()> {
    let next = offset.checked_add(count)?;
    if next > bytes.len() {
        return None;
    }
    *offset = next;
    Some(())
}

fn queue_value_ticks(
    value: &Value,
    instrument_names: &Arc<RwLock<BTreeMap<DbinternationalInstrumentKey, InstrumentName>>>,
    sender: &Sender<DbinternationalLiveMessage>,
) -> Result<(), FeedError> {
    match value {
        Value::String(text) => queue_text_ticks(text, instrument_names, sender),
        Value::Array(values) => {
            for value in values {
                queue_value_ticks(value, instrument_names, sender)?;
            }
            Ok(())
        }
        Value::Object(_) => {
            if let Some(event) = parse_tick_event(value, instrument_names)? {
                let _ = sender.send(DbinternationalLiveMessage::Event(event));
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn queue_text_ticks(
    text: &str,
    instrument_names: &Arc<RwLock<BTreeMap<DbinternationalInstrumentKey, InstrumentName>>>,
    sender: &Sender<DbinternationalLiveMessage>,
) -> Result<(), FeedError> {
    let text = text.trim();
    if text.is_empty() {
        return Ok(());
    }
    if !(text.starts_with('{') || text.starts_with('[')) {
        return Ok(());
    }

    let value: Value = serde_json::from_str(text)?;
    queue_value_ticks(&value, instrument_names, sender)
}

fn parse_tick_event(
    value: &Value,
    instrument_names: &Arc<RwLock<BTreeMap<DbinternationalInstrumentKey, InstrumentName>>>,
) -> Result<Option<PriceEvent>, FeedError> {
    let Some(key) = tick_instrument_key(value) else {
        return Ok(None);
    };
    let Some(price) = tick_ltp(value) else {
        return Ok(None);
    };
    let price = Price::new(price).map_err(|error| {
        FeedError::Parse(format!(
            "invalid DBInternational tick price {price}: {error}"
        ))
    })?;
    let instrument_name = instrument_names
        .read()
        .expect("DBInternational instrument map lock poisoned")
        .get(&key)
        .cloned()
        .unwrap_or_else(|| {
            InstrumentName::new(format!(
                "DBINTERNATIONAL:{}:{}",
                key.exchange_segment, key.exchange_instrument_id
            ))
        });
    let time = tick_time_millis(value).unwrap_or_else(current_time_millis);

    Ok(Some(PriceEvent::Tick(PriceTick::new(
        instrument_name,
        price,
        UnixMillis::new(time),
    ))))
}

fn tick_instrument_key(value: &Value) -> Option<DbinternationalInstrumentKey> {
    let exchange_segment =
        value_field(value, &["ExchangeSegment", "exchangeSegment"]).and_then(parse_segment)?;
    let exchange_instrument_id = value_field(
        value,
        &[
            "ExchangeInstrumentID",
            "exchangeInstrumentID",
            "exchangeInstrumentId",
        ],
    )
    .and_then(value_as_u64)?;

    Some(DbinternationalInstrumentKey {
        exchange_segment,
        exchange_instrument_id,
    })
}

fn tick_ltp(value: &Value) -> Option<f64> {
    value_field(value, &["LastTradedPrice", "lastTradedPrice"])
        .and_then(value_as_f64)
        .or_else(|| {
            value
                .get("Touchline")
                .or_else(|| value.get("touchline"))
                .and_then(|touchline| {
                    value_field(touchline, &["LastTradedPrice", "lastTradedPrice"])
                        .and_then(value_as_f64)
                })
        })
}

fn tick_time_millis(value: &Value) -> Option<u64> {
    let seconds_or_millis = value_field(
        value,
        &[
            "LastUpdateTime",
            "lastUpdateTime",
            "LastTradedTime",
            "lastTradedTime",
            "ExchangeTimeStamp",
            "exchangeTimeStamp",
        ],
    )
    .and_then(value_as_u64)?;
    Some(seconds_or_millis_to_millis(seconds_or_millis))
}

fn seconds_or_millis_to_millis(seconds_or_millis: u64) -> u64 {
    if seconds_or_millis < 100_000_000_000 {
        seconds_or_millis.saturating_mul(1_000)
    } else {
        seconds_or_millis
    }
}

fn value_field<'a>(value: &'a Value, names: &[&str]) -> Option<&'a Value> {
    names.iter().find_map(|name| value.get(*name))
}

fn value_as_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number
            .as_u64()
            .or_else(|| number.as_i64().and_then(|value| u64::try_from(value).ok()))
            .or_else(|| {
                let value = number.as_f64()?;
                if value.is_finite() && value >= 0.0 {
                    Some(value as u64)
                } else {
                    None
                }
            }),
        Value::String(value) => value.trim().parse::<u64>().ok().or_else(|| {
            let value = value.trim().parse::<f64>().ok()?;
            if value.is_finite() && value >= 0.0 {
                Some(value as u64)
            } else {
                None
            }
        }),
        _ => None,
    }
}

fn value_as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(value) => value.trim().parse().ok(),
        _ => None,
    }
}

fn parse_segment(value: &Value) -> Option<u16> {
    value_as_u64(value)
        .and_then(|value| u16::try_from(value).ok())
        .or_else(|| {
            let label = value.as_str()?.trim();
            exchange_segment_code(label)
        })
}

fn parse_master_catalog(content: &str) -> Result<DbinternationalMasterCatalog, FeedError> {
    let mut catalog = DbinternationalMasterCatalog::default();
    for (index, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Some(instrument) = parse_master_line(line).map_err(|error| {
            FeedError::Parse(format!(
                "DBInternational master line {} failed: {error}",
                index + 1
            ))
        })?
        else {
            continue;
        };

        for alias in &instrument.aliases {
            catalog
                .by_alias
                .entry(normalize_alias(alias))
                .or_default()
                .push(instrument.clone());
        }
    }

    Ok(catalog)
}

fn parse_master_line(line: &str) -> Result<Option<DbinternationalMasterInstrument>, FeedError> {
    let fields = line.split('|').map(str::trim).collect::<Vec<_>>();
    if fields.len() < 7 {
        return Ok(None);
    }

    let Some(exchange_segment) = exchange_segment_code(fields[0]) else {
        return Ok(None);
    };
    let exchange_instrument_id = fields[1].parse::<u64>().map_err(|error| {
        FeedError::Parse(format!(
            "invalid DBInternational exchangeInstrumentID {}: {error}",
            fields[1]
        ))
    })?;

    let aliases = alias_fields(&fields);
    if aliases.is_empty() {
        return Ok(None);
    }

    Ok(Some(DbinternationalMasterInstrument {
        key: DbinternationalInstrumentKey {
            exchange_segment,
            exchange_instrument_id,
        },
        aliases,
    }))
}

fn alias_fields(fields: &[&str]) -> Vec<String> {
    let mut aliases = Vec::new();
    let is_derivative = fields
        .get(2)
        .is_some_and(|instrument_type| matches!(*instrument_type, "1" | "2" | "4"));
    for index in [3usize, 4, 6, 14, 15, 17, 18, 19, 20, 22] {
        if is_derivative && matches!(index, 3 | 15) {
            continue;
        }
        let Some(value) = fields.get(index) else {
            continue;
        };
        if usable_alias(value) {
            push_unique_alias(&mut aliases, value);
        }
    }
    aliases
}

fn usable_alias(value: &str) -> bool {
    let value = value.trim();
    !value.is_empty()
        && value != "-1"
        && value != "0"
        && !value.eq_ignore_ascii_case("null")
        && !value
            .chars()
            .all(|ch| ch.is_ascii_digit() || matches!(ch, '.' | '-'))
}

fn push_unique_alias(aliases: &mut Vec<String>, value: &str) {
    let value = value.trim();
    let normalized = normalize_alias(value);
    if aliases
        .iter()
        .all(|existing| normalize_alias(existing) != normalized)
    {
        aliases.push(value.to_string());
    }
}

fn exchange_segment_code(value: &str) -> Option<u16> {
    match value.trim().to_ascii_uppercase().as_str() {
        "NSECM" => Some(1),
        "NSEFO" => Some(2),
        "NSECD" => Some(3),
        "NSECO" => Some(4),
        "BSECM" => Some(11),
        "BSEFO" => Some(12),
        "BSECD" => Some(13),
        "NCDEX" => Some(21),
        "MCXFO" => Some(51),
        _ => None,
    }
}

fn normalize_alias(value: &str) -> String {
    value.trim().to_ascii_uppercase()
}

fn market_data_socket_url(
    base_url: &str,
    token: &str,
    user_id: &str,
    publish_format: &str,
    broadcast_mode: &str,
) -> Result<String, FeedError> {
    let root = base_root_url(base_url)?;
    Ok(format!(
        "{root}/?token={}&userID={}&publishFormat={}&broadcastMode={}",
        token.trim(),
        user_id.trim(),
        publish_format.trim(),
        broadcast_mode.trim()
    ))
}

fn market_data_subscription_url_from_base_url(base_url: &str) -> Result<String, FeedError> {
    let base_url = normalize_market_data_base_url(base_url);
    if base_url.is_empty() {
        return Err(FeedError::Config(
            "DBInternational market-data session base_url is empty".to_string(),
        ));
    }
    Ok(format!("{base_url}/instruments/subscription"))
}

fn base_root_url(base_url: &str) -> Result<String, FeedError> {
    let normalized = normalize_market_data_base_url(base_url);
    let Some((scheme, rest)) = normalized.split_once("://") else {
        return Err(FeedError::Config(format!(
            "invalid DBInternational base_url {base_url}"
        )));
    };
    let host = rest.split('/').next().unwrap_or("").trim();
    if scheme.is_empty() || host.is_empty() {
        return Err(FeedError::Config(format!(
            "invalid DBInternational base_url {base_url}"
        )));
    }
    Ok(format!("{scheme}://{host}"))
}

fn normalize_market_data_base_url(value: &str) -> String {
    let mut value = value.trim().trim_end_matches('/').to_string();
    for suffix in [
        "/instruments/subscription",
        "/instruments/master",
        "/auth/login",
    ] {
        let lower = value.to_ascii_lowercase();
        if lower.ends_with(suffix) {
            value.truncate(value.len() - suffix.len());
        }
    }
    value
}

fn socketio_manager_path(value: &str) -> String {
    let mut value = value.trim().trim_matches('/').to_string();
    for suffix in ["/socket.io", "/socketio"] {
        let lower = value.to_ascii_lowercase();
        if lower.ends_with(suffix) {
            value.truncate(value.len() - suffix.len());
        }
    }
    if value.eq_ignore_ascii_case("socket.io") || value.eq_ignore_ascii_case("socketio") {
        value.clear();
    }

    if value.is_empty() {
        "/".to_string()
    } else {
        format!("/{value}")
    }
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

fn values_to_log_string(values: &[Value]) -> String {
    values
        .iter()
        .map(Value::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

fn response_snippet(value: &str) -> String {
    let normalized = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.len() > 500 {
        format!("{}...", &normalized[..500])
    } else {
        normalized
    }
}

fn current_time_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_socket_url_from_saved_session_base_url() {
        let url = market_data_socket_url(
            "https://xts3.dbonlinetrade.com/apibinarymarketdata",
            "token.jwt",
            "41415",
            "JSON",
            "Full",
        )
        .expect("socket url");

        assert_eq!(
            url,
            "https://xts3.dbonlinetrade.com/?token=token.jwt&userID=41415&publishFormat=JSON&broadcastMode=Full"
        );
    }

    #[test]
    fn normalizes_socketio_path_for_engineio_v3_client() {
        assert_eq!(
            socketio_manager_path("/apibinarymarketdata/socket.io"),
            "/apibinarymarketdata"
        );
        assert_eq!(
            socketio_manager_path("/apibinarymarketdata/socketio"),
            "/apibinarymarketdata"
        );
        assert_eq!(socketio_manager_path("socket.io"), "/");
    }

    #[test]
    fn parses_cash_and_derivative_master_aliases() {
        let content = "\
NSECM|2885|8|RELIANCE|RELIANCE-EQ|EQ|RELIANCE-EQ|1100100002885|1440.4|1178.6|67662|0.1|1|1|RELIANCE|INE002A01018|1|1|RELIANCE INDUSTRIES LTD-EQ|0|-1|-1
NSEFO|62329|1|NIFTY|NIFTY26JUNFUT|FUTIDX|NIFTY-FUTIDX|2618100062329|26237.7|21467.3|1801|0.1|65|1|-1|Nifty 50|2026-06-30T14:30:00|NIFTY 30JUN2026|1|1|NIFTY26JUNFUT
";
        let catalog = parse_master_catalog(content).expect("catalog");

        let reliance = catalog.resolve_symbol("RELIANCE-EQ").expect("reliance");
        assert_eq!(reliance.key.exchange_segment, 1);
        assert_eq!(reliance.key.exchange_instrument_id, 2885);

        let nifty = catalog.resolve_symbol("NIFTY26JUNFUT").expect("nifty fut");
        assert_eq!(nifty.key.exchange_segment, 2);
        assert_eq!(nifty.key.exchange_instrument_id, 62329);
    }

    #[test]
    fn rejects_ambiguous_alias() {
        let content = "\
NSEFO|62329|1|NIFTY|NIFTY26JUNFUT|FUTIDX|NIFTY-FUTIDX|2618100062329|26237.7|21467.3|1801|0.1|65|1|-1|Nifty 50|2026-06-30T14:30:00|NIFTY 30JUN2026|1|1|NIFTY26JUNFUT
NSEFO|61093|1|NIFTY|NIFTY26JULFUT|FUTIDX|NIFTY-FUTIDX|2620900061093|26345.4|21555.4|1801|0.1|65|1|-1|Nifty 50|2026-07-28T14:30:00|NIFTY 28JUL2026|1|1|NIFTY26JULFUT
";
        let catalog = parse_master_catalog(content).expect("catalog");

        assert!(catalog.resolve_symbol("NIFTY-FUTIDX").is_err());
    }

    #[test]
    fn resolves_index_cache_rows_without_derivative_underlying_ambiguity() {
        let content = "\
NSEFO|75452|2|BANKNIFTY|BANKNIFTY26JUN52100PE|OPTIDX|BANKNIFTY-OPTIDX|2618100075452|27.25|0.05|901|0.05|30|1|-1|Nifty Bank|2026-06-30T14:30:00|52100|4|BANKNIFTY 30JUN2026 PE 52100|1|1|BANKNIFTY26JUN52100PE
NSECM|26001|16|NIFTY BANK|NIFTY BANK|INDEX|NIFTY BANK-INDEX|26001|0|0|0|0.05|1|1|NIFTY BANK|NIFTYBANK|1|1||||-1|NIFTY BANK
BSECM|26065|16|SENSEX|SENSEX|INDEX|SENSEX-INDEX|26065|0|0|0|0.05|1|1|SENSEX|SENSEX|1|1||||-1|SENSEX
";
        let catalog = parse_master_catalog(content).expect("catalog");

        let bank_nifty = catalog.resolve_symbol("NIFTY BANK").expect("bank nifty");
        assert_eq!(bank_nifty.key.exchange_segment, 1);
        assert_eq!(bank_nifty.key.exchange_instrument_id, 26001);

        let sensex = catalog.resolve_symbol("SENSEX").expect("sensex");
        assert_eq!(sensex.key.exchange_segment, 11);
        assert_eq!(sensex.key.exchange_instrument_id, 26065);
    }

    #[test]
    fn parses_exact_subscription_key_from_universe_token() {
        let key = instrument_key_from_token("NSEFO:65238").expect("key");

        assert_eq!(key.exchange_segment, 2);
        assert_eq!(key.exchange_instrument_id, 65238);
    }

    #[test]
    fn parses_touchline_tick_json_to_price_event() {
        let names = Arc::new(RwLock::new(BTreeMap::from([(
            DbinternationalInstrumentKey {
                exchange_segment: 1,
                exchange_instrument_id: 2885,
            },
            InstrumentName::new("RELIANCE"),
        )])));
        let value = serde_json::json!({
            "MessageCode": 1501,
            "ExchangeSegment": 1,
            "ExchangeInstrumentID": "2885",
            "LastTradedPrice": 1430.25,
            "LastUpdateTime": 1782300000
        });

        let event = parse_tick_event(&value, &names)
            .expect("tick parse")
            .expect("tick event");

        let PriceEvent::Tick(tick) = event else {
            panic!("expected tick");
        };
        assert_eq!(tick.instrument_name.as_str(), "RELIANCE");
        assert_eq!(tick.price.as_f64(), 1430.25);
        assert_eq!(tick.time.as_u64(), 1_782_300_000_000);
    }

    #[test]
    fn queues_subscription_list_quotes() {
        let names = Arc::new(RwLock::new(BTreeMap::from([(
            DbinternationalInstrumentKey {
                exchange_segment: 1,
                exchange_instrument_id: 22,
            },
            InstrumentName::new("ACC"),
        )])));
        let (sender, receiver) = mpsc::channel();
        let response = serde_json::json!({
            "type": "success",
            "result": {
                "listQuotes": [
                    "{\"ExchangeSegment\":1,\"ExchangeInstrumentID\":\"22\",\"LastTradedPrice\":1723,\"LastUpdateTime\":1453476469}"
                ]
            }
        });

        queue_subscription_response_ticks(&response, &names, &sender).expect("queue ticks");

        let DbinternationalLiveMessage::Event(PriceEvent::Tick(tick)) =
            receiver.try_recv().expect("queued tick")
        else {
            panic!("expected queued tick");
        };
        assert_eq!(tick.instrument_name.as_str(), "ACC");
        assert_eq!(tick.price.as_f64(), 1723.0);
    }
}
