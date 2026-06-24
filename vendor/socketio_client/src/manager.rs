use crate::errors::Result;
use crate::events::EventEmitter;
use crate::parser::{Decoder, Encoder, Packet};
use crate::socket::Socket;
use crate::transport::{EngineMessage, EngineTransport};
use crate::url;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration, Instant};

#[derive(Debug, Clone)]
pub struct ManagerOptions {
    pub path: String,
    pub reconnection: bool,
    pub reconnection_attempts: Option<u32>, // None = Infinity
    pub reconnection_delay: u64,
    pub reconnection_delay_max: u64,
    pub randomization_factor: f64,
    pub timeout: Option<u64>, // None = disabled
    pub auto_connect: bool,
    pub query: Option<String>,
}

impl Default for ManagerOptions {
    fn default() -> Self {
        Self {
            path: "/socket.io".to_string(),
            reconnection: true,
            reconnection_attempts: None, // Infinity
            reconnection_delay: 1000,
            reconnection_delay_max: 5000,
            randomization_factor: 0.5,
            timeout: Some(20000),
            auto_connect: true,
            query: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadyState {
    Closed,
    Opening,
    Open,
}

pub struct Manager {
    inner: Arc<ManagerInner>,
}

#[derive(Debug)]
pub(crate) struct ManagerInner {
    pub(crate) uri: String,
    pub(crate) opts: ManagerOptions,
    pub(crate) nsps: Arc<Mutex<HashMap<String, Arc<Socket>>>>,
    pub(crate) emitter: EventEmitter,
    pub(crate) transport: Arc<Mutex<Option<EngineTransport>>>,
    pub(crate) ready_state: Arc<Mutex<ReadyState>>,
    pub(crate) encoder: Encoder,
    pub(crate) decoder: Arc<Mutex<Decoder>>,
    pub(crate) reconnecting: Arc<Mutex<bool>>,
    pub(crate) skip_reconnect: Arc<Mutex<bool>>,
    #[allow(dead_code)]
    pub(crate) last_ping: Arc<Mutex<Option<Instant>>>,
    pub(crate) encoding: Arc<Mutex<bool>>,
    pub(crate) packet_buffer: Arc<Mutex<Vec<Packet>>>,
    #[allow(dead_code)]
    pub(crate) backoff: Arc<Mutex<Backoff>>,
}

#[derive(Debug)]
pub(crate) struct Backoff {
    min: u64,
    max: u64,
    jitter: f64,
    attempts: u32,
}

impl Backoff {
    fn new(min: u64, max: u64, jitter: f64) -> Self {
        Self {
            min,
            max,
            jitter,
            attempts: 0,
        }
    }

    #[allow(dead_code)]
    fn duration(&mut self) -> u64 {
        let base = self.min * 2_u64.pow(self.attempts.min(10));
        let delay = base.min(self.max);
        let jitter_amount = (delay as f64 * self.jitter) as u64;
        let jittered = delay + jitter_amount;
        self.attempts += 1;
        jittered
    }

    #[allow(dead_code)]
    fn reset(&mut self) {
        self.attempts = 0;
    }
}

impl Manager {
    pub fn new(uri: &str, opts: Option<ManagerOptions>) -> Result<Self> {
        let opts = opts.unwrap_or_default();
        let parsed_url = url::parse(Some(uri))?;

        let backoff = Backoff::new(
            opts.reconnection_delay,
            opts.reconnection_delay_max,
            opts.randomization_factor,
        );

        let inner = Arc::new(ManagerInner {
            uri: parsed_url.href.clone(),
            opts: opts.clone(),
            nsps: Arc::new(Mutex::new(HashMap::new())),
            emitter: EventEmitter::new(),
            transport: Arc::new(Mutex::new(None)),
            ready_state: Arc::new(Mutex::new(ReadyState::Closed)),
            encoder: Encoder,
            decoder: Arc::new(Mutex::new(Decoder::new())),
            reconnecting: Arc::new(Mutex::new(false)),
            skip_reconnect: Arc::new(Mutex::new(false)),
            last_ping: Arc::new(Mutex::new(None)),
            encoding: Arc::new(Mutex::new(false)),
            packet_buffer: Arc::new(Mutex::new(Vec::new())),
            backoff: Arc::new(Mutex::new(backoff)),
        });

        let manager = Self {
            inner: inner.clone(),
        };

        if opts.auto_connect {
            let inner_clone = inner.clone();
            tokio::spawn(async move {
                if let Err(e) = Manager::open_inner(&inner_clone).await {
                    log::error!("Auto-connect failed: {}", e);
                }
            });
        }

        Ok(manager)
    }

    async fn open_inner(inner: &Arc<ManagerInner>) -> Result<()> {
        let mut ready_state = inner.ready_state.lock().await;
        if *ready_state == ReadyState::Open || *ready_state == ReadyState::Opening {
            return Ok(());
        }
        *ready_state = ReadyState::Opening;
        drop(ready_state);

        log::debug!("Opening connection to {}", inner.uri);

        // Build WebSocket URL
        let ws_url = if inner.uri.starts_with("http://") {
            inner.uri.replace("http://", "ws://")
        } else if inner.uri.starts_with("https://") {
            inner.uri.replace("https://", "wss://")
        } else {
            inner.uri.clone()
        };

        let mut transport = EngineTransport::new(&ws_url, &inner.opts.path)?;
        transport.connect().await?;

        *inner.transport.lock().await = Some(transport);

        // Set timeout if configured
        if let Some(timeout_ms) = inner.opts.timeout {
            let inner_clone = inner.clone();
            let timeout_duration = Duration::from_millis(timeout_ms);
            tokio::spawn(async move {
                sleep(timeout_duration).await;
                if let Err(e) = Manager::handle_timeout_inner(&inner_clone).await {
                    log::error!("Timeout handling error: {}", e);
                }
            });
        }

        // Start receiving loop (will emit "open" event when OPEN packet is received)
        Manager::start_receive_loop_inner(inner).await;

        Ok(())
    }

    pub async fn open(&self) -> Result<()> {
        Self::open_inner(&self.inner).await
    }

    async fn handle_timeout_inner(inner: &Arc<ManagerInner>) -> Result<()> {
        let ready_state = inner.ready_state.lock().await;
        if *ready_state == ReadyState::Opening {
            drop(ready_state);
            inner.emitter.emit("connect_timeout", vec![]);
            if let Some(ref mut transport) = *inner.transport.lock().await {
                transport.close().await?;
            }
        }
        Ok(())
    }

    async fn start_receive_loop_inner(inner: &Arc<ManagerInner>) {
        let transport = inner.transport.clone();
        let decoder = inner.decoder.clone();
        let emitter = inner.emitter.clone();
        let ready_state = inner.ready_state.clone();

        tokio::spawn(async move {
            // Get rx channel reference once, outside the loop
            // This avoids holding transport lock during recv()
            let rx_arc = {
                let transport_guard = transport.lock().await;
                transport_guard.as_ref().map(|t| t.rx.clone())
            };

            if let Some(rx) = rx_arc {
                let ping_interval_ms: u64 = 25000; // Default ping interval (used for initialization only)

                // Spawn a task to send PING periodically (like engine.io-client)
                // In Engine.IO, the CLIENT sends PING to the server, and the server responds with PONG
                let transport_clone_for_ping = transport.clone();
                let ready_state_for_ping = ready_state.clone();
                let ping_interval_clone = Arc::new(Mutex::new(ping_interval_ms));
                let ping_interval_for_task = ping_interval_clone.clone();
                tokio::spawn(async move {
                    let mut interval = tokio::time::interval(Duration::from_millis(25000));
                    // Skip the first tick (it fires immediately), we want to wait for the interval
                    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    // Wait for the first interval before sending the first PING
                    interval.tick().await;

                    loop {
                        // Update interval if pingInterval changed
                        let current_interval = *ping_interval_for_task.lock().await;
                        if current_interval != 25000 {
                            interval =
                                tokio::time::interval(Duration::from_millis(current_interval));
                            interval
                                .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                            // Skip the first tick after interval change
                            interval.tick().await;
                        }

                        // Send PING to server
                        let transport_ping = transport_clone_for_ping.clone();
                        let ready_state_ping = ready_state_for_ping.clone();
                        tokio::spawn(async move {
                            // Check if connection is still open
                            let state = ready_state_ping.lock().await;
                            if *state != ReadyState::Open {
                                return;
                            }
                            drop(state);

                            let mut transport_guard = transport_ping.lock().await;
                            if let Some(ref mut t) = *transport_guard {
                                if let Err(e) = t.send("2").await {
                                    log::error!("Failed to send PING: {}", e);
                                } else {
                                    log::debug!("Sent PING");
                                }
                            }
                        });

                        // Wait for next interval
                        interval.tick().await;
                    }
                });

                loop {
                    // Use rx directly without holding transport lock
                    let msg = {
                        let mut rx_guard = rx.lock().await;
                        rx_guard.recv().await
                    };

                    if let Some(msg) = msg {
                        let msg = match msg {
                            EngineMessage::Text(msg) => msg,
                            EngineMessage::Binary(data) => {
                                let mut decoder_guard = decoder.lock().await;
                                let packets = decoder_guard.add_binary(data);
                                for packet_result in packets {
                                    match packet_result {
                                        Ok(packet) => {
                                            emitter.emit(
                                                "packet",
                                                vec![serde_json::to_value(packet).unwrap()],
                                            );
                                        }
                                        Err(e) => {
                                            log::error!("Packet decode error: {}", e);
                                            emitter.emit(
                                                "error",
                                                vec![serde_json::Value::String(format!(
                                                    "Packet decode error: {}",
                                                    e
                                                ))],
                                            );
                                        }
                                    }
                                }
                                continue;
                            }
                        };
                        log::debug!("Received Engine.IO message: {}", msg);
                        // Process Engine.IO message
                        // Engine.IO v3 format: <type><data>
                        // Type is single character: 0=open, 1=close, 2=ping, 3=pong, 4=message, 5=upgrade, 6=noop
                        if msg.is_empty() {
                            log::warn!("Received empty message");
                            continue;
                        }
                        if msg.starts_with('0') {
                            // OPEN packet - handshake response (contains JSON data)
                            log::debug!("Received OPEN packet");
                            if msg.len() > 1 {
                                let handshake_data = &msg[1..];
                                // Parse handshake JSON (contains sid, upgrades, pingInterval, pingTimeout)
                                if let Ok(handshake) =
                                    serde_json::from_str::<serde_json::Value>(handshake_data)
                                {
                                    log::debug!("Handshake data: {:?}", handshake);
                                    // Update pingInterval from handshake
                                    if let Some(ping_interval) =
                                        handshake.get("pingInterval").and_then(|v| v.as_u64())
                                    {
                                        *ping_interval_clone.lock().await = ping_interval;
                                    }
                                }
                            }
                            // OPEN packet received - connection is now open
                            *ready_state.lock().await = ReadyState::Open;
                            emitter.emit("open", vec![]);
                            continue;
                        } else if msg.starts_with('2') {
                            // PING - emit 'ping' event (like socket.io-client)
                            log::debug!("Received PING");
                            emitter.emit("ping", vec![]);
                            // Send PONG - clone transport and send in background task
                            // This avoids blocking the receive loop
                            let transport_clone = transport.clone();
                            tokio::spawn(async move {
                                // Lock transport and send PONG
                                let mut transport_guard = transport_clone.lock().await;
                                if let Some(ref mut t) = *transport_guard {
                                    if let Err(e) = t.send("3").await {
                                        log::error!("Failed to send PONG: {}", e);
                                    } else {
                                        log::debug!("Sent PONG");
                                    }
                                }
                            });
                            continue;
                        } else if msg.starts_with('3') {
                            // PONG - emit 'pong' event (like socket.io-client)
                            log::debug!("Received PONG");
                            emitter.emit("pong", vec![]);
                            continue;
                        } else if msg.starts_with('4') {
                            // MESSAGE - contains Socket.IO packet
                            let socket_io_packet = &msg[1..];
                            let mut decoder_guard = decoder.lock().await;
                            let packets = decoder_guard.add(socket_io_packet);

                            for packet_result in packets {
                                match packet_result {
                                    Ok(packet) => {
                                        emitter.emit(
                                            "packet",
                                            vec![serde_json::to_value(packet).unwrap()],
                                        );
                                    }
                                    Err(e) => {
                                        log::error!("Packet decode error: {}", e);
                                        emitter.emit(
                                            "error",
                                            vec![serde_json::Value::String(format!(
                                                "Packet decode error: {}",
                                                e
                                            ))],
                                        );
                                    }
                                }
                            }
                        } else if msg.starts_with('1') {
                            // CLOSE
                            log::debug!("Received CLOSE packet");
                            *ready_state.lock().await = ReadyState::Closed;
                            break;
                        } else if msg.starts_with('5') {
                            // UPGRADE (not used in WebSocket-only transport)
                            log::debug!("Received UPGRADE packet");
                            continue;
                        } else if msg.starts_with('6') {
                            // NOOP
                            log::debug!("Received NOOP packet");
                            continue;
                        } else {
                            // Unknown message type
                            log::warn!("Unknown Engine.IO message type: {}", msg);
                            continue;
                        }
                    } else {
                        // Connection closed - no more messages from channel
                        *ready_state.lock().await = ReadyState::Closed;
                        emitter.emit(
                            "close",
                            vec![serde_json::Value::String("transport close".to_string())],
                        );
                        break;
                    }
                }
            }
        });
    }

    pub async fn packet(&self, packet: Packet) -> Result<()> {
        Self::packet_inner(&self.inner, packet).await
    }

    pub(crate) async fn packet_inner(inner: &Arc<ManagerInner>, packet: Packet) -> Result<()> {
        log::debug!("Writing packet: {:?}", packet);

        let mut encoding = inner.encoding.lock().await;
        if *encoding {
            // Add to buffer
            inner.packet_buffer.lock().await.push(packet.clone());
            return Ok(());
        }

        *encoding = true;
        drop(encoding);

        // Encode packet
        // Note: Query string handling for CONNECT packets is done in Packet::encode()
        let encoded = match inner.encoder.encode(&packet) {
            Ok(enc) => enc,
            Err(e) => {
                *inner.encoding.lock().await = false;
                return Err(e);
            }
        };

        // Send through transport
        let mut transport_guard = inner.transport.lock().await;
        if let Some(ref mut transport) = *transport_guard {
            for enc in encoded {
                // Prepend Engine.IO MESSAGE type (4)
                let engine_packet = format!("4{}", enc);
                if let Err(e) = transport.send(&engine_packet).await {
                    *inner.encoding.lock().await = false;
                    return Err(e);
                }
            }
        } else {
            *inner.encoding.lock().await = false;
            return Err(crate::errors::SocketError::Transport(
                "Transport not found".to_string(),
            ));
        }
        *inner.encoding.lock().await = false;
        Manager::process_packet_queue_inner(inner).await;

        Ok(())
    }

    #[allow(dead_code)]
    async fn process_packet_queue(&self) {
        Self::process_packet_queue_inner(&self.inner).await
    }

    async fn process_packet_queue_inner(inner: &Arc<ManagerInner>) {
        loop {
            let packet = {
                let mut buffer = inner.packet_buffer.lock().await;
                if buffer.is_empty() {
                    break;
                }
                buffer.remove(0)
            };

            // Use Box::pin to avoid recursion issues
            let fut = Box::pin(Self::packet_inner(inner, packet));
            if let Err(e) = fut.await {
                log::error!("Error processing queued packet: {}", e);
                break;
            }
        }
    }

    pub async fn socket(&self, nsp: &str) -> Arc<Socket> {
        let mut nsps_guard = self.inner.nsps.lock().await;
        if let Some(socket) = nsps_guard.get(nsp) {
            return socket.clone();
        }

        let socket = Arc::new(Socket::new(self.inner.clone(), nsp.to_string()));
        nsps_guard.insert(nsp.to_string(), socket.clone());
        socket
    }

    pub fn on<F>(&self, event: &str, callback: F)
    where
        F: Fn(Vec<serde_json::Value>) + Send + Sync + 'static,
    {
        self.inner.emitter.on(event, callback);
    }

    pub async fn close(&self) -> Result<()> {
        *self.inner.skip_reconnect.lock().await = true;
        *self.inner.reconnecting.lock().await = false;

        if let Some(ref mut transport) = *self.inner.transport.lock().await {
            transport.close().await?;
        }

        *self.inner.ready_state.lock().await = ReadyState::Closed;
        Ok(())
    }
}
