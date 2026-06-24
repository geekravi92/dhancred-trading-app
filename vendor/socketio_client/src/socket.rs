use crate::errors::{Result, SocketError};
use crate::events::EventEmitter;
use crate::manager::{Manager, ManagerInner};
use crate::parser::{Packet, PacketType};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct Socket {
    manager: Arc<ManagerInner>,
    nsp: String,
    emitter: EventEmitter,
    connected: Arc<Mutex<bool>>,
    disconnected: Arc<Mutex<bool>>,
    #[allow(dead_code)]
    ids: Arc<Mutex<u64>>,
    acks: Arc<Mutex<std::collections::HashMap<u64, Box<dyn Fn(Vec<Value>) + Send + Sync>>>>,
    receive_buffer: Arc<Mutex<Vec<Vec<Value>>>>,
    send_buffer: Arc<Mutex<Vec<Packet>>>,
    query: Option<String>,
    packet_tx: mpsc::UnboundedSender<Packet>,
}

impl std::fmt::Debug for Socket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Socket").field("nsp", &self.nsp).finish()
    }
}

impl Socket {
    pub(crate) fn new(manager: Arc<ManagerInner>, nsp: String) -> Self {
        // Create a channel for sending packets to manager
        let (tx, mut rx) = mpsc::unbounded_channel::<Packet>();
        let manager_clone = manager.clone();

        // Spawn task to handle packets
        tokio::spawn(async move {
            while let Some(packet) = rx.recv().await {
                if let Err(e) = Manager::packet_inner(&manager_clone, packet).await {
                    log::error!("Error sending packet: {}", e);
                }
            }
        });

        let socket = Self {
            manager,
            nsp: nsp.clone(),
            emitter: EventEmitter::new(),
            connected: Arc::new(Mutex::new(false)),
            disconnected: Arc::new(Mutex::new(true)),
            ids: Arc::new(Mutex::new(0)),
            acks: Arc::new(Mutex::new(std::collections::HashMap::new())),
            receive_buffer: Arc::new(Mutex::new(Vec::new())),
            send_buffer: Arc::new(Mutex::new(Vec::new())),
            query: None,
            packet_tx: tx,
        };

        // Subscribe to manager events
        let socket_arc = Arc::new(socket);
        let socket_for_packet = socket_arc.clone();
        let socket_for_open = socket_arc.clone();
        let socket_for_close = socket_arc.clone();

        socket_arc
            .manager
            .emitter
            .on("packet", move |data: Vec<Value>| {
                if let Some(packet_value) = data.first() {
                    if let Ok(packet) = serde_json::from_value::<Packet>(packet_value.clone()) {
                        let socket = socket_for_packet.clone();
                        tokio::spawn(async move {
                            if let Err(e) = socket.onpacket(packet).await {
                                log::error!("Error handling packet: {}", e);
                            }
                        });
                    }
                }
            });

        socket_arc.manager.emitter.on("open", move |_| {
            let socket = socket_for_open.clone();
            tokio::spawn(async move {
                if let Err(e) = socket.onopen().await {
                    log::error!("Error on open: {}", e);
                }
            });
        });

        socket_arc.manager.emitter.on("close", move |_| {
            let socket = socket_for_close.clone();
            tokio::spawn(async move {
                socket.onclose("io server disconnect").await;
            });
        });

        // Extract Socket from Arc
        // If it fails (due to references in handlers), clone the data
        Arc::try_unwrap(socket_arc).unwrap_or_else(|arc| {
            // If we can't unwrap, create a new Socket with the same data
            // This can happen if handlers still hold references
            let socket_ref = arc.as_ref();
            Self {
                manager: socket_ref.manager.clone(),
                nsp: socket_ref.nsp.clone(),
                emitter: socket_ref.emitter.clone(),
                connected: socket_ref.connected.clone(),
                disconnected: socket_ref.disconnected.clone(),
                ids: socket_ref.ids.clone(),
                acks: socket_ref.acks.clone(),
                receive_buffer: socket_ref.receive_buffer.clone(),
                send_buffer: socket_ref.send_buffer.clone(),
                query: socket_ref.query.clone(),
                packet_tx: socket_ref.packet_tx.clone(),
            }
        })
    }

    pub async fn connect(&self) -> Result<()> {
        let connected = *self.connected.lock().await;
        if connected {
            return Ok(());
        }

        self.sub_events();

        // Ensure manager is open
        if *self.manager.ready_state.lock().await == crate::manager::ReadyState::Closed {
            // Manager will handle opening
        }

        if *self.manager.ready_state.lock().await == crate::manager::ReadyState::Open {
            self.onopen().await?;
        }

        self.emitter.emit("connecting", vec![]);
        Ok(())
    }

    fn sub_events(&self) {
        // Events are already subscribed in constructor
    }

    async fn onopen(&self) -> Result<()> {
        log::debug!("Transport is open - connecting");

        // Write connect packet if necessary
        if self.nsp != "/" {
            let mut packet = Packet::new(PacketType::Connect).with_namespace(self.nsp.clone());
            if let Some(ref query) = self.query {
                packet = packet.with_query(query.clone());
            }
            self.packet(packet).await?;
        }

        Ok(())
    }

    async fn onpacket(&self, packet: Packet) -> Result<()> {
        let same_namespace = packet.nsp.as_ref().map(|n| n == &self.nsp).unwrap_or(false);
        let root_namespace_error = packet.packet_type == PacketType::Error.to_u8()
            && packet.nsp.as_ref().map(|n| n == "/").unwrap_or(false);

        if !same_namespace && !root_namespace_error {
            return Ok(());
        }

        match PacketType::from_u8(packet.packet_type)? {
            PacketType::Connect => {
                self.onconnect().await?;
            }
            PacketType::Event | PacketType::BinaryEvent => {
                self.onevent(packet).await?;
            }
            PacketType::Ack | PacketType::BinaryAck => {
                self.onack(packet).await?;
            }
            PacketType::Disconnect => {
                self.ondisconnect().await?;
            }
            PacketType::Error => {
                self.emitter
                    .emit("error", vec![packet.data.unwrap_or(Value::Null)]);
            }
        }

        Ok(())
    }

    async fn onevent(&self, packet: Packet) -> Result<()> {
        let args: Vec<Value> = if let Some(data) = packet.data {
            if let Value::Array(arr) = data {
                arr
            } else {
                vec![data]
            }
        } else {
            vec![]
        };

        log::info!("Socket.IO event received, args count: {}, first arg: {:?}", args.len(), args.first());
        log::debug!("Emitting event with args: {:?}", args);

        let connected = *self.connected.lock().await;
        if connected {
            if let Some(id) = packet.id {
                // Attach ack callback - store it for later use
                let _ack_fn = self.ack(id);
                // Note: In a real implementation, we'd need to handle the ack callback properly
                if !args.is_empty() {
                    let event_name = args[0].as_str().unwrap_or("");
                    let event_data = args[1..].to_vec();
                    log::info!("Emitting event '{}' with {} data items", event_name, event_data.len());
                    self.emitter.emit(event_name, event_data);
                }
            } else {
                if !args.is_empty() {
                    let event_name = args[0].as_str().unwrap_or("");
                    let event_data = args[1..].to_vec();
                    log::info!("Emitting event '{}' with {} data items", event_name, event_data.len());
                    self.emitter.emit(event_name, event_data);
                }
            }
        } else {
            log::debug!("Socket not connected, buffering event");
            self.receive_buffer.lock().await.push(args);
        }

        Ok(())
    }

    fn ack(&self, id: u64) -> Box<dyn Fn(Vec<Value>) + Send + Sync> {
        let packet_tx = self.packet_tx.clone();
        let nsp = self.nsp.clone();
        Box::new(move |args: Vec<Value>| {
            let packet_tx_clone = packet_tx.clone();
            let nsp_clone = nsp.clone();
            tokio::spawn(async move {
                let packet = Packet::new(PacketType::Ack)
                    .with_namespace(nsp_clone)
                    .with_id(id)
                    .with_data(Value::Array(args));
                if let Err(_) = packet_tx_clone.send(packet) {
                    log::error!("Error sending ack: channel closed");
                }
            });
        })
    }

    async fn onack(&self, packet: Packet) -> Result<()> {
        if let Some(id) = packet.id {
            let mut acks = self.acks.lock().await;
            if let Some(ack_fn) = acks.remove(&id) {
                let data: Vec<Value> = if let Some(packet_data) = packet.data {
                    if let Value::Array(arr) = packet_data {
                        arr
                    } else {
                        vec![packet_data]
                    }
                } else {
                    vec![]
                };
                ack_fn(data);
            }
        }
        Ok(())
    }

    async fn onconnect(&self) -> Result<()> {
        *self.connected.lock().await = true;
        *self.disconnected.lock().await = false;
        self.emit_buffered().await;
        self.emitter.emit("connect", vec![]);
        Ok(())
    }

    async fn emit_buffered(&self) {
        // Emit buffered received events
        let receive_buffer = std::mem::take(&mut *self.receive_buffer.lock().await);
        for args in receive_buffer {
            if !args.is_empty() {
                let event_name = args[0].as_str().unwrap_or("");
                self.emitter.emit(event_name, args[1..].to_vec());
            }
        }

        // Send buffered packets
        let send_buffer = std::mem::take(&mut *self.send_buffer.lock().await);
        for packet in send_buffer {
            if let Err(e) = self.packet(packet).await {
                log::error!("Error sending buffered packet: {}", e);
            }
        }
    }

    async fn ondisconnect(&self) -> Result<()> {
        log::debug!("Server disconnect ({})", self.nsp);
        self.destroy().await;
        self.onclose("io server disconnect").await;
        Ok(())
    }

    async fn onclose(&self, reason: &str) {
        log::debug!("Close ({})", reason);
        *self.connected.lock().await = false;
        *self.disconnected.lock().await = true;
        self.emitter
            .emit("disconnect", vec![Value::String(reason.to_string())]);
    }

    async fn destroy(&self) {
        // Cleanup is handled by manager
    }

    pub fn nsp(&self) -> &str {
        &self.nsp
    }

    async fn packet(&self, packet: Packet) -> Result<()> {
        let mut packet = packet;
        packet.nsp = Some(self.nsp.clone());
        self.packet_tx
            .send(packet)
            .map_err(|e| {
                log::error!("Failed to send packet to manager channel: {}", e);
                SocketError::Transport(format!("Failed to send packet: {}", e))
            })?;
        Ok(())
    }

    pub async fn emit(&self, event: &str, data: Vec<Value>) -> Result<()> {
        // Check if it's a reserved event
        let reserved_events = [
            "connect",
            "connect_error",
            "connect_timeout",
            "connecting",
            "disconnect",
            "error",
            "reconnect",
            "reconnect_attempt",
            "reconnect_failed",
            "reconnect_error",
            "reconnecting",
            "ping",
            "pong",
        ];

        if reserved_events.contains(&event) {
            self.emitter.emit(event, data);
            return Ok(());
        }

        let mut args = vec![Value::String(event.to_string())];
        args.extend(data);

        let packet = Packet::new(PacketType::Event).with_data(Value::Array(args));

        let connected = *self.connected.lock().await;
        if connected {
            self.packet(packet).await?;
        } else {
            self.send_buffer.lock().await.push(packet);
        }

        Ok(())
    }

    pub fn on<F>(&self, event: &str, callback: F)
    where
        F: Fn(Vec<Value>) + Send + Sync + 'static,
    {
        self.emitter.on(event, callback);
    }

    pub async fn disconnect(&self) -> Result<()> {
        let connected = *self.connected.lock().await;
        if connected {
            log::debug!("Performing disconnect ({})", self.nsp);
            let packet = Packet::new(PacketType::Disconnect);
            self.packet(packet).await?;
        }

        self.destroy().await;

        let connected = *self.connected.lock().await;
        if connected {
            self.onclose("io client disconnect").await;
        }

        Ok(())
    }
}
