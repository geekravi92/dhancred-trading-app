use crate::errors::{Result, SocketError};
use crate::EIO_VERSION;
use futures::stream::SplitSink;
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use url::Url;

/// Engine.IO packet types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnginePacketType {
    Open = 0,
    Close = 1,
    Ping = 2,
    Pong = 3,
    Message = 4,
    Upgrade = 5,
    Noop = 6,
}

impl EnginePacketType {
    pub fn from_u8(value: u8) -> Result<Self> {
        match value {
            0 => Ok(EnginePacketType::Open),
            1 => Ok(EnginePacketType::Close),
            2 => Ok(EnginePacketType::Ping),
            3 => Ok(EnginePacketType::Pong),
            4 => Ok(EnginePacketType::Message),
            5 => Ok(EnginePacketType::Upgrade),
            6 => Ok(EnginePacketType::Noop),
            _ => Err(SocketError::InvalidPacketType(value)),
        }
    }

    pub fn to_u8(self) -> u8 {
        self as u8
    }
}

/// Engine.IO transport message.
#[derive(Debug)]
pub(crate) enum EngineMessage {
    Text(String),
    Binary(Vec<u8>),
}

/// Engine.IO transport using WebSocket
#[derive(Debug)]
pub struct EngineTransport {
    ws_sink: Arc<
        Mutex<Option<SplitSink<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>, Message>>>,
    >,
    tx: mpsc::UnboundedSender<EngineMessage>,
    pub(crate) rx: Arc<Mutex<mpsc::UnboundedReceiver<EngineMessage>>>,
    url: String,
    path: String,
}

impl EngineTransport {
    pub fn new(uri: &str, path: &str) -> Result<Self> {
        let (tx, rx) = mpsc::unbounded_channel();

        Ok(Self {
            ws_sink: Arc::new(Mutex::new(None)),
            tx,
            rx: Arc::new(Mutex::new(rx)),
            url: uri.to_string(),
            path: path.to_string(),
        })
    }

    /// Connect to the server
    pub async fn connect(&mut self) -> Result<()> {
        let url = self.build_handshake_url()?;
        log::info!("Connecting to: {}", url);

        let (ws_stream, _) = connect_async(&url)
            .await
            .map_err(|e| SocketError::WebSocket(format!("Connection failed: {}", e)))?;

        // Split the stream into sink (for writing) and stream (for reading)
        let (ws_sink, ws_stream_read) = ws_stream.split();

        // Store the sink for writing
        *self.ws_sink.lock().await = Some(ws_sink);

        // Start receiving messages in a separate task
        let tx_clone = self.tx.clone();

        tokio::spawn(async move {
            let mut ws_stream_read = ws_stream_read;
            while let Some(msg) = ws_stream_read.next().await {
                match msg {
                    Ok(Message::Text(text)) => {
                        log::debug!("Received WebSocket text message: {}", text);
                        if let Err(_) = tx_clone.send(EngineMessage::Text(text)) {
                            break;
                        }
                    }
                    Ok(Message::Binary(data)) => {
                        // Engine.IO v3: binary data is prefixed with packet type
                        if let Some(&packet_type) = data.first() {
                            if packet_type == EnginePacketType::Message.to_u8() {
                                if let Err(_) =
                                    tx_clone.send(EngineMessage::Binary(data[1..].to_vec()))
                                {
                                    break;
                                }
                            }
                        }
                    }
                    Ok(Message::Close(_)) => {
                        break;
                    }
                    Ok(Message::Ping(_)) => {
                        // Engine.IO handles PING as text message "2", not as WebSocket PING frame
                        // But some servers might send WebSocket PING frames
                        // tokio-tungstenite automatically responds with PONG, but Engine.IO
                        // expects a text message "2" to be sent to the channel
                        // So we always send "2" to the channel when we receive a WebSocket PING
                        if let Err(_) = tx_clone.send(EngineMessage::Text("2".to_string())) {
                            break;
                        }
                    }
                    Ok(Message::Pong(_)) => {
                        // Engine.IO handles PONG as text message "3", not as WebSocket PONG frame
                        // So we don't need to handle it here
                    }
                    Err(e) => {
                        log::error!("WebSocket error: {}", e);
                        break;
                    }
                    _ => {}
                }
            }
        });

        Ok(())
    }

    /// Build handshake URL with EIO=3 parameter
    fn build_handshake_url(&self) -> Result<String> {
        let mut url =
            Url::parse(&self.url).map_err(|e| SocketError::Url(format!("Invalid URL: {}", e)))?;

        // Add Engine.IO version 3 parameter
        url.query_pairs_mut()
            .append_pair("EIO", &EIO_VERSION.to_string())
            .append_pair("transport", "websocket");

        // Add path
        let path = if self.path.is_empty() {
            "/socket.io/".to_string()
        } else if self.path.ends_with('/') {
            format!("{}/socket.io/", self.path.trim_end_matches('/'))
        } else {
            format!("{}/socket.io/", self.path)
        };

        url.set_path(&path);

        Ok(url.to_string())
    }

    /// Send a message
    pub async fn send(&self, data: &str) -> Result<()> {
        let mut sink = self.ws_sink.lock().await;
        if let Some(ref mut ws_sink) = *sink {
            // Note: data already contains the Engine.IO packet type prefix
            // (e.g., "4" for MESSAGE, "3" for PONG)
            // So we send it as-is
            log::debug!("Sending WebSocket message: {}", data);
            ws_sink
                .send(Message::Text(data.to_string()))
                .await
                .map_err(|e| SocketError::Transport(format!("Send failed: {}", e)))?;
            Ok(())
        } else {
            Err(SocketError::Transport("Not connected".to_string()))
        }
    }

    /// Receive a message
    #[allow(dead_code)]
    pub(crate) async fn recv(&self) -> Option<EngineMessage> {
        let mut rx = self.rx.lock().await;
        rx.recv().await
    }

    /// Close the connection
    pub async fn close(&mut self) -> Result<()> {
        let mut sink = self.ws_sink.lock().await;
        if let Some(ref mut ws_sink) = *sink {
            ws_sink
                .close()
                .await
                .map_err(|e| SocketError::Transport(format!("Close failed: {}", e)))?;
        }
        *sink = None;
        Ok(())
    }

    /// Check if connected
    pub async fn is_connected(&self) -> bool {
        self.ws_sink.lock().await.is_some()
    }
}
