use crate::errors::{Result, SocketError};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Socket.IO packet types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketType {
    Connect = 0,
    Disconnect = 1,
    Event = 2,
    Ack = 3,
    Error = 4,
    BinaryEvent = 5,
    BinaryAck = 6,
}

impl PacketType {
    pub fn from_u8(value: u8) -> Result<Self> {
        match value {
            0 => Ok(PacketType::Connect),
            1 => Ok(PacketType::Disconnect),
            2 => Ok(PacketType::Event),
            3 => Ok(PacketType::Ack),
            4 => Ok(PacketType::Error),
            5 => Ok(PacketType::BinaryEvent),
            6 => Ok(PacketType::BinaryAck),
            _ => Err(SocketError::InvalidPacketType(value)),
        }
    }

    pub fn to_u8(self) -> u8 {
        self as u8
    }
}

/// Socket.IO packet
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Packet {
    #[serde(rename = "type")]
    pub packet_type: u8,
    pub nsp: Option<String>,
    pub data: Option<Value>,
    pub id: Option<u64>,
    pub query: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attachments: Option<usize>,
}

impl Packet {
    pub fn new(packet_type: PacketType) -> Self {
        Self {
            packet_type: packet_type.to_u8(),
            nsp: Some("/".to_string()), // Default namespace is "/"
            data: None,
            id: None,
            query: None,
            attachments: None,
        }
    }

    pub fn with_namespace(mut self, nsp: String) -> Self {
        self.nsp = Some(nsp);
        self
    }

    pub fn with_data(mut self, data: Value) -> Self {
        self.data = Some(data);
        self
    }

    pub fn with_id(mut self, id: u64) -> Self {
        self.id = Some(id);
        self
    }

    pub fn with_query(mut self, query: String) -> Self {
        self.query = Some(query);
        self
    }

    /// Encode packet to string (for Engine.IO transport)
    /// Format: type[attachments-]nsp,id[data]
    /// Based on socket.io-parser encodeAsString function
    pub fn encode(&self) -> Result<String> {
        let mut result = String::new();

        // Packet type
        result.push_str(&self.packet_type.to_string());

        // Attachments (only for BINARY_EVENT and BINARY_ACK)
        // Note: Binary packets are handled separately in Encoder with binary encoding
        // For non-binary encoding, we skip attachments

        // Namespace (if not "/", append it followed by a comma)
        if let Some(ref nsp) = self.nsp {
            if nsp != "/" {
                result.push_str(nsp);
                // For CONNECT packets, append query to namespace before comma
                if self.packet_type == PacketType::Connect as u8 {
                    if let Some(ref query) = self.query {
                        if !nsp.contains('?') {
                            result.push('?');
                            result.push_str(query);
                        }
                    }
                }
                result.push(',');
            }
        }

        // ID (immediately followed by namespace comma, or after type if no namespace)
        if let Some(id) = self.id {
            result.push_str(&id.to_string());
        }

        // Data (JSON encoded, immediately after ID, no comma)
        if let Some(ref data) = self.data {
            let json_str = serde_json::to_string(data)?;
            result.push_str(&json_str);
        }

        Ok(result)
    }

    /// Decode packet from string (from Engine.IO transport)
    /// Format: type[attachments-]nsp,id[data]
    /// Based on socket.io-parser decodeString function
    pub fn decode(s: &str) -> Result<Self> {
        if s.is_empty() {
            return Err(SocketError::Parser("Empty packet string".to_string()));
        }

        let mut i = 0;
        let chars: Vec<char> = s.chars().collect();

        // Parse packet type
        let packet_type = PacketType::from_u8(
            chars
                .get(i)
                .ok_or_else(|| SocketError::Parser("Invalid packet".to_string()))?
                .to_digit(10)
                .ok_or_else(|| SocketError::Parser("Invalid packet type".to_string()))?
                as u8,
        )?;
        i += 1;

        let mut packet = Packet::new(packet_type);

        // Parse attachments (only for BINARY_EVENT and BINARY_ACK)
        if packet_type == PacketType::BinaryEvent || packet_type == PacketType::BinaryAck {
            if i < chars.len() && chars[i] != '-' && chars[i] != '/' && chars[i] != ',' {
                let start = i;
                while i < chars.len() && chars[i] != '-' {
                    i += 1;
                }
                if i < chars.len() && chars[i] == '-' {
                    let attachment_str: String = chars[start..i].iter().collect();
                    packet.attachments = attachment_str.parse::<usize>().ok();
                    i += 1;
                } else {
                    i = start; // No attachments, reset
                }
            }
        }

        // Parse namespace (if starts with '/')
        if i < chars.len() && chars[i] == '/' {
            let start = i;
            while i < chars.len() {
                let c = chars[i];
                if c == ',' {
                    break;
                }
                i += 1;
            }
            let nsp_str: String = chars[start..i].iter().collect();

            // Check if query string is in namespace (for CONNECT packets)
            if nsp_str.contains('?') {
                let parts: Vec<&str> = nsp_str.splitn(2, '?').collect();
                packet.nsp = Some(parts[0].to_string());
                if parts.len() > 1 {
                    packet.query = Some(parts[1].to_string());
                }
            } else {
                packet.nsp = Some(nsp_str);
            }

            // Skip comma after namespace
            if i < chars.len() && chars[i] == ',' {
                i += 1;
            }
        } else {
            packet.nsp = Some("/".to_string());
        }

        // Parse ID (immediately after namespace comma, or after type if no namespace)
        // ID is numeric, stop at non-numeric (which would be start of JSON)
        if i < chars.len() {
            let start = i;
            while i < chars.len() {
                let c = chars[i];
                // ID is numeric, stop at non-numeric (start of JSON data)
                if !c.is_ascii_digit() {
                    break;
                }
                i += 1;
            }
            if i > start {
                let id_str: String = chars[start..i].iter().collect();
                if let Ok(id) = id_str.parse::<u64>() {
                    packet.id = Some(id);
                }
            }
        }

        // Parse data (JSON, immediately after ID, no comma)
        if i < chars.len() {
            let data_str: String = chars[i..].iter().collect();
            if !data_str.is_empty() {
                packet.data = Some(serde_json::from_str(&data_str)?);
            }
        }

        Ok(packet)
    }
}

/// Encoder for Socket.IO packets
#[derive(Debug)]
pub struct Encoder;

impl Encoder {
    pub fn encode(&self, packet: &Packet) -> Result<Vec<String>> {
        let encoded = packet.encode()?;
        Ok(vec![encoded])
    }
}

/// Decoder for Socket.IO packets
#[derive(Debug)]
pub struct Decoder {
    buffer: String,
    pending_binary_packet: Option<Packet>,
    pending_binary_values: Vec<Value>,
}

impl Decoder {
    pub fn new() -> Self {
        Self {
            buffer: String::new(),
            pending_binary_packet: None,
            pending_binary_values: Vec::new(),
        }
    }

    pub fn add(&mut self, data: &str) -> Vec<Result<Packet>> {
        self.buffer.push_str(data);
        let mut packets = Vec::new();

        // Engine.IO v4+ uses '\x1e' (record separator) to separate packets
        // Engine.IO v3 sends packets separately, but we support both formats
        let parts: Vec<&str> = self.buffer.split('\x1e').collect();

        // Determine what to keep in buffer (incomplete packet at the end)
        let buffer_content = if parts.len() > 1 {
            // Check if last part should be kept (if buffer doesn't end with separator)
            if !self.buffer.ends_with('\x1e') {
                parts.last().map(|s| s.to_string()).unwrap_or_default()
            } else {
                String::new()
            }
        } else {
            // Single packet - in Engine.IO v3, each packet is sent separately
            // Try to decode it, but keep in buffer if it fails (might be incomplete)
            String::new()
        };

        // Process complete packets (all but the last if multiple parts)
        let process_count = if parts.len() > 1 && !self.buffer.ends_with('\x1e') {
            parts.len() - 1
        } else {
            parts.len()
        };

        for part in parts.iter().take(process_count) {
            if !part.is_empty() {
                match Packet::decode(part) {
                    Ok(packet) if binary_attachment_count(&packet) > 0 => {
                        self.pending_binary_packet = Some(packet);
                        self.pending_binary_values.clear();
                    }
                    result => packets.push(result),
                }
            }
        }

        // Update buffer with remaining content
        self.buffer = buffer_content;

        packets
    }

    pub fn add_binary(&mut self, data: Vec<u8>) -> Vec<Result<Packet>> {
        let mut packets = Vec::new();
        let Some(packet) = self.pending_binary_packet.as_ref() else {
            return packets;
        };

        self.pending_binary_values
            .push(Value::Array(data.into_iter().map(|byte| Value::from(byte)).collect()));
        let expected = binary_attachment_count(packet);
        if self.pending_binary_values.len() < expected {
            return packets;
        }

        let mut packet = self.pending_binary_packet.take().unwrap();
        if let Some(data) = packet.data.take() {
            packet.data = Some(replace_placeholders(data, &self.pending_binary_values));
        }
        self.pending_binary_values.clear();
        packets.push(Ok(packet));
        packets
    }

    pub fn destroy(&mut self) {
        self.buffer.clear();
        self.pending_binary_packet = None;
        self.pending_binary_values.clear();
    }
}

fn replace_placeholders(value: Value, attachments: &[Value]) -> Value {
    match value {
        Value::Array(values) => Value::Array(
            values
                .into_iter()
                .map(|value| replace_placeholders(value, attachments))
                .collect(),
        ),
        Value::Object(map) => {
            let is_placeholder = map
                .get("_placeholder")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let index = map.get("num").and_then(Value::as_u64).map(|value| value as usize);
            if is_placeholder {
                if let Some(value) = index.and_then(|index| attachments.get(index)).cloned() {
                    return value;
                }
            }
            Value::Object(map)
        }
        other => other,
    }
}

fn binary_attachment_count(packet: &Packet) -> usize {
    packet
        .attachments
        .unwrap_or_else(|| packet.data.as_ref().map(max_placeholder_count).unwrap_or(0))
}

fn max_placeholder_count(value: &Value) -> usize {
    match value {
        Value::Array(values) => values.iter().map(max_placeholder_count).max().unwrap_or(0),
        Value::Object(map) => {
            let nested = map.values().map(max_placeholder_count).max().unwrap_or(0);
            let current = if map
                .get("_placeholder")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                map.get("num")
                    .and_then(Value::as_u64)
                    .map(|value| value as usize + 1)
                    .unwrap_or(0)
            } else {
                0
            };
            nested.max(current)
        }
        _ => 0,
    }
}

impl Default for Decoder {
    fn default() -> Self {
        Self::new()
    }
}
