# Rust Socket.IO Client v3

Rust implementation of Socket.IO client with support for Engine.IO protocol version 3 (EIO=3).

## Features

- ✅ Engine.IO protocol version 3 support
- ✅ WebSocket transport
- ✅ Automatic reconnection
- ✅ Event handling (emit/on)
- ✅ Namespace support
- ✅ Acknowledgment support

## Usage

```rust
use rust_socketio_v3::{connect, ManagerOptions};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create connection
    let manager = connect("http://localhost:3000")?;

    // Get socket for namespace
    let socket = manager.socket("/").await;

    // Subscribe to events
    socket.on("message", |data| {
        println!("Received: {:?}", data);
    });

    // Connect
    socket.connect().await?;

    // Emit event
    socket.emit("chat", vec![json!({"message": "Hello!"})]).await?;

    // Keep connection open
    tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;

    Ok(())
}
```

## Protocol

The library supports Engine.IO version 3 (EIO=3), which ensures compatibility with Socket.IO servers version 2.x.

## License

MIT
