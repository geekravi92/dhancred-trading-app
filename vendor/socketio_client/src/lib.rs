pub mod errors;
pub mod events;
pub mod manager;
pub mod parser;
pub mod socket;
pub mod transport;
pub mod url;

pub use errors::{Result, SocketError};
pub use manager::Manager;
pub use socket::Socket;

/// Protocol version constant - Engine.IO version 3
pub const EIO_VERSION: u8 = 3;

/// Socket.IO protocol version
pub const PROTOCOL_VERSION: u8 = 4;

/// Main entry point - creates a new Socket.IO connection
pub fn connect(uri: &str) -> Result<Manager> {
    Manager::new(uri, None)
}

/// Creates a new Socket.IO connection with options
pub fn connect_with_opts(uri: &str, opts: manager::ManagerOptions) -> Result<Manager> {
    Manager::new(uri, Some(opts))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_eio_version() {
        assert_eq!(EIO_VERSION, 3);
    }
}
