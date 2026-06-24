use socketio_client::{connect_with_opts, manager::ManagerOptions};
use serde_json::json;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

#[tokio::test]
#[ignore] // Ignored by default, as it requires a real token
          // To view output use: cargo test --test donation_alerts_test -- --ignored --nocapture
async fn test_donation_alerts_connection() {
    // Initialize logger for debugging
    let _ = env_logger::try_init();

    // Load variables from .env file (if it exists)
    // If file is not found, it's not an error - can use system environment variables
    let _ = dotenvy::dotenv();

    // Token for connection (from .env file or environment variable)
    let token = std::env::var("DONATION_ALERTS_TOKEN")
        .expect("DONATION_ALERTS_TOKEN must be set in .env file or environment variable");

    // Check that socket is not yet created (analog of socket check in TypeScript)
    let processed_ids: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));

    // Create connection options similar to TypeScript version
    // Use empty string to get path /socket.io/ (standard for Socket.IO)
    // Try to disable auto_connect and connect manually for better debugging
    let opts = ManagerOptions {
        path: "".to_string(), // Empty string will give path /socket.io/
        reconnection: true,
        reconnection_attempts: None, // Infinity
        reconnection_delay: 1000,
        reconnection_delay_max: 5000,
        randomization_factor: 0.5,
        timeout: Some(20000),
        auto_connect: true,
        query: None,
    };

    // Connect to DonationAlerts WebSocket server
    let manager = connect_with_opts("wss://socket.donationalerts.ru:443", opts)
        .expect("Failed to create manager");

    // Event handlers at manager level
    manager.on("open", |_| {
        eprintln!("Manager: Transport opened");
    });

    manager.on("close", |_| {
        eprintln!("Manager: Transport closed");
    });

    manager.on("error", |data| {
        if let Some(err) = data.first() {
            eprintln!("Manager error: {}", err);
        } else {
            eprintln!("Manager error occurred");
        }
    });

    // Get socket for root namespace
    let socket = manager.socket("/").await;

    // Handler for 'donation' event
    let processed_ids_clone = processed_ids.clone();
    socket.on("donation", move |data: Vec<serde_json::Value>| {
        eprintln!("Received data: {:?}", data);
        let processed_ids = processed_ids_clone.clone();
        if data.is_empty() {
            return;
        }

        // Parse message (in TypeScript this is JSON.parse(message))
        let message = match data[0].as_str() {
            Some(msg) => msg,
            None => return,
        };

        let donation: serde_json::Value = match serde_json::from_str(message) {
            Ok(d) => d,
            Err(_) => {
                eprintln!("Ignored DonationAlerts message: {}", message);
                return;
            }
        };

        // Check conditions, as in TypeScript version
        // alert_type can be string or number
        let alert_type = donation
            .get("alert_type")
            .map(|v| {
                if let Some(s) = v.as_str() {
                    s.to_string()
                } else if let Some(n) = v.as_u64() {
                    n.to_string()
                } else if let Some(n) = v.as_i64() {
                    n.to_string()
                } else {
                    String::new()
                }
            })
            .unwrap_or_default();

        if alert_type != "1"
            || donation.get("currency").is_none()
            || donation.get("billing_system").is_none()
            || donation.get("amount").is_none()
            || donation.get("amount_main").is_none()
            || donation.get("id").is_none()
        {
            return;
        }

        // id can be number or string
        let id = donation
            .get("id")
            .map(|v| {
                if let Some(s) = v.as_str() {
                    s.to_string()
                } else if let Some(n) = v.as_u64() {
                    n.to_string()
                } else if let Some(n) = v.as_i64() {
                    n.to_string()
                } else {
                    String::new()
                }
            })
            .unwrap_or_default();

        // Check if this ID has already been processed
        // Note: Callback is sync, but we need async lock, so spawn a task
        let processed_ids_clone2 = processed_ids.clone();
        let id_clone = id.clone();
        let donation_clone = donation.clone();
        tokio::spawn(async move {
            let mut ids = processed_ids_clone2.lock().await;
            if ids.contains(&id_clone) {
                return;
            }

            // Emulate CustomEvent (in Rust we just log or can use channels)
            // Format amount_main - can be number or string
            let amount_main_str = donation_clone
                .get("amount_main")
                .map(|v| {
                    if let Some(s) = v.as_str() {
                        s.to_string()
                    } else if let Some(n) = v.as_f64() {
                        n.to_string()
                    } else if let Some(n) = v.as_u64() {
                        n.to_string()
                    } else if let Some(n) = v.as_i64() {
                        n.to_string()
                    } else {
                        String::new()
                    }
                })
                .unwrap_or_default();

            eprintln!(
                "Donation received: id={}, amount={}, currency={}, amount_main={}",
                id_clone,
                donation_clone
                    .get("amount")
                    .and_then(|v| v.as_str())
                    .unwrap_or(""),
                donation_clone
                    .get("currency")
                    .and_then(|v| v.as_str())
                    .unwrap_or(""),
                amount_main_str
            );

            ids.insert(id_clone);
        });
    });

    // Handler for 'connect' event
    socket.on("connect", |_| {
        eprintln!("Connected to Donation Alerts");
    });

    // Handler for 'connect_error' event
    socket.on("connect_error", |data| {
        if let Some(err) = data.first() {
            eprintln!("Donation Alerts Connection Error! {}", err);
        }
    });

    // Handler for 'disconnect' event
    socket.on("disconnect", |data| {
        if let Some(reason) = data.first().and_then(|v| v.as_str()) {
            eprintln!("Donation Alerts Disconnected! Reason: {}", reason);
        } else {
            eprintln!("Donation Alerts Disconnected!");
        }
    });

    // Handler for 'error' event
    socket.on("error", |data| {
        if let Some(err) = data.first() {
            eprintln!("Socket error: {}", err);
        } else {
            eprintln!("Socket error occurred");
        }
    });

    // Handler for 'reconnect' event
    socket.on("reconnect", |data| {
        if let Some(attempt) = data.first() {
            eprintln!("Reconnected! Attempt: {}", attempt);
        } else {
            eprintln!("Reconnected!");
        }
    });

    // Handler for 'reconnecting' event
    socket.on("reconnecting", |data| {
        if let Some(attempt) = data.first() {
            eprintln!("Reconnecting... Attempt: {}", attempt);
        } else {
            eprintln!("Reconnecting...");
        }
    });

    // Handler for 'reconnect_error' event
    socket.on("reconnect_error", |data| {
        if let Some(err) = data.first() {
            eprintln!("Reconnect error: {}", err);
        } else {
            eprintln!("Reconnect error occurred");
        }
    });

    // Handler for 'reconnect_failed' event
    socket.on("reconnect_failed", |_| {
        eprintln!("Reconnect failed!");
    });

    // Handler for any message (all events)
    socket.on("*", |data| {
        eprintln!("Received event with data: {:?}", &data[1..]);
    });

    // Connect
    socket.connect().await.expect("Failed to connect");

    // Wait a bit for connection establishment
    sleep(Duration::from_millis(1000)).await;

    // Send 'add-user' event, as in TypeScript version
    socket
        .emit(
            "add-user",
            vec![json!({
                "token": token,
                "type": "minor"
            })],
        )
        .await
        .expect("Failed to emit add-user");

    eprintln!("Sent 'add-user' event, waiting for donations...");

    // Wait some time for receiving donations
    // In a real application this would be an infinite loop
    sleep(Duration::from_mins(30)).await;

    // Check that we processed at least some IDs
    let ids = processed_ids.lock().await;
    eprintln!("Processed {} unique donation IDs", ids.len());
}
