use socketio_client::{connect_with_opts, manager::ManagerOptions};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

#[tokio::test]
async fn test_server_connection() {
    // Initialize logger for debugging
    let _ = env_logger::try_init();

    // Flag to track response reception
    let received_response: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));
    let received_data: Arc<Mutex<Option<serde_json::Value>>> = Arc::new(Mutex::new(None));

    // Create connection options
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

    // Connect to test server
    let manager = connect_with_opts("http://localhost:3000", opts)
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

    // Handler for 'test-client' event
    let received_response_clone = received_response.clone();
    let received_data_clone = received_data.clone();
    socket.on("test-client", move |data: Vec<serde_json::Value>| {
        eprintln!("Received 'test-client' event with data: {:?}", data);
        let received_response = received_response_clone.clone();
        let received_data = received_data_clone.clone();

        if !data.is_empty() {
            let mut flag = received_response.blocking_lock();
            *flag = true;

            let mut data_mutex = received_data.blocking_lock();
            *data_mutex = Some(data[0].clone());
        }
    });

    // Handler for 'connect' event
    socket.on("connect", |_| {
        eprintln!("Connected to test server");
    });

    // Handler for 'connect_error' event
    socket.on("connect_error", |data| {
        if let Some(err) = data.first() {
            eprintln!("Connection Error! {}", err);
        }
    });

    // Handler for 'disconnect' event
    socket.on("disconnect", |data| {
        if let Some(reason) = data.first().and_then(|v| v.as_str()) {
            eprintln!("Disconnected! Reason: {}", reason);
        } else {
            eprintln!("Disconnected!");
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

    // Connect
    socket.connect().await.expect("Failed to connect");

    // Wait a bit for connection establishment
    sleep(Duration::from_secs(1000)).await;

    // Send 'test-server' event
    socket
        .emit("test-server", vec![])
        .await
        .expect("Failed to emit test-server");

    eprintln!("Sent 'test-server' event, waiting for response...");

    // Wait for response (maximum 5 seconds)
    for _ in 0..50 {
        sleep(Duration::from_millis(100)).await;
        let flag = received_response.lock().await;
        if *flag {
            break;
        }
    }

    // Check that we received a response
    let flag = received_response.lock().await;
    assert!(*flag, "Expected to receive 'test-client' event");

    // Check data
    let data = received_data.lock().await;
    if let Some(ref value) = *data {
        eprintln!("Received data: {}", value);
        assert_eq!(
            value.get("foo").and_then(|v| v.as_str()),
            Some("bar"),
            "Expected foo='bar' in response"
        );
    } else {
        panic!("Expected to receive data in 'test-client' event");
    }
}

