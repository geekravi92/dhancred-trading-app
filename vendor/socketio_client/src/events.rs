use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub type EventCallback = Box<dyn Fn(Vec<serde_json::Value>) + Send + Sync>;

/// Event emitter implementation
#[derive(Clone)]
pub struct EventEmitter {
    listeners: Arc<Mutex<HashMap<String, Vec<EventCallback>>>>,
}

impl std::fmt::Debug for EventEmitter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventEmitter")
            .field("listeners_count", &self.listeners.lock().unwrap().len())
            .finish()
    }
}

impl EventEmitter {
    pub fn new() -> Self {
        Self {
            listeners: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Register an event listener
    pub fn on<F>(&self, event: &str, callback: F)
    where
        F: Fn(Vec<serde_json::Value>) + Send + Sync + 'static,
    {
        let mut listeners = self.listeners.lock().unwrap();
        listeners
            .entry(event.to_string())
            .or_insert_with(Vec::new)
            .push(Box::new(callback));
    }

    /// Emit an event
    pub fn emit(&self, event: &str, data: Vec<serde_json::Value>) {
        let listeners = self.listeners.lock().unwrap();

        // Call handlers for the specific event
        if let Some(callbacks) = listeners.get(event) {
            for callback in callbacks {
                callback(data.clone());
            }
        }

        // Call handlers for all events ("*")
        if event != "*" {
            if let Some(callbacks) = listeners.get("*") {
                // Pass event name as the first argument
                let mut all_event_data = vec![serde_json::Value::String(event.to_string())];
                all_event_data.extend(data.clone());
                for callback in callbacks {
                    callback(all_event_data.clone());
                }
            }
        }
    }

    /// Remove all listeners for an event
    pub fn remove_all_listeners(&self, event: &str) {
        let mut listeners = self.listeners.lock().unwrap();
        listeners.remove(event);
    }

    /// Remove all listeners
    pub fn remove_all(&self) {
        let mut listeners = self.listeners.lock().unwrap();
        listeners.clear();
    }
}

impl Default for EventEmitter {
    fn default() -> Self {
        Self::new()
    }
}

// Remove Clone derive since we're using Arc internally
