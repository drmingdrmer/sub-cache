//! TestSource: a testable source for integration tests of the distributed cache.
//! Provides a real event source with state, sequence, and event emission for EventWatcher tests.

use crate::errors::{ConnectionClosed, SubscribeError};
use crate::event_stream::{Change, Event, EventStream};
use crate::type_config::Source;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};

#[derive(Debug, Clone, PartialEq)]
pub struct Val {
    pub seq: u64,
    pub data: String,
}

impl Val {
    pub fn new(seq: u64, data: &str) -> Self {
        Self {
            seq,
            data: data.to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TestSource {
    pub data: Arc<Mutex<BTreeMap<String, Val>>>,
    pub seq_counter: Arc<Mutex<u64>>,
    pub event_sender:
        Arc<Mutex<Option<mpsc::UnboundedSender<Result<Event<Val>, ConnectionClosed>>>>>,
}

impl TestSource {
    pub fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(BTreeMap::new())),
            seq_counter: Arc::new(Mutex::new(0)),
            event_sender: Arc::new(Mutex::new(None)),
        }
    }

    /// Insert, update, or delete a key-value pair.
    /// If value is Some, insert/update; if None, delete.
    pub async fn set(&self, key: &str, value: Option<&str>) {
        let mut seq_counter = self.seq_counter.lock().await;
        *seq_counter += 1;
        let new_seq = *seq_counter;
        drop(seq_counter);

        let mut data = self.data.lock().await;
        let prev_value = data.get(key).cloned();
        let new_value = value.map(|v| Val::new(new_seq, v));
        match &new_value {
            Some(v) => {
                data.insert(key.to_string(), v.clone());
            }
            None => {
                data.remove(key);
            }
        }
        drop(data);

        // Emit change event
        self.emit_event(Event::Change(Change::new(key, prev_value, new_value)))
            .await;
    }

    async fn emit_event(&self, event: Event<Val>) {
        let sender_guard = self.event_sender.lock().await;
        if let Some(sender) = sender_guard.as_ref() {
            let _ = sender.send(Ok(event));
        }
    }

    async fn complete_initialization(&self) {
        self.emit_event(Event::InitializationComplete).await;
    }

    async fn send_initial_data(&self) {
        let data = self.data.lock().await;
        for (key, value) in data.iter() {
            self.emit_event(Event::Initialization(Change::new(
                key,
                None,
                Some(value.clone()),
            )))
            .await;
        }
        drop(data);
        self.complete_initialization().await;
    }
}

#[async_trait::async_trait]
impl Source<Val> for TestSource {
    async fn subscribe(
        &self,
        _left: &str,
        _right: &str,
    ) -> Result<EventStream<Val>, SubscribeError> {
        let (tx, rx) = mpsc::unbounded_channel();
        {
            let mut sender_guard = self.event_sender.lock().await;
            *sender_guard = Some(tx);
        }
        self.send_initial_data().await;
        let stream = tokio_stream::wrappers::UnboundedReceiverStream::new(rx);
        Ok(Box::pin(stream))
    }
}
