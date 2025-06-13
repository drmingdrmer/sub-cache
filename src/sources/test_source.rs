//! TestSource: a testable source for integration tests of the distributed cache.
//! Provides a real event source with state, sequence, and event emission for EventWatcher tests.

use crate::errors::{ConnectionClosed, SubscribeError};
use crate::event_stream::{Change, Event, EventStream};
use crate::type_config::Source;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};

struct Subscription {
    left: String,
    right: String,
    sender: mpsc::UnboundedSender<Result<Event<Val>, ConnectionClosed>>,
}

impl Subscription {
    fn emit_event(&self, key: Option<&str>, event: Event<Val>) -> Result<(), &'static str> {
        if let Some(key) = key {
            if key < self.left.as_str() || key >= self.right.as_str() {
                return Ok(());
            }
        }
        self.sender.send(Ok(event)).map_err(|_| "send")
    }
}

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

#[derive(Debug)]
struct State {
    data: BTreeMap<String, Val>,
    seq_counter: u64,
    subscriptions: Vec<Subscription>,
}

#[derive(Debug, Clone)]
pub struct TestSource {
    pub state: Arc<Mutex<State>>,
}

impl TestSource {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                data: BTreeMap::new(),
                seq_counter: 0,
                subscriptions: Vec::new(),
            })),
        }
    }

    /// Insert, update, or delete a key-value pair.
    /// If value is Some, insert/update; if None, delete.
    pub async fn set(&self, key: &str, value: Option<&str>) {
        let (prev_value, new_value) = {
            let mut state = self.state.lock().await;

            state.seq_counter += 1;
            let new_seq = state.seq_counter;

            let prev_value = state.data.get(key).cloned();
            let new_value = value.map(|v| Val::new(new_seq, v));

            match &new_value {
                Some(v) => {
                    state.data.insert(key.to_string(), v.clone());
                }
                None => {
                    state.data.remove(key);
                }
            }

            // Emit change event and remove failed subscriptions in-place
            state.subscriptions.retain_mut(|sub| {
                sub.emit_event(
                    Some(key),
                    Event::Change(Change::new(key, prev_value.clone(), new_value.clone())),
                )
                .is_ok()
            });

            (prev_value, new_value)
        };
    }

    async fn send_initial_data(&self) {
        let mut state = self.state.lock().await;

        for (key, value) in state.data.iter() {
            state.subscriptions.retain_mut(|sub| {
                sub.emit_event(
                    Some(key),
                    Event::Initialization(Change::new(key, None, Some(value.clone()))),
                )
                .is_ok()
            });
        }

        state
            .subscriptions
            .retain_mut(|sub| sub.emit_event(None, Event::InitializationComplete).is_ok());
    }
}

#[async_trait::async_trait]
impl Source<Val> for TestSource {
    async fn subscribe(&self, left: &str, right: &str) -> Result<EventStream<Val>, SubscribeError> {
        let (tx, rx) = mpsc::unbounded_channel();

        {
            let mut state = self.state.lock().await;
            state.subscriptions.push(Subscription {
                left: left.to_string(),
                right: right.to_string(),
                sender: tx,
            });
        }

        self.send_initial_data().await;

        let stream = tokio_stream::wrappers::UnboundedReceiverStream::new(rx);
        Ok(Box::pin(stream))
    }
}
