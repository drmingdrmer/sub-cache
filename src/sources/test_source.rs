//! TestSource: a testable source for integration tests of the distributed cache.
//! Provides a real event source with state, sequence, and event emission for EventWatcher tests.

use crate::errors::{ConnectionClosed, SubscribeError};
use crate::event_stream::{Change, Event, EventStream};
use crate::type_config::Source;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};

#[derive(Debug)]
struct Subscription {
    left: String,
    right: String,
    sender: mpsc::UnboundedSender<Result<Event<Val>, ConnectionClosed>>,
}

impl Subscription {
    fn emit_event(&self, key: Option<&str>, event: Event<Val>) -> Result<(), &'static str> {
        if let Some(key) = key.clone() {
            if key < self.left.as_str() || key >= self.right.as_str() {
                return Ok(());
            }
        }

        let res = self.sender.send(Ok(event.clone())).map_err(|_| "send");
        // println!("Emitting event: {:?} for key: {:?}", event, key);

        res
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
pub struct State {
    pub data: BTreeMap<String, Val>,
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
        let (_prev_value, _new_value) = {
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
        // 先收集 data 快照，避免借用冲突
        let (data_snapshot, mut subscriptions) = {
            let mut state = self.state.lock().await;
            (state.data.clone(), std::mem::take(&mut state.subscriptions))
        };

        for (key, value) in data_snapshot.iter() {
            subscriptions.retain_mut(|sub| {
                sub.emit_event(
                    Some(key),
                    Event::Initialization(Change::new(key, None, Some(value.clone()))),
                )
                .is_ok()
            });
        }

        subscriptions.retain_mut(|sub| sub.emit_event(None, Event::InitializationComplete).is_ok());

        // 重新放回 subscriptions
        let mut state = self.state.lock().await;
        state.subscriptions = subscriptions;
    }

    pub async fn get_data_snapshot(&self) -> BTreeMap<String, Val> {
        let state = self.state.lock().await;
        state.data.clone()
    }

    #[cfg(test)]
    pub async fn insert_for_test(&self, key: impl ToString, val: Val) {
        let mut state = self.state.lock().await;
        state.data.insert(key.to_string(), val);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_stream::Event;
    use futures::StreamExt;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_set_and_get() {
        let src = TestSource::new();
        src.set("a", Some("v1")).await;
        src.set("b", Some("v2")).await;
        src.set("a", Some("v3")).await;
        src.set("b", None).await;

        let state = src.state.lock().await;
        assert_eq!(state.data.get("a"), Some(&Val::new(3, "v3")));
        assert!(!state.data.contains_key("b"));
        assert_eq!(state.seq_counter, 4);
    }

    #[tokio::test]
    async fn test_subscribe_and_event_delivery() {
        let src = TestSource::new();

        // Subscribe to all keys
        let mut stream1 = src.subscribe("a", "z").await.unwrap();
        // Subscribe to only keys >= "m"
        let mut stream2 = src.subscribe("m", "z").await.unwrap();

        src.set("a", Some("v1")).await;
        src.set("m", Some("v2")).await;
        src.set("z", Some("v3")).await;
        src.set("m", None).await;

        // stream1 should see all changes except "z"
        let mut seen1 = vec![];
        for _ in 0..5 {
            if let Some(Ok(Event::Change(change))) = stream1.next().await {
                let (k, _, v) = change.unpack();
                seen1.push((k, v.map(|v| v.data)));
            }
        }
        assert_eq!(
            seen1,
            vec![
                ("a".to_string(), Some("v1".to_string())),
                ("m".to_string(), Some("v2".to_string())),
                ("m".to_string(), None),
            ]
        );

        // stream2 should only see changes for "m"
        let mut seen2 = vec![];
        for _ in 0..3 {
            if let Some(Ok(Event::Change(change))) = stream2.next().await {
                let (k, _, v) = change.unpack();
                seen2.push((k, v.map(|v| v.data)));
            }
        }
        assert_eq!(
            seen2,
            vec![
                ("m".to_string(), Some("v2".to_string())),
                ("m".to_string(), None),
            ]
        );
    }

    #[tokio::test]
    async fn test_subscription_removal_on_send_fail() {
        let src = TestSource::new();
        // Create a subscription and drop the receiver immediately
        let (tx, rx) = mpsc::unbounded_channel();
        {
            let mut state = src.state.lock().await;
            state.subscriptions.push(Subscription {
                left: "a".to_string(),
                right: "z".to_string(),
                sender: tx,
            });
        }
        drop(rx); // Simulate receiver dropped
        src.set("a", Some("v1")).await;
        let state = src.state.lock().await;
        assert!(state.subscriptions.is_empty());
    }

    #[tokio::test]
    async fn test_initialization_and_change_events() {
        let src = TestSource::new();

        // Pre-populate some data
        src.set("a", Some("v1")).await;
        src.set("b", Some("v2")).await;
        src.set("c", Some("v3")).await;

        // Subscribe to all keys
        let mut stream = src.subscribe("a", "z").await.unwrap();

        // Should receive initialization for all keys in order, then InitializationComplete
        let mut events = vec![];
        for _ in 0..3 {
            if let Some(Ok(Event::Initialization(change))) = stream.next().await {
                let (k, _, v) = change.unpack();
                events.push(("init", k, v.map(|v| v.data)));
            }
        }
        if let Some(Ok(Event::InitializationComplete)) = stream.next().await {
            events.push(("complete", String::new(), None));
        }

        assert_eq!(events[0].0, "init");
        assert_eq!(events[1].0, "init");
        assert_eq!(events[2].0, "init");
        assert_eq!(events[3].0, "complete");
        let keys: Vec<_> = events
            .iter()
            .filter(|e| e.0 == "init")
            .map(|e| &e.1)
            .collect();
        assert!(keys.contains(&&"a".to_string()));
        assert!(keys.contains(&&"b".to_string()));
        assert!(keys.contains(&&"c".to_string()));

        // After initialization, should receive change events
        src.set("b", Some("v2x")).await;
        src.set("d", Some("v4")).await;
        src.set("a", None).await;

        let mut changes = vec![];
        for _ in 0..3 {
            if let Some(Ok(Event::Change(change))) = stream.next().await {
                let (k, _, v) = change.unpack();
                changes.push((k, v.map(|v| v.data)));
            }
        }
        assert_eq!(
            changes,
            vec![
                ("b".to_string(), Some("v2x".to_string())),
                ("d".to_string(), Some("v4".to_string())),
                ("a".to_string(), None),
            ]
        );
    }
}
