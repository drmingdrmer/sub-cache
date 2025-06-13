// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use futures::Stream;
use futures::TryStreamExt;
use log::debug;
use log::error;
use log::info;
use log::warn;
use tokio::sync::Mutex;
use tokio::sync::oneshot;

use crate::Source;
use crate::TypeConfig;
use crate::cache_data::CacheData;
use crate::errors::ConnectionClosed;
use crate::errors::SubscribeError;
use crate::errors::Unsupported;
use crate::event_stream::Event;
use crate::event_stream::EventStream;

/// Watch cache events and update local copy.
pub(crate) struct EventWatcher<C: TypeConfig> {
    /// The left-closed bound of the key range to watch.
    pub(crate) left: String,

    /// The right-open bound of the key range to watch.
    pub(crate) right: String,

    /// The client to interact with the remote data store.
    pub(crate) source: C::Source,

    /// The shared cache data protected by a mutex.
    pub(crate) data: Arc<Mutex<Result<CacheData<C>, Unsupported>>>,

    /// Contains descriptive information of this watcher.
    pub(crate) name: String,
}

impl<C> EventWatcher<C>
where
    C: TypeConfig,
{
    /// Subscribe to the key-value changes in the interested range and feed them into the local cache.
    ///
    /// This method continuously monitors the remote data store for changes within the specified key range
    /// and updates the local cache accordingly.
    ///
    /// # Parameters
    ///
    /// - `started` - An optional oneshot channel sender that is consumed when the cache initialization
    ///   begins. This signals that it's safe for users to acquire the cache data lock.
    /// - `cancel` - A future that, when completed, will terminate the subscription loop.
    ///
    /// # Error Handling
    ///
    /// - Connection failures are automatically retried with exponential backoff
    /// - On error, the cache is reset and re-fetched to ensure consistency
    /// - The watcher continues running until explicitly canceled
    pub(crate) async fn main(
        mut self,
        mut started: Option<oneshot::Sender<()>>,
        mut cancel: impl Future<Output = ()> + Send + 'static,
    ) {
        // sleep time and reason
        let mut sleep = None::<(Duration, String)>;

        let mut c = std::pin::pin!(cancel);

        loop {
            if let Some((sleep_time, reason)) = sleep.take() {
                info!(
                    "{}: retry establish cache watcher in {:?} because {}",
                    self.name, sleep_time, reason
                );
                tokio::time::sleep(sleep_time).await;
            }

            // 1. Retry until a successful connection is established and cache is initialized.

            let strm = {
                // Hold the lock until the cache is fully initialized.
                let mut cache_data = self.data.lock().await;

                // The data lock is acquired and will be kept until the cache is fully initialized.
                // At this point, we notify the caller that initialization has started by consuming
                // the `started` sender. This signals to the receiving end that it's now safe to
                // acquire the data lock, as we're about to populate the cache with initial data.
                started.take();

                let strm_res = self.retry_new_stream().await;

                // Everytime when establishing a cache, the old data must be cleared and receive a new one.
                *cache_data = Ok(Default::default());

                let mut strm = match strm_res {
                    Ok(strm) => {
                        info!("{}: cache watch stream established", self.name);
                        strm
                    }
                    Err(unsupported) => {
                        let sleep_time = Duration::from_secs(60 * 5);

                        let mes = format!(
                            "{}: watch stream not supported: {}; retry in {:?}",
                            self.name, unsupported, sleep_time
                        );

                        warn!("{}", mes);
                        sleep = Some((sleep_time, mes));

                        // Mark the cache as unavailable
                        *cache_data = Err(unsupported);
                        continue;
                    }
                };

                let init_res = {
                    // Safe unwrap: before entering this method, the cache data is ensured to be Ok.
                    let d = cache_data.as_mut().unwrap();
                    self.initialize_cache(d, &mut strm).await
                };

                match init_res {
                    Ok(_) => {
                        info!("{}: cache initialized successfully", self.name);
                        strm
                    }
                    Err(conn_err) => {
                        error!(
                            "{}: cache initialization failed: {}; retry re-establish cache",
                            self.name, conn_err
                        );
                        continue;
                    }
                }
            };

            // 2. Watch for changes in the stream and apply them to the local cache.

            let res = self.watch_kv_changes(strm, c.as_mut()).await;
            match res {
                Ok(_) => {
                    info!("{} watch loop exited normally(canceled by user)", self.name);
                    return;
                }
                Err(e) => {
                    error!(
                        "{} watcher loop exited with error: {}; reset cache and re-fetch all data to re-establish",
                        self.name, e
                    );
                    // continue
                }
            }
        }
    }

    /// Drain all initialization events from the watch stream and apply them to the cache.
    async fn initialize_cache(
        &self,
        cache_data: &mut CacheData<C>,
        strm: &mut EventStream<C::Value>,
    ) -> Result<(), ConnectionClosed> {
        while let Some(event) = strm.try_next().await? {
            let change = match event {
                Event::Initialization(change) => change,
                Event::InitializationComplete => {
                    info!(
                        "{}: cache is ready, initial_flush finished upto seq={}",
                        self.name, cache_data.last_seq
                    );
                    break;
                }
                Event::Change(ref _change) => {
                    unreachable!(
                        "expected only initialization events, got change event: {:?}",
                        event
                    );
                }
            };

            let (key, before, after) = change.unpack();

            cache_data.apply_update(key, before, after);
        }

        Ok(())
    }

    /// Keep retrying to establish a new watch stream until a successful one is established, or the server
    /// reports that the watch stream is not supported.
    async fn retry_new_stream(&self) -> Result<EventStream<C::Value>, Unsupported> {
        let mut sleep_duration = Duration::from_millis(50);
        let max_sleep = Duration::from_secs(5);

        loop {
            let res = self.source.subscribe(&self.left, &self.right).await;
            let conn_err = match res {
                Ok(stream) => return Ok(stream),
                Err(SubscribeError::Unsupported(u)) => return Err(u),
                Err(SubscribeError::Connection(c)) => c,
            };

            error!(
                "{}: while establish cache, error: {}; retrying in {:?}",
                self.name, conn_err, sleep_duration
            );

            tokio::time::sleep(sleep_duration).await;
            sleep_duration = std::cmp::min(sleep_duration * 3 / 2, max_sleep);
        }
    }

    /// The main loop of the cache engine.
    ///
    /// This function watches for key-value changes in the remote data store and processes them.
    /// Changes are applied to the local in-memory cache atomically.
    ///
    /// # Arguments
    ///
    /// * `strm` - The watch stream from the remote data store
    /// * `cancel` - A future that, when ready, signals this loop to terminate
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if terminated normally (i.e., the `cancel` future is ready), or
    /// `Err(ConnectionClosed)` if the remote connection was closed unexpectedly.
    pub(crate) async fn watch_kv_changes(
        &mut self,
        mut strm: impl Stream<Item = Result<Event<C::Value>, ConnectionClosed>> + Send + Unpin + 'static,
        mut cancel: impl Future<Output = ()> + Send,
    ) -> Result<(), ConnectionClosed> {
        let mut c = std::pin::pin!(cancel);

        loop {
            let event_result = futures::select! {
                _ = c.as_mut().fuse() => {
                    info!("cache loop canceled by user");
                    return Ok(());
                }

                watch_result = strm.try_next().fuse() => {
                    watch_result
                }
            };

            let Some(event) = event_result? else {
                error!("{} watch-stream closed", self.name);
                return Err(ConnectionClosed::new_str("watch-stream closed").context(&self.name));
            };

            let (key, before, after) = match event {
                Event::Change(change) => change.unpack(),
                _ => {
                    unreachable!(
                        "{}: expected only change events, got: {:?}",
                        self.name, event
                    );
                }
            };

            let mut cache_data = self.data.lock().await;

            // Safe unwrap: before entering this method, the cache data is ensured to be Ok.
            let d = cache_data.as_mut().unwrap();

            let new_seq = d.apply_update(key.clone(), before.clone(), after.clone());

            debug!(
                "{}: process update(key: {}, prev: {:?}, current: {:?}), new_seq={:?}",
                self.name, key, before, after, new_seq
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::test_source::{TestSource, Val};
    use futures::StreamExt;
    use std::future::Future;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    // Test TypeConfig
    #[derive(Debug, Default)]
    struct TestConfig;

    impl TypeConfig for TestConfig {
        type Value = Val;
        type Source = TestSource;

        fn value_seq(value: &Self::Value) -> u64 {
            value.seq
        }

        fn spawn<F>(future: F, _name: impl ToString)
        where
            F: Future + Send + 'static,
            F::Output: Send + 'static,
        {
            tokio::spawn(future);
        }
    }

    #[test]
    fn test_event_watcher_construction() {
        let test_source = TestSource::new();
        let watcher = EventWatcher::<TestConfig> {
            left: "start".to_string(),
            right: "end".to_string(),
            source: test_source,
            data: Arc::new(Mutex::new(Ok(CacheData::default()))),
            name: "test-watcher".to_string(),
        };

        assert_eq!(watcher.left, "start");
        assert_eq!(watcher.right, "end");
        assert_eq!(watcher.name, "test-watcher");
    }

    #[test]
    fn test_cache_data_initialization() {
        let mut cache_data: CacheData<TestConfig> = CacheData::default();

        // Test applying updates directly (testing the underlying logic)
        let test_value = Val::new(5, "test");
        cache_data.apply_update("key1".to_string(), None, Some(test_value.clone()));

        assert_eq!(cache_data.last_seq, 5);
        assert_eq!(cache_data.data.get("key1"), Some(&test_value));
    }

    #[tokio::test]
    async fn test_real_event_watcher_integration() {
        let test_source = TestSource::new();

        // Pre-populate some initial data
        test_source
            .insert_for_test("initial_key".to_string(), Val::new(1, "initial_value"))
            .await;

        let watcher = EventWatcher::<TestConfig> {
            left: "".to_string(),
            right: "z".to_string(),
            source: test_source.clone(),
            data: Arc::new(Mutex::new(Ok(CacheData::default()))),
            name: "test-integration".to_string(),
        };

        // Get the event stream
        let mut stream = watcher.source.subscribe("", "z").await.unwrap();

        // Process initialization events
        let mut cache_data = CacheData::<TestConfig>::default();
        watcher
            .initialize_cache(&mut cache_data, &mut stream)
            .await
            .unwrap();

        // Verify initial data was loaded
        assert_eq!(
            cache_data.data.get("initial_key").unwrap(),
            &Val::new(1, "initial_value")
        );
        assert_eq!(cache_data.last_seq, 1);

        // Test updates through the test source
        test_source.set("key1", Some("value1")).await;
        test_source.set("key2", Some("value2")).await;
        test_source.set("key1", Some("updated_value1")).await;
        test_source.set("key2", None).await;

        // Process the change events
        for _ in 0..4 {
            // 4 change events
            if let Some(Ok(Event::Change(change))) = stream.next().await {
                let (key, before, after) = change.unpack();
                cache_data.apply_update(key, before, after);
            }
        }

        // Verify final state
        assert_eq!(
            cache_data.data.get("key1").unwrap(),
            &Val::new(3, "updated_value1")
        );
        assert!(!cache_data.data.contains_key("key2")); // deleted
        assert_eq!(
            cache_data.data.get("initial_key").unwrap(),
            &Val::new(1, "initial_value")
        ); // unchanged
        assert_eq!(cache_data.last_seq, 3); // highest sequence number from key1 update
    }

    #[tokio::test]
    async fn test_cache_consistency_scenarios() {
        let _test_source = TestSource::new();
        let mut cache_data = CacheData::<TestConfig>::default();

        // Test various update scenarios

        // 1. Insert new key
        cache_data.apply_update("new_key".to_string(), None, Some(Val::new(1, "new_value")));
        assert_eq!(
            cache_data.data.get("new_key"),
            Some(&Val::new(1, "new_value"))
        );
        assert_eq!(cache_data.last_seq, 1);

        // 2. Update existing key
        cache_data.apply_update(
            "new_key".to_string(),
            Some(Val::new(1, "new_value")),
            Some(Val::new(2, "updated_value")),
        );
        assert_eq!(
            cache_data.data.get("new_key"),
            Some(&Val::new(2, "updated_value"))
        );
        assert_eq!(cache_data.last_seq, 2);

        // 3. Delete key
        cache_data.apply_update(
            "new_key".to_string(),
            Some(Val::new(2, "updated_value")),
            None,
        );
        assert!(!cache_data.data.contains_key("new_key"));
        assert_eq!(cache_data.last_seq, 2);

        // 4. Test sequence ordering
        cache_data.apply_update("seq_test1".to_string(), None, Some(Val::new(5, "value1")));
        cache_data.apply_update("seq_test2".to_string(), None, Some(Val::new(3, "value2"))); // Lower seq
        cache_data.apply_update("seq_test3".to_string(), None, Some(Val::new(7, "value3")));

        assert_eq!(cache_data.last_seq, 7); // Should be highest
        assert_eq!(cache_data.data.len(), 3);
    }

    #[tokio::test]
    async fn test_test_source_operations() {
        let test_source = TestSource::new();

        // Test direct operations on the test source
        test_source.set("key1", Some("value1")).await;
        test_source.set("key2", Some("value2")).await;

        // Verify the source's internal state
        let source_data = test_source.get_data_snapshot().await;
        assert_eq!(source_data.get("key1").unwrap(), &Val::new(1, "value1"));
        assert_eq!(source_data.get("key2").unwrap(), &Val::new(2, "value2"));
        drop(source_data);

        // Test update operation
        test_source.set("key1", Some("updated_value1")).await;
        let source_data = test_source.get_data_snapshot().await;
        assert_eq!(
            source_data.get("key1").unwrap(),
            &Val::new(3, "updated_value1")
        );
        assert_eq!(source_data.len(), 2);
        drop(source_data);

        // Test delete operation
        test_source.set("key2", None).await;
        let source_data = test_source.get_data_snapshot().await;
        assert!(!source_data.contains_key("key2"));
        assert_eq!(source_data.len(), 1);
        assert_eq!(
            source_data.get("key1").unwrap(),
            &Val::new(3, "updated_value1")
        );
    }
}
