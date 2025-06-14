//! Concurrent same-range watchers test
//!
//! This test validates that multiple cache instances (watchers) can subscribe to
//! the same key range and maintain consistency when data is updated concurrently.
//! The key aspects being tested are:
//!
//! 1. **Same Range Subscriptions**: Multiple caches subscribe to identical key ranges
//! 2. **Concurrent Data Updates**: Data is written concurrently to a shared data source
//! 3. **Synchronization**: All caches stay synchronized with the data source
//! 4. **Consistency**: All caches see the same final state
//! 5. **Real-time Updates**: Changes propagate to all caches simultaneously
//! 6. **Load Distribution**: Multiple watchers don't interfere with each other
//!
//! The test simulates a scenario where multiple cache instances monitor
//! the same data range and must all stay up-to-date with concurrent changes.

use std::time::Duration;

use sub_cache::testing::source::TestSource;
use sub_cache::testing::source::Val;
use sub_cache::testing::types::TestConfig;
use sub_cache::testing::util::wait_for_cache_state;
use sub_cache::Cache;
use tokio::time::sleep;

#[tokio::test]
async fn test_concurrent_same_range_watchers() {
    // 1. Create a single shared test source
    let source = TestSource::new();

    // 2. Pre-populate some initial data
    source.set("data/item1", Some("initial_value_1")).await;
    source.set("data/item2", Some("initial_value_2")).await;
    source.set("data/item3", Some("initial_value_3")).await;

    // 3. Create multiple cache instances with the SAME key range
    let mut cache_a = Cache::<TestConfig>::new(source.clone(), "data", "watcher-a").await;
    let mut cache_b = Cache::<TestConfig>::new(source.clone(), "data", "watcher-b").await;
    let mut cache_c = Cache::<TestConfig>::new(source.clone(), "data", "watcher-c").await;
    let mut cache_d = Cache::<TestConfig>::new(source.clone(), "data", "watcher-d").await;

    // 4. Wait for all caches to initialize with the same initial data
    let expected_initial = &[
        ("data/item1", Some(Val::new(1, "initial_value_1"))),
        ("data/item2", Some(Val::new(2, "initial_value_2"))),
        ("data/item3", Some(Val::new(3, "initial_value_3"))),
    ];

    wait_for_cache_state(&mut cache_a, 3, expected_initial)
        .await
        .unwrap()
        .unwrap();
    wait_for_cache_state(&mut cache_b, 3, expected_initial)
        .await
        .unwrap()
        .unwrap();
    wait_for_cache_state(&mut cache_c, 3, expected_initial)
        .await
        .unwrap()
        .unwrap();
    wait_for_cache_state(&mut cache_d, 3, expected_initial)
        .await
        .unwrap()
        .unwrap();

    println!("All 4 caches initialized with same data");

    // 5. Start concurrent data updates from multiple threads
    let update_task_1 = tokio::spawn({
        let source = source.clone();
        async move {
            for i in 10..15 {
                let key = format!("data/batch1_item{}", i);
                let value = format!("batch1_value_{}", i);
                source.set(&key, Some(&value)).await;
                sleep(Duration::from_millis(10)).await;
            }
        }
    });

    let update_task_2 = tokio::spawn({
        let source = source.clone();
        async move {
            for i in 20..25 {
                let key = format!("data/batch2_item{}", i);
                let value = format!("batch2_value_{}", i);
                source.set(&key, Some(&value)).await;
                sleep(Duration::from_millis(15)).await;
            }
        }
    });

    let update_task_3 = tokio::spawn({
        let source = source.clone();
        async move {
            for i in 30..35 {
                let key = format!("data/batch3_item{}", i);
                let value = format!("batch3_value_{}", i);
                source.set(&key, Some(&value)).await;
                sleep(Duration::from_millis(12)).await;
            }
        }
    });

    let update_task_4 = tokio::spawn({
        let source = source.clone();
        async move {
            // Update existing items
            source.set("data/item1", Some("updated_value_1")).await;
            sleep(Duration::from_millis(20)).await;
            source.set("data/item2", Some("updated_value_2")).await;
            sleep(Duration::from_millis(20)).await;
            source.set("data/item3", Some("updated_value_3")).await;
        }
    });

    // 6. Wait for all concurrent updates to complete
    let _ = tokio::join!(update_task_1, update_task_2, update_task_3, update_task_4);

    // 7. Wait for synchronization across all caches
    sleep(Duration::from_millis(500)).await;

    // 8. Verify all caches have the same final state
    let entries_a = cache_a.try_list_dir("data").await.unwrap();
    let entries_b = cache_b.try_list_dir("data").await.unwrap();
    let entries_c = cache_c.try_list_dir("data").await.unwrap();
    let entries_d = cache_d.try_list_dir("data").await.unwrap();

    // All caches should have the same number of entries
    let expected_count = 18; // 3 initial + 15 new items
    assert_eq!(
        entries_a.len(),
        expected_count,
        "Cache A should have {} entries",
        expected_count
    );
    assert_eq!(
        entries_b.len(),
        expected_count,
        "Cache B should have {} entries",
        expected_count
    );
    assert_eq!(
        entries_c.len(),
        expected_count,
        "Cache C should have {} entries",
        expected_count
    );
    assert_eq!(
        entries_d.len(),
        expected_count,
        "Cache D should have {} entries",
        expected_count
    );

    println!("All caches have {} entries", expected_count);

    // 9. Verify all caches have identical data for each key
    let mut all_keys = std::collections::HashSet::new();
    for entry in &entries_a {
        all_keys.insert(entry.0.clone());
    }

    for key in &all_keys {
        let val_a = cache_a.try_get(key).await.unwrap().unwrap();
        let val_b = cache_b.try_get(key).await.unwrap().unwrap();
        let val_c = cache_c.try_get(key).await.unwrap().unwrap();
        let val_d = cache_d.try_get(key).await.unwrap().unwrap();

        // All caches should have identical data and sequence for the same key
        assert_eq!(val_a.data, val_b.data, "Data mismatch for key {}", key);
        assert_eq!(val_a.data, val_c.data, "Data mismatch for key {}", key);
        assert_eq!(val_a.data, val_d.data, "Data mismatch for key {}", key);

        assert_eq!(val_a.seq, val_b.seq, "Sequence mismatch for key {}", key);
        assert_eq!(val_a.seq, val_c.seq, "Sequence mismatch for key {}", key);
        assert_eq!(val_a.seq, val_d.seq, "Sequence mismatch for key {}", key);
    }

    println!("All caches have identical data for all keys");

    // 10. Verify updated values are correct
    let item1_a = cache_a.try_get("data/item1").await.unwrap().unwrap();
    assert_eq!(item1_a.data, "updated_value_1");

    let item2_b = cache_b.try_get("data/item2").await.unwrap().unwrap();
    assert_eq!(item2_b.data, "updated_value_2");

    let item3_c = cache_c.try_get("data/item3").await.unwrap().unwrap();
    assert_eq!(item3_c.data, "updated_value_3");

    // 11. Verify batch data is present in all caches
    for i in 10..15 {
        let key = format!("data/batch1_item{}", i);
        let expected_value = format!("batch1_value_{}", i);

        let val_a = cache_a.try_get(&key).await.unwrap().unwrap();
        let val_b = cache_b.try_get(&key).await.unwrap().unwrap();
        let val_c = cache_c.try_get(&key).await.unwrap().unwrap();
        let val_d = cache_d.try_get(&key).await.unwrap().unwrap();

        assert_eq!(val_a.data, expected_value);
        assert_eq!(val_b.data, expected_value);
        assert_eq!(val_c.data, expected_value);
        assert_eq!(val_d.data, expected_value);
    }

    // 12. Check sequence consistency across all caches
    let seq_a = cache_a.try_last_seq().await.unwrap();
    let seq_b = cache_b.try_last_seq().await.unwrap();
    let seq_c = cache_c.try_last_seq().await.unwrap();
    let seq_d = cache_d.try_last_seq().await.unwrap();

    assert_eq!(seq_a, seq_b, "Cache A and B should have same last sequence");
    assert_eq!(seq_a, seq_c, "Cache A and C should have same last sequence");
    assert_eq!(seq_a, seq_d, "Cache A and D should have same last sequence");

    println!("All caches have same last sequence: {}", seq_a);

    // 13. Test rapid concurrent operations with all caches active
    let rapid_updates = tokio::spawn({
        let source = source.clone();
        async move {
            for i in 100..110 {
                let key = format!("data/rapid{}", i);
                let value = format!("rapid_value_{}", i);
                source.set(&key, Some(&value)).await;
                sleep(Duration::from_millis(5)).await;
            }
        }
    });

    // Concurrent reads from all caches while updates are happening
    let read_task_a = tokio::spawn({
        let mut cache_a = cache_a;
        async move {
            for _ in 0..20 {
                let _ = cache_a.try_list_dir("data").await;
                let _ = cache_a.try_get("data/item1").await;
                sleep(Duration::from_millis(8)).await;
            }
            cache_a
        }
    });

    let read_task_b = tokio::spawn({
        let mut cache_b = cache_b;
        async move {
            for _ in 0..20 {
                let _ = cache_b.try_list_dir("data").await;
                let _ = cache_b.try_get("data/item2").await;
                sleep(Duration::from_millis(7)).await;
            }
            cache_b
        }
    });

    let read_task_c = tokio::spawn({
        let mut cache_c = cache_c;
        async move {
            for _ in 0..20 {
                let _ = cache_c.try_list_dir("data").await;
                let _ = cache_c.try_get("data/item3").await;
                sleep(Duration::from_millis(9)).await;
            }
            cache_c
        }
    });

    let read_task_d = tokio::spawn({
        let mut cache_d = cache_d;
        async move {
            for _ in 0..20 {
                let _ = cache_d.try_list_dir("data").await;
                sleep(Duration::from_millis(6)).await;
            }
            cache_d
        }
    });

    // Wait for all tasks to complete
    let (_, cache_a, cache_b, cache_c, cache_d) = tokio::join!(
        rapid_updates,
        read_task_a,
        read_task_b,
        read_task_c,
        read_task_d
    );

    let mut cache_a = cache_a.unwrap();
    let mut cache_b = cache_b.unwrap();
    let mut cache_c = cache_c.unwrap();
    let mut cache_d = cache_d.unwrap();

    // Final synchronization wait
    sleep(Duration::from_millis(300)).await;

    // 14. Final verification - all caches should have the rapid updates
    let final_entries_a = cache_a.try_list_dir("data").await.unwrap();
    let final_entries_b = cache_b.try_list_dir("data").await.unwrap();
    let final_entries_c = cache_c.try_list_dir("data").await.unwrap();
    let final_entries_d = cache_d.try_list_dir("data").await.unwrap();

    let final_expected_count = 28; // 18 previous + 10 rapid
    assert_eq!(final_entries_a.len(), final_expected_count);
    assert_eq!(final_entries_b.len(), final_expected_count);
    assert_eq!(final_entries_c.len(), final_expected_count);
    assert_eq!(final_entries_d.len(), final_expected_count);

    // Verify rapid updates are present in all caches
    for i in 100..110 {
        let key = format!("data/rapid{}", i);
        let expected_value = format!("rapid_value_{}", i);

        let val_a = cache_a.try_get(&key).await.unwrap().unwrap();
        let val_b = cache_b.try_get(&key).await.unwrap().unwrap();
        let val_c = cache_c.try_get(&key).await.unwrap().unwrap();
        let val_d = cache_d.try_get(&key).await.unwrap().unwrap();

        assert_eq!(val_a.data, expected_value);
        assert_eq!(val_b.data, expected_value);
        assert_eq!(val_c.data, expected_value);
        assert_eq!(val_d.data, expected_value);

        // All should have same sequence for the same key
        assert_eq!(val_a.seq, val_b.seq);
        assert_eq!(val_a.seq, val_c.seq);
        assert_eq!(val_a.seq, val_d.seq);
    }

    // 15. Final sequence consistency check
    let final_seq_a = cache_a.try_last_seq().await.unwrap();
    let final_seq_b = cache_b.try_last_seq().await.unwrap();
    let final_seq_c = cache_c.try_last_seq().await.unwrap();
    let final_seq_d = cache_d.try_last_seq().await.unwrap();

    assert_eq!(final_seq_a, final_seq_b);
    assert_eq!(final_seq_a, final_seq_c);
    assert_eq!(final_seq_a, final_seq_d);

    println!("Concurrent same-range watchers test completed successfully!");
    println!("All 4 caches maintained perfect synchronization:");
    println!("  - Final entries: {}", final_expected_count);
    println!("  - Final sequence: {}", final_seq_a);
    println!("  - All caches have identical data for all keys");
    println!("  - Concurrent reads and writes worked flawlessly");
    println!("Multiple watchers on same range stayed perfectly synchronized!");
}
