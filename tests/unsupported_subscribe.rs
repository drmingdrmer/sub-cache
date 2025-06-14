// Unsupported subscription integration test
#[tokio::test]
async fn test_unsupported_subscribe() {
    // Mock a data source that doesn't support subscription
    // Watcher should enter degraded/waiting state
    // Can assert cache returns Err(Unsupported)
}
