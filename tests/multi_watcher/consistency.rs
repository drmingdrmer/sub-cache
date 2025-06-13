// 多watcher一致性集成测试
#[tokio::test]
async fn test_multi_watcher_consistency() {
    // 多个watcher订阅同一区间
    // 变更数据后，所有watcher的cache内容应完全一致
}
