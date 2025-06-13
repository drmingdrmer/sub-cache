// 多watcher并发协作集成测试
#[tokio::test]
async fn test_multi_watcher_concurrent() {
    // 启动多个watcher，分别订阅同一区间
    // 并发写入数据，所有watcher都应同步到最新
}
