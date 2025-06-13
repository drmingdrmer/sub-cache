// 并发写入一致性集成测试
#[tokio::test]
async fn test_concurrent_write_consistency() {
    // 多个客户端并发写入同一key
    // watcher应最终同步到最大seq的值
}
