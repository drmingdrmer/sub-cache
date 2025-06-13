// 不支持订阅集成测试
#[tokio::test]
async fn test_unsupported_subscribe() {
    // mock一个不支持subscribe的数据源
    // watcher应进入降级/等待状态
    // 可断言cache为Err(Unsupported)
}
