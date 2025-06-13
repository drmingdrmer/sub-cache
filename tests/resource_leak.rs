// 资源释放集成测试
#[tokio::test]
async fn test_resource_leak() {
    // watcher频繁启动/停止
    // 检查无panic、无内存泄漏（可用valgrind/heaptrack等工具辅助）
}
