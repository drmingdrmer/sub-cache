// 异常恢复集成测试
#[tokio::test]
async fn test_recovery_after_crash() {
    // 启动watcher，写入数据
    // 模拟watcher crash（drop/kill），重启watcher
    // 检查cache能自动恢复到最新
}
