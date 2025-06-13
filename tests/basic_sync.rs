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

// 基本同步集成测试
// 启动 watcher，数据源变更，验证本地 cache 自动同步

use sub_cache::testing::source::TestSource;
use sub_cache::testing::source::Val;
use sub_cache::testing::types::TestConfig;
use sub_cache::Cache;

#[tokio::test]
async fn test_basic_sync() {
    // 1. 启动一个mock数据源和一个Cache
    let test_source = TestSource::new();
    let mut cache =
        Cache::<TestConfig>::new(test_source.clone(), "", "integration-basic-sync").await;

    // 2. 在数据源插入/修改/删除数据
    test_source.set("/k1", Some("v1")).await;
    test_source.set("/k2", Some("v2")).await;
    test_source.set("/k1", Some("v1x")).await;
    test_source.set("/k2", None).await;

    // 3. 等待cache同步（轮询等待，最多1秒）
    let mut ok = false;
    for _ in 0..20 {
        let v1 = cache.try_get("/k1").await.unwrap();
        let v2 = cache.try_get("/k2").await.unwrap();
        if v1 == Some(Val::new(3, "v1x")) && v2.is_none() {
            ok = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(ok, "cache did not sync in time");

    // 4. 检查本地cache内容与数据源一致
    let v1 = cache.try_get("/k1").await.unwrap();
    let v2 = cache.try_get("/k2").await.unwrap();
    assert_eq!(v1, Some(Val::new(3, "v1x")));
    assert_eq!(v2, None);
    assert_eq!(cache.try_last_seq().await.unwrap(), 3);
}
