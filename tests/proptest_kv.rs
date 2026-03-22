// =============================================================================
// tests/proptest_kv.rs -- KV Property-Based Testing
// =============================================================================
//
// Property-based testing 不是只測少數範例，而是讓測試框架自動產生大量輸入，
// 驗證「某個性質永遠成立」。
//
// 這裡的核心不變式是：
// 1. put 之後立刻 get，值必須一致
// 2. 任意 put/delete/get 序列後，最終狀態必須跟參照 HashMap 一樣
// 3. list_all 的內容必須等於最終資料集，而且依 key 排序

use std::collections::{BTreeMap, HashMap};

use ferrisdb::storage::memory::MemTable;
use ferrisdb::storage::traits::StorageEngine;
use proptest::prelude::*;

#[derive(Debug, Clone)]
enum KvOp {
    Put(String, String),
    Delete(String),
    Get(String),
}

fn kv_op_strategy() -> impl Strategy<Value = KvOp> {
    let key = "[a-z0-9]{1,8}";
    let value = "[a-zA-Z0-9]{0,12}";
    prop_oneof![
        (key.prop_map(String::from), value.prop_map(String::from))
            .prop_map(|(k, v)| KvOp::Put(k, v)),
        key.prop_map(String::from).prop_map(KvOp::Delete),
        key.prop_map(String::from).prop_map(KvOp::Get),
    ]
}

proptest! {
    #[test]
    fn prop_put_then_get_returns_same_value(
        key in "[a-z0-9]{1,12}",
        value in "[a-zA-Z0-9]{0,24}"
    ) {
        let mut table = MemTable::new();
        table.put(key.clone().into_bytes(), value.clone().into_bytes()).unwrap();

        let got = table.get(key.as_bytes()).unwrap();
        prop_assert_eq!(got, Some(value.into_bytes()));
    }

    #[test]
    fn prop_operation_sequence_matches_hashmap_model(
        ops in prop::collection::vec(kv_op_strategy(), 1..80)
    ) {
        let mut table = MemTable::new();
        let mut model = HashMap::<String, String>::new();

        for op in ops {
            match op {
                KvOp::Put(key, value) => {
                    table.put(key.clone().into_bytes(), value.clone().into_bytes()).unwrap();
                    model.insert(key, value);
                }
                KvOp::Delete(key) => {
                    table.delete(key.as_bytes()).unwrap();
                    model.remove(&key);
                }
                KvOp::Get(key) => {
                    let actual = table.get(key.as_bytes()).unwrap();
                    let expected = model.get(&key).cloned().map(String::into_bytes);
                    prop_assert_eq!(actual, expected);
                }
            }
        }

        let actual: HashMap<String, String> = table
            .list_all()
            .unwrap()
            .into_iter()
            .map(|(k, v)| {
                (
                    String::from_utf8(k).unwrap(),
                    String::from_utf8(v).unwrap(),
                )
            })
            .collect();
        prop_assert_eq!(actual, model);
    }

    #[test]
    fn prop_list_all_matches_sorted_btreemap(
        entries in prop::collection::vec(
            ("[a-z0-9]{1,8}", "[a-zA-Z0-9]{0,12}"),
            1..60
        )
    ) {
        let mut table = MemTable::new();
        let mut model = BTreeMap::<String, String>::new();

        for (key, value) in entries {
            table.put(key.clone().into_bytes(), value.clone().into_bytes()).unwrap();
            model.insert(key, value);
        }

        let actual = table.list_all().unwrap()
            .into_iter()
            .map(|(k, v)| (String::from_utf8(k).unwrap(), String::from_utf8(v).unwrap()))
            .collect::<Vec<_>>();
        let expected = model.into_iter().collect::<Vec<_>>();
        prop_assert_eq!(actual, expected);
    }
}
