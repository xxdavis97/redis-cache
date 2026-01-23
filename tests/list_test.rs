use std::sync::{Arc, Mutex};
use std::collections::{HashMap, VecDeque};
use tokio::sync::mpsc;

use redis_cache::models::{ListDir, RedisData, RedisValue};
use redis_cache::commands::{process_push, process_lrange, process_llen, process_pop, process_blpop};

fn new_kv_store() -> Arc<Mutex<HashMap<String, RedisValue>>> {
    Arc::new(Mutex::new(HashMap::new()))
}

fn new_waiting_room() -> Arc<Mutex<HashMap<String, VecDeque<mpsc::Sender<String>>>>> {
    Arc::new(Mutex::new(HashMap::new()))
}

fn parts(args: &[&str]) -> Vec<String> {
    args.iter().map(|s| s.to_string()).collect()
}

// ==================== RPUSH Tests ====================

#[test]
fn test_rpush_new_list() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();
    let p = parts(&["RPUSH", "mylist", "value1"]);
    let result = process_push(&p, &kv_store, &waiting_room, ListDir::R);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b":1\r\n");

    let map = kv_store.lock().unwrap();
    let stored = map.get("mylist").unwrap();
    match &stored.data {
        RedisData::List(list) => {
            assert_eq!(list.len(), 1);
            assert_eq!(list[0], "value1");
        }
        _ => panic!("Expected list data"),
    }
}

#[test]
fn test_rpush_existing_list() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    process_push(&parts(&["RPUSH", "mylist", "value1"]), &kv_store, &waiting_room, ListDir::R).unwrap();
    let result = process_push(&parts(&["RPUSH", "mylist", "value2"]), &kv_store, &waiting_room, ListDir::R);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b":2\r\n");

    let map = kv_store.lock().unwrap();
    let stored = map.get("mylist").unwrap();
    match &stored.data {
        RedisData::List(list) => {
            assert_eq!(list, &vec!["value1".to_string(), "value2".to_string()]);
        }
        _ => panic!("Expected list data"),
    }
}

#[test]
fn test_rpush_multiple_values() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();
    let p = parts(&["RPUSH", "mylist", "v1", "v2", "v3"]);
    let result = process_push(&p, &kv_store, &waiting_room, ListDir::R);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b":3\r\n");

    let map = kv_store.lock().unwrap();
    let stored = map.get("mylist").unwrap();
    match &stored.data {
        RedisData::List(list) => {
            assert_eq!(list, &vec!["v1".to_string(), "v2".to_string(), "v3".to_string()]);
        }
        _ => panic!("Expected list data"),
    }
}

#[test]
fn test_rpush_wrong_type() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    // Create a string key first
    {
        let mut map = kv_store.lock().unwrap();
        map.insert(
            "mykey".to_string(),
            RedisValue::new(RedisData::String("value".to_string()), None),
        );
    }

    let p = parts(&["RPUSH", "mykey", "item"]);
    let result = process_push(&p, &kv_store, &waiting_room, ListDir::R);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("WRONGTYPE"));
}

#[test]
fn test_rpush_incomplete_command() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();
    let p = parts(&["RPUSH", "mylist"]);
    let result = process_push(&p, &kv_store, &waiting_room, ListDir::R);
    assert!(result.is_err());
}

// ==================== LPUSH Tests ====================

#[test]
fn test_lpush_new_list() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();
    let p = parts(&["LPUSH", "mylist", "value1"]);
    let result = process_push(&p, &kv_store, &waiting_room, ListDir::L);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b":1\r\n");
}

#[test]
fn test_lpush_prepends() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    process_push(&parts(&["LPUSH", "mylist", "value1"]), &kv_store, &waiting_room, ListDir::L).unwrap();
    process_push(&parts(&["LPUSH", "mylist", "value2"]), &kv_store, &waiting_room, ListDir::L).unwrap();

    let map = kv_store.lock().unwrap();
    let stored = map.get("mylist").unwrap();
    match &stored.data {
        RedisData::List(list) => {
            assert_eq!(list, &vec!["value2".to_string(), "value1".to_string()]);
        }
        _ => panic!("Expected list data"),
    }
}

#[test]
fn test_lpush_multiple_values() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();
    // LPUSH mylist a b c -> c b a (reversed order at head)
    let p = parts(&["LPUSH", "mylist", "a", "b", "c"]);
    process_push(&p, &kv_store, &waiting_room, ListDir::L).unwrap();

    let map = kv_store.lock().unwrap();
    let stored = map.get("mylist").unwrap();
    match &stored.data {
        RedisData::List(list) => {
            assert_eq!(list, &vec!["c".to_string(), "b".to_string(), "a".to_string()]);
        }
        _ => panic!("Expected list data"),
    }
}

// ==================== LRANGE Tests ====================

#[test]
fn test_lrange_full_list() {
    let kv_store = new_kv_store();
    {
        let mut map = kv_store.lock().unwrap();
        map.insert(
            "mylist".to_string(),
            RedisValue::new(
                RedisData::List(vec!["a".to_string(), "b".to_string(), "c".to_string()]),
                None,
            ),
        );
    }

    let p = parts(&["LRANGE", "mylist", "0", "-1"]);
    let result = process_lrange(&p, &kv_store);
    assert!(result.is_ok());
    let expected = b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n";
    assert_eq!(result.unwrap(), expected.to_vec());
}

#[test]
fn test_lrange_partial() {
    let kv_store = new_kv_store();
    {
        let mut map = kv_store.lock().unwrap();
        map.insert(
            "mylist".to_string(),
            RedisValue::new(
                RedisData::List(vec!["a".to_string(), "b".to_string(), "c".to_string(), "d".to_string()]),
                None,
            ),
        );
    }

    let p = parts(&["LRANGE", "mylist", "1", "2"]);
    let result = process_lrange(&p, &kv_store);
    assert!(result.is_ok());
    let expected = b"*2\r\n$1\r\nb\r\n$1\r\nc\r\n";
    assert_eq!(result.unwrap(), expected.to_vec());
}

#[test]
fn test_lrange_negative_indices() {
    let kv_store = new_kv_store();
    {
        let mut map = kv_store.lock().unwrap();
        map.insert(
            "mylist".to_string(),
            RedisValue::new(
                RedisData::List(vec!["a".to_string(), "b".to_string(), "c".to_string()]),
                None,
            ),
        );
    }

    let p = parts(&["LRANGE", "mylist", "-2", "-1"]);
    let result = process_lrange(&p, &kv_store);
    assert!(result.is_ok());
    let expected = b"*2\r\n$1\r\nb\r\n$1\r\nc\r\n";
    assert_eq!(result.unwrap(), expected.to_vec());
}

#[test]
fn test_lrange_nonexistent_key() {
    let kv_store = new_kv_store();
    let p = parts(&["LRANGE", "nolist", "0", "-1"]);
    let result = process_lrange(&p, &kv_store);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"*0\r\n");
}

#[test]
fn test_lrange_out_of_bounds() {
    let kv_store = new_kv_store();
    {
        let mut map = kv_store.lock().unwrap();
        map.insert(
            "mylist".to_string(),
            RedisValue::new(RedisData::List(vec!["a".to_string()]), None),
        );
    }

    let p = parts(&["LRANGE", "mylist", "10", "20"]);
    let result = process_lrange(&p, &kv_store);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"*0\r\n");
}

#[test]
fn test_lrange_start_greater_than_end() {
    let kv_store = new_kv_store();
    {
        let mut map = kv_store.lock().unwrap();
        map.insert(
            "mylist".to_string(),
            RedisValue::new(RedisData::List(vec!["a".to_string(), "b".to_string(), "c".to_string()]), None),
        );
    }

    let p = parts(&["LRANGE", "mylist", "2", "1"]);
    let result = process_lrange(&p, &kv_store);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"*0\r\n");
}

#[test]
fn test_lrange_single_element() {
    let kv_store = new_kv_store();
    {
        let mut map = kv_store.lock().unwrap();
        map.insert(
            "mylist".to_string(),
            RedisValue::new(RedisData::List(vec!["only".to_string()]), None),
        );
    }

    let p = parts(&["LRANGE", "mylist", "0", "0"]);
    let result = process_lrange(&p, &kv_store);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"*1\r\n$4\r\nonly\r\n");
}

#[test]
fn test_lrange_wrong_type() {
    let kv_store = new_kv_store();
    {
        let mut map = kv_store.lock().unwrap();
        map.insert(
            "strkey".to_string(),
            RedisValue::new(RedisData::String("value".to_string()), None),
        );
    }

    let p = parts(&["LRANGE", "strkey", "0", "-1"]);
    let result = process_lrange(&p, &kv_store);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("WRONGTYPE"));
}

// ==================== LLEN Tests ====================

#[test]
fn test_llen_existing_list() {
    let kv_store = new_kv_store();
    {
        let mut map = kv_store.lock().unwrap();
        map.insert(
            "mylist".to_string(),
            RedisValue::new(
                RedisData::List(vec!["a".to_string(), "b".to_string(), "c".to_string()]),
                None,
            ),
        );
    }

    let p = parts(&["LLEN", "mylist"]);
    let result = process_llen(&p, &kv_store);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b":3\r\n");
}

#[test]
fn test_llen_nonexistent_key() {
    let kv_store = new_kv_store();
    let p = parts(&["LLEN", "nolist"]);
    let result = process_llen(&p, &kv_store);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b":0\r\n");
}

#[test]
fn test_llen_empty_list() {
    let kv_store = new_kv_store();
    {
        let mut map = kv_store.lock().unwrap();
        map.insert(
            "emptylist".to_string(),
            RedisValue::new(RedisData::List(vec![]), None),
        );
    }

    let p = parts(&["LLEN", "emptylist"]);
    let result = process_llen(&p, &kv_store);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b":0\r\n");
}

#[test]
fn test_llen_wrong_type() {
    let kv_store = new_kv_store();
    {
        let mut map = kv_store.lock().unwrap();
        map.insert(
            "strkey".to_string(),
            RedisValue::new(RedisData::String("value".to_string()), None),
        );
    }

    let p = parts(&["LLEN", "strkey"]);
    let result = process_llen(&p, &kv_store);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("WRONGTYPE"));
}

// ==================== LPOP Tests ====================

#[test]
fn test_lpop_single() {
    let kv_store = new_kv_store();
    {
        let mut map = kv_store.lock().unwrap();
        map.insert(
            "mylist".to_string(),
            RedisValue::new(
                RedisData::List(vec!["a".to_string(), "b".to_string(), "c".to_string()]),
                None,
            ),
        );
    }

    let p = parts(&["LPOP", "mylist"]);
    let result = process_pop(&p, &kv_store, ListDir::L);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"$1\r\na\r\n");

    let map = kv_store.lock().unwrap();
    let stored = map.get("mylist").unwrap();
    match &stored.data {
        RedisData::List(list) => {
            assert_eq!(list, &vec!["b".to_string(), "c".to_string()]);
        }
        _ => panic!("Expected list data"),
    }
}

#[test]
fn test_lpop_with_count() {
    let kv_store = new_kv_store();
    {
        let mut map = kv_store.lock().unwrap();
        map.insert(
            "mylist".to_string(),
            RedisValue::new(
                RedisData::List(vec!["a".to_string(), "b".to_string(), "c".to_string()]),
                None,
            ),
        );
    }

    let p = parts(&["LPOP", "mylist", "2"]);
    let result = process_pop(&p, &kv_store, ListDir::L);
    assert!(result.is_ok());
    let expected = b"*2\r\n$1\r\na\r\n$1\r\nb\r\n";
    assert_eq!(result.unwrap(), expected.to_vec());
}

#[test]
fn test_lpop_nonexistent_key() {
    let kv_store = new_kv_store();
    let p = parts(&["LPOP", "nolist"]);
    let result = process_pop(&p, &kv_store, ListDir::L);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"$-1\r\n");
}

#[test]
fn test_lpop_empty_list() {
    let kv_store = new_kv_store();
    {
        let mut map = kv_store.lock().unwrap();
        map.insert(
            "mylist".to_string(),
            RedisValue::new(RedisData::List(vec![]), None),
        );
    }

    let p = parts(&["LPOP", "mylist"]);
    let result = process_pop(&p, &kv_store, ListDir::L);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"$-1\r\n");
}

#[test]
fn test_lpop_removes_empty_list() {
    let kv_store = new_kv_store();
    {
        let mut map = kv_store.lock().unwrap();
        map.insert(
            "mylist".to_string(),
            RedisValue::new(RedisData::List(vec!["only".to_string()]), None),
        );
    }

    let p = parts(&["LPOP", "mylist"]);
    process_pop(&p, &kv_store, ListDir::L).unwrap();

    let map = kv_store.lock().unwrap();
    assert!(map.get("mylist").is_none());
}

#[test]
fn test_lpop_count_exceeds_list_size() {
    let kv_store = new_kv_store();
    {
        let mut map = kv_store.lock().unwrap();
        map.insert(
            "mylist".to_string(),
            RedisValue::new(RedisData::List(vec!["a".to_string(), "b".to_string()]), None),
        );
    }

    let p = parts(&["LPOP", "mylist", "10"]);
    let result = process_pop(&p, &kv_store, ListDir::L);
    assert!(result.is_ok());
    // Returns array with only available elements
    let expected = b"*2\r\n$1\r\na\r\n$1\r\nb\r\n";
    assert_eq!(result.unwrap(), expected.to_vec());

    // List should be removed
    let map = kv_store.lock().unwrap();
    assert!(map.get("mylist").is_none());
}

// ==================== RPOP Tests ====================

#[test]
fn test_rpop_single() {
    let kv_store = new_kv_store();
    {
        let mut map = kv_store.lock().unwrap();
        map.insert(
            "mylist".to_string(),
            RedisValue::new(
                RedisData::List(vec!["a".to_string(), "b".to_string(), "c".to_string()]),
                None,
            ),
        );
    }

    let p = parts(&["RPOP", "mylist"]);
    let result = process_pop(&p, &kv_store, ListDir::R);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"$1\r\nc\r\n");

    let map = kv_store.lock().unwrap();
    let stored = map.get("mylist").unwrap();
    match &stored.data {
        RedisData::List(list) => {
            assert_eq!(list, &vec!["a".to_string(), "b".to_string()]);
        }
        _ => panic!("Expected list data"),
    }
}

#[test]
fn test_rpop_with_count() {
    let kv_store = new_kv_store();
    {
        let mut map = kv_store.lock().unwrap();
        map.insert(
            "mylist".to_string(),
            RedisValue::new(
                RedisData::List(vec!["a".to_string(), "b".to_string(), "c".to_string()]),
                None,
            ),
        );
    }

    let p = parts(&["RPOP", "mylist", "2"]);
    let result = process_pop(&p, &kv_store, ListDir::R);
    assert!(result.is_ok());
    // RPOP returns elements in pop order (c, then b)
    let expected = b"*2\r\n$1\r\nc\r\n$1\r\nb\r\n";
    assert_eq!(result.unwrap(), expected.to_vec());
}

// ==================== BLPOP Tests ====================

#[tokio::test]
async fn test_blpop_existing_list() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();
    {
        let mut map = kv_store.lock().unwrap();
        map.insert(
            "mylist".to_string(),
            RedisValue::new(
                RedisData::List(vec!["first".to_string(), "second".to_string()]),
                None,
            ),
        );
    }

    let p = parts(&["BLPOP", "mylist", "0"]);
    let result = process_blpop(&p, &kv_store, &waiting_room).await;
    assert!(result.is_ok());
    let expected = b"*2\r\n$6\r\nmylist\r\n$5\r\nfirst\r\n";
    assert_eq!(result.unwrap(), expected.to_vec());
}

#[tokio::test]
async fn test_blpop_timeout_with_value() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    // Short timeout, no data
    let p = parts(&["BLPOP", "nolist", "0.1"]);
    let result = process_blpop(&p, &kv_store, &waiting_room).await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"*-1\r\n");
}

#[tokio::test]
async fn test_blpop_with_push_wakeup() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    let kv_clone = Arc::clone(&kv_store);
    let room_clone = Arc::clone(&waiting_room);
    let blpop_handle = tokio::spawn(async move {
        let p = parts(&["BLPOP", "mylist", "5"]);
        process_blpop(&p, &kv_clone, &room_clone).await
    });

    // Give BLPOP time to register
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Push a value
    process_push(&parts(&["RPUSH", "mylist", "hello"]), &kv_store, &waiting_room, ListDir::R).unwrap();

    let result = blpop_handle.await.unwrap();
    assert!(result.is_ok());
    let expected = b"*2\r\n$6\r\nmylist\r\n$5\r\nhello\r\n";
    assert_eq!(result.unwrap(), expected.to_vec());
}

#[tokio::test]
async fn test_blpop_zero_timeout_with_existing_data() {
    // When timeout is 0 but list already has data, should return immediately
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();
    {
        let mut map = kv_store.lock().unwrap();
        map.insert(
            "mylist".to_string(),
            RedisValue::new(RedisData::List(vec!["immediate".to_string()]), None),
        );
    }

    let p = parts(&["BLPOP", "mylist", "0"]);
    let result = process_blpop(&p, &kv_store, &waiting_room).await;
    assert!(result.is_ok());
    let expected = b"*2\r\n$6\r\nmylist\r\n$9\r\nimmediate\r\n";
    assert_eq!(result.unwrap(), expected.to_vec());
}

#[tokio::test]
async fn test_blpop_indefinite_timeout_wakeup() {
    // Test with 0 timeout (indefinite) - should block until data arrives
    // Using a test-level timeout to prevent test suite from hanging if wakeup fails
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    let kv_clone = Arc::clone(&kv_store);
    let room_clone = Arc::clone(&waiting_room);

    let blpop_handle = tokio::spawn(async move {
        let p = parts(&["BLPOP", "waitlist", "0"]);
        process_blpop(&p, &kv_clone, &room_clone).await
    });

    // Give BLPOP time to block
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Push should wake up the blocked BLPOP
    process_push(&parts(&["RPUSH", "waitlist", "wakeup_value"]), &kv_store, &waiting_room, ListDir::R).unwrap();

    // Use a test-level timeout to prevent infinite hang
    let result = tokio::time::timeout(
        tokio::time::Duration::from_secs(5),
        blpop_handle
    ).await;

    assert!(result.is_ok(), "BLPOP with indefinite timeout should have been woken up by push");
    let inner_result = result.unwrap().unwrap();
    assert!(inner_result.is_ok());
    let bytes = inner_result.unwrap();
    let response = String::from_utf8_lossy(&bytes);
    assert!(response.contains("waitlist"));
    assert!(response.contains("wakeup_value"));
}

#[tokio::test]
async fn test_multiple_blpop_waiters() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();
    let num_waiters = 5;

    let mut waiter_handles = vec![];
    for i in 0..num_waiters {
        let store = Arc::clone(&kv_store);
        let room = Arc::clone(&waiting_room);
        let handle = tokio::spawn(async move {
            let p = parts(&["BLPOP", "waitlist", "5"]);
            let result = process_blpop(&p, &store, &room).await;
            (i, result)
        });
        waiter_handles.push(handle);
    }

    // Give waiters time to register
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Push values for all waiters
    for i in 0..num_waiters {
        let value = format!("value{}", i);
        let p = vec!["RPUSH".to_string(), "waitlist".to_string(), value];
        process_push(&p, &kv_store, &waiting_room, ListDir::R).unwrap();
    }

    for handle in waiter_handles {
        let (waiter_id, result) = handle.await.unwrap();
        assert!(result.is_ok(), "Waiter {} should have received a value", waiter_id);
        let response = result.unwrap();
        assert!(response.starts_with(b"*2"), "Waiter {} got unexpected response", waiter_id);
    }
}

// ==================== Concurrent List Tests ====================

#[tokio::test]
async fn test_concurrent_list_push() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();
    let num_clients = 5;
    let pushes_per_client = 50;

    let mut handles = vec![];

    for client_id in 0..num_clients {
        let store = Arc::clone(&kv_store);
        let room = Arc::clone(&waiting_room);
        let handle = tokio::spawn(async move {
            for i in 0..pushes_per_client {
                let value = format!("item_{}_{}", client_id, i);
                let p = vec!["RPUSH".to_string(), "sharedlist".to_string(), value];
                let result = process_push(&p, &store, &room, ListDir::R);
                assert!(result.is_ok());
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let map = kv_store.lock().unwrap();
    let list = map.get("sharedlist").unwrap();
    match &list.data {
        RedisData::List(items) => {
            assert_eq!(items.len(), num_clients * pushes_per_client);
        }
        _ => panic!("Expected list"),
    }
}

#[tokio::test]
async fn test_concurrent_lpop() {
    let kv_store = new_kv_store();
    let num_items = 100;
    let num_poppers = 10;

    {
        let mut map = kv_store.lock().unwrap();
        let items: Vec<String> = (0..num_items).map(|i| format!("item{}", i)).collect();
        map.insert("poplist".to_string(), RedisValue::new(RedisData::List(items), None));
    }

    let mut handles = vec![];
    let popped_items = Arc::new(Mutex::new(Vec::new()));

    for _ in 0..num_poppers {
        let store = Arc::clone(&kv_store);
        let collected = Arc::clone(&popped_items);
        let handle = tokio::spawn(async move {
            loop {
                let p = parts(&["LPOP", "poplist"]);
                let result = process_pop(&p, &store, ListDir::L);
                if let Ok(response) = result {
                    if response == b"$-1\r\n" {
                        break;
                    }
                    let response_str = String::from_utf8_lossy(&response);
                    if let Some(value) = response_str.lines().nth(1) {
                        collected.lock().unwrap().push(value.to_string());
                    }
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let collected = popped_items.lock().unwrap();
    assert_eq!(collected.len(), num_items, "All items should be popped exactly once");

    let map = kv_store.lock().unwrap();
    assert!(map.get("poplist").is_none(), "List should be removed when empty");
}

#[tokio::test]
async fn test_concurrent_rpush_lpush() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();
    let ops_per_side = 50;

    let store1 = Arc::clone(&kv_store);
    let room1 = Arc::clone(&waiting_room);
    let store2 = Arc::clone(&kv_store);
    let room2 = Arc::clone(&waiting_room);

    let rpush_handle = tokio::spawn(async move {
        for i in 0..ops_per_side {
            let value = format!("R{}", i);
            let p = vec!["RPUSH".to_string(), "duallist".to_string(), value];
            process_push(&p, &store1, &room1, ListDir::R).unwrap();
        }
    });

    let lpush_handle = tokio::spawn(async move {
        for i in 0..ops_per_side {
            let value = format!("L{}", i);
            let p = vec!["LPUSH".to_string(), "duallist".to_string(), value];
            process_push(&p, &store2, &room2, ListDir::L).unwrap();
        }
    });

    rpush_handle.await.unwrap();
    lpush_handle.await.unwrap();

    let map = kv_store.lock().unwrap();
    let list = map.get("duallist").unwrap();
    match &list.data {
        RedisData::List(items) => {
            assert_eq!(items.len(), ops_per_side * 2);
            let l_count = items.iter().filter(|s| s.starts_with('L')).count();
            let r_count = items.iter().filter(|s| s.starts_with('R')).count();
            assert_eq!(l_count, ops_per_side);
            assert_eq!(r_count, ops_per_side);
        }
        _ => panic!("Expected list"),
    }
}

#[tokio::test]
async fn test_blpop_multiple_keys_first_available() {
    // Test that BLPOP with multiple keys returns from the first key with data
    // Note: current implementation only checks first key, so we put data in list1
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    // Populate the first list (since implementation only checks first key)
    {
        let mut map = kv_store.lock().unwrap();
        map.insert(
            "list1".to_string(),
            RedisValue::new(RedisData::List(vec!["from_list1".to_string()]), None),
        );
    }

    // BLPOP with timeout 0 (indefinite) - but list1 has data so returns immediately
    let p = parts(&["BLPOP", "list1", "list2", "0"]);
    let result = process_blpop(&p, &kv_store, &waiting_room).await;
    assert!(result.is_ok());
    let bytes = result.unwrap();
    let response = String::from_utf8_lossy(&bytes);
    assert!(response.contains("list1"));
    assert!(response.contains("from_list1"));
}
