use std::sync::{Arc, Mutex};
use std::collections::{HashMap, VecDeque};
use std::time::Instant;
use tokio::sync::mpsc;

use codecrafters_redis::models::{ListDir, RedisData, RedisValue};
use codecrafters_redis::respcommands::*;

// Helper to create a fresh kv_store for tests
fn new_kv_store() -> Arc<Mutex<HashMap<String, RedisValue>>> {
    Arc::new(Mutex::new(HashMap::new()))
}

// Helper to create a fresh waiting_room for tests
fn new_waiting_room() -> Arc<Mutex<HashMap<String, VecDeque<mpsc::Sender<String>>>>> {
    Arc::new(Mutex::new(HashMap::new()))
}

// ==================== PING Tests ====================
#[test]
fn test_ping_returns_pong() {
    let result = process_ping();
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"+PONG\r\n");
}

// ==================== ECHO Tests ====================
#[test]
fn test_echo_returns_message() {
    // RESP: *2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n
    let parts = vec!["*2", "$4", "ECHO", "$5", "hello"];
    let result = process_echo(&parts);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"$5\r\nhello\r\n");
}

#[test]
fn test_echo_with_longer_message() {
    let parts = vec!["*2", "$4", "ECHO", "$11", "hello world"];
    let result = process_echo(&parts);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"$11\r\nhello world\r\n");
}

#[test]
fn test_echo_invalid_length() {
    let parts = vec!["*2", "$4", "ECHO"]; // Missing the message
    let result = process_echo(&parts);
    assert!(result.is_err());
}

// ==================== SET Tests ====================
#[test]
fn test_set_basic() {
    let kv_store = new_kv_store();
    // RESP: *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
    let parts = vec!["*3", "$3", "SET", "$3", "key", "$5", "value"];
    let result = process_set(&parts, &kv_store);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"+OK\r\n");

    // Verify value was stored
    let map = kv_store.lock().unwrap();
    let stored = map.get("key").unwrap();
    match &stored.data {
        RedisData::String(s) => assert_eq!(s, "value"),
        _ => panic!("Expected string data"),
    }
}

#[test]
fn test_set_overwrites_existing() {
    let kv_store = new_kv_store();
    let parts1 = vec!["*3", "$3", "SET", "$3", "key", "$6", "value1"];
    process_set(&parts1, &kv_store).unwrap();

    let parts2 = vec!["*3", "$3", "SET", "$3", "key", "$6", "value2"];
    process_set(&parts2, &kv_store).unwrap();

    let map = kv_store.lock().unwrap();
    let stored = map.get("key").unwrap();
    match &stored.data {
        RedisData::String(s) => assert_eq!(s, "value2"),
        _ => panic!("Expected string data"),
    }
}

#[test]
fn test_set_with_ex_expiry() {
    let kv_store = new_kv_store();
    // SET key value EX 10
    let parts = vec!["*5", "$3", "SET", "$3", "key", "$5", "value", "$2", "EX", "$2", "10"];
    let result = process_set(&parts, &kv_store);
    assert!(result.is_ok());

    let map = kv_store.lock().unwrap();
    let stored = map.get("key").unwrap();
    assert!(stored.expires_at.is_some());
}

#[test]
fn test_set_with_px_expiry() {
    let kv_store = new_kv_store();
    // SET key value PX 1000
    let parts = vec!["*5", "$3", "SET", "$3", "key", "$5", "value", "$2", "PX", "$4", "1000"];
    let result = process_set(&parts, &kv_store);
    assert!(result.is_ok());

    let map = kv_store.lock().unwrap();
    let stored = map.get("key").unwrap();
    assert!(stored.expires_at.is_some());
}

#[test]
fn test_set_incomplete_command() {
    let kv_store = new_kv_store();
    let parts = vec!["*3", "$3", "SET", "$3", "key"]; // Missing value
    let result = process_set(&parts, &kv_store);
    assert!(result.is_err());
}

// ==================== GET Tests ====================
#[test]
fn test_get_existing_key() {
    let kv_store = new_kv_store();
    // First set a value
    {
        let mut map = kv_store.lock().unwrap();
        map.insert(
            "mykey".to_string(),
            RedisValue::new(RedisData::String("myvalue".to_string()), None),
        );
    }

    let parts = vec!["*2", "$3", "GET", "$5", "mykey"];
    let result = process_get(&parts, &kv_store);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"$7\r\nmyvalue\r\n");
}

#[test]
fn test_get_nonexistent_key() {
    let kv_store = new_kv_store();
    let parts = vec!["*2", "$3", "GET", "$5", "nokey"];
    let result = process_get(&parts, &kv_store);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"$-1\r\n"); // Null bulk string
}

#[test]
fn test_get_expired_key() {
    let kv_store = new_kv_store();
    // Set a value that has already expired
    {
        let mut map = kv_store.lock().unwrap();
        let expired_time = Instant::now() - std::time::Duration::from_secs(10);
        map.insert(
            "expired".to_string(),
            RedisValue::new(RedisData::String("value".to_string()), Some(expired_time)),
        );
    }

    let parts = vec!["*2", "$3", "GET", "$7", "expired"];
    let result = process_get(&parts, &kv_store);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"$-1\r\n"); // Should return null

    // Verify key was removed
    let map = kv_store.lock().unwrap();
    assert!(map.get("expired").is_none());
}

#[test]
fn test_get_wrong_type() {
    let kv_store = new_kv_store();
    // Store a list instead of string
    {
        let mut map = kv_store.lock().unwrap();
        map.insert(
            "listkey".to_string(),
            RedisValue::new(RedisData::List(vec!["item".to_string()]), None),
        );
    }

    let parts = vec!["*2", "$3", "GET", "$7", "listkey"];
    let result = process_get(&parts, &kv_store);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("WRONGTYPE"));
}

// ==================== RPUSH Tests ====================
#[test]
fn test_rpush_new_list() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();
    // RPUSH mylist value1
    let parts = vec!["*3", "$5", "RPUSH", "$6", "mylist", "$6", "value1"];
    let result = process_push(&parts, &kv_store, &waiting_room, ListDir::R);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b":1\r\n"); // Returns length

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

    // First push
    let parts1 = vec!["*3", "$5", "RPUSH", "$6", "mylist", "$6", "value1"];
    process_push(&parts1, &kv_store, &waiting_room, ListDir::R).unwrap();

    // Second push
    let parts2 = vec!["*3", "$5", "RPUSH", "$6", "mylist", "$6", "value2"];
    let result = process_push(&parts2, &kv_store, &waiting_room, ListDir::R);
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
    // RPUSH mylist v1 v2 v3
    let parts = vec![
        "*5", "$5", "RPUSH", "$6", "mylist", "$2", "v1", "$2", "v2", "$2", "v3",
    ];
    let result = process_push(&parts, &kv_store, &waiting_room, ListDir::R);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b":3\r\n");

    let map = kv_store.lock().unwrap();
    let stored = map.get("mylist").unwrap();
    match &stored.data {
        RedisData::List(list) => {
            assert_eq!(
                list,
                &vec!["v1".to_string(), "v2".to_string(), "v3".to_string()]
            );
        }
        _ => panic!("Expected list data"),
    }
}

// ==================== LPUSH Tests ====================
#[test]
fn test_lpush_new_list() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();
    let parts = vec!["*3", "$5", "LPUSH", "$6", "mylist", "$6", "value1"];
    let result = process_push(&parts, &kv_store, &waiting_room, ListDir::L);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b":1\r\n");
}

#[test]
fn test_lpush_prepends() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    let parts1 = vec!["*3", "$5", "LPUSH", "$6", "mylist", "$6", "value1"];
    process_push(&parts1, &kv_store, &waiting_room, ListDir::L).unwrap();

    let parts2 = vec!["*3", "$5", "LPUSH", "$6", "mylist", "$6", "value2"];
    process_push(&parts2, &kv_store, &waiting_room, ListDir::L).unwrap();

    let map = kv_store.lock().unwrap();
    let stored = map.get("mylist").unwrap();
    match &stored.data {
        RedisData::List(list) => {
            // LPUSH prepends, so value2 should be first
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
    let parts = vec![
        "*5", "$5", "LPUSH", "$6", "mylist", "$1", "a", "$1", "b", "$1", "c",
    ];
    process_push(&parts, &kv_store, &waiting_room, ListDir::L).unwrap();

    let map = kv_store.lock().unwrap();
    let stored = map.get("mylist").unwrap();
    match &stored.data {
        RedisData::List(list) => {
            // LPUSH a b c inserts in reverse at head: c b a
            assert_eq!(
                list,
                &vec!["c".to_string(), "b".to_string(), "a".to_string()]
            );
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
                RedisData::List(vec![
                    "a".to_string(),
                    "b".to_string(),
                    "c".to_string(),
                ]),
                None,
            ),
        );
    }

    // LRANGE mylist 0 -1
    let parts = vec!["*4", "$6", "LRANGE", "$6", "mylist", "$1", "0", "$2", "-1"];
    let result = process_lrange(&parts, &kv_store);
    assert!(result.is_ok());
    // *3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n
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
                RedisData::List(vec![
                    "a".to_string(),
                    "b".to_string(),
                    "c".to_string(),
                    "d".to_string(),
                ]),
                None,
            ),
        );
    }

    // LRANGE mylist 1 2
    let parts = vec!["*4", "$6", "LRANGE", "$6", "mylist", "$1", "1", "$1", "2"];
    let result = process_lrange(&parts, &kv_store);
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
                RedisData::List(vec![
                    "a".to_string(),
                    "b".to_string(),
                    "c".to_string(),
                ]),
                None,
            ),
        );
    }

    // LRANGE mylist -2 -1 (last two elements)
    let parts = vec![
        "*4", "$6", "LRANGE", "$6", "mylist", "$2", "-2", "$2", "-1",
    ];
    let result = process_lrange(&parts, &kv_store);
    assert!(result.is_ok());
    let expected = b"*2\r\n$1\r\nb\r\n$1\r\nc\r\n";
    assert_eq!(result.unwrap(), expected.to_vec());
}

#[test]
fn test_lrange_nonexistent_key() {
    let kv_store = new_kv_store();
    let parts = vec!["*4", "$6", "LRANGE", "$6", "nolist", "$1", "0", "$2", "-1"];
    let result = process_lrange(&parts, &kv_store);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"*0\r\n"); // Empty array
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

    // Start index beyond list length
    let parts = vec![
        "*4", "$6", "LRANGE", "$6", "mylist", "$2", "10", "$2", "20",
    ];
    let result = process_lrange(&parts, &kv_store);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"*0\r\n"); // Empty array
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
                RedisData::List(vec![
                    "a".to_string(),
                    "b".to_string(),
                    "c".to_string(),
                ]),
                None,
            ),
        );
    }

    let parts = vec!["*2", "$4", "LLEN", "$6", "mylist"];
    let result = process_llen(&parts, &kv_store);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b":3\r\n");
}

#[test]
fn test_llen_nonexistent_key() {
    let kv_store = new_kv_store();
    let parts = vec!["*2", "$4", "LLEN", "$6", "nolist"];
    let result = process_llen(&parts, &kv_store);
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

    let parts = vec!["*2", "$4", "LLEN", "$6", "strkey"];
    let result = process_llen(&parts, &kv_store);
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
                RedisData::List(vec![
                    "a".to_string(),
                    "b".to_string(),
                    "c".to_string(),
                ]),
                None,
            ),
        );
    }

    let parts = vec!["*2", "$4", "LPOP", "$6", "mylist"];
    let result = process_pop(&parts, &kv_store, ListDir::L);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"$1\r\na\r\n");

    // Verify list state
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
                RedisData::List(vec![
                    "a".to_string(),
                    "b".to_string(),
                    "c".to_string(),
                ]),
                None,
            ),
        );
    }

    // LPOP mylist 2
    let parts = vec!["*3", "$4", "LPOP", "$6", "mylist", "$1", "2"];
    let result = process_pop(&parts, &kv_store, ListDir::L);
    assert!(result.is_ok());
    // Returns array when count > 1
    let expected = b"*2\r\n$1\r\na\r\n$1\r\nb\r\n";
    assert_eq!(result.unwrap(), expected.to_vec());
}

#[test]
fn test_lpop_nonexistent_key() {
    let kv_store = new_kv_store();
    let parts = vec!["*2", "$4", "LPOP", "$6", "nolist"];
    let result = process_pop(&parts, &kv_store, ListDir::L);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"$-1\r\n"); // Null
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

    let parts = vec!["*2", "$4", "LPOP", "$6", "mylist"];
    let result = process_pop(&parts, &kv_store, ListDir::L);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"$-1\r\n"); // Null
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

    let parts = vec!["*2", "$4", "LPOP", "$6", "mylist"];
    process_pop(&parts, &kv_store, ListDir::L).unwrap();

    // Verify key was removed when list became empty
    let map = kv_store.lock().unwrap();
    assert!(map.get("mylist").is_none());
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

    // BLPOP mylist 0
    let parts = vec!["*3", "$5", "BLPOP", "$6", "mylist", "$1", "0"];
    let result = process_blpop(&parts, &kv_store, &waiting_room).await;
    assert!(result.is_ok());
    // Returns [key, value] array
    let expected = b"*2\r\n$6\r\nmylist\r\n$5\r\nfirst\r\n";
    assert_eq!(result.unwrap(), expected.to_vec());
}

#[tokio::test]
async fn test_blpop_timeout() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    // BLPOP nolist 0.1 (100ms timeout)
    let parts = vec!["*3", "$5", "BLPOP", "$6", "nolist", "$3", "0.1"];
    let result = process_blpop(&parts, &kv_store, &waiting_room).await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"*-1\r\n"); // Null array on timeout
}

#[tokio::test]
async fn test_blpop_with_push() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    // Spawn BLPOP in background
    let kv_clone = Arc::clone(&kv_store);
    let room_clone = Arc::clone(&waiting_room);
    let blpop_handle = tokio::spawn(async move {
        let parts = vec!["*3", "$5", "BLPOP", "$6", "mylist", "$1", "5"];
        process_blpop(&parts, &kv_clone, &room_clone).await
    });

    // Give BLPOP time to register in waiting room
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Push a value
    let push_parts = vec!["*3", "$5", "RPUSH", "$6", "mylist", "$5", "hello"];
    process_push(&push_parts, &kv_store, &waiting_room, ListDir::R).unwrap();

    // BLPOP should complete
    let result = blpop_handle.await.unwrap();
    assert!(result.is_ok());
    let expected = b"*2\r\n$6\r\nmylist\r\n$5\r\nhello\r\n";
    assert_eq!(result.unwrap(), expected.to_vec());
}

// ==================== Concurrent Client Tests ====================

/// Test multiple clients doing SET/GET operations concurrently
#[tokio::test]
async fn test_concurrent_set_get_operations() {
    let kv_store = new_kv_store();
    let num_clients = 10;
    let ops_per_client = 100;

    let mut handles = vec![];

    for client_id in 0..num_clients {
        let store = Arc::clone(&kv_store);
        let handle = tokio::spawn(async move {
            for op in 0..ops_per_client {
                let key = format!("key_{}_{}", client_id, op);
                let value = format!("value_{}_{}", client_id, op);

                // SET
                let set_parts: Vec<&str> = vec![
                    "*3", "$3", "SET", "$3", &key, "$5", &value,
                ];
                let set_parts_owned: Vec<String> = set_parts.iter().map(|s| s.to_string()).collect();
                let set_parts_ref: Vec<&str> = set_parts_owned.iter().map(|s| s.as_str()).collect();
                let result = process_set(&set_parts_ref, &store);
                assert!(result.is_ok());

                // GET
                let get_parts: Vec<&str> = vec!["*2", "$3", "GET", "$3", &key];
                let get_parts_owned: Vec<String> = get_parts.iter().map(|s| s.to_string()).collect();
                let get_parts_ref: Vec<&str> = get_parts_owned.iter().map(|s| s.as_str()).collect();
                let result = process_get(&get_parts_ref, &store);
                assert!(result.is_ok());
            }
        });
        handles.push(handle);
    }

    // Wait for all clients to complete
    for handle in handles {
        handle.await.unwrap();
    }

    // Verify final state - all keys should exist
    let map = kv_store.lock().unwrap();
    assert_eq!(map.len(), num_clients * ops_per_client);
}

/// Test multiple clients pushing to the same list concurrently
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
                let parts = vec![
                    "*3".to_string(),
                    "$5".to_string(),
                    "RPUSH".to_string(),
                    "$10".to_string(),
                    "sharedlist".to_string(),
                    format!("${}", value.len()),
                    value,
                ];
                let parts_ref: Vec<&str> = parts.iter().map(|s| s.as_str()).collect();
                let result = process_push(&parts_ref, &store, &room, ListDir::R);
                assert!(result.is_ok());
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // Verify all items were pushed
    let map = kv_store.lock().unwrap();
    let list = map.get("sharedlist").unwrap();
    match &list.data {
        RedisData::List(items) => {
            assert_eq!(items.len(), num_clients * pushes_per_client);
        }
        _ => panic!("Expected list"),
    }
}

/// Test multiple BLPOP waiters being served by pushes
#[tokio::test]
async fn test_multiple_blpop_waiters() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();
    let num_waiters = 5;

    // Spawn multiple BLPOP waiters
    let mut waiter_handles = vec![];
    for i in 0..num_waiters {
        let store = Arc::clone(&kv_store);
        let room = Arc::clone(&waiting_room);
        let handle = tokio::spawn(async move {
            let parts = vec!["*3", "$5", "BLPOP", "$8", "waitlist", "$1", "5"];
            let result = process_blpop(&parts, &store, &room).await;
            (i, result)
        });
        waiter_handles.push(handle);
    }

    // Give waiters time to register
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Push values to satisfy all waiters
    for i in 0..num_waiters {
        let value = format!("value{}", i);
        let parts = vec![
            "*3".to_string(),
            "$5".to_string(),
            "RPUSH".to_string(),
            "$8".to_string(),
            "waitlist".to_string(),
            format!("${}", value.len()),
            value,
        ];
        let parts_ref: Vec<&str> = parts.iter().map(|s| s.as_str()).collect();
        process_push(&parts_ref, &kv_store, &waiting_room, ListDir::R).unwrap();
    }

    // All waiters should complete successfully
    for handle in waiter_handles {
        let (waiter_id, result) = handle.await.unwrap();
        assert!(
            result.is_ok(),
            "Waiter {} should have received a value",
            waiter_id
        );
        let response = result.unwrap();
        // Should be an array response, not null
        assert!(
            response.starts_with(b"*2"),
            "Waiter {} got unexpected response",
            waiter_id
        );
    }
}

/// Test concurrent LPOP operations don't cause race conditions
#[tokio::test]
async fn test_concurrent_lpop() {
    let kv_store = new_kv_store();
    let num_items = 100;
    let num_poppers = 10;

    // Pre-populate list
    {
        let mut map = kv_store.lock().unwrap();
        let items: Vec<String> = (0..num_items).map(|i| format!("item{}", i)).collect();
        map.insert(
            "poplist".to_string(),
            RedisValue::new(RedisData::List(items), None),
        );
    }

    let mut handles = vec![];
    let popped_items = Arc::new(Mutex::new(Vec::new()));

    for _ in 0..num_poppers {
        let store = Arc::clone(&kv_store);
        let collected = Arc::clone(&popped_items);
        let handle = tokio::spawn(async move {
            loop {
                let parts = vec!["*2", "$4", "LPOP", "$7", "poplist"];
                let result = process_pop(&parts, &store, ListDir::L);
                if let Ok(response) = result {
                    if response == b"$-1\r\n" {
                        // List is empty
                        break;
                    }
                    // Parse the popped value and collect it
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

    // Verify all items were popped exactly once
    let collected = popped_items.lock().unwrap();
    assert_eq!(
        collected.len(),
        num_items,
        "All items should be popped exactly once"
    );

    // Verify list is empty
    let map = kv_store.lock().unwrap();
    assert!(
        map.get("poplist").is_none(),
        "List should be removed when empty"
    );
}

/// Test interleaved SET/GET operations on the same key
#[tokio::test]
async fn test_interleaved_set_get_same_key() {
    let kv_store = new_kv_store();
    let num_operations = 1000;

    let store1 = Arc::clone(&kv_store);
    let store2 = Arc::clone(&kv_store);

    // Writer task - continuously updates "counter"
    let writer = tokio::spawn(async move {
        for i in 0..num_operations {
            let value = format!("{}", i);
            let parts = vec![
                "*3".to_string(),
                "$3".to_string(),
                "SET".to_string(),
                "$7".to_string(),
                "counter".to_string(),
                format!("${}", value.len()),
                value,
            ];
            let parts_ref: Vec<&str> = parts.iter().map(|s| s.as_str()).collect();
            process_set(&parts_ref, &store1).unwrap();
        }
    });

    // Reader task - continuously reads "counter"
    let reader = tokio::spawn(async move {
        let mut reads = 0;
        for _ in 0..num_operations {
            let parts = vec!["*2", "$3", "GET", "$7", "counter"];
            let result = process_get(&parts, &store2);
            if result.is_ok() {
                reads += 1;
            }
        }
        reads
    });

    writer.await.unwrap();
    let reads = reader.await.unwrap();

    // Reader should have completed all reads without errors
    assert_eq!(reads, num_operations);
}

/// Test RPUSH and LPUSH happening concurrently on the same list
#[tokio::test]
async fn test_concurrent_rpush_lpush() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();
    let ops_per_side = 50;

    let store1 = Arc::clone(&kv_store);
    let room1 = Arc::clone(&waiting_room);
    let store2 = Arc::clone(&kv_store);
    let room2 = Arc::clone(&waiting_room);

    // RPUSH task
    let rpush_handle = tokio::spawn(async move {
        for i in 0..ops_per_side {
            let value = format!("R{}", i);
            let parts = vec![
                "*3".to_string(),
                "$5".to_string(),
                "RPUSH".to_string(),
                "$8".to_string(),
                "duallist".to_string(),
                format!("${}", value.len()),
                value,
            ];
            let parts_ref: Vec<&str> = parts.iter().map(|s| s.as_str()).collect();
            process_push(&parts_ref, &store1, &room1, ListDir::R).unwrap();
        }
    });

    // LPUSH task
    let lpush_handle = tokio::spawn(async move {
        for i in 0..ops_per_side {
            let value = format!("L{}", i);
            let parts = vec![
                "*3".to_string(),
                "$5".to_string(),
                "LPUSH".to_string(),
                "$8".to_string(),
                "duallist".to_string(),
                format!("${}", value.len()),
                value,
            ];
            let parts_ref: Vec<&str> = parts.iter().map(|s| s.as_str()).collect();
            process_push(&parts_ref, &store2, &room2, ListDir::L).unwrap();
        }
    });

    rpush_handle.await.unwrap();
    lpush_handle.await.unwrap();

    // Verify list has all items
    let map = kv_store.lock().unwrap();
    let list = map.get("duallist").unwrap();
    match &list.data {
        RedisData::List(items) => {
            assert_eq!(items.len(), ops_per_side * 2);
            // Count L and R prefixed items
            let l_count = items.iter().filter(|s| s.starts_with('L')).count();
            let r_count = items.iter().filter(|s| s.starts_with('R')).count();
            assert_eq!(l_count, ops_per_side);
            assert_eq!(r_count, ops_per_side);
        }
        _ => panic!("Expected list"),
    }
}
