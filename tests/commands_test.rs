use std::sync::{Arc, Mutex};
use std::collections::{HashMap, VecDeque};
use std::time::Instant;
use tokio::sync::mpsc;

use codecrafters_redis::models::{ListDir, RedisData, RedisValue};
use codecrafters_redis::commands::*;
use codecrafters_redis::utils::decoder::decode_resp;

// Helper to create a fresh kv_store for tests
fn new_kv_store() -> Arc<Mutex<HashMap<String, RedisValue>>> {
    Arc::new(Mutex::new(HashMap::new()))
}

// Helper to create a fresh waiting_room for tests
fn new_waiting_room() -> Arc<Mutex<HashMap<String, VecDeque<mpsc::Sender<String>>>>> {
    Arc::new(Mutex::new(HashMap::new()))
}

// Helper to create clean parts from command strings
fn parts(args: &[&str]) -> Vec<String> {
    args.iter().map(|s| s.to_string()).collect()
}

// ==================== decode_resp Tests ====================
#[test]
fn test_decode_resp_ping() {
    let raw = "*1\r\n$4\r\nPING\r\n";
    let result = decode_resp(raw);
    assert_eq!(result, vec!["PING"]);
}

#[test]
fn test_decode_resp_echo() {
    let raw = "*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n";
    let result = decode_resp(raw);
    assert_eq!(result, vec!["ECHO", "hello"]);
}

#[test]
fn test_decode_resp_set() {
    let raw = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
    let result = decode_resp(raw);
    assert_eq!(result, vec!["SET", "key", "value"]);
}

#[test]
fn test_decode_resp_set_with_expiry() {
    let raw = "*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nEX\r\n$2\r\n10\r\n";
    let result = decode_resp(raw);
    assert_eq!(result, vec!["SET", "key", "value", "EX", "10"]);
}

#[test]
fn test_decode_resp_xadd() {
    let raw = "*6\r\n$4\r\nXADD\r\n$10\r\nstream_key\r\n$3\r\n0-1\r\n$11\r\ntemperature\r\n$2\r\n96\r\n";
    let result = decode_resp(raw);
    assert_eq!(result, vec!["XADD", "stream_key", "0-1", "temperature", "96"]);
}

#[test]
fn test_decode_resp_rpush_multiple() {
    let raw = "*4\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$2\r\nv1\r\n$2\r\nv2\r\n";
    let result = decode_resp(raw);
    assert_eq!(result, vec!["RPUSH", "mylist", "v1", "v2"]);
}

#[test]
fn test_decode_resp_lrange() {
    let raw = "*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$1\r\n0\r\n$2\r\n-1\r\n";
    let result = decode_resp(raw);
    assert_eq!(result, vec!["LRANGE", "mylist", "0", "-1"]);
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
    let p = parts(&["ECHO", "hello"]);
    let result = process_echo(&p);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"$5\r\nhello\r\n");
}

#[test]
fn test_echo_with_longer_message() {
    let p = parts(&["ECHO", "hello world"]);
    let result = process_echo(&p);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"$11\r\nhello world\r\n");
}

#[test]
fn test_echo_invalid_length() {
    let p = parts(&["ECHO"]); // Missing the message
    let result = process_echo(&p);
    assert!(result.is_err());
}

// ==================== SET Tests ====================
#[test]
fn test_set_basic() {
    let kv_store = new_kv_store();
    let p = parts(&["SET", "key", "value"]);
    let result = process_set(&p, &kv_store);
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
    process_set(&parts(&["SET", "key", "value1"]), &kv_store).unwrap();
    process_set(&parts(&["SET", "key", "value2"]), &kv_store).unwrap();

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
    let p = parts(&["SET", "key", "value", "EX", "10"]);
    let result = process_set(&p, &kv_store);
    assert!(result.is_ok());

    let map = kv_store.lock().unwrap();
    let stored = map.get("key").unwrap();
    assert!(stored.expires_at.is_some());
}

#[test]
fn test_set_with_px_expiry() {
    let kv_store = new_kv_store();
    let p = parts(&["SET", "key", "value", "PX", "1000"]);
    let result = process_set(&p, &kv_store);
    assert!(result.is_ok());

    let map = kv_store.lock().unwrap();
    let stored = map.get("key").unwrap();
    assert!(stored.expires_at.is_some());
}

#[test]
fn test_set_incomplete_command() {
    let kv_store = new_kv_store();
    let p = parts(&["SET", "key"]); // Missing value
    let result = process_set(&p, &kv_store);
    assert!(result.is_err());
}

// ==================== GET Tests ====================
#[test]
fn test_get_existing_key() {
    let kv_store = new_kv_store();
    {
        let mut map = kv_store.lock().unwrap();
        map.insert(
            "mykey".to_string(),
            RedisValue::new(RedisData::String("myvalue".to_string()), None),
        );
    }

    let p = parts(&["GET", "mykey"]);
    let result = process_get(&p, &kv_store);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"$7\r\nmyvalue\r\n");
}

#[test]
fn test_get_nonexistent_key() {
    let kv_store = new_kv_store();
    let p = parts(&["GET", "nokey"]);
    let result = process_get(&p, &kv_store);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"$-1\r\n"); // Null bulk string
}

#[test]
fn test_get_expired_key() {
    let kv_store = new_kv_store();
    {
        let mut map = kv_store.lock().unwrap();
        let expired_time = Instant::now() - std::time::Duration::from_secs(10);
        map.insert(
            "expired".to_string(),
            RedisValue::new(RedisData::String("value".to_string()), Some(expired_time)),
        );
    }

    let p = parts(&["GET", "expired"]);
    let result = process_get(&p, &kv_store);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"$-1\r\n"); // Should return null

    // Verify key was removed
    let map = kv_store.lock().unwrap();
    assert!(map.get("expired").is_none());
}

#[test]
fn test_get_wrong_type() {
    let kv_store = new_kv_store();
    {
        let mut map = kv_store.lock().unwrap();
        map.insert(
            "listkey".to_string(),
            RedisValue::new(RedisData::List(vec!["item".to_string()]), None),
        );
    }

    let p = parts(&["GET", "listkey"]);
    let result = process_get(&p, &kv_store);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("WRONGTYPE"));
}

// ==================== RPUSH Tests ====================
#[test]
fn test_rpush_new_list() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();
    let p = parts(&["RPUSH", "mylist", "value1"]);
    let result = process_push(&p, &kv_store, &waiting_room, ListDir::R);
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
    let p = parts(&["LPUSH", "mylist", "a", "b", "c"]);
    process_push(&p, &kv_store, &waiting_room, ListDir::L).unwrap();

    let map = kv_store.lock().unwrap();
    let stored = map.get("mylist").unwrap();
    match &stored.data {
        RedisData::List(list) => {
            // LPUSH a b c inserts in reverse at head: c b a
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

    let p = parts(&["LRANGE", "mylist", "10", "20"]);
    let result = process_lrange(&p, &kv_store);
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
                RedisData::List(vec!["a".to_string(), "b".to_string(), "c".to_string()]),
                None,
            ),
        );
    }

    let p = parts(&["LPOP", "mylist", "2"]);
    let result = process_pop(&p, &kv_store, ListDir::L);
    assert!(result.is_ok());
    // Returns array when count > 1
    let expected = b"*2\r\n$1\r\na\r\n$1\r\nb\r\n";
    assert_eq!(result.unwrap(), expected.to_vec());
}

#[test]
fn test_lpop_nonexistent_key() {
    let kv_store = new_kv_store();
    let p = parts(&["LPOP", "nolist"]);
    let result = process_pop(&p, &kv_store, ListDir::L);
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

    let p = parts(&["LPOP", "mylist"]);
    let result = process_pop(&p, &kv_store, ListDir::L);
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

    let p = parts(&["LPOP", "mylist"]);
    process_pop(&p, &kv_store, ListDir::L).unwrap();

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

    let p = parts(&["BLPOP", "mylist", "0"]);
    let result = process_blpop(&p, &kv_store, &waiting_room).await;
    assert!(result.is_ok());
    // Returns [key, value] array
    let expected = b"*2\r\n$6\r\nmylist\r\n$5\r\nfirst\r\n";
    assert_eq!(result.unwrap(), expected.to_vec());
}

#[tokio::test]
async fn test_blpop_timeout() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    let p = parts(&["BLPOP", "nolist", "0.1"]);
    let result = process_blpop(&p, &kv_store, &waiting_room).await;
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
        let p = parts(&["BLPOP", "mylist", "5"]);
        process_blpop(&p, &kv_clone, &room_clone).await
    });

    // Give BLPOP time to register in waiting room
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Push a value
    process_push(&parts(&["RPUSH", "mylist", "hello"]), &kv_store, &waiting_room, ListDir::R).unwrap();

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
                let p = vec!["SET".to_string(), key.clone(), value];
                let result = process_set(&p, &store);
                assert!(result.is_ok());

                // GET
                let p = vec!["GET".to_string(), key];
                let result = process_get(&p, &store);
                assert!(result.is_ok());
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

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

/// Test multiple BLPOP waiters being served by pushes
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

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

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

/// Test concurrent LPOP operations don't cause race conditions
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

/// Test interleaved SET/GET operations on the same key
#[tokio::test]
async fn test_interleaved_set_get_same_key() {
    let kv_store = new_kv_store();
    let num_operations = 1000;

    let store1 = Arc::clone(&kv_store);
    let store2 = Arc::clone(&kv_store);

    let writer = tokio::spawn(async move {
        for i in 0..num_operations {
            let value = format!("{}", i);
            let p = vec!["SET".to_string(), "counter".to_string(), value];
            process_set(&p, &store1).unwrap();
        }
    });

    let reader = tokio::spawn(async move {
        let mut reads = 0;
        for _ in 0..num_operations {
            let p = parts(&["GET", "counter"]);
            let result = process_get(&p, &store2);
            if result.is_ok() {
                reads += 1;
            }
        }
        reads
    });

    writer.await.unwrap();
    let reads = reader.await.unwrap();
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
