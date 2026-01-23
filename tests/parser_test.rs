use std::sync::{Arc, Mutex};
use std::collections::{HashMap, VecDeque};
use tokio::sync::mpsc;

use redis_cache::models::RedisValue;
use redis_cache::parser::parse_resp;

fn new_kv_store() -> Arc<Mutex<HashMap<String, RedisValue>>> {
    Arc::new(Mutex::new(HashMap::new()))
}

fn new_waiting_room() -> Arc<Mutex<HashMap<String, VecDeque<mpsc::Sender<String>>>>> {
    Arc::new(Mutex::new(HashMap::new()))
}

// Helper to create raw RESP format from parts
fn make_resp(parts: &[&str]) -> Vec<u8> {
    let mut result = format!("*{}\r\n", parts.len());
    for part in parts {
        result.push_str(&format!("${}\r\n{}\r\n", part.len(), part));
    }
    result.into_bytes()
}

// ==================== PING Tests ====================

#[tokio::test]
async fn test_parser_ping() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    let mut buffer = make_resp(&["PING"]);
    let bytes_read = buffer.len();

    let result = parse_resp(&mut buffer, bytes_read, &kv_store, &waiting_room).await;
    assert_eq!(result, b"+PONG\r\n");
}

#[tokio::test]
async fn test_parser_ping_lowercase() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    let mut buffer = make_resp(&["ping"]);
    let bytes_read = buffer.len();

    let result = parse_resp(&mut buffer, bytes_read, &kv_store, &waiting_room).await;
    assert_eq!(result, b"+PONG\r\n");
}

// ==================== ECHO Tests ====================

#[tokio::test]
async fn test_parser_echo() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    let mut buffer = make_resp(&["ECHO", "hello"]);
    let bytes_read = buffer.len();

    let result = parse_resp(&mut buffer, bytes_read, &kv_store, &waiting_room).await;
    assert_eq!(result, b"$5\r\nhello\r\n");
}

#[tokio::test]
async fn test_parser_echo_strawberry() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    let mut buffer = make_resp(&["ECHO", "strawberry"]);
    let bytes_read = buffer.len();

    let result = parse_resp(&mut buffer, bytes_read, &kv_store, &waiting_room).await;
    assert_eq!(result, b"$10\r\nstrawberry\r\n");
}

// ==================== SET/GET Tests ====================

#[tokio::test]
async fn test_parser_set_get() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    // SET
    let mut buffer = make_resp(&["SET", "orange", "mango"]);
    let bytes_read = buffer.len();
    let result = parse_resp(&mut buffer, bytes_read, &kv_store, &waiting_room).await;
    assert_eq!(result, b"+OK\r\n");

    // GET
    let mut buffer = make_resp(&["GET", "orange"]);
    let bytes_read = buffer.len();
    let result = parse_resp(&mut buffer, bytes_read, &kv_store, &waiting_room).await;
    assert_eq!(result, b"$5\r\nmango\r\n");
}

#[tokio::test]
async fn test_parser_set_with_expiry() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    let mut buffer = make_resp(&["SET", "banana", "pineapple", "PX", "100"]);
    let bytes_read = buffer.len();
    let result = parse_resp(&mut buffer, bytes_read, &kv_store, &waiting_room).await;
    assert_eq!(result, b"+OK\r\n");

    // GET immediately - should succeed
    let mut buffer = make_resp(&["GET", "banana"]);
    let bytes_read = buffer.len();
    let result = parse_resp(&mut buffer, bytes_read, &kv_store, &waiting_room).await;
    assert_eq!(result, b"$9\r\npineapple\r\n");

    // Wait for expiry
    tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

    // GET after expiry
    let mut buffer = make_resp(&["GET", "banana"]);
    let bytes_read = buffer.len();
    let result = parse_resp(&mut buffer, bytes_read, &kv_store, &waiting_room).await;
    assert_eq!(result, b"$-1\r\n");
}

#[tokio::test]
async fn test_parser_get_nonexistent() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    let mut buffer = make_resp(&["GET", "nokey"]);
    let bytes_read = buffer.len();
    let result = parse_resp(&mut buffer, bytes_read, &kv_store, &waiting_room).await;
    assert_eq!(result, b"$-1\r\n");
}

// ==================== TYPE Tests ====================

#[tokio::test]
async fn test_parser_type_string() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    // SET creates a string
    let mut buffer = make_resp(&["SET", "banana", "blueberry"]);
    let bytes_read = buffer.len();
    parse_resp(&mut buffer, bytes_read, &kv_store, &waiting_room).await;

    // TYPE
    let mut buffer = make_resp(&["TYPE", "banana"]);
    let bytes_read = buffer.len();
    let result = parse_resp(&mut buffer, bytes_read, &kv_store, &waiting_room).await;
    assert_eq!(result, b"+string\r\n");
}

#[tokio::test]
async fn test_parser_type_none() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    let mut buffer = make_resp(&["TYPE", "missing_key"]);
    let bytes_read = buffer.len();
    let result = parse_resp(&mut buffer, bytes_read, &kv_store, &waiting_room).await;
    assert_eq!(result, b"+none\r\n");
}

// ==================== List Command Tests ====================

#[tokio::test]
async fn test_parser_rpush_lrange() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    // RPUSH
    let mut buffer = make_resp(&["RPUSH", "pear", "mango"]);
    let bytes_read = buffer.len();
    let result = parse_resp(&mut buffer, bytes_read, &kv_store, &waiting_room).await;
    assert_eq!(result, b":1\r\n");

    // RPUSH more
    let mut buffer = make_resp(&["RPUSH", "pear", "banana", "grape"]);
    let bytes_read = buffer.len();
    let result = parse_resp(&mut buffer, bytes_read, &kv_store, &waiting_room).await;
    assert_eq!(result, b":3\r\n");

    // LRANGE
    let mut buffer = make_resp(&["LRANGE", "pear", "0", "-1"]);
    let bytes_read = buffer.len();
    let result = parse_resp(&mut buffer, bytes_read, &kv_store, &waiting_room).await;
    // Should contain all 3 items
    assert!(result.starts_with(b"*3\r\n"));
}

#[tokio::test]
async fn test_parser_lpush() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    // LPUSH
    let mut buffer = make_resp(&["LPUSH", "grape", "raspberry"]);
    let bytes_read = buffer.len();
    let result = parse_resp(&mut buffer, bytes_read, &kv_store, &waiting_room).await;
    assert_eq!(result, b":1\r\n");

    // LPUSH more (prepends)
    let mut buffer = make_resp(&["LPUSH", "grape", "blueberry", "grape"]);
    let bytes_read = buffer.len();
    let result = parse_resp(&mut buffer, bytes_read, &kv_store, &waiting_room).await;
    assert_eq!(result, b":3\r\n");
}

#[tokio::test]
async fn test_parser_llen() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    // Create list
    let mut buffer = make_resp(&["RPUSH", "orange", "a", "b", "c", "d"]);
    let bytes_read = buffer.len();
    parse_resp(&mut buffer, bytes_read, &kv_store, &waiting_room).await;

    // LLEN
    let mut buffer = make_resp(&["LLEN", "orange"]);
    let bytes_read = buffer.len();
    let result = parse_resp(&mut buffer, bytes_read, &kv_store, &waiting_room).await;
    assert_eq!(result, b":4\r\n");

    // LLEN nonexistent
    let mut buffer = make_resp(&["LLEN", "missing_key"]);
    let bytes_read = buffer.len();
    let result = parse_resp(&mut buffer, bytes_read, &kv_store, &waiting_room).await;
    assert_eq!(result, b":0\r\n");
}

#[tokio::test]
async fn test_parser_lpop() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    // Create list
    let mut buffer = make_resp(&["RPUSH", "mango", "pear", "grape", "pineapple"]);
    let bytes_read = buffer.len();
    parse_resp(&mut buffer, bytes_read, &kv_store, &waiting_room).await;

    // LPOP single
    let mut buffer = make_resp(&["LPOP", "mango"]);
    let bytes_read = buffer.len();
    let result = parse_resp(&mut buffer, bytes_read, &kv_store, &waiting_room).await;
    assert_eq!(result, b"$4\r\npear\r\n");

    // LPOP with count
    let mut buffer = make_resp(&["LPOP", "mango", "2"]);
    let bytes_read = buffer.len();
    let result = parse_resp(&mut buffer, bytes_read, &kv_store, &waiting_room).await;
    assert!(result.starts_with(b"*2\r\n"));
}

// ==================== BLPOP Tests ====================

#[tokio::test]
async fn test_parser_blpop_immediate() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    // Create list with data
    let mut buffer = make_resp(&["RPUSH", "mylist", "value"]);
    let bytes_read = buffer.len();
    parse_resp(&mut buffer, bytes_read, &kv_store, &waiting_room).await;

    // BLPOP should return immediately
    let mut buffer = make_resp(&["BLPOP", "mylist", "0"]);
    let bytes_read = buffer.len();
    let result = parse_resp(&mut buffer, bytes_read, &kv_store, &waiting_room).await;
    assert!(result.starts_with(b"*2\r\n"));
}

#[tokio::test]
async fn test_parser_blpop_timeout() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    // BLPOP on empty list with timeout
    let mut buffer = make_resp(&["BLPOP", "nolist", "0.1"]);
    let bytes_read = buffer.len();
    let result = parse_resp(&mut buffer, bytes_read, &kv_store, &waiting_room).await;
    assert_eq!(result, b"*-1\r\n");
}

// ==================== Stream Command Tests ====================

#[tokio::test]
async fn test_parser_xadd_explicit_id() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    let mut buffer = make_resp(&["XADD", "strawberry", "0-1", "foo", "bar"]);
    let bytes_read = buffer.len();
    let result = parse_resp(&mut buffer, bytes_read, &kv_store, &waiting_room).await;

    let response = String::from_utf8_lossy(&result);
    assert!(response.contains("0-1"));
}

#[tokio::test]
async fn test_parser_xadd_type_check() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    // XADD creates stream
    let mut buffer = make_resp(&["XADD", "strawberry", "0-1", "foo", "bar"]);
    let bytes_read = buffer.len();
    parse_resp(&mut buffer, bytes_read, &kv_store, &waiting_room).await;

    // TYPE should be stream
    let mut buffer = make_resp(&["TYPE", "strawberry"]);
    let bytes_read = buffer.len();
    let result = parse_resp(&mut buffer, bytes_read, &kv_store, &waiting_room).await;
    assert_eq!(result, b"+stream\r\n");
}

#[tokio::test]
async fn test_parser_xadd_partial_wildcard() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    // 0-* should auto-generate sequence
    let mut buffer = make_resp(&["XADD", "raspberry", "0-*", "blueberry", "pear"]);
    let bytes_read = buffer.len();
    let result = parse_resp(&mut buffer, bytes_read, &kv_store, &waiting_room).await;

    let response = String::from_utf8_lossy(&result);
    assert!(response.contains("0-1"));
}

#[tokio::test]
async fn test_parser_xadd_validation() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    // Add first entry
    let mut buffer = make_resp(&["XADD", "banana", "1-1", "pear", "pineapple"]);
    let bytes_read = buffer.len();
    parse_resp(&mut buffer, bytes_read, &kv_store, &waiting_room).await;

    // Try to add with same ID - should error
    let mut buffer = make_resp(&["XADD", "banana", "1-1", "apple", "orange"]);
    let bytes_read = buffer.len();
    let result = parse_resp(&mut buffer, bytes_read, &kv_store, &waiting_room).await;

    let response = String::from_utf8_lossy(&result);
    assert!(response.contains("ERR"));

    // Try 0-0 - should error
    let mut buffer = make_resp(&["XADD", "newstream", "0-0", "a", "b"]);
    let bytes_read = buffer.len();
    let result = parse_resp(&mut buffer, bytes_read, &kv_store, &waiting_room).await;

    let response = String::from_utf8_lossy(&result);
    assert!(response.contains("ERR") && response.contains("0-0"));
}

#[tokio::test]
async fn test_parser_xrange() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    // Add entries
    let mut buffer = make_resp(&["XADD", "orange", "0-1", "blueberry", "mango"]);
    let bytes_read = buffer.len();
    parse_resp(&mut buffer, bytes_read, &kv_store, &waiting_room).await;

    let mut buffer = make_resp(&["XADD", "orange", "0-2", "strawberry", "orange"]);
    let bytes_read = buffer.len();
    parse_resp(&mut buffer, bytes_read, &kv_store, &waiting_room).await;

    // XRANGE full
    let mut buffer = make_resp(&["XRANGE", "orange", "-", "+"]);
    let bytes_read = buffer.len();
    let result = parse_resp(&mut buffer, bytes_read, &kv_store, &waiting_room).await;

    // Should have 2 entries
    let response = String::from_utf8_lossy(&result);
    assert!(response.contains("0-1"));
    assert!(response.contains("0-2"));
}

#[tokio::test]
async fn test_parser_xread() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    // Add entry
    let mut buffer = make_resp(&["XADD", "orange", "0-1", "temperature", "36"]);
    let bytes_read = buffer.len();
    parse_resp(&mut buffer, bytes_read, &kv_store, &waiting_room).await;

    // XREAD
    let mut buffer = make_resp(&["XREAD", "streams", "orange", "0-0"]);
    let bytes_read = buffer.len();
    let result = parse_resp(&mut buffer, bytes_read, &kv_store, &waiting_room).await;

    let response = String::from_utf8_lossy(&result);
    assert!(response.contains("orange"));
    assert!(response.contains("0-1"));
}

#[tokio::test]
async fn test_parser_xread_multiple_streams() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    // Add to two streams
    let mut buffer = make_resp(&["XADD", "apple", "0-1", "temperature", "0"]);
    let bytes_read = buffer.len();
    parse_resp(&mut buffer, bytes_read, &kv_store, &waiting_room).await;

    let mut buffer = make_resp(&["XADD", "blueberry", "0-2", "humidity", "1"]);
    let bytes_read = buffer.len();
    parse_resp(&mut buffer, bytes_read, &kv_store, &waiting_room).await;

    // XREAD both streams
    let mut buffer = make_resp(&["XREAD", "streams", "apple", "blueberry", "0-0", "0-1"]);
    let bytes_read = buffer.len();
    let result = parse_resp(&mut buffer, bytes_read, &kv_store, &waiting_room).await;

    let response = String::from_utf8_lossy(&result);
    assert!(response.contains("apple"));
    assert!(response.contains("blueberry"));
}

// ==================== Concurrent Client Simulation ====================

#[tokio::test]
async fn test_parser_concurrent_clients() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();
    let num_clients = 5;

    let mut handles = vec![];

    for client_id in 0..num_clients {
        let store = Arc::clone(&kv_store);
        let room = Arc::clone(&waiting_room);
        let handle = tokio::spawn(async move {
            // Each client does PING
            let mut buffer = make_resp(&["PING"]);
            let bytes_read = buffer.len();
            let result = parse_resp(&mut buffer, bytes_read, &store, &room).await;
            assert_eq!(result, b"+PONG\r\n", "Client {} PING failed", client_id);

            // Each client SETs a unique key
            let key = format!("key{}", client_id);
            let value = format!("value{}", client_id);
            let mut buffer = make_resp(&["SET", &key, &value]);
            let bytes_read = buffer.len();
            let result = parse_resp(&mut buffer, bytes_read, &store, &room).await;
            assert_eq!(result, b"+OK\r\n", "Client {} SET failed", client_id);
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // Verify all keys exist
    let map = kv_store.lock().unwrap();
    assert_eq!(map.len(), num_clients);
}

// ==================== Unknown Command Test ====================

#[tokio::test]
async fn test_parser_unknown_command() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    let mut buffer = make_resp(&["UNKNOWNCMD", "arg"]);
    let bytes_read = buffer.len();
    let result = parse_resp(&mut buffer, bytes_read, &kv_store, &waiting_room).await;

    // Should return empty (error case)
    assert!(result.is_empty());
}

// ==================== Empty Input Test ====================

#[tokio::test]
async fn test_parser_empty_input() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    let mut buffer = vec![];
    let result = parse_resp(&mut buffer, 0, &kv_store, &waiting_room).await;
    assert!(result.is_empty());
}
