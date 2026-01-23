use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::time::Instant;

use redis_cache::models::{RedisData, RedisValue};
use redis_cache::commands::{process_ping, process_echo, process_type};

fn new_kv_store() -> Arc<Mutex<HashMap<String, RedisValue>>> {
    Arc::new(Mutex::new(HashMap::new()))
}

fn parts(args: &[&str]) -> Vec<String> {
    args.iter().map(|s| s.to_string()).collect()
}

// ==================== PING Tests ====================

#[test]
fn test_ping_returns_pong() {
    let result = process_ping();
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"+PONG\r\n");
}

#[test]
fn test_ping_multiple_calls() {
    for _ in 0..100 {
        let result = process_ping();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), b"+PONG\r\n");
    }
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
fn test_echo_empty_string() {
    let p = parts(&["ECHO", ""]);
    let result = process_echo(&p);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"$0\r\n\r\n");
}

#[test]
fn test_echo_with_special_characters() {
    let p = parts(&["ECHO", "hello\tworld\ntest"]);
    let result = process_echo(&p);
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(response.starts_with(b"$16\r\n"));
}

#[test]
fn test_echo_with_numbers() {
    let p = parts(&["ECHO", "12345"]);
    let result = process_echo(&p);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"$5\r\n12345\r\n");
}

#[test]
fn test_echo_missing_message() {
    let p = parts(&["ECHO"]);
    let result = process_echo(&p);
    assert!(result.is_err());
}

#[test]
fn test_echo_only_uses_first_argument() {
    let p = parts(&["ECHO", "first", "second", "third"]);
    let result = process_echo(&p);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"$5\r\nfirst\r\n");
}

// ==================== TYPE Tests ====================

#[test]
fn test_type_string() {
    let kv_store = new_kv_store();
    {
        let mut map = kv_store.lock().unwrap();
        map.insert(
            "mykey".to_string(),
            RedisValue::new(RedisData::String("value".to_string()), None),
        );
    }

    let p = parts(&["TYPE", "mykey"]);
    let result = process_type(&p, &kv_store);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"+string\r\n");
}

#[test]
fn test_type_list() {
    let kv_store = new_kv_store();
    {
        let mut map = kv_store.lock().unwrap();
        map.insert(
            "mylist".to_string(),
            RedisValue::new(RedisData::List(vec!["item".to_string()]), None),
        );
    }

    let p = parts(&["TYPE", "mylist"]);
    let result = process_type(&p, &kv_store);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"+list\r\n");
}

#[test]
fn test_type_stream() {
    let kv_store = new_kv_store();
    {
        let mut map = kv_store.lock().unwrap();
        map.insert(
            "mystream".to_string(),
            RedisValue::new(RedisData::Stream(vec![]), None),
        );
    }

    let p = parts(&["TYPE", "mystream"]);
    let result = process_type(&p, &kv_store);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"+stream\r\n");
}

#[test]
fn test_type_nonexistent_key() {
    let kv_store = new_kv_store();
    let p = parts(&["TYPE", "nokey"]);
    let result = process_type(&p, &kv_store);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"+none\r\n");
}

#[test]
fn test_type_expired_key() {
    let kv_store = new_kv_store();
    {
        let mut map = kv_store.lock().unwrap();
        let expired_time = Instant::now() - std::time::Duration::from_secs(10);
        map.insert(
            "expired".to_string(),
            RedisValue::new(RedisData::String("value".to_string()), Some(expired_time)),
        );
    }

    let p = parts(&["TYPE", "expired"]);
    let result = process_type(&p, &kv_store);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"+none\r\n");

    // Verify key was removed
    let map = kv_store.lock().unwrap();
    assert!(map.get("expired").is_none());
}

#[test]
fn test_type_missing_key_argument() {
    let kv_store = new_kv_store();
    let p = parts(&["TYPE"]);
    let result = process_type(&p, &kv_store);
    assert!(result.is_err());
}

// ==================== Concurrent Tests ====================

#[tokio::test]
async fn test_concurrent_ping() {
    let num_clients = 50;
    let mut handles = vec![];

    for _ in 0..num_clients {
        let handle = tokio::spawn(async move {
            for _ in 0..100 {
                let result = process_ping();
                assert!(result.is_ok());
                assert_eq!(result.unwrap(), b"+PONG\r\n");
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }
}

#[tokio::test]
async fn test_concurrent_type_checks() {
    let kv_store = new_kv_store();

    // Pre-populate with different types
    {
        let mut map = kv_store.lock().unwrap();
        for i in 0..10 {
            map.insert(
                format!("string_{}", i),
                RedisValue::new(RedisData::String("value".to_string()), None),
            );
            map.insert(
                format!("list_{}", i),
                RedisValue::new(RedisData::List(vec!["item".to_string()]), None),
            );
            map.insert(
                format!("stream_{}", i),
                RedisValue::new(RedisData::Stream(vec![]), None),
            );
        }
    }

    let num_clients = 20;
    let mut handles = vec![];

    for client_id in 0..num_clients {
        let store = Arc::clone(&kv_store);
        let handle = tokio::spawn(async move {
            for i in 0..10 {
                let p = parts(&["TYPE", &format!("string_{}", i)]);
                let result = process_type(&p, &store);
                assert!(result.is_ok());
                assert_eq!(result.unwrap(), b"+string\r\n", "Client {} failed on string_{}", client_id, i);

                let p = parts(&["TYPE", &format!("list_{}", i)]);
                let result = process_type(&p, &store);
                assert!(result.is_ok());
                assert_eq!(result.unwrap(), b"+list\r\n");

                let p = parts(&["TYPE", &format!("stream_{}", i)]);
                let result = process_type(&p, &store);
                assert!(result.is_ok());
                assert_eq!(result.unwrap(), b"+stream\r\n");
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }
}
