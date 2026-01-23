use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::time::Instant;

use redis_cache::models::{RedisData, RedisValue};
use redis_cache::commands::{process_set, process_get};

fn new_kv_store() -> Arc<Mutex<HashMap<String, RedisValue>>> {
    Arc::new(Mutex::new(HashMap::new()))
}

fn parts(args: &[&str]) -> Vec<String> {
    args.iter().map(|s| s.to_string()).collect()
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

    // Verify expiry is approximately 10 seconds in the future
    let expiry = stored.expires_at.unwrap();
    let now = Instant::now();
    let diff = expiry.duration_since(now);
    assert!(diff.as_secs() >= 9 && diff.as_secs() <= 10);
}

#[test]
fn test_set_with_px_expiry() {
    let kv_store = new_kv_store();
    let p = parts(&["SET", "key", "value", "PX", "5000"]);
    let result = process_set(&p, &kv_store);
    assert!(result.is_ok());

    let map = kv_store.lock().unwrap();
    let stored = map.get("key").unwrap();
    assert!(stored.expires_at.is_some());

    // Verify expiry is approximately 5000 milliseconds in the future
    let expiry = stored.expires_at.unwrap();
    let now = Instant::now();
    let diff = expiry.duration_since(now);
    assert!(diff.as_millis() >= 4900 && diff.as_millis() <= 5000);
}

#[test]
fn test_set_with_lowercase_ex() {
    let kv_store = new_kv_store();
    let p = parts(&["SET", "key", "value", "ex", "10"]);
    let result = process_set(&p, &kv_store);
    assert!(result.is_ok());

    let map = kv_store.lock().unwrap();
    let stored = map.get("key").unwrap();
    assert!(stored.expires_at.is_some());
}

#[test]
fn test_set_with_lowercase_px() {
    let kv_store = new_kv_store();
    let p = parts(&["SET", "key", "value", "px", "1000"]);
    let result = process_set(&p, &kv_store);
    assert!(result.is_ok());

    let map = kv_store.lock().unwrap();
    let stored = map.get("key").unwrap();
    assert!(stored.expires_at.is_some());
}

#[test]
fn test_set_incomplete_command() {
    let kv_store = new_kv_store();
    let p = parts(&["SET", "key"]);
    let result = process_set(&p, &kv_store);
    assert!(result.is_err());
}

#[test]
fn test_set_empty_value() {
    let kv_store = new_kv_store();
    let p = parts(&["SET", "key", ""]);
    let result = process_set(&p, &kv_store);
    assert!(result.is_ok());

    let map = kv_store.lock().unwrap();
    let stored = map.get("key").unwrap();
    match &stored.data {
        RedisData::String(s) => assert_eq!(s, ""),
        _ => panic!("Expected string data"),
    }
}

#[test]
fn test_set_with_spaces_in_value() {
    let kv_store = new_kv_store();
    let p = parts(&["SET", "key", "hello world"]);
    let result = process_set(&p, &kv_store);
    assert!(result.is_ok());

    let map = kv_store.lock().unwrap();
    let stored = map.get("key").unwrap();
    match &stored.data {
        RedisData::String(s) => assert_eq!(s, "hello world"),
        _ => panic!("Expected string data"),
    }
}

#[test]
fn test_set_invalid_expiry_flag() {
    let kv_store = new_kv_store();
    let p = parts(&["SET", "key", "value", "XX", "10"]);
    let result = process_set(&p, &kv_store);
    assert!(result.is_err());
}

#[test]
fn test_set_without_expiry_has_none() {
    let kv_store = new_kv_store();
    let p = parts(&["SET", "key", "value"]);
    process_set(&p, &kv_store).unwrap();

    let map = kv_store.lock().unwrap();
    let stored = map.get("key").unwrap();
    assert!(stored.expires_at.is_none());
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
    assert_eq!(result.unwrap(), b"$-1\r\n");
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
    assert_eq!(result.unwrap(), b"$-1\r\n");

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

#[test]
fn test_get_missing_key_argument() {
    let kv_store = new_kv_store();
    let p = parts(&["GET"]);
    let result = process_get(&p, &kv_store);
    assert!(result.is_err());
}

#[test]
fn test_get_empty_string_value() {
    let kv_store = new_kv_store();
    {
        let mut map = kv_store.lock().unwrap();
        map.insert(
            "emptykey".to_string(),
            RedisValue::new(RedisData::String("".to_string()), None),
        );
    }

    let p = parts(&["GET", "emptykey"]);
    let result = process_get(&p, &kv_store);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"$0\r\n\r\n");
}

#[test]
fn test_get_not_yet_expired() {
    let kv_store = new_kv_store();
    {
        let mut map = kv_store.lock().unwrap();
        let future_time = Instant::now() + std::time::Duration::from_secs(100);
        map.insert(
            "future".to_string(),
            RedisValue::new(RedisData::String("stillvalid".to_string()), Some(future_time)),
        );
    }

    let p = parts(&["GET", "future"]);
    let result = process_get(&p, &kv_store);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"$10\r\nstillvalid\r\n");
}

// ==================== SET + GET Integration Tests ====================

#[test]
fn test_set_then_get() {
    let kv_store = new_kv_store();

    process_set(&parts(&["SET", "testkey", "testvalue"]), &kv_store).unwrap();

    let result = process_get(&parts(&["GET", "testkey"]), &kv_store);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"$9\r\ntestvalue\r\n");
}

#[test]
fn test_set_overwrite_then_get() {
    let kv_store = new_kv_store();

    process_set(&parts(&["SET", "key", "first"]), &kv_store).unwrap();
    process_set(&parts(&["SET", "key", "second"]), &kv_store).unwrap();

    let result = process_get(&parts(&["GET", "key"]), &kv_store);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"$6\r\nsecond\r\n");
}

#[tokio::test]
async fn test_set_with_expiry_then_wait_and_get() {
    let kv_store = new_kv_store();

    // Set with 100ms expiry
    process_set(&parts(&["SET", "tempkey", "tempvalue", "PX", "100"]), &kv_store).unwrap();

    // Get immediately - should succeed
    let result = process_get(&parts(&["GET", "tempkey"]), &kv_store);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"$9\r\ntempvalue\r\n");

    // Wait for expiry
    tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

    // Get after expiry - should return null
    let result = process_get(&parts(&["GET", "tempkey"]), &kv_store);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"$-1\r\n");
}

// ==================== Concurrent Tests ====================

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
            let p = vec!["GET".to_string(), "counter".to_string()];
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

#[tokio::test]
async fn test_concurrent_set_same_key() {
    let kv_store = new_kv_store();
    let num_clients = 50;

    let mut handles = vec![];

    for client_id in 0..num_clients {
        let store = Arc::clone(&kv_store);
        let handle = tokio::spawn(async move {
            let value = format!("value_from_client_{}", client_id);
            let p = vec!["SET".to_string(), "shared_key".to_string(), value];
            process_set(&p, &store).unwrap();
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // Should have exactly one value (the last one to win)
    let map = kv_store.lock().unwrap();
    assert_eq!(map.len(), 1);
    assert!(map.contains_key("shared_key"));
}

#[tokio::test]
async fn test_concurrent_expiry_race() {
    let kv_store = new_kv_store();
    let num_clients = 20;

    let mut handles = vec![];

    for client_id in 0..num_clients {
        let store = Arc::clone(&kv_store);
        let handle = tokio::spawn(async move {
            let key = format!("expiring_key_{}", client_id);

            // Set with very short expiry
            let p = vec!["SET".to_string(), key.clone(), "value".to_string(), "PX".to_string(), "50".to_string()];
            process_set(&p, &store).unwrap();

            // Immediately try to get
            let p = vec!["GET".to_string(), key.clone()];
            let result1 = process_get(&p, &store);
            assert!(result1.is_ok());

            // Wait and try again
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

            let p = vec!["GET".to_string(), key];
            let result2 = process_get(&p, &store);
            assert!(result2.is_ok());
            assert_eq!(result2.unwrap(), b"$-1\r\n"); // Should be expired
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }
}
