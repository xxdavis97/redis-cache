use std::sync::{Arc, Mutex};
use std::collections::{HashMap, VecDeque};
use tokio::sync::mpsc;

use redis_cache::models::{RedisData, RedisValue};
use redis_cache::commands::{process_xadd, process_xrange, process_xread};

fn new_kv_store() -> Arc<Mutex<HashMap<String, RedisValue>>> {
    Arc::new(Mutex::new(HashMap::new()))
}

fn new_waiting_room() -> Arc<Mutex<HashMap<String, VecDeque<mpsc::Sender<String>>>>> {
    Arc::new(Mutex::new(HashMap::new()))
}

fn parts(args: &[&str]) -> Vec<String> {
    args.iter().map(|s| s.to_string()).collect()
}

// ==================== XADD Tests - Explicit IDs (ms-seq) ====================

#[test]
fn test_xadd_explicit_id() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    let p = parts(&["XADD", "mystream", "1-1", "field1", "value1"]);
    let result = process_xadd(&p, &kv_store, &waiting_room);
    assert!(result.is_ok());
    let bytes = result.unwrap();
    let response = String::from_utf8_lossy(&bytes);
    assert!(response.contains("1-1"));
}

#[test]
fn test_xadd_multiple_fields() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    let p = parts(&["XADD", "mystream", "1-1", "field1", "value1", "field2", "value2"]);
    let result = process_xadd(&p, &kv_store, &waiting_room);
    assert!(result.is_ok());

    let map = kv_store.lock().unwrap();
    let stream = map.get("mystream").unwrap();
    match &stream.data {
        RedisData::Stream(entries) => {
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].fields.len(), 2);
            assert_eq!(entries[0].fields.get("field1"), Some(&"value1".to_string()));
            assert_eq!(entries[0].fields.get("field2"), Some(&"value2".to_string()));
        }
        _ => panic!("Expected stream data"),
    }
}

#[test]
fn test_xadd_sequential_ids() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    process_xadd(&parts(&["XADD", "mystream", "1-1", "a", "1"]), &kv_store, &waiting_room).unwrap();
    process_xadd(&parts(&["XADD", "mystream", "1-2", "b", "2"]), &kv_store, &waiting_room).unwrap();
    process_xadd(&parts(&["XADD", "mystream", "2-0", "c", "3"]), &kv_store, &waiting_room).unwrap();

    let map = kv_store.lock().unwrap();
    let stream = map.get("mystream").unwrap();
    match &stream.data {
        RedisData::Stream(entries) => {
            assert_eq!(entries.len(), 3);
        }
        _ => panic!("Expected stream data"),
    }
}

#[test]
fn test_xadd_rejects_zero_id() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    let p = parts(&["XADD", "mystream", "0-0", "field", "value"]);
    let result = process_xadd(&p, &kv_store, &waiting_room);
    assert!(result.is_ok());
    let bytes = result.unwrap();
    let response = String::from_utf8_lossy(&bytes);
    assert!(response.contains("ERR") && response.contains("greater than 0-0"));
}

#[test]
fn test_xadd_rejects_non_increasing_id() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    // Add first entry
    process_xadd(&parts(&["XADD", "mystream", "5-5", "a", "1"]), &kv_store, &waiting_room).unwrap();

    // Try to add with smaller ID
    let result = process_xadd(&parts(&["XADD", "mystream", "5-4", "b", "2"]), &kv_store, &waiting_room);
    assert!(result.is_ok());
    let bytes = result.unwrap();
    let response = String::from_utf8_lossy(&bytes);
    assert!(response.contains("ERR") && response.contains("equal or smaller"));
}

#[test]
fn test_xadd_rejects_equal_id() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    process_xadd(&parts(&["XADD", "mystream", "5-5", "a", "1"]), &kv_store, &waiting_room).unwrap();

    let result = process_xadd(&parts(&["XADD", "mystream", "5-5", "b", "2"]), &kv_store, &waiting_room);
    assert!(result.is_ok());
    let bytes = result.unwrap();
    let response = String::from_utf8_lossy(&bytes);
    assert!(response.contains("ERR"));
}

// ==================== XADD Tests - Partial Wildcard (ms-*) ====================

#[test]
fn test_xadd_partial_wildcard_new_stream() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    let p = parts(&["XADD", "mystream", "100-*", "field", "value"]);
    let result = process_xadd(&p, &kv_store, &waiting_room);
    assert!(result.is_ok());
    let bytes = result.unwrap();
    let response = String::from_utf8_lossy(&bytes);
    // Should auto-generate sequence 0 for ms > 0
    assert!(response.contains("100-0"));
}

#[test]
fn test_xadd_partial_wildcard_zero_ms() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    // When ms=0, sequence must be >= 1
    let p = parts(&["XADD", "mystream", "0-*", "field", "value"]);
    let result = process_xadd(&p, &kv_store, &waiting_room);
    assert!(result.is_ok());
    let bytes = result.unwrap();
    let response = String::from_utf8_lossy(&bytes);
    assert!(response.contains("0-1"));
}

#[test]
fn test_xadd_partial_wildcard_increments_seq() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    // Add first entry with explicit ID
    process_xadd(&parts(&["XADD", "mystream", "100-5", "a", "1"]), &kv_store, &waiting_room).unwrap();

    // Add with same ms and wildcard - should increment
    let result = process_xadd(&parts(&["XADD", "mystream", "100-*", "b", "2"]), &kv_store, &waiting_room);
    assert!(result.is_ok());
    let bytes = result.unwrap();
    let response = String::from_utf8_lossy(&bytes);
    assert!(response.contains("100-6"));
}

#[test]
fn test_xadd_partial_wildcard_different_ms() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    process_xadd(&parts(&["XADD", "mystream", "100-5", "a", "1"]), &kv_store, &waiting_room).unwrap();

    // Different ms, should start at 0
    let result = process_xadd(&parts(&["XADD", "mystream", "200-*", "b", "2"]), &kv_store, &waiting_room);
    assert!(result.is_ok());
    let bytes = result.unwrap();
    let response = String::from_utf8_lossy(&bytes);
    assert!(response.contains("200-0"));
}

// ==================== XADD Tests - Wrong Type ====================

#[test]
fn test_xadd_wrong_type() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    // Create a string key
    {
        let mut map = kv_store.lock().unwrap();
        map.insert(
            "mykey".to_string(),
            RedisValue::new(RedisData::String("value".to_string()), None),
        );
    }

    let p = parts(&["XADD", "mykey", "1-1", "field", "value"]);
    let result = process_xadd(&p, &kv_store, &waiting_room);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("WRONGTYPE"));
}

#[test]
fn test_xadd_incomplete_command() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    // Missing field-value pair
    let p = parts(&["XADD", "mystream", "1-1", "field"]);
    let result = process_xadd(&p, &kv_store, &waiting_room);
    assert!(result.is_err());
}

// ==================== XRANGE Tests ====================

#[test]
fn test_xrange_full_range() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    // Populate stream
    process_xadd(&parts(&["XADD", "mystream", "1-0", "a", "1"]), &kv_store, &waiting_room).unwrap();
    process_xadd(&parts(&["XADD", "mystream", "2-0", "b", "2"]), &kv_store, &waiting_room).unwrap();
    process_xadd(&parts(&["XADD", "mystream", "3-0", "c", "3"]), &kv_store, &waiting_room).unwrap();

    let p = parts(&["XRANGE", "mystream", "-", "+"]);
    let result = process_xrange(&p, &kv_store);
    assert!(result.is_ok());
    let response = result.unwrap();
    // Should contain all 3 entries
    assert!(response.starts_with(b"*3"));
}

#[test]
fn test_xrange_partial_range() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    process_xadd(&parts(&["XADD", "mystream", "1-0", "a", "1"]), &kv_store, &waiting_room).unwrap();
    process_xadd(&parts(&["XADD", "mystream", "2-0", "b", "2"]), &kv_store, &waiting_room).unwrap();
    process_xadd(&parts(&["XADD", "mystream", "3-0", "c", "3"]), &kv_store, &waiting_room).unwrap();

    let p = parts(&["XRANGE", "mystream", "2-0", "3-0"]);
    let result = process_xrange(&p, &kv_store);
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(response.starts_with(b"*2"));
}

#[test]
fn test_xrange_single_entry() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    process_xadd(&parts(&["XADD", "mystream", "1-0", "a", "1"]), &kv_store, &waiting_room).unwrap();
    process_xadd(&parts(&["XADD", "mystream", "2-0", "b", "2"]), &kv_store, &waiting_room).unwrap();

    let p = parts(&["XRANGE", "mystream", "1-0", "1-0"]);
    let result = process_xrange(&p, &kv_store);
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(response.starts_with(b"*1"));
}

#[test]
fn test_xrange_no_entries_in_range() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    process_xadd(&parts(&["XADD", "mystream", "1-0", "a", "1"]), &kv_store, &waiting_room).unwrap();

    let p = parts(&["XRANGE", "mystream", "5-0", "10-0"]);
    let result = process_xrange(&p, &kv_store);
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(response.starts_with(b"*0"));
}

#[test]
fn test_xrange_nonexistent_stream() {
    let kv_store = new_kv_store();

    let p = parts(&["XRANGE", "nostream", "-", "+"]);
    let result = process_xrange(&p, &kv_store);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"*0\r\n");
}

#[test]
fn test_xrange_minus_start() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    process_xadd(&parts(&["XADD", "mystream", "5-0", "a", "1"]), &kv_store, &waiting_room).unwrap();

    // "-" means minimum possible ID (0-0)
    let p = parts(&["XRANGE", "mystream", "-", "5-0"]);
    let result = process_xrange(&p, &kv_store);
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(response.starts_with(b"*1"));
}

#[test]
fn test_xrange_plus_end() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    process_xadd(&parts(&["XADD", "mystream", "1-0", "a", "1"]), &kv_store, &waiting_room).unwrap();

    // "+" means maximum possible ID
    let p = parts(&["XRANGE", "mystream", "1-0", "+"]);
    let result = process_xrange(&p, &kv_store);
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(response.starts_with(b"*1"));
}

// ==================== XREAD Tests - Without BLOCK ====================

#[tokio::test]
async fn test_xread_basic() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    process_xadd(&parts(&["XADD", "mystream", "1-0", "a", "1"]), &kv_store, &waiting_room).unwrap();
    process_xadd(&parts(&["XADD", "mystream", "2-0", "b", "2"]), &kv_store, &waiting_room).unwrap();

    let p = parts(&["XREAD", "STREAMS", "mystream", "0-0"]);
    let result = process_xread(&p, &kv_store, &waiting_room).await;
    assert!(result.is_ok());
    let response = result.unwrap();
    // Should return both entries (after 0-0)
    assert!(response.len() > 10);
}

#[tokio::test]
async fn test_xread_from_specific_id() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    process_xadd(&parts(&["XADD", "mystream", "1-0", "a", "1"]), &kv_store, &waiting_room).unwrap();
    process_xadd(&parts(&["XADD", "mystream", "2-0", "b", "2"]), &kv_store, &waiting_room).unwrap();
    process_xadd(&parts(&["XADD", "mystream", "3-0", "c", "3"]), &kv_store, &waiting_room).unwrap();

    // Read entries after 1-0 (should get 2-0 and 3-0)
    let p = parts(&["XREAD", "STREAMS", "mystream", "1-0"]);
    let result = process_xread(&p, &kv_store, &waiting_room).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_xread_no_new_entries() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    process_xadd(&parts(&["XADD", "mystream", "1-0", "a", "1"]), &kv_store, &waiting_room).unwrap();

    // Read after last entry - should return null
    let p = parts(&["XREAD", "STREAMS", "mystream", "1-0"]);
    let result = process_xread(&p, &kv_store, &waiting_room).await;
    assert!(result.is_ok());
    // No entries after 1-0
    assert_eq!(result.unwrap(), b"*-1\r\n");
}

#[tokio::test]
async fn test_xread_nonexistent_stream() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    let p = parts(&["XREAD", "STREAMS", "nostream", "0-0"]);
    let result = process_xread(&p, &kv_store, &waiting_room).await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"*-1\r\n");
}

#[tokio::test]
async fn test_xread_multiple_streams() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    process_xadd(&parts(&["XADD", "stream1", "1-0", "a", "1"]), &kv_store, &waiting_room).unwrap();
    process_xadd(&parts(&["XADD", "stream2", "1-0", "b", "2"]), &kv_store, &waiting_room).unwrap();

    let p = parts(&["XREAD", "STREAMS", "stream1", "stream2", "0-0", "0-0"]);
    let result = process_xread(&p, &kv_store, &waiting_room).await;
    assert!(result.is_ok());
    let response = result.unwrap();
    // Should contain data from both streams
    assert!(response.len() > 20);
}

// ==================== XREAD Tests - With $ (Special ID) ====================

#[tokio::test]
async fn test_xread_dollar_no_block_returns_null() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    // Add some existing data
    process_xadd(&parts(&["XADD", "mystream", "1-0", "a", "1"]), &kv_store, &waiting_room).unwrap();

    // $ means "only new entries after this point" - without BLOCK, should return null
    let p = parts(&["XREAD", "STREAMS", "mystream", "$"]);
    let result = process_xread(&p, &kv_store, &waiting_room).await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"*-1\r\n");
}

#[tokio::test]
async fn test_xread_dollar_on_nonexistent_stream() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    // $ on non-existent stream should effectively be 0-0
    let p = parts(&["XREAD", "STREAMS", "nostream", "$"]);
    let result = process_xread(&p, &kv_store, &waiting_room).await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"*-1\r\n");
}

// ==================== XREAD Tests - With BLOCK ====================

#[tokio::test]
async fn test_xread_block_with_existing_data() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    process_xadd(&parts(&["XADD", "mystream", "1-0", "a", "1"]), &kv_store, &waiting_room).unwrap();

    // BLOCK but data already exists - should return immediately
    let p = parts(&["XREAD", "BLOCK", "1000", "STREAMS", "mystream", "0-0"]);
    let result = process_xread(&p, &kv_store, &waiting_room).await;
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(response.len() > 10);
}

#[tokio::test]
async fn test_xread_block_timeout() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    // Short timeout, no data
    let p = parts(&["XREAD", "BLOCK", "100", "STREAMS", "mystream", "0-0"]);
    let start = std::time::Instant::now();
    let result = process_xread(&p, &kv_store, &waiting_room).await;
    let elapsed = start.elapsed();

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"*-1\r\n");
    // Should have waited approximately 100ms
    assert!(elapsed.as_millis() >= 90);
}

#[tokio::test]
async fn test_xread_block_wakeup_on_xadd() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    let kv_clone = Arc::clone(&kv_store);
    let room_clone = Arc::clone(&waiting_room);

    // Start blocking read
    let xread_handle = tokio::spawn(async move {
        let p = parts(&["XREAD", "BLOCK", "5000", "STREAMS", "mystream", "0-0"]);
        process_xread(&p, &kv_clone, &room_clone).await
    });

    // Give XREAD time to block
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Add data - should wake up the blocked XREAD
    process_xadd(&parts(&["XADD", "mystream", "1-0", "wakeup", "data"]), &kv_store, &waiting_room).unwrap();

    let result = xread_handle.await.unwrap();
    assert!(result.is_ok());
    let response = result.unwrap();
    // Should have received the new entry
    assert!(response.len() > 10);
    assert!(!response.starts_with(b"*-1"));
}

#[tokio::test]
async fn test_xread_block_zero_indefinite_wakeup() {
    // BLOCK 0 = indefinite wait
    // Using a test-level timeout to prevent test suite from hanging if wakeup fails
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    let kv_clone = Arc::clone(&kv_store);
    let room_clone = Arc::clone(&waiting_room);

    let xread_handle = tokio::spawn(async move {
        let p = parts(&["XREAD", "BLOCK", "0", "STREAMS", "mystream", "$"]);
        process_xread(&p, &kv_clone, &room_clone).await
    });

    // Give XREAD time to block
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Add data - should wake up
    process_xadd(&parts(&["XADD", "mystream", "1-0", "indefinite", "wakeup"]), &kv_store, &waiting_room).unwrap();

    // Use a test-level timeout to prevent infinite hang
    let result = tokio::time::timeout(
        tokio::time::Duration::from_secs(5),
        xread_handle
    ).await;

    assert!(result.is_ok(), "XREAD BLOCK 0 should have been woken up by XADD");
    let inner_result = result.unwrap().unwrap();
    assert!(inner_result.is_ok());
    let response = inner_result.unwrap();
    assert!(response.len() > 10);
}

#[tokio::test]
async fn test_xread_block_dollar_with_new_data() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    // Pre-populate stream
    process_xadd(&parts(&["XADD", "mystream", "1-0", "old", "data"]), &kv_store, &waiting_room).unwrap();

    let kv_clone = Arc::clone(&kv_store);
    let room_clone = Arc::clone(&waiting_room);

    // BLOCK with $ - should only see new entries after this point
    let xread_handle = tokio::spawn(async move {
        let p = parts(&["XREAD", "BLOCK", "5000", "STREAMS", "mystream", "$"]);
        process_xread(&p, &kv_clone, &room_clone).await
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Add new data
    process_xadd(&parts(&["XADD", "mystream", "2-0", "new", "data"]), &kv_store, &waiting_room).unwrap();

    let result = xread_handle.await.unwrap();
    assert!(result.is_ok());
    let bytes = result.unwrap();
    let response = String::from_utf8_lossy(&bytes);
    // Should contain the new entry
    assert!(response.contains("2-0") || response.contains("new"));
}

// ==================== XREAD Tests - Multiple Blocked Readers ====================

#[tokio::test]
async fn test_xread_multiple_blocked_readers() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();
    let num_readers = 3;

    let mut handles = vec![];
    for _ in 0..num_readers {
        let kv_clone = Arc::clone(&kv_store);
        let room_clone = Arc::clone(&waiting_room);
        let handle = tokio::spawn(async move {
            let p = parts(&["XREAD", "BLOCK", "5000", "STREAMS", "mystream", "0-0"]);
            process_xread(&p, &kv_clone, &room_clone).await
        });
        handles.push(handle);
    }

    // Give readers time to block
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Single XADD should wake all readers
    process_xadd(&parts(&["XADD", "mystream", "1-0", "broadcast", "data"]), &kv_store, &waiting_room).unwrap();

    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok());
        let response = result.unwrap();
        assert!(response.len() > 10);
    }
}

// ==================== Concurrent Stream Tests ====================

#[tokio::test]
async fn test_concurrent_xadd() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();
    let num_writers = 5;
    let writes_per_writer = 20;

    let mut handles = vec![];
    for writer_id in 0..num_writers {
        let store = Arc::clone(&kv_store);
        let room = Arc::clone(&waiting_room);
        let handle = tokio::spawn(async move {
            for i in 0..writes_per_writer {
                // Use unique IDs based on writer_id and iteration
                let ms = (writer_id * 1000 + i) as u64;
                let p = vec![
                    "XADD".to_string(),
                    "sharedstream".to_string(),
                    format!("{}-0", ms),
                    "writer".to_string(),
                    format!("{}", writer_id),
                ];
                let result = process_xadd(&p, &store, &room);
                // Some may fail due to ID conflicts, that's expected
                let _ = result;
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let map = kv_store.lock().unwrap();
    let stream = map.get("sharedstream").unwrap();
    match &stream.data {
        RedisData::Stream(entries) => {
            // Should have some entries (exact count depends on ordering)
            assert!(entries.len() > 0);
        }
        _ => panic!("Expected stream"),
    }
}

#[tokio::test]
async fn test_xread_while_xadd() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    // Start reader
    let kv_clone = Arc::clone(&kv_store);
    let room_clone = Arc::clone(&waiting_room);
    let reader_handle = tokio::spawn(async move {
        let p = parts(&["XREAD", "BLOCK", "2000", "STREAMS", "mystream", "$"]);
        process_xread(&p, &kv_clone, &room_clone).await
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Rapid writes
    for i in 0..10 {
        let p = vec![
            "XADD".to_string(),
            "mystream".to_string(),
            format!("{}-0", i + 1),
            "count".to_string(),
            format!("{}", i),
        ];
        process_xadd(&p, &kv_store, &waiting_room).unwrap();
    }

    let result = reader_handle.await.unwrap();
    assert!(result.is_ok());
    // Reader should have woken up
    let response = result.unwrap();
    assert!(response.len() > 10);
}

// ==================== Edge Cases ====================

#[test]
fn test_xadd_large_ms_value() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    let p = parts(&["XADD", "mystream", "9999999999999-0", "field", "value"]);
    let result = process_xadd(&p, &kv_store, &waiting_room);
    assert!(result.is_ok());
}

#[test]
fn test_xadd_large_seq_value() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    let p = parts(&["XADD", "mystream", "1-9999999999", "field", "value"]);
    let result = process_xadd(&p, &kv_store, &waiting_room);
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_xread_various_block_values() {
    let kv_store = new_kv_store();
    let waiting_room = new_waiting_room();

    // Pre-populate
    process_xadd(&parts(&["XADD", "mystream", "1-0", "a", "1"]), &kv_store, &waiting_room).unwrap();

    // Test various block values
    for block_ms in [1, 10, 50, 100] {
        let p = vec![
            "XREAD".to_string(),
            "BLOCK".to_string(),
            format!("{}", block_ms),
            "STREAMS".to_string(),
            "mystream".to_string(),
            "0-0".to_string(),
        ];
        let result = process_xread(&p, &kv_store, &waiting_room).await;
        assert!(result.is_ok());
    }
}

#[test]
fn test_xrange_empty_stream() {
    let kv_store = new_kv_store();

    // Create empty stream
    {
        let mut map = kv_store.lock().unwrap();
        map.insert(
            "emptystream".to_string(),
            RedisValue::new(RedisData::Stream(vec![]), None),
        );
    }

    let p = parts(&["XRANGE", "emptystream", "-", "+"]);
    let result = process_xrange(&p, &kv_store);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), b"*0\r\n");
}
