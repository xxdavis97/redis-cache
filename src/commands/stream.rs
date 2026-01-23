use std::sync::{Arc, Mutex};
use std::collections::{VecDeque, HashMap};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

use crate::models::{RedisData, RedisValue, StreamEntry};
use crate::utils::async_helpers::*;
use crate::utils::encoder::*;

pub fn process_xadd(
    parts: &[String],
    kv_store: &Arc<Mutex<HashMap<String, RedisValue>>>,
    waiting_room: &Arc<Mutex<HashMap<String, VecDeque<mpsc::Sender<String>>>>>
) -> RespResult {
    // parts[0] = "XADD", parts[1] = key, parts[2] = entry_id, parts[3..] = field value pairs
    if parts.len() < 5 {
        return Err("Malformed XADD".to_string());
    }
    let key = parts[1].clone();
    let entity_id = parts[2].clone();

    // Collect field-value pairs (no more step_by needed!)
    let map_elements: HashMap<String, String> = parts[3..]
        .chunks_exact(2)
        .map(|chunk| (chunk[0].clone(), chunk[1].clone()))
        .collect();

    let stream_entry = StreamEntry { id: entity_id.clone(), fields: map_elements };

    let mut map = kv_store.lock().unwrap();

    let entry = map.entry(key.clone()).or_insert(RedisValue::new(
        RedisData::Stream(Vec::new()),
        None
    ));

    match &mut entry.data {
        RedisData::Stream(stream) => {
            let (initial_ms, initial_seq) = parse_entity_id(&entity_id);

            // Handle sequence auto-generation if the ID was "1234-*"
            let (new_ms, new_seq) = if parts[2].ends_with("-*") {
                if let Some(last_entry) = stream.last() {
                    let (last_ms, last_seq) = parse_entity_id(&last_entry.id);

                    if initial_ms == last_ms {
                        (initial_ms, last_seq + 1)
                    } else if initial_ms == 0 {
                        (initial_ms, 1)
                    } else {
                        (initial_ms, 0)
                    }
                } else {
                    let seq = if initial_ms == 0 { 1 } else { 0 };
                    (initial_ms, seq)
                }
            } else {
                (initial_ms, initial_seq)
            };

            if new_ms == 0 && new_seq == 0 {
                return Ok("-ERR The ID specified in XADD must be greater than 0-0\r\n".as_bytes().to_vec());
            }

            let resolved_id = format!("{}-{}", new_ms, new_seq);
            println!("{} RESOLVED ID", resolved_id);

            let is_valid = valid_entity_id(stream, &resolved_id);
            match is_valid {
                true => {
                    let mut room = waiting_room.lock().unwrap();
                    let mut finalized_entry = stream_entry;
                    finalized_entry.id = resolved_id.clone();
                    stream.push(finalized_entry);

                    if let Some(queue) = room.get_mut(&key) {
                        while let Some(tx) = queue.pop_front() {
                            // Send the ID to wake up the XREAD thread
                            if tx.try_send(resolved_id.clone()).is_ok() {
                                println!("DEBUG: XADD successfully notified a waiter");
                                // In Redis, XREAD BLOCK usually wakes up ALL waiters, 
                                // but BLPOP only wakes up one. For XREAD, empty full queue
                            } else {
                                println!("DEBUG: Found a dead waiter, moving to next");
                            }
                        }
                    }
                    Ok(encode_bulk_string(&resolved_id))
                },
                false => Ok("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n".as_bytes().to_vec())
            }
        },
        _ => Err("WRONGTYPE Operation against a key that is not a stream".to_string())
    }
}

pub async fn process_xread(
    parts: &[String],
    kv_store: &Arc<Mutex<HashMap<String, RedisValue>>>,
    waiting_room: &Arc<Mutex<HashMap<String, VecDeque<mpsc::Sender<String>>>>>
) -> RespResult {
    // parts[0] = "XREAD", optionally [BLOCK ms], then "STREAMS", then keys..., then ids...
    if parts.len() < 4 {
        return Err("Malformed XREAD".to_string());
    }

    // Find STREAMS keyword position
    let streams_idx = parts.iter()
        .position(|r| r.to_uppercase() == "STREAMS")
        .ok_or_else(|| "Missing STREAMS keyword".to_string())?;

    // Check for BLOCK option
    let block_ms: Option<f64> = parts.iter()
        .position(|r| r.to_uppercase() == "BLOCK")
        .and_then(|idx| parts.get(idx + 1))
        .and_then(|v| v.parse().ok());

    let remaining = &parts[streams_idx + 1..];
    let num_streams = remaining.len() / 2;
    let keys = &remaining[..num_streams];
    let ids = &remaining[num_streams..];

    // handle dollar sign inputs
    let effective_ids = get_effective_ids_for_xread(&keys, &ids, &kv_store);

    // Try to read stream immediately 
    let mut result = perform_xread(&keys, &effective_ids, &kv_store);

    if !result.is_empty() {
        return Ok(encode_raw_array(result));
    }

    if let Some(timeout_val) = block_ms {
        let (_tx, mut rx) = init_waiting_room(&keys, &waiting_room);
        if timeout_val > 0.0 {
            let duration = tokio::time::Duration::from_millis(timeout_val as u64);
            let _ = tokio::time::timeout(duration, rx.recv()).await;
        } else {
            rx.recv().await;
        }
        // Wake up and try to read again (Second pass)
        result = perform_xread(&keys, &effective_ids, &kv_store);
    }

    if result.is_empty() {
        Ok(encode_null_array())
    } else {
        Ok(encode_raw_array(result))
    }
}

fn get_effective_ids_for_xread(
    keys: &[String],
    ids: &[String],
    kv_store: &Arc<Mutex<HashMap<String, RedisValue>>>
) -> Vec<String> {
    let mut effective_ids = ids.to_vec();
    // scope the map lock
    {
        let map = kv_store.lock().unwrap();
        for i in 0..keys.len() {
            if ids[i] == "$" {
                if let Some(RedisValue { data: RedisData::Stream(stream), .. }) = map.get(&keys[i]) {
                    // If the stream exists, $ becomes the last ID currently in it
                    if let Some(last_entry) = stream.last() {
                        effective_ids[i] = last_entry.id.clone();
                    } else {
                        effective_ids[i] = "0-0".to_string();
                    }
                } else {
                    // If key doesn't exist, $ is effectively 0-0
                    effective_ids[i] = "0-0".to_string();
                }
            }
        }
    }
    effective_ids
}

fn perform_xread(
    keys: &[String], 
    ids: &[String], 
    kv_store: &Arc<Mutex<HashMap<String, RedisValue>>>
) -> Vec<Vec<u8>> {
    let map = kv_store.lock().unwrap();
    let mut result = Vec::new();

    for i in 0..keys.len() {
        let key = &keys[i];
        let filter_id = parse_entity_id(&ids[i]);

        if let Some(RedisValue { data: RedisData::Stream(stream), .. }) = map.get(key.as_str()) {
            let mut results_for_stream: Vec<Vec<u8>> = Vec::new();
            for entry in stream {
                let entity_id_in_stream = parse_entity_id(&entry.id);
                if entity_id_in_stream > filter_id {
                    results_for_stream.push(encode_stream_entry(&entry));
                }
            }
            if !results_for_stream.is_empty() {
                let stream_result = vec![
                    encode_bulk_string(key),
                    encode_raw_array(results_for_stream)
                ];
                result.push(encode_raw_array(stream_result));
            }
        }
    }
    result
}

pub fn process_xrange(
    parts: &[String],
    kv_store: &Arc<Mutex<HashMap<String, RedisValue>>>
) -> RespResult {
    // parts[0] = "XRANGE", parts[1] = key, parts[2] = start, parts[3] = end
    if parts.len() < 4 {
        return Err("Malformed XRANGE".to_string());
    }
    let key = &parts[1];
    let start_raw = &parts[2];
    let end_raw = &parts[3];

    let start_bound = if start_raw == "-" {
        (0, 0)
    } else {
        parse_entity_id(start_raw)
    };

    let end_bound = if end_raw == "+" {
        (u64::MAX, u64::MAX)
    } else {
        let id_parts: Vec<&str> = end_raw.split('-').collect();
        let ms = id_parts[0].parse::<u64>().unwrap_or(u64::MAX);
        let seq = if id_parts.len() > 1 {
            id_parts[1].parse::<u64>().unwrap_or(u64::MAX)
        } else {
            u64::MAX
        };
        (ms, seq)
    };

    let map = kv_store.lock().unwrap();
    match map.get(key) {
        Some(entry) => match &entry.data {
            RedisData::Stream(stream) => {
                let mut entries_resp = Vec::new();

                for entry in stream {
                    let entry_id = parse_entity_id(&entry.id);
                    if entry_id >= start_bound && entry_id <= end_bound {
                        entries_resp.push(encode_stream_entry(&entry))
                    }
                }
                Ok(encode_raw_array(entries_resp))
            },
            _ => Err("WRONGTYPE ...".to_string()),
        },
        None => Ok(encode_array(&[])),
    }
}

fn valid_entity_id(stream: &Vec<StreamEntry>, entity_id: &str) -> bool {
    let (last_ms, last_seq): (u64, u64) = if let Some(last_entry) = stream.last() {
        parse_entity_id(&last_entry.id)
    } else {
        (0, 0)
    };

    let (new_ms, new_seq) = parse_entity_id(entity_id);
    if (new_ms < last_ms) || (new_ms == last_ms && new_seq <= last_seq) {
        return false;
    }
    true
}

fn parse_entity_id(entity_id: &str) -> (u64, u64) {
    let parts: Vec<&str> = entity_id.split('-').collect();
    let ms = if parts[0] == "*" {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64
    } else {
        parts[0].parse::<u64>().unwrap_or(0)
    };

    let seq = if parts.len() > 1 {
        if parts[1] == "*" {
            0 // Placeholder: actual auto-seq logic should happen in parent
        } else {
            parts[1].parse::<u64>().unwrap_or(0)
        }
    } else {
        0
    };
    (ms, seq)
}
