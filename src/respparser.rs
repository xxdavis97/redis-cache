use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::time::{Instant, Duration};

use crate::models::{ListPush, RedisData, RedisValue};

type RespResult = Result<Vec<u8>, String>;

pub fn parse_resp(buffer: &mut [u8], bytes_read: usize, kv_store: &Arc<Mutex<HashMap<String, RedisValue>>>) -> Vec<u8> {
    let data =  String::from_utf8_lossy(&buffer[..bytes_read]);
    let parts: Vec<&str> = data.lines().collect();

    let result = match parts[2].to_uppercase().as_str() {
        "PING" => process_ping(),
        "ECHO" => process_echo(&parts),
        "SET" => process_set(&parts, &kv_store),
        "GET" => process_get(&parts, &kv_store),
        "RPUSH" => process_push(&parts, &kv_store, ListPush::RPUSH),
        "LRANGE" => process_lrange(&parts, &kv_store),
        "LPUSH" => process_push(&parts, &kv_store, ListPush::LPUSH),
        _ => Err("Not supported".to_string()),
    };
    match result {
        Ok(bytes) => bytes,
        Err(e) => {
            eprintln!("Command Error: {}", e);
            vec![] 
        }
    }
}

fn process_ping() -> RespResult {
    Ok(encode_simple_string("PONG"))
}

fn process_echo(parts: &Vec<&str>) -> RespResult {
    match parts.len() {
        5 => Ok(encode_bulk_string(parts[4])),
        _ => Err("Error, echo command must be of length 5".to_string())
    }   
}

fn process_set(parts: &Vec<&str>, kv_store: &Arc<Mutex<HashMap<String, RedisValue>>>) -> RespResult {
    if parts.len() < 7 {
        return Err("Incomplete SET command".to_string());
    }

    let key = parts[4].to_string();
    let value = parts[6].to_string();
    let mut expires_at = None;

    // 1. Prepare the Expiry (if it exists)
    if parts.len() >= 11 {
        let time_val = parts[10].parse::<u64>().unwrap_or(0);
        match parts[8].to_uppercase().as_str() {
            "EX" => expires_at = Some(Instant::now() + std::time::Duration::from_secs(time_val)),
            "PX" => expires_at = Some(Instant::now() + std::time::Duration::from_millis(time_val)),
            _ => return Err("Invalid expiry flag".to_string()),
        }
    }

    // 2. Commit the change once
    let mut map = kv_store.lock().unwrap();
    map.insert(key, RedisValue::new(RedisData::String(value), expires_at));
    
    Ok(encode_simple_string("OK"))
}

fn process_get(parts: &Vec<&str>, kv_store: &Arc<Mutex<HashMap<String, RedisValue>>>) -> RespResult {
    if parts.len() < 5 {
        return Err("Malformed GET".to_string());
    }
    let key = parts[4].to_string();
    let mut map = kv_store.lock().unwrap();

    // Have to do get twice, we can't remove from map in rust while get reference
    // is still alive so check expiry first then get again if not expired 
    // Check key exists and if expired 
    let is_expired = match map.get(&key) {
        Some(redis_value) => {
            match redis_value.expires_at {
                Some(expiry) => Instant::now() > expiry,
                None => false
            }
        },
        None => return Ok(encode_null_string()),
    };
    if is_expired {
        map.remove(&key);
        Ok(encode_null_string())
    } else {
        let val = map.get(&key).unwrap();
        match &val.data {
            RedisData::String(s) => Ok(encode_bulk_string(s)),
            _ => Err("WRONGTYPE Operation against a key not holding a string".to_string()),
        }
    }
}

fn process_push(parts: &Vec<&str>, kv_store: &Arc<Mutex<HashMap<String, RedisValue>>>, push_type: ListPush) -> RespResult {
    if parts.len() < 7 {
        return Err("Incomplete RPUSH command".to_string());
    }
    let key = parts[4].to_string();
    let mut map = kv_store.lock().unwrap();

    let new_elements: Vec<String> = parts[6..] // RPUSH can take multiple values
        .iter()
        .step_by(2) // Skip the RESP length lines
        .map(|s| s.to_string())
        .collect();

    // Get existing list from map or initialize to empty
    let entry = map.entry(key).or_insert(RedisValue::new(
        RedisData::List(Vec::new()), 
        None
    ));

    match &mut entry.data {
        RedisData::List(list) => {
            match push_type {
                ListPush::LPUSH => { list.splice(0..0, new_elements.into_iter().rev()); },
                ListPush::RPUSH => { list.extend(new_elements); },
            };
            // list.extend(new_elements);
            Ok(encode_integer(list.len())) 
        },
        _ => Err("WRONGTYPE Operation against a key that is not a list".to_string())
    }
}

fn process_lrange(parts: &Vec<&str>, kv_store: &Arc<Mutex<HashMap<String, RedisValue>>>) -> RespResult {
    if parts.len() < 9 {
        return Err("Incomplete LRANGE command".to_string());
    }
    let key = parts[4].to_string();
    // Mutable to allow for negative indices
    let mut start: i64 = parts[6].parse().map_err(|_| "Invalid start index")?;
    let mut end: i64 = parts[8].parse().map_err(|_| "Invalid end index")?;

    let map = kv_store.lock().unwrap();
    match map.get(&key) {
        Some(value) => {
            match &value.data {
                RedisData::List(list) => {
                    // Allow for negative indices
                    if start < 0 {
                        start = list.len() as i64 + start;
                    }
                    if end < 0 {
                        end = list.len() as i64 + end;
                    }
                    let start_idx = start.max(0) as usize;
                    let mut end_idx = end.max(0) as usize;

                    if start_idx >= list.len() {
                        return Ok(encode_array(&[]));
                    }
                    end_idx = (end_idx + 1).min(list.len());
                    if start_idx >= end_idx {
                        return Ok(encode_array(&[]));
                    }
                    Ok(encode_array(&list[start_idx..end_idx]))
                },
                _ => Err("WRONGTYPE Operation against a key holding a string".to_string()),
            }
        },
        None => Ok(encode_array(&[]))
    }
}

fn encode_simple_string(s: &str) -> Vec<u8> {
    format!("+{}\r\n", s).into_bytes()
}

fn encode_bulk_string(s: &str) -> Vec<u8> {
    format!("${}\r\n{}\r\n", s.len(), s).into_bytes()
}

fn encode_null_string() -> Vec<u8> {
    "$-1\r\n".as_bytes().to_vec()
}

fn encode_integer(n: usize) -> Vec<u8> {
    format!(":{}\r\n", n).into_bytes()
}

fn encode_array(arr: &[String]) -> Vec<u8> {
    let mut bytes = format!("*{}\r\n", arr.len()).into_bytes();
    bytes.extend(arr.iter().flat_map(|s| encode_bulk_string(s)));
    bytes
}