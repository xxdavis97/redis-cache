use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::time::Instant;

use crate::models::{RedisData, RedisValue};
use crate::utils::encoder::*;

pub fn process_set(
    parts: &[String],
    kv_store: &Arc<Mutex<HashMap<String, RedisValue>>>
) -> RespResult {
    // parts[0] = "SET", parts[1] = key, parts[2] = value, [parts[3] = EX/PX, parts[4] = time]
    if parts.len() < 3 {
        return Err("Incomplete SET command".to_string());
    }

    let key = parts[1].clone();
    let value = parts[2].clone();
    let mut expires_at = None;

    // Handle expiry if present: SET key value EX 10 or SET key value PX 1000
    if parts.len() >= 5 {
        let time_val = parts[4].parse::<u64>().unwrap_or(0);
        match parts[3].to_uppercase().as_str() {
            "EX" => expires_at = Some(Instant::now() + std::time::Duration::from_secs(time_val)),
            "PX" => expires_at = Some(Instant::now() + std::time::Duration::from_millis(time_val)),
            _ => return Err("Invalid expiry flag".to_string()),
        }
    }

    let mut map = kv_store.lock().unwrap();
    map.insert(key, RedisValue::new(RedisData::String(value), expires_at));

    Ok(encode_simple_string("OK"))
}

pub fn process_get(
    parts: &[String],
    kv_store: &Arc<Mutex<HashMap<String, RedisValue>>>
) -> RespResult {
    // parts[0] = "GET", parts[1] = key
    if parts.len() < 2 {
        return Err("Malformed GET".to_string());
    }
    let key = &parts[1];
    let mut map = kv_store.lock().unwrap();

    let is_expired = match map.get(key) {
        Some(redis_value) => {
            match redis_value.expires_at {
                Some(expiry) => Instant::now() > expiry,
                None => false
            }
        },
        None => return Ok(encode_null_string()),
    };

    if is_expired {
        map.remove(key);
        Ok(encode_null_string())
    } else {
        let val = map.get(key).unwrap();
        match &val.data {
            RedisData::String(s) => Ok(encode_bulk_string(s)),
            _ => Err("WRONGTYPE Operation against a key not holding a string".to_string()),
        }
    }
}
