use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::time::Instant;

use crate::models::{RedisData, RedisValue, RespResult};
use crate::utils::encoder::*;

pub fn process_ping() -> RespResult {
    Ok(encode_simple_string("PONG"))
}

pub fn process_echo(parts: &[String]) -> RespResult {
    // parts[0] = "ECHO", parts[1] = message
    if parts.len() < 2 {
        return Err("Error, ECHO requires a message".to_string());
    }
    Ok(encode_bulk_string(&parts[1]))
}

pub fn process_type(
    parts: &[String],
    kv_store: &Arc<Mutex<HashMap<String, RedisValue>>>
) -> RespResult {
    // parts[0] = "TYPE", parts[1] = key
    if parts.len() < 2 {
        return Err("Malformed TYPE".to_string());
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
        None => return Ok(encode_simple_string("none")),
    };

    if is_expired {
        map.remove(key);
        Ok(encode_simple_string("none"))
    } else {
        let val = map.get(key).unwrap();
        match &val.data {
            RedisData::String(_) => Ok(encode_simple_string("string")),
            RedisData::List(_) => Ok(encode_simple_string("list")),
            RedisData::Stream(_) => Ok(encode_simple_string("stream")),
        }
    }
}
