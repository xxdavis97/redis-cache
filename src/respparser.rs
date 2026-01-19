use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::time::{Instant, Duration};

use crate::models::RedisValue;

type RespResult = Result<Vec<u8>, String>;

pub fn parse_resp(buffer: &mut [u8], bytes_read: usize, kv_store: &Arc<Mutex<HashMap<String, RedisValue>>>) -> Vec<u8> {
    let data =  String::from_utf8_lossy(&buffer[..bytes_read]);
    let parts: Vec<&str> = data.lines().collect();

    let result = match parts[2].to_uppercase().as_str() {
        "PING" => process_ping(),
        "ECHO" => process_echo(&parts),
        "SET" => process_set(&parts, &kv_store),
        "GET" => process_get(&parts, &kv_store),
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
    map.insert(key, RedisValue::new(value, expires_at));
    
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
        Ok(encode_bulk_string(&val.data))
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