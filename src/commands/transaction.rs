use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use crate::utils::encoder::*;
use crate::models::*;

pub fn process_incr(
    parts: &[String],
    kv_store: &Arc<Mutex<HashMap<String, RedisValue>>>
) -> RespResult {
    if parts.len() < 2 {
        return Err("Incomplete INCR command".to_string());
    }

    let key = &parts[1];
    let mut map = kv_store.lock().unwrap();
    let entry = map.get_mut(key.as_str());

    match entry {
        Some(value) => {
            match &mut value.data {
                RedisData::String(item) => {
                    if let Ok(num) = item.parse::<i64>() {
                        let new_num = num + 1;
                        *item = new_num.to_string(); 
                        Ok(encode_integer(new_num))
                    } else {
                        Ok(b"-ERR value is not an integer or out of range\r\n".to_vec())
                    }
                },
                _ => Ok(b"-WRONGTYPE Operation against a key not holding a string\r\n".to_vec()),
            }
        },
        None => {
            map.insert(key.clone(), RedisValue::new(RedisData::String("1".to_string()), None));
            Ok(encode_integer(1))
        },
    }
}

pub fn process_multi(
    command_queue: &mut Option<VecDeque<Vec<String>>>
) -> RespResult {
    if command_queue.is_some() {
        return Ok(b"-ERR MULTI calls can not be nested\r\n".to_vec());
    }
    *command_queue = Some(VecDeque::new());
    Ok(encode_simple_string("OK"))
}

pub fn process_exec(
    command_queue: &mut Option<VecDeque<Vec<String>>>
) -> RespResult {
    if command_queue.is_none() {
        // todo: have encode_error_string
        return Ok(b"-ERR EXEC without MULTI\r\n".to_vec());
    }
    return Ok(encode_simple_string("Hello"));
}

pub fn handle_push_command_queue(
    parts: &[String],
    command_queue: &mut VecDeque<Vec<String>>
) -> RespResult {
    command_queue.push_back(parts.to_vec());
    Ok(encode_simple_string("QUEUED"))
}