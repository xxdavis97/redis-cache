use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use async_recursion::async_recursion;
use crate::utils::encoder::*;
use crate::models::*;
use crate::executor::*;

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
                        Ok(encode_error_string("ERR value is not an integer or out of range"))
                    }
                },
                _ => Ok(encode_error_string("WRONGTYPE Operation against a key not holding a string")),
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
        return Ok(encode_error_string("ERR MULTI calls can not be nested"));
    }
    *command_queue = Some(VecDeque::new());
    Ok(encode_simple_string("OK"))
}

#[async_recursion]
pub async fn process_exec(
    command_queue: &mut Option<VecDeque<Vec<String>>>,
    kv_store: &Arc<Mutex<HashMap<String, RedisValue>>>,
    waiting_room: &Arc<Mutex<HashMap<String, VecDeque<mpsc::Sender<String>>>>>,
) -> RespResult {
    let queue = match command_queue.take() {
        Some(q) => q,
        None => return Ok(encode_error_string("ERR EXEC without MULTI")),
    };
    if queue.is_empty() {
        return Ok(encode_array(&vec![]));
    }
    let mut responses: Vec<Vec<u8>> = Vec::new();
    for parts in queue {
        let command_result = execute_commands(
            parts[0].to_uppercase(), 
            &parts, 
            kv_store, 
            waiting_room, 
            &mut None // MULTI/EXEC can't be nested so null command queue
        ).await;
        responses.push(command_result);
    }
    Ok(encode_raw_array(responses))
}

pub fn process_discard(
    command_queue: &mut Option<VecDeque<Vec<String>>>,
) -> RespResult {
    match command_queue.take() {
        Some(_) => Ok(encode_simple_string("OK")),
        None => Ok(encode_error_string("ERR DISCARD without MULTI"))
    }
}

pub fn handle_push_command_queue(
    parts: &[String],
    command_queue: &mut VecDeque<Vec<String>>
) -> RespResult {
    command_queue.push_back(parts.to_vec());
    Ok(encode_simple_string("QUEUED"))
}

