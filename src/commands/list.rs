use std::sync::{Arc, Mutex};
use std::collections::{VecDeque, HashMap};
use tokio::sync::mpsc;

use crate::models::{ListDir, RedisData, RedisValue};
use crate::utils::async_helpers::*;
use crate::utils::encoder::*;

pub fn process_push(
    parts: &[String],
    kv_store: &Arc<Mutex<HashMap<String, RedisValue>>>,
    waiting_room: &Arc<Mutex<HashMap<String, VecDeque<mpsc::Sender<String>>>>>,
    push_type: ListDir
) -> RespResult {
    // parts[0] = "RPUSH"/"LPUSH", parts[1] = key, parts[2..] = values
    if parts.len() < 3 {
        return Err("Incomplete RPUSH/LPUSH command".to_string());
    }
    let key = parts[1].clone();
    let mut map = kv_store.lock().unwrap();

    // Collect all values to push
    let new_elements: Vec<String> = parts[2..].to_vec();

    let entry = map.entry(key.clone()).or_insert(RedisValue::new(
        RedisData::List(Vec::new()),
        None
    ));

    match &mut entry.data {
        RedisData::List(list) => {
            let mut room = waiting_room.lock().unwrap();
            let total_new_elements = new_elements.len();
            let mut remaining_elements = new_elements.into_iter();

            if let Some(queue) = room.get_mut(&key) {
                println!("DEBUG: PUSH found {} waiters for {}", queue.len(), key);
                while let Some(tx) = queue.front() {
                    let Some(next_val) = remaining_elements.next() else {
                        println!("DEBUG: PUSH ran out of elements for waiters");
                        break;
                    };
                    if tx.try_send(next_val).is_ok() {
                        println!("DEBUG: PUSH successfully handed off element");
                        queue.pop_front();
                    } else {
                        println!("DEBUG: PUSH found a dead waiter (receiver dropped)");
                        queue.pop_front();
                    }
                }
            } else {
                println!("DEBUG: PUSH found NO waiters in room for {}", key);
            }

            let leftovers: Vec<String> = remaining_elements.collect();
            let leftovers_count = leftovers.len();
            if !leftovers.is_empty() {
                match push_type {
                    ListDir::L => { list.splice(0..0, leftovers.into_iter().rev()); },
                    ListDir::R => { list.extend(leftovers); },
                };
            }

            let final_len = list.len() + (total_new_elements - leftovers_count);
            Ok(encode_integer(final_len as i64))
        },
        _ => Err("WRONGTYPE Operation against a key that is not a list".to_string())
    }
}

pub fn process_lrange(
    parts: &[String],
    kv_store: &Arc<Mutex<HashMap<String, RedisValue>>>
) -> RespResult {
    // parts[0] = "LRANGE", parts[1] = key, parts[2] = start, parts[3] = end
    if parts.len() < 4 {
        return Err("Incomplete LRANGE command".to_string());
    }
    let key = &parts[1];
    let mut start: i64 = parts[2].parse().map_err(|_| "Invalid start index")?;
    let mut end: i64 = parts[3].parse().map_err(|_| "Invalid end index")?;

    let map = kv_store.lock().unwrap();
    match map.get(key) {
        Some(value) => {
            match &value.data {
                RedisData::List(list) => {
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
                _ => Err("WRONGTYPE Operation against a key not holding a list".to_string()),
            }
        },
        None => Ok(encode_array(&[]))
    }
}

pub fn process_llen(
    parts: &[String],
    kv_store: &Arc<Mutex<HashMap<String, RedisValue>>>
) -> RespResult {
    // parts[0] = "LLEN", parts[1] = key
    if parts.len() < 2 {
        return Err("Incomplete LLEN command".to_string());
    }
    let key = &parts[1];
    let map = kv_store.lock().unwrap();
    match map.get(key) {
        Some(value) => {
            match &value.data {
                RedisData::List(list) => Ok(encode_integer(list.len() as i64)),
                _ => Err("WRONGTYPE Operation against a key not holding a list".to_string()),
            }
        },
        None => Ok(encode_integer(0))
    }
}

pub fn process_pop(
    parts: &[String],
    kv_store: &Arc<Mutex<HashMap<String, RedisValue>>>,
    push_type: ListDir
) -> RespResult {
    // parts[0] = "LPOP"/"RPOP", parts[1] = key, [parts[2] = count]
    if parts.len() < 2 {
        return Err("Incomplete RPOP/LPOP command".to_string());
    }

    let mut delete_amt: i64 = 1;
    if parts.len() >= 3 {
        delete_amt = parts[2].parse().unwrap_or(1);
    }

    let key = &parts[1];
    let mut map = kv_store.lock().unwrap();
    let mut should_remove = false;

    let response = match map.get_mut(key) {
        Some(value) => {
            match &mut value.data {
                RedisData::List(list) => {
                    if list.is_empty() {
                        Ok(encode_null_string())
                    } else {
                        let mut dropped_items = vec![];
                        while delete_amt > 0 && !list.is_empty() {
                            let dropped_item = match push_type {
                                ListDir::L => list.remove(0),
                                ListDir::R => list.pop().unwrap()
                            };
                            dropped_items.push(dropped_item);
                            delete_amt -= 1;
                        }

                        if list.is_empty() {
                            should_remove = true;
                        }
                        if dropped_items.len() > 1 {
                            Ok(encode_array(&dropped_items))
                        } else {
                            Ok(encode_bulk_string(&dropped_items[0]))
                        }
                    }
                },
                _ => Err("WRONGTYPE Operation against a key not holding a list".to_string()),
            }
        },
        None => Ok(encode_null_string())
    };

    if should_remove {
        map.remove(key);
    }
    response
}

pub async fn process_blpop(
    parts: &[String],
    kv_store: &Arc<Mutex<HashMap<String, RedisValue>>>,
    waiting_room: &Arc<Mutex<HashMap<String, VecDeque<mpsc::Sender<String>>>>>
) -> RespResult {
    // parts[0] = "BLPOP", parts[1] = key, parts[2] = timeout
    if parts.len() < 3 {
        return Err("Incomplete BLPOP command".to_string());
    }

    let key = parts[1].clone();
    println!("DEBUG: BLPOP checking kv_store for {}", key);
    let timeout_val: f64 = parts.last().unwrap().parse().unwrap_or(0.0);

    // If list exists and has items, return immediately
    {
        let mut map = kv_store.lock().unwrap();
        if let Some(val) = map.get_mut(&key) {
            if let RedisData::List(list) = &mut val.data {
                if !list.is_empty() {
                    let item = list.remove(0);
                    return Ok(encode_array(&[key, item]));
                }
            }
        }
    }
    println!("DEBUG: BLPOP blocking on key: {}", key);

    // List empty/didn't exist, block
    let (_tx, mut rx) = init_waiting_room(&vec![key.to_string()], &waiting_room);

    let result = if timeout_val > 0.0 {
        let duration = tokio::time::Duration::from_secs_f64(timeout_val);
        match tokio::time::timeout(duration, rx.recv()).await {
            Ok(maybe_data) => maybe_data,
            Err(_) => {
                let mut room = waiting_room.lock().unwrap();
                if let Some(queue) = room.get_mut(&key) {
                    queue.retain(|sender| !sender.is_closed());
                }
                None
            },
        }
    } else {
        rx.recv().await
    };

    match result {
        Some(data) => {
            println!("DEBUG: BLPOP Woke up! Received: {}", data);
            Ok(encode_array(&[key, data]))
        },
        None => Ok(encode_null_array()),
    }
}
