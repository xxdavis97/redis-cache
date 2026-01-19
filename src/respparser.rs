use std::sync::{Arc, Mutex};
use std::collections::{VecDeque, HashMap};
use std::time::{Instant, Duration};
use tokio::sync::mpsc;

use crate::models::{ListDir, RedisData, RedisValue};

type RespResult = Result<Vec<u8>, String>;

pub async fn parse_resp(
    buffer: &mut [u8], 
    bytes_read: usize, 
    kv_store: &Arc<Mutex<HashMap<String, RedisValue>>>,
    waiting_room: &Arc<Mutex<HashMap<String, VecDeque<mpsc::Sender<String>>>>>
) -> Vec<u8> {

    let data =  String::from_utf8_lossy(&buffer[..bytes_read]);
    let parts: Vec<&str> = data.lines().collect();
    println!("DEBUG: Received parts: {:?}", parts);

    let result = match parts[2].to_uppercase().as_str() {
        "PING" => process_ping(),
        "ECHO" => process_echo(&parts),
        "SET" => process_set(&parts, &kv_store),
        "GET" => process_get(&parts, &kv_store),
        "RPUSH" => process_push(&parts, &kv_store, &waiting_room, ListDir::R),
        "LRANGE" => process_lrange(&parts, &kv_store),
        "LPUSH" => process_push(&parts, &kv_store, &waiting_room, ListDir::L),
        "LLEN" => process_llen(&parts, &kv_store),
        "LPOP" => process_pop(&parts, &kv_store, ListDir::L),
        "BLPOP" => process_blpop(&parts, &kv_store, &waiting_room).await,
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

fn process_echo(
    parts: &Vec<&str>
) -> RespResult {
    match parts.len() {
        5 => Ok(encode_bulk_string(parts[4])),
        _ => Err("Error, echo command must be of length 5".to_string())
    }   
}

fn process_set(
    parts: &Vec<&str>, 
    kv_store: &Arc<Mutex<HashMap<String, RedisValue>>>
) -> RespResult {
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

fn process_get(
    parts: &Vec<&str>, 
    kv_store: &Arc<Mutex<HashMap<String, RedisValue>>>
) -> RespResult {
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

fn process_push(
    parts: &Vec<&str>, 
    kv_store: &Arc<Mutex<HashMap<String, RedisValue>>>, 
    waiting_room: &Arc<Mutex<HashMap<String, VecDeque<mpsc::Sender<String>>>>>, 
    push_type: ListDir
) -> RespResult {
    if parts.len() < 7 {
        return Err("Incomplete RPUSH/LPUSH command".to_string());
    }
    let key = parts[4].to_string();
    let mut map = kv_store.lock().unwrap();

    let new_elements: Vec<String> = parts[6..] // RPUSH can take multiple values
        .iter()
        .step_by(2) // Skip the RESP length lines
        .map(|s| s.to_string())
        .collect();

    // Get existing list from map or initialize to empty
    let entry = map.entry(key.clone()).or_insert(RedisValue::new(
        RedisData::List(Vec::new()), 
        None
    ));

    match &mut entry.data {
        RedisData::List(list) => {
            let mut room = waiting_room.lock().unwrap();
            let total_new_elements = new_elements.len(); 
            // We use into_iter because we want to consume the elements
            let mut remaining_elements = new_elements.into_iter();
            if let Some(queue) = room.get_mut(&key) {
                println!("DEBUG: PUSH found {} waiters for {}", queue.len(), key);
                while let Some(tx) = queue.front() {
                    // If we ran out of new elements to give, stop waking people up
                    let Some(next_val) = remaining_elements.next() else { println!("DEBUG: PUSH ran out of elements for waiters");break; };
                    if tx.try_send(next_val).is_ok() {
                        println!("DEBUG: PUSH successfully handed off element");
                        queue.pop_front(); 
                    } else {
                        println!("DEBUG: PUSH found a dead waiter (receiver dropped)");
                        // Receiver is dead, remove this sender and try the next person in line
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
            // REDIS SPEC: Return the length after the operation.
            // If the list was empty (0) and we pushed 1 (raspberry), 
            // even if raspberry was popped immediately, the result of PUSH is 1.
            
            let final_len = list.len() + (total_new_elements - leftovers_count);
            Ok(encode_integer(final_len)) 
        },
        _ => Err("WRONGTYPE Operation against a key that is not a list".to_string())
    }
}

fn process_lrange(
    parts: &Vec<&str>, 
    kv_store: &Arc<Mutex<HashMap<String, RedisValue>>>
) -> RespResult {
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
                _ => Err("WRONGTYPE Operation against a key not holding a list".to_string()),
            }
        },
        None => Ok(encode_array(&[]))
    }
}

fn process_llen(
    parts: &Vec<&str>, 
    kv_store: &Arc<Mutex<HashMap<String, RedisValue>>>
) -> RespResult {
    if parts.len() < 5 {
        return Err("Incomplete LRANGE command".to_string());
    }
    let key = parts[4].to_string();
    let map = kv_store.lock().unwrap();
    match map.get(&key) {
        Some(value) => {
            match &value.data {
                RedisData::List(list) => Ok(encode_integer(list.len())),
                _ => Err("WRONGTYPE Operation against a key not holding a list".to_string()),
            }
        },
        None => Ok(encode_integer(0))
    }
}

fn process_pop(
    parts: &Vec<&str>, 
    kv_store: &Arc<Mutex<HashMap<String, RedisValue>>>, 
    push_type: ListDir
) -> RespResult {
    if parts.len() < 5 {
        return Err("Incomplete RPOP/LPOP command".to_string());
    }
    let mut delete_amt: i64 = 1;
    if parts.len() == 7 {
        delete_amt = parts[6].parse().unwrap_or(1);
    }
    let key = parts[4].to_string();
    let mut map = kv_store.lock().unwrap();
    let mut should_remove = false;

    let response = match map.get_mut(&key) {
        Some(value) => {
            match &mut value.data {
                RedisData::List(list) => {
                    if list.len() == 0 {
                        Ok(encode_null_string())
                    } else {
                        let mut dropped_items = vec![];
                        while delete_amt > 0 && list.len() > 0 {
                            let dropped_item = match push_type {
                                ListDir::L => list.remove(0),
                                ListDir::R => list.pop().unwrap()
                            };
                            dropped_items.push(dropped_item);
                            delete_amt -= 1;
                        };
                        
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
        map.remove(&key);
    }
    response
}

async fn process_blpop(
    parts: &Vec<&str>, 
    kv_store: &Arc<Mutex<HashMap<String, RedisValue>>>, 
    waiting_room: &Arc<Mutex<HashMap<String, VecDeque<mpsc::Sender<String>>>>>
) -> RespResult {
    if parts.len() < 7 {
        return Err("Incomplete LBPOP command".to_string());
    }

    let key = parts[4].to_string();
    println!("DEBUG: LBPOP checking kv_store for {}", key);
    let timeout_val: u64 = parts[parts.len() - 1].parse().unwrap_or(0);

    // If exists just return, after check we want to remove the lock
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
    println!("DEBUG: LBPOP blocking on key: {}", key);

    // List empty/didn't exist, block
    let (tx, mut rx) = mpsc::channel(1);
    {
        let mut room = waiting_room.lock().unwrap();
        room.entry(key.clone()).or_default().push_back(tx);
        println!("DEBUG: Waiter added to room. Current queue size for {}: {}", 
                 key, room.get(&key).unwrap().len());
    }

    let result = if timeout_val > 0 {
        match tokio::time::timeout(tokio::time::Duration::from_secs(timeout_val), rx.recv()).await {
            Ok(maybe_data) => maybe_data, // Success or channel closed
            Err(_) => None,               // Timeout
        }
    } else {
        rx.recv().await
    };

    match result {
        Some(data) => {
            println!("DEBUG: LBPOP Woke up! Received: {}", data);
            Ok(encode_array(&[key, data]))
        },
        None => Ok(encode_null_array()), // Timeout or PUSH closed the channel
    }
}

fn encode_simple_string(
    s: &str
) -> Vec<u8> {
    format!("+{}\r\n", s).into_bytes()
}

fn encode_bulk_string(
    s: &str
) -> Vec<u8> {
    format!("${}\r\n{}\r\n", s.len(), s).into_bytes()
}

fn encode_null_string() -> Vec<u8> {
    "$-1\r\n".as_bytes().to_vec()
}

fn encode_integer(
    n: usize
) -> Vec<u8> {
    format!(":{}\r\n", n).into_bytes()
}

fn encode_array(
    arr: &[String]
) -> Vec<u8> {
    let mut bytes = format!("*{}\r\n", arr.len()).into_bytes();
    bytes.extend(arr.iter().flat_map(|s| encode_bulk_string(s)));
    bytes
}

fn encode_null_array() -> Vec<u8> {
    "*-1\r\n".as_bytes().to_vec()
}