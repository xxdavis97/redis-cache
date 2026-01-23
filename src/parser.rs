use std::sync::{Arc, Mutex};
use std::collections::{VecDeque, HashMap};
use tokio::sync::mpsc;

use crate::models::{ListDir, RedisValue, RespResult};
use crate::commands::*;
use crate::utils::decoder::decode_resp;

pub async fn parse_resp(
    buffer: &mut [u8],
    bytes_read: usize,
    kv_store: &Arc<Mutex<HashMap<String, RedisValue>>>,
    waiting_room: &Arc<Mutex<HashMap<String, VecDeque<mpsc::Sender<String>>>>>,
    command_queue: &mut Option<VecDeque<Vec<String>>>
) -> Vec<u8> {

    let data = String::from_utf8_lossy(&buffer[..bytes_read]);
    let parts = decode_resp(&data);
    println!("DEBUG: Received parts: {:?}", parts);

    if parts.is_empty() {
        return vec![];
    }
    let command = parts[0].to_uppercase();

    if let Some(queue) = command_queue {
        match command.as_str() {
            "EXEC" | "DISCARD" => {},
            _ => {
                let queue_push_result = handle_push_command_queue(&parts, queue);
                return match_result(queue_push_result);
            }
        }
    }

    let result = match command.as_str() {
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
        "TYPE" => process_type(&parts, &kv_store),
        "XADD" => process_xadd(&parts, &kv_store, &waiting_room),
        "XRANGE" => process_xrange(&parts, &kv_store),
        "XREAD" => process_xread(&parts, &kv_store, &waiting_room).await,
        "INCR" => process_incr(&parts, &kv_store),
        "MULTI" => process_multi(command_queue),
        "EXEC" => process_exec(command_queue),
        _ => Err("Not supported".to_string()),
    };
    match_result(result)
}

fn match_result(result: RespResult) -> Vec<u8> {
    match result {
        Ok(bytes) => bytes,
        Err(e) => {
            eprintln!("Command Error: {}", e);
            vec![]
        }
    }
}
