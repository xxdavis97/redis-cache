use std::sync::{Arc, Mutex};
use std::collections::{VecDeque, HashMap};
use tokio::sync::mpsc;
use async_recursion::async_recursion;

use crate::models::{ListDir, RedisValue, RespResult};
use crate::commands::*;

#[async_recursion]
pub async fn execute_commands(
    command: String,
    parts: &Vec<String>, 
    kv_store: &Arc<Mutex<HashMap<String, RedisValue>>>,
    waiting_room: &Arc<Mutex<HashMap<String, VecDeque<mpsc::Sender<String>>>>>,
    command_queue: &mut Option<VecDeque<Vec<String>>>
) -> Vec<u8> {
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
        "EXEC" => process_exec(command_queue, &kv_store, &waiting_room).await,
        "DISCARD" => process_discard(command_queue),
        _ => Err("Not supported".to_string()),
    };
    match_result(result)
}

pub fn match_result(result: RespResult) -> Vec<u8> {
    match result {
        Ok(bytes) => bytes,
        Err(e) => {
            eprintln!("Command Error: {}", e);
            vec![]
        }
    }
}