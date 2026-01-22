use std::sync::{Arc, Mutex};
use std::collections::{VecDeque, HashMap};
use tokio::sync::mpsc;

use crate::models::{ListDir, RedisValue};
use crate::respcommands::*;

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
        "TYPE" => process_type(&parts, &kv_store),
        "XADD" => process_xadd(&parts, &kv_store),
        "XRANGE" => process_xrange(&parts, &kv_store),
        "XREAD" => process_xread(&parts, &kv_store),
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


