use std::sync::{Arc, Mutex};
use std::collections::{VecDeque, HashMap};
use tokio::sync::mpsc;

use crate::models::{RedisValue};
use crate::commands::*;
use crate::utils::decoder::decode_resp;
use crate::executor::*;

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

    // If multi is active, push all commands onto queue and return unless command is exec or discard
    if let Some(queue) = command_queue {
        match command.as_str() {
            "EXEC" | "DISCARD" => {},
            _ => {
                let queue_push_result = handle_push_command_queue(&parts, queue);
                return match_result(queue_push_result);
            }
        }
    }
    execute_commands(command, &parts, &kv_store, &waiting_room, command_queue).await
}


