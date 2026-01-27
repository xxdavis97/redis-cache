
use std::sync::{Arc, Mutex};
use crate::models::{InfoOption, ServerInfo, RespResult};
use crate::utils::encoder::encode_bulk_string;

pub fn process_info(
    parts: &[String],
    server_info: &Arc<Mutex<ServerInfo>>
) -> RespResult {
    // Don't need length check because can only pass INFO 
    let mut info_option: Option<InfoOption> = None;
    if parts.len() > 1 {
        info_option = match parts[1].to_uppercase().as_str() {
            "REPLICATION" => {
                Some(InfoOption::Replication)
            },
            _ => None //todo: maybe throw err
        }
    }

    let info = server_info.lock().unwrap();

    match info_option {
        //todo: make work for all infooption since all can implement the string
        Some(InfoOption::Replication) => Ok(encode_bulk_string(&info.replication_info.to_info_string())), 
        None => Ok(encode_bulk_string(&info.replication_info.to_info_string())) //todo: update
    }
}