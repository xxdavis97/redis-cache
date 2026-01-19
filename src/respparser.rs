use std::sync::{Arc, Mutex};
use std::collections::HashMap;

pub fn parse_resp(buffer: &mut [u8], bytes_read: usize, kv_store: &Arc<Mutex<HashMap<String, String>>>) -> Vec<u8> {
    let data =  String::from_utf8_lossy(&buffer[..bytes_read]);
    let parts: Vec<&str> = data.lines().collect();

    match parts[2].to_uppercase().as_str() {
        "PING" => encode_simple_string("PONG"),
        "ECHO" => {
            process_echo(&parts)
        },
        "SET" => {
            process_set(&parts, &kv_store)
        },
        "GET" => {
            process_get(&parts, &kv_store)
        },
        _ => {
            eprintln!("Not supported resp command");
            vec![]
        }
    }
}

fn process_echo(parts: &Vec<&str>) -> Vec<u8> {
    if parts.len() != 5 {
        eprintln!("Error, echo command must be of length 5");
        vec![]
    } else {
        encode_bulk_string(parts[4])
    }        
}

fn process_set(parts: &Vec<&str>, kv_store: &Arc<Mutex<HashMap<String, String>>>) -> Vec<u8> {
    if parts.len() != 7 {
        eprintln!("Error, set command must be of length 7");
        vec![]
    } else {
        let key = parts[4].to_string();
        let value = parts[6].to_string();
        let mut map = kv_store.lock().unwrap();
        map.insert(key, value);
        encode_simple_string("OK")
    }
}

fn process_get(parts: &Vec<&str>, kv_store: &Arc<Mutex<HashMap<String, String>>>) -> Vec<u8> {
    if parts.len() >= 5 {
        let key = parts[4].to_string();
        let map = kv_store.lock().unwrap();
        match map.get(&key) {
            Some(value) => encode_bulk_string(value),
            None => encode_null_string()
        }
    } else {
        vec![]
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