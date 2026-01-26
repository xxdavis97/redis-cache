#![allow(unused_imports)]
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::sync::{Arc, Mutex};
use std::collections::{HashMap, VecDeque};
use std::env;
use tokio::sync::mpsc;

use redis_cache::models::{ServerInfo, ReplicationInfo, RedisValue};
use redis_cache::parser;
use redis_cache::constants::*;

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment the code below to pass the first stage
    let args: Vec<String> = env::args().collect();
    let port_num = args.iter()
        .position(|arg| arg == PORT)
        .map_or("6379", |idx| &args[idx+1]);
    
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port_num)).await.unwrap();

    let store = Arc::new(Mutex::new(HashMap::new()));
    let waiting_room: Arc<Mutex<HashMap<String, VecDeque<mpsc::Sender<String>>>>> = Arc::new(Mutex::new(HashMap::new()));
    
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let kv_store = Arc::clone(&store);
                let room_clone = Arc::clone(&waiting_room);
                tokio::spawn(async move { 
                    handle_client(stream, kv_store, room_clone).await;
                });
            },
            Err(e) => eprintln!("Connection error: {}", e)
        }
    }
}

async fn handle_client(
    mut stream: tokio::net::TcpStream, 
    kv_store: Arc<Mutex<HashMap<String, RedisValue>>>,           
    waiting_room: Arc<Mutex<HashMap<String, VecDeque<mpsc::Sender<String>>>>>
) {
    let mut buffer = [0; 512];
    // For MULTI will keep track of pending commands by client, None
    // should signal MULTI is not on
    let mut command_queue: Option<VecDeque<Vec<String>>> = None;
    //todo: update for more info
    let mut server_info: ServerInfo = ServerInfo{replication_info: ReplicationInfo::new("master".to_string())};
    loop {
        match run_command(&mut stream, &mut buffer, &kv_store, &waiting_room, &mut command_queue, &mut server_info).await {
            Ok(alive) if !alive => break, // EOF reached
            Ok(_) => (),                 // Command handled, keep going
            Err(e) => {
                eprintln!("Connection error: {}", e);
                break;
            }
        }
        
    }
}

async fn run_command(
    stream: &mut tokio::net::TcpStream, // Use &mut here
    buffer: &mut [u8],
    kv_store: &Arc<Mutex<HashMap<String, RedisValue>>>,           
    waiting_room: &Arc<Mutex<HashMap<String, VecDeque<mpsc::Sender<String>>>>>,
    command_queue: &mut Option<VecDeque<Vec<String>>>, // Mutable ref to the state
    server_info: &mut ServerInfo
) -> Result<bool, Box<dyn std::error::Error>> {
    match stream.read(buffer).await? {
        0 => return Ok(false), // Signal disconnect
        bytes_read => {
            let parsed_bytes = parser::parse_resp(
                buffer, 
                bytes_read, 
                kv_store, 
                waiting_room, 
                command_queue,
                server_info
            ).await;
            
            stream.write_all(&parsed_bytes).await?;
            Ok(true) // Keep loop alive
        }
    }   
}