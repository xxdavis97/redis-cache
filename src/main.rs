#![allow(unused_imports)]
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::sync::{Arc, Mutex};
use std::collections::{HashMap, VecDeque};
use tokio::sync::mpsc;

use redis_cache::models::RedisValue;
use redis_cache::parser;

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment the code below to pass the first stage
    
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

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

async fn handle_client(mut stream: tokio::net::TcpStream, kv_store: Arc<Mutex<HashMap<String, RedisValue>>>, 
                    waiting_room: Arc<Mutex<HashMap<String, VecDeque<mpsc::Sender<String>>>>>) {
    let mut buffer = [0; 512];
    
    loop {
        match stream.read(&mut buffer).await {
            Ok(0) => {
                // Client disconnected (EOF)
                println!("Client disconnected");
                break;
            }
            Ok(bytes_read) => {
                println!("Received {} bytes", bytes_read);
                let parsed_bytes = parser::parse_resp(&mut buffer, bytes_read, &kv_store, &waiting_room).await;
                if let Err(e) = stream.write_all(&parsed_bytes).await {
                    eprintln!("Failed to write: {}", e);
                    break;
                }
            }
            Err(e) => {
                eprintln!("Failed to read: {}", e);
                break;
            }
        }
    }
}