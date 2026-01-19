#![allow(unused_imports)]
use std::net::TcpListener;
use std::io::{Read, Write};
use std::thread;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;

mod respparser;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment the code below to pass the first stage
    
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    let env_variables = Arc::new(Mutex::new(HashMap::new()));
    
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let kv_store = Arc::clone(&env_variables);
                thread::spawn(move || { 
                    handle_client(stream, kv_store);
                });
            }
            Err(e) => eprintln!("Connection error: {}", e)
        }
    }
}

fn handle_client(mut stream: std::net::TcpStream, kv_store: Arc<Mutex<HashMap<String, String>>>) {
    let mut buffer = [0; 512];
    
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => {
                // Client disconnected (EOF)
                println!("Client disconnected");
                break;
            }
            Ok(bytes_read) => {
                println!("Received {} bytes", bytes_read);
                let parsed_bytes = respparser::parse_resp(&mut buffer, bytes_read, &kv_store);
                if let Err(e) = stream.write_all(&parsed_bytes) {
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