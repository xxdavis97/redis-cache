#![allow(unused_imports)]
use std::net::TcpListener;
use std::io::{Read, Write};
use std::thread;


fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment the code below to pass the first stage
    
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                thread::spawn(move || { 
                    handle_client(stream);
                });
            }
            Err(e) => eprintln!("Connection error: {}", e)
        }
    }
}

fn handle_client(mut stream: std::net::TcpStream) {
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
                let parsed_bytes = parse_resp(&mut buffer, bytes_read);
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

fn parse_resp(buffer: &mut [u8], bytes_read: usize) -> Vec<u8> {
    let data =  String::from_utf8_lossy(&buffer[..bytes_read]);
    let parts: Vec<&str> = data.lines().collect();
    let command = parts[2].to_uppercase();
    match parts[2].to_uppercase().as_str() {
        "PING" => encode_simple_string("PONG"),
        "ECHO" => {
            if parts.len() != 5 {
                eprintln!("Error, echo command must be of length 5");
                vec![]
            } else {
                encode_bulk_string(parts[4])
            }
        },
        _ => {
            eprintln!("Not supported resp command");
            vec![]
        }
    }
}

fn encode_simple_string(s: &str) -> Vec<u8> {
    format!("+{}\r\n", s).into_bytes()
}

fn encode_bulk_string(s: &str) -> Vec<u8> {
    format!("${}\r\n{}\r\n", s.len(), s).into_bytes()
}