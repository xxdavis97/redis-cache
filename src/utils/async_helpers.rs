use std::sync::{Arc, Mutex};
use std::collections::{HashMap, VecDeque};
use tokio::sync::mpsc;

pub fn init_waiting_room(
    keys: &[String],
    waiting_room: &Arc<Mutex<HashMap<String, VecDeque<mpsc::Sender<String>>>>>
) -> (mpsc::Sender<String>, mpsc::Receiver<String>) {
    let (tx, rx) = mpsc::channel(1);
    {
        let mut room = waiting_room.lock().unwrap();
        for key in keys {
            room.entry(key.to_string()).or_default().push_back(tx.clone());
            println!("DEBUG: Waiter added to room. Current queue size for {}: {}",
                    key, room.get(key).unwrap().len());
        }
    }
    (tx, rx)
}
