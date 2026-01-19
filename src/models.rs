use std::time::Instant;
use std::collections::HashMap;

pub enum RedisData {
    String(String),
    List(Vec<String>),
    Stream(Vec<StreamEntry>)
    // Future: Set(HashSet<String>), Hash(HashMap<String, String>)
}

pub struct StreamEntry {
    pub id: String,
    pub fields: HashMap<String, String>,
}

// For RPUSH, LPUSH, RPOP, LPOP, etc. to get direction
pub enum ListDir {
    L,
    R
}

pub struct RedisValue {
    pub data: RedisData,
    pub expires_at: Option<Instant>, // None means it never expires
}

impl RedisValue {
    pub fn new(data: RedisData, expires_at: Option<Instant>) -> Self {
        Self {
            data,
            expires_at,
        }
    }
}