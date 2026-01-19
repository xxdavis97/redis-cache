use std::time::Instant;

pub enum RedisData {
    String(String),
    List(Vec<String>),
    // Future: Set(HashSet<String>), Hash(HashMap<String, String>)
}

pub enum ListPush {
    LPUSH,
    RPUSH
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