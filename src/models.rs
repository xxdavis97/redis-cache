use std::time::Instant;

pub struct RedisValue {
    pub data: String,
    pub expires_at: Option<Instant>, // None means it never expires
}

impl RedisValue {
    pub fn new(data: String, expires_at: Option<Instant>) -> Self {
        Self {
            data,
            expires_at,
        }
    }
}