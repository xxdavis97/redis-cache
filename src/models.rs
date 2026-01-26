use std::time::Instant;
use std::collections::HashMap;

pub type RespResult = Result<Vec<u8>, String>;

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

pub enum InfoOption {
    Replication
}

pub struct ServerInfo {
    pub replication_info: ReplicationInfo
}

pub struct ReplicationInfo {
    pub info_type_name: String, //todo: maybe use enum and interface
    pub role: String,
    // pub connected_slaves: u64,
    // pub master_replid: String,
    // pub master_repl_offset: i64,
    // pub second_repl_offset: i64,
    // pub repl_backlog_active: u64,
    // pub repl_backlog_size: u64,
    // pub repl_backlog_first_byte_offset: i64,
    // pub repl_backlog_histlen: u64
}

impl ReplicationInfo {
    pub fn new(role: String) -> Self {
        Self {
            info_type_name: "Replication".to_string(),
            role,
        }
    }
    pub fn to_info_string(&self) -> String {
        format!(
            "# {}\r\nrole:{}\r\n",
            self.info_type_name, self.role
        )
    }
}