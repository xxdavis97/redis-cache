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
    pub master_replid: String,
    pub master_repl_offset: u64,
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
            master_replid: Self::generate_replid(),
            master_repl_offset: 0
        }
    }
    pub fn to_info_string(&self) -> String {
        format!(
            "# {}\r\nrole:{}\r\nmaster_replid:{}\r\nmaster_repl_offset:{}\r\n",
            self.info_type_name, self.role, self.master_replid, self.master_repl_offset
        )
    }
    fn generate_replid() -> String {
        "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string()
    }
}
