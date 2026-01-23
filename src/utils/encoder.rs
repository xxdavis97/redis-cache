use crate::models::StreamEntry;

pub type RespResult = Result<Vec<u8>, String>;

pub fn encode_simple_string(s: &str) -> Vec<u8> {
    format!("+{}\r\n", s).into_bytes()
}

pub fn encode_bulk_string(s: &str) -> Vec<u8> {
    format!("${}\r\n{}\r\n", s.len(), s).into_bytes()
}

pub fn encode_null_string() -> Vec<u8> {
    "$-1\r\n".as_bytes().to_vec()
}

pub fn encode_integer(n: usize) -> Vec<u8> {
    format!(":{}\r\n", n).into_bytes()
}

pub fn encode_array(arr: &[String]) -> Vec<u8> {
    let mut bytes = format!("*{}\r\n", arr.len()).into_bytes();
    for s in arr {
        bytes.extend(encode_bulk_string(s));
    }
    bytes
}

pub fn encode_raw_array(parts: Vec<Vec<u8>>) -> Vec<u8> {
    let mut response = format!("*{}\r\n", parts.len()).into_bytes();
    for part in parts {
        response.extend(part);
    }
    response
}

pub fn encode_stream_entry(entry: &StreamEntry) -> Vec<u8> {
    let mut fields_resp = Vec::new();
    for (k, v) in &entry.fields {
        fields_resp.push(encode_bulk_string(k));
        fields_resp.push(encode_bulk_string(v));
    }
    let encoded_fields = encode_raw_array(fields_resp);
    let mut entry_resp = Vec::new();
    entry_resp.push(encode_bulk_string(&entry.id));
    entry_resp.push(encoded_fields);
    encode_raw_array(entry_resp)
}

pub fn encode_null_array() -> Vec<u8> {
    "*-1\r\n".as_bytes().to_vec()
}
