/// Parses a raw RESP message and extracts only the meaningful parts.
///
/// Takes a raw RESP string like:
/// `"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n"`
///
/// Returns only the meaningful parts as owned Strings:
/// `["ECHO", "hey"]`
///
/// This allows command handlers to access arguments directly:
/// - parts[0] = command name (e.g., "SET", "XADD")
/// - parts[1] = first argument (e.g., key)
/// - parts[2] = second argument, etc.
pub fn decode_resp(data: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut lines = data.lines();

    while let Some(line) = lines.next() {
        if line.starts_with('$') {
            // The NEXT line is the actual string data
            if let Some(actual_data) = lines.next() {
                parts.push(actual_data.to_string());
            }
        } else if line.starts_with('+') {
            // Simple String (e.g. +PING)
            parts.push(line[1..].to_string());
        }
    }
    parts
}
