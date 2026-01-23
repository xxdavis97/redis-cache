use redis_cache::utils::decoder::decode_resp;

// ==================== Basic RESP Decoding ====================

#[test]
fn test_decode_resp_ping() {
    let raw = "*1\r\n$4\r\nPING\r\n";
    let result = decode_resp(raw);
    assert_eq!(result, vec!["PING"]);
}

#[test]
fn test_decode_resp_echo() {
    let raw = "*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n";
    let result = decode_resp(raw);
    assert_eq!(result, vec!["ECHO", "hello"]);
}

#[test]
fn test_decode_resp_set() {
    let raw = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
    let result = decode_resp(raw);
    assert_eq!(result, vec!["SET", "key", "value"]);
}

#[test]
fn test_decode_resp_set_with_expiry() {
    let raw = "*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nEX\r\n$2\r\n10\r\n";
    let result = decode_resp(raw);
    assert_eq!(result, vec!["SET", "key", "value", "EX", "10"]);
}

#[test]
fn test_decode_resp_get() {
    let raw = "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
    let result = decode_resp(raw);
    assert_eq!(result, vec!["GET", "key"]);
}

// ==================== List Commands Decoding ====================

#[test]
fn test_decode_resp_rpush_single() {
    let raw = "*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$5\r\nvalue\r\n";
    let result = decode_resp(raw);
    assert_eq!(result, vec!["RPUSH", "mylist", "value"]);
}

#[test]
fn test_decode_resp_rpush_multiple() {
    let raw = "*4\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$2\r\nv1\r\n$2\r\nv2\r\n";
    let result = decode_resp(raw);
    assert_eq!(result, vec!["RPUSH", "mylist", "v1", "v2"]);
}

#[test]
fn test_decode_resp_lpush() {
    let raw = "*3\r\n$5\r\nLPUSH\r\n$6\r\nmylist\r\n$5\r\nvalue\r\n";
    let result = decode_resp(raw);
    assert_eq!(result, vec!["LPUSH", "mylist", "value"]);
}

#[test]
fn test_decode_resp_lrange() {
    let raw = "*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$1\r\n0\r\n$2\r\n-1\r\n";
    let result = decode_resp(raw);
    assert_eq!(result, vec!["LRANGE", "mylist", "0", "-1"]);
}

#[test]
fn test_decode_resp_lpop() {
    let raw = "*2\r\n$4\r\nLPOP\r\n$6\r\nmylist\r\n";
    let result = decode_resp(raw);
    assert_eq!(result, vec!["LPOP", "mylist"]);
}

#[test]
fn test_decode_resp_lpop_with_count() {
    let raw = "*3\r\n$4\r\nLPOP\r\n$6\r\nmylist\r\n$1\r\n3\r\n";
    let result = decode_resp(raw);
    assert_eq!(result, vec!["LPOP", "mylist", "3"]);
}

#[test]
fn test_decode_resp_blpop() {
    let raw = "*3\r\n$5\r\nBLPOP\r\n$6\r\nmylist\r\n$1\r\n0\r\n";
    let result = decode_resp(raw);
    assert_eq!(result, vec!["BLPOP", "mylist", "0"]);
}

#[test]
fn test_decode_resp_blpop_with_timeout() {
    let raw = "*3\r\n$5\r\nBLPOP\r\n$6\r\nmylist\r\n$3\r\n0.1\r\n";
    let result = decode_resp(raw);
    assert_eq!(result, vec!["BLPOP", "mylist", "0.1"]);
}

// ==================== Stream Commands Decoding ====================

#[test]
fn test_decode_resp_xadd() {
    let raw = "*6\r\n$4\r\nXADD\r\n$10\r\nstream_key\r\n$3\r\n0-1\r\n$11\r\ntemperature\r\n$2\r\n96\r\n";
    let result = decode_resp(raw);
    assert_eq!(result, vec!["XADD", "stream_key", "0-1", "temperature", "96"]);
}

#[test]
fn test_decode_resp_xadd_with_star() {
    let raw = "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$1\r\n*\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    let result = decode_resp(raw);
    assert_eq!(result, vec!["XADD", "mystream", "*", "foo", "bar"]);
}

#[test]
fn test_decode_resp_xadd_partial_wildcard() {
    let raw = "*5\r\n$4\r\nXADD\r\n$8\r\nmystream\r\n$3\r\n0-*\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    let result = decode_resp(raw);
    assert_eq!(result, vec!["XADD", "mystream", "0-*", "foo", "bar"]);
}

#[test]
fn test_decode_resp_xrange() {
    let raw = "*4\r\n$6\r\nXRANGE\r\n$8\r\nmystream\r\n$1\r\n-\r\n$1\r\n+\r\n";
    let result = decode_resp(raw);
    assert_eq!(result, vec!["XRANGE", "mystream", "-", "+"]);
}

#[test]
fn test_decode_resp_xrange_specific() {
    let raw = "*4\r\n$6\r\nXRANGE\r\n$8\r\nmystream\r\n$3\r\n0-1\r\n$3\r\n0-3\r\n";
    let result = decode_resp(raw);
    assert_eq!(result, vec!["XRANGE", "mystream", "0-1", "0-3"]);
}

#[test]
fn test_decode_resp_xread_simple() {
    let raw = "*4\r\n$5\r\nXREAD\r\n$7\r\nstreams\r\n$8\r\nmystream\r\n$3\r\n0-0\r\n";
    let result = decode_resp(raw);
    assert_eq!(result, vec!["XREAD", "streams", "mystream", "0-0"]);
}

#[test]
fn test_decode_resp_xread_with_block() {
    let raw = "*6\r\n$5\r\nXREAD\r\n$5\r\nblock\r\n$4\r\n1000\r\n$7\r\nstreams\r\n$8\r\nmystream\r\n$3\r\n0-0\r\n";
    let result = decode_resp(raw);
    assert_eq!(result, vec!["XREAD", "block", "1000", "streams", "mystream", "0-0"]);
}

#[test]
fn test_decode_resp_xread_with_dollar() {
    let raw = "*6\r\n$5\r\nXREAD\r\n$5\r\nblock\r\n$1\r\n0\r\n$7\r\nstreams\r\n$4\r\npear\r\n$1\r\n$\r\n";
    let result = decode_resp(raw);
    assert_eq!(result, vec!["XREAD", "block", "0", "streams", "pear", "$"]);
}

#[test]
fn test_decode_resp_xread_multiple_streams() {
    let raw = "*6\r\n$5\r\nXREAD\r\n$7\r\nstreams\r\n$5\r\napple\r\n$9\r\nblueberry\r\n$3\r\n0-0\r\n$3\r\n0-1\r\n";
    let result = decode_resp(raw);
    assert_eq!(result, vec!["XREAD", "streams", "apple", "blueberry", "0-0", "0-1"]);
}

// ==================== Other Commands Decoding ====================

#[test]
fn test_decode_resp_type() {
    let raw = "*2\r\n$4\r\nTYPE\r\n$5\r\nmykey\r\n";
    let result = decode_resp(raw);
    assert_eq!(result, vec!["TYPE", "mykey"]);
}

#[test]
fn test_decode_resp_llen() {
    let raw = "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n";
    let result = decode_resp(raw);
    assert_eq!(result, vec!["LLEN", "mylist"]);
}

// ==================== Edge Cases ====================

#[test]
fn test_decode_resp_empty_value() {
    let raw = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$0\r\n\r\n";
    let result = decode_resp(raw);
    assert_eq!(result, vec!["SET", "key", ""]);
}

#[test]
fn test_decode_resp_value_with_spaces() {
    let raw = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$11\r\nhello world\r\n";
    let result = decode_resp(raw);
    assert_eq!(result, vec!["SET", "key", "hello world"]);
}

#[test]
fn test_decode_resp_numeric_values() {
    let raw = "*3\r\n$3\r\nSET\r\n$7\r\ncounter\r\n$5\r\n12345\r\n";
    let result = decode_resp(raw);
    assert_eq!(result, vec!["SET", "counter", "12345"]);
}

#[test]
fn test_decode_resp_long_command() {
    let raw = "*7\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n";
    let result = decode_resp(raw);
    assert_eq!(result, vec!["RPUSH", "mylist", "a", "b", "c", "d", "e"]);
}

#[test]
fn test_decode_resp_simple_string() {
    // Simple string format (starts with +)
    let raw = "+PING\r\n";
    let result = decode_resp(raw);
    assert_eq!(result, vec!["PING"]);
}

#[test]
fn test_decode_resp_case_preserved() {
    let raw = "*2\r\n$4\r\necho\r\n$5\r\nHELLO\r\n";
    let result = decode_resp(raw);
    assert_eq!(result, vec!["echo", "HELLO"]);
}
