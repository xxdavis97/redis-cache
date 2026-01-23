use redis_cache::utils::encoder::*;

// ==================== Simple String Encoding ====================

#[test]
fn test_encode_simple_string_pong() {
    let result = encode_simple_string("PONG");
    assert_eq!(result, b"+PONG\r\n");
}

#[test]
fn test_encode_simple_string_ok() {
    let result = encode_simple_string("OK");
    assert_eq!(result, b"+OK\r\n");
}

#[test]
fn test_encode_simple_string_none() {
    let result = encode_simple_string("none");
    assert_eq!(result, b"+none\r\n");
}

#[test]
fn test_encode_simple_string_types() {
    assert_eq!(encode_simple_string("string"), b"+string\r\n");
    assert_eq!(encode_simple_string("list"), b"+list\r\n");
    assert_eq!(encode_simple_string("stream"), b"+stream\r\n");
}

#[test]
fn test_encode_simple_string_empty() {
    let result = encode_simple_string("");
    assert_eq!(result, b"+\r\n");
}

// ==================== Bulk String Encoding ====================

#[test]
fn test_encode_bulk_string_basic() {
    let result = encode_bulk_string("hello");
    assert_eq!(result, b"$5\r\nhello\r\n");
}

#[test]
fn test_encode_bulk_string_longer() {
    let result = encode_bulk_string("hello world");
    assert_eq!(result, b"$11\r\nhello world\r\n");
}

#[test]
fn test_encode_bulk_string_empty() {
    let result = encode_bulk_string("");
    assert_eq!(result, b"$0\r\n\r\n");
}

#[test]
fn test_encode_bulk_string_numbers() {
    let result = encode_bulk_string("12345");
    assert_eq!(result, b"$5\r\n12345\r\n");
}

#[test]
fn test_encode_bulk_string_special_chars() {
    let result = encode_bulk_string("a\tb\nc");
    // "a\tb\nc" = 5 chars, so: $5\r\n(4 bytes) + content(5 bytes) + \r\n(2 bytes) = 11
    assert_eq!(result.len(), 4 + 5 + 2);
}

// ==================== Null String Encoding ====================

#[test]
fn test_encode_null_string() {
    let result = encode_null_string();
    assert_eq!(result, b"$-1\r\n");
}

// ==================== Integer Encoding ====================

#[test]
fn test_encode_integer_zero() {
    let result = encode_integer(0);
    assert_eq!(result, b":0\r\n");
}

#[test]
fn test_encode_integer_positive() {
    let result = encode_integer(42);
    assert_eq!(result, b":42\r\n");
}

#[test]
fn test_encode_integer_large() {
    let result = encode_integer(1000000);
    assert_eq!(result, b":1000000\r\n");
}

#[test]
fn test_encode_integer_one() {
    let result = encode_integer(1);
    assert_eq!(result, b":1\r\n");
}

// ==================== Array Encoding ====================

#[test]
fn test_encode_array_empty() {
    let arr: [String; 0] = [];
    let result = encode_array(&arr);
    assert_eq!(result, b"*0\r\n");
}

#[test]
fn test_encode_array_single() {
    let arr = ["hello".to_string()];
    let result = encode_array(&arr);
    assert_eq!(result, b"*1\r\n$5\r\nhello\r\n");
}

#[test]
fn test_encode_array_multiple() {
    let arr = ["a".to_string(), "b".to_string(), "c".to_string()];
    let result = encode_array(&arr);
    assert_eq!(result, b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n");
}

#[test]
fn test_encode_array_blpop_response() {
    let arr = ["mylist".to_string(), "value".to_string()];
    let result = encode_array(&arr);
    assert_eq!(result, b"*2\r\n$6\r\nmylist\r\n$5\r\nvalue\r\n");
}

#[test]
fn test_encode_array_varying_lengths() {
    let arr = ["short".to_string(), "a".to_string(), "longer string".to_string()];
    let result = encode_array(&arr);
    let expected = b"*3\r\n$5\r\nshort\r\n$1\r\na\r\n$13\r\nlonger string\r\n";
    assert_eq!(result, expected.to_vec());
}

// ==================== Raw Array Encoding ====================

#[test]
fn test_encode_raw_array_empty() {
    let parts: Vec<Vec<u8>> = vec![];
    let result = encode_raw_array(parts);
    assert_eq!(result, b"*0\r\n");
}

#[test]
fn test_encode_raw_array_single() {
    let parts = vec![b"$5\r\nhello\r\n".to_vec()];
    let result = encode_raw_array(parts);
    assert_eq!(result, b"*1\r\n$5\r\nhello\r\n");
}

#[test]
fn test_encode_raw_array_nested() {
    let inner = encode_array(&["a".to_string(), "b".to_string()]);
    let parts = vec![inner];
    let result = encode_raw_array(parts);
    // *1\r\n + *2\r\n$1\r\na\r\n$1\r\nb\r\n
    assert!(result.starts_with(b"*1\r\n*2\r\n"));
}

// ==================== Null Array Encoding ====================

#[test]
fn test_encode_null_array() {
    let result = encode_null_array();
    assert_eq!(result, b"*-1\r\n");
}

// ==================== Integration Tests ====================

#[test]
fn test_encode_lrange_response() {
    let list = ["grape".to_string(), "apple".to_string(), "raspberry".to_string()];
    let result = encode_array(&list);

    // Verify format
    assert!(result.starts_with(b"*3\r\n"));
    assert!(result.windows(5).any(|w| w == b"grape"));
    assert!(result.windows(5).any(|w| w == b"apple"));
}

#[test]
fn test_encode_xread_like_response() {
    // Simulate XREAD response structure
    let stream_name = encode_bulk_string("mystream");
    let entry_id = encode_bulk_string("0-1");
    let field = encode_bulk_string("temperature");
    let value = encode_bulk_string("36");

    let field_value_array = encode_raw_array(vec![field, value]);
    let entry = encode_raw_array(vec![entry_id, field_value_array]);
    let entries_array = encode_raw_array(vec![entry]);
    let stream_response = encode_raw_array(vec![stream_name, entries_array]);
    let final_response = encode_raw_array(vec![stream_response]);

    // Should be a nested array structure
    assert!(final_response.starts_with(b"*1\r\n*2\r\n"));
}

// ==================== Performance/Stress Tests ====================

#[test]
fn test_encode_large_array() {
    let large_arr: Vec<String> = (0..1000).map(|i| format!("item{}", i)).collect();
    let result = encode_array(&large_arr);

    assert!(result.starts_with(b"*1000\r\n"));
    assert!(result.len() > 10000);
}

#[test]
fn test_encode_many_integers() {
    for i in 0..1000 {
        let result = encode_integer(i);
        assert!(result.starts_with(b":"));
        assert!(result.ends_with(b"\r\n"));
    }
}

#[test]
fn test_encode_bulk_string_various_sizes() {
    for size in [0, 1, 10, 100, 1000] {
        let s: String = "x".repeat(size);
        let result = encode_bulk_string(&s);

        // Check format: $<length>\r\n<content>\r\n
        let expected_len = format!("${}\r\n", size).len() + size + 2;
        assert_eq!(result.len(), expected_len);
    }
}
