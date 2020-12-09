use crate::byte_key::*;

#[test]
fn test_i32() {
    assert_ordered(vec![
        ByteKey::key(i32::MIN),
        ByteKey::key(-2i32),
        ByteKey::key(-1i32),
        ByteKey::key(0i32),
        ByteKey::key(1i32),
        ByteKey::key(2i32),
        ByteKey::key(i32::MAX),
    ]);
}

#[test]
fn test_i64() {
    assert_ordered(vec![
        ByteKey::key(i64::MIN),
        ByteKey::key(-2i64),
        ByteKey::key(-1i64),
        ByteKey::key(0i64),
        ByteKey::key(1i64),
        ByteKey::key(2i64),
        ByteKey::key(i64::MAX),
    ]);
}

#[test]
fn test_f64() {
    assert_ordered(vec![
        ByteKey::key(f64::NEG_INFINITY),
        ByteKey::key(f64::MIN),
        ByteKey::key(-1f64),
        ByteKey::key(-f64::from_bits(2)),
        ByteKey::key(-f64::from_bits(1)),
        ByteKey::key(-0f64),
        ByteKey::key(0f64),
        ByteKey::key(f64::from_bits(1)),
        ByteKey::key(f64::from_bits(2)),
        ByteKey::key(1f64),
        ByteKey::key(f64::MAX),
        ByteKey::key(f64::INFINITY),
        ByteKey::key(f64::NAN),
    ])
}

fn assert_ordered(examples: Vec<Vec<u8>>) {
    for i in 0..examples.len() {
        for j in 0..examples.len() {
            if i < j {
                assert!(examples[i] < examples[j]);
            } else if j < i {
                assert!(examples[j] < examples[i]);
            } else {
                assert!(examples[i] == examples[j]);
            }
        }
    }
}
