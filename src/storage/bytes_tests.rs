use crate::bytes::*;

#[test]
fn test_bool() {
    assert_ordered(vec![bool_key(false), bool_key(true)]);
}

#[test]
fn test_i32() {
    assert_ordered(vec![
        i32_key(i32::MIN),
        i32_key(-2i32),
        i32_key(-1i32),
        i32_key(0i32),
        i32_key(1i32),
        i32_key(2i32),
        i32_key(i32::MAX),
    ]);
}

#[test]
fn test_i64() {
    assert_ordered(vec![
        i64_key(i64::MIN),
        i64_key(-2i64),
        i64_key(-1i64),
        i64_key(0i64),
        i64_key(1i64),
        i64_key(2i64),
        i64_key(i64::MAX),
    ]);
}

#[test]
fn test_f64() {
    assert_ordered(vec![
        f64_key(f64::NEG_INFINITY),
        f64_key(f64::MIN),
        f64_key(-1f64),
        f64_key(-f64::from_bits(2)),
        f64_key(-f64::from_bits(1)),
        f64_key(-0f64),
        f64_key(0f64),
        f64_key(f64::from_bits(1)),
        f64_key(f64::from_bits(2)),
        f64_key(1f64),
        f64_key(f64::MAX),
        f64_key(f64::INFINITY),
        f64_key(f64::NAN),
    ])
}

fn assert_ordered(examples: Vec<[u8; 8]>) {
    for i in 0..examples.len() {
        for j in 0..examples.len() {
            if i < j {
                assert!(examples[i] < examples[j]);
            } else if j < i {
                assert!(examples[j] < examples[i]);
            }
        }
    }
}
