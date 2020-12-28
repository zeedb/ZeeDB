use crate::byte_key::*;

#[test]
fn test_i32() {
    assert_ordered(vec![
        byte_key_i32(i32::MIN).to_vec(),
        byte_key_i32(-2i32).to_vec(),
        byte_key_i32(-1i32).to_vec(),
        byte_key_i32(0i32).to_vec(),
        byte_key_i32(1i32).to_vec(),
        byte_key_i32(2i32).to_vec(),
        byte_key_i32(i32::MAX).to_vec(),
    ]);
}

#[test]
fn test_i64() {
    assert_ordered(vec![
        byte_key_i64(i64::MIN).to_vec(),
        byte_key_i64(-2i64).to_vec(),
        byte_key_i64(-1i64).to_vec(),
        byte_key_i64(0i64).to_vec(),
        byte_key_i64(1i64).to_vec(),
        byte_key_i64(2i64).to_vec(),
        byte_key_i64(i64::MAX).to_vec(),
    ]);
}

#[test]
fn test_f64() {
    assert_ordered(vec![
        byte_key_f64(f64::NEG_INFINITY).to_vec(),
        byte_key_f64(f64::MIN).to_vec(),
        byte_key_f64(-1f64).to_vec(),
        byte_key_f64(-f64::from_bits(2)).to_vec(),
        byte_key_f64(-f64::from_bits(1)).to_vec(),
        byte_key_f64(-0f64).to_vec(),
        byte_key_f64(0f64).to_vec(),
        byte_key_f64(f64::from_bits(1)).to_vec(),
        byte_key_f64(f64::from_bits(2)).to_vec(),
        byte_key_f64(1f64).to_vec(),
        byte_key_f64(f64::MAX).to_vec(),
        byte_key_f64(f64::INFINITY).to_vec(),
        byte_key_f64(f64::NAN).to_vec(),
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
