use super::varint64::{read, write, MAX_LEN};

macro_rules! assert_round_trip {
    ($input:expr, $bytes:expr) => {
        let mut buf = [0 as u8; MAX_LEN];
        let size = $bytes.len();
        assert_eq!(size, read($input, &mut buf));
        let mut output = 0 as i64;
        assert_eq!(size, write(&mut output, &buf[..]));
        assert_eq!($input, output);
    };
}

#[test]
#[rustfmt::skip]
fn test_read() {
    assert_round_trip!(1, [0b00000010]);
    assert_round_trip!(-1, [0b00000001]);
    assert_round_trip!(std::i64::MAX, [0b10000001, 0b11111111, 0b11111111, 0b11111111, 0b11111111, 0b11111111, 0b11111111, 0b11111111, 0b11111111, 0b01111110]);
    assert_round_trip!(std::i64::MIN, [0b10000001, 0b11111111, 0b11111111, 0b11111111, 0b11111111, 0b11111111, 0b11111111, 0b11111111, 0b11111111, 0b01111111]);
}
