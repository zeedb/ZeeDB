// Key serialization strategies from "The Adaptive Radix Tree: ARTful Indexing for Main-Memory Databases"
// https://db.in.tum.de/~leis/papers/ART.pdf

pub trait ByteKey {
    fn key(self) -> Vec<u8>;
}

impl ByteKey for i64 {
    fn key(self) -> Vec<u8> {
        let flip_sign_bit = self ^ (1 << 63);
        flip_sign_bit.to_be_bytes().to_vec()
    }
}

impl ByteKey for i32 {
    fn key(self) -> Vec<u8> {
        let flip_sign_bit = self ^ (1 << 31);
        flip_sign_bit.to_be_bytes().to_vec()
    }
}

impl ByteKey for f64 {
    fn key(self) -> Vec<u8> {
        let bits = self.to_bits();
        let bits = if bits & (1 << 63) != 0 {
            bits ^ !0
        } else {
            bits ^ (1 << 63)
        };
        bits.to_be_bytes().to_vec()
    }
}

impl ByteKey for u64 {
    fn key(self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }
}

impl ByteKey for u32 {
    fn key(self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }
}

// 0111111111111000000000000000000000000000000000000000000000000000        1111111111111000000000000000000000000000000000000000000000000000        f64::NAN                nan
// 0111111111110000000000000000000000000000000000000000000000000000        1111111111110000000000000000000000000000000000000000000000000000        f64::INFINITY           positive infinity
// 1111111111110000000000000000000000000000000000000000000000000000        0111111111110000000000000000000000000000000000000000000000000000        f64::NEG_INFINITY       negative infinity
// 0000000000000000000000000000000000000000000000000000000000000000        1000000000000000000000000000000000000000000000000000000000000000        0.0                     positive zero
// 1000000000000000000000000000000000000000000000000000000000000000        0000000000000000000000000000000000000000000000000000000000000000        -0.0                    negative zero
// 0000000000000000000000000000000000000000000000000000000000000001        1000000000000000000000000000000000000000000000000000000000000001        f64::from_bits(1)       positive subnormal 0.0000000000000000000000000000000000000000000000000001
// 1000000000000000000000000000000000000000000000000000000000000001        0000000000000000000000000000000000000000000000000000000000000001        -f64::from_bits(1)      negative subnormal 0.0000000000000000000000000000000000000000000000000001
// 0000000000000000000000000000000000000000000000000000000000000010        1000000000000000000000000000000000000000000000000000000000000010        f64::from_bits(2)       positive subnormal 0.0000000000000000000000000000000000000000000000000010
// 1000000000000000000000000000000000000000000000000000000000000010        0000000000000000000000000000000000000000000000000000000000000010        -f64::from_bits(2)      negative subnormal 0.0000000000000000000000000000000000000000000000000010
// 0011111111110000000000000000000000000000000000000000000000000000        1011111111110000000000000000000000000000000000000000000000000000        1.0                     positive 2e00000000000 1.0000000000000000000000000000000000000000000000000000
// 1011111111110000000000000000000000000000000000000000000000000000        0011111111110000000000000000000000000000000000000000000000000000        -1.0                    negative 2e00000000000 1.0000000000000000000000000000000000000000000000000000
// 0111111111101111111111111111111111111111111111111111111111111111        1111111111101111111111111111111111111111111111111111111111111111        f64::MAX                positive 2e01111111111 1.1111111111111111111111111111111111111111111111111111
// 1111111111101111111111111111111111111111111111111111111111111111        0111111111101111111111111111111111111111111111111111111111111111        f64::MIN                negative 2e01111111111 1.1111111111111111111111111111111111111111111111111111

// fn main() {
//     print("f64::NAN\t", f64::NAN);
//     print("f64::INFINITY\t", f64::INFINITY);
//     print("f64::NEG_INFINITY", f64::NEG_INFINITY);
//     print("0.0\t\t", 0.0);
//     print("-0.0\t\t", -0.0);
//     print("f64::from_bits(1)", f64::from_bits(1));
//     print("-f64::from_bits(1)", -f64::from_bits(1));
//     print("f64::from_bits(2)", f64::from_bits(2));
//     print("-f64::from_bits(2)", -f64::from_bits(2));
//     print("1.0\t\t", 1.0);
//     print("-1.0\t\t", -1.0);
//     print("f64::MAX\t", f64::MAX);
//     print("f64::MIN\t", f64::MIN);
// }

// fn print(name: &str, number: f64) {
//     println!(
//         "{:064b}\t{:064b}\t{:064b}\t{}\t{}",
//         number.to_bits(),
//         order(number),
//         f64_to_bytes(number),
//         name,
//         bits(number)
//     );
// }

// fn bits(number: f64) -> String {
//     const SIGN_MASK: u64 = 0x8000_0000_0000_0000;
//     const EXP_MASK: u64 = 0x7ff0000000000000;
//     const MAN_MASK: u64 = 0x000fffffffffffff;
//     let bits = number.to_bits();
//     match (bits & SIGN_MASK, bits & EXP_MASK, bits & MAN_MASK) {
//         (0, exp, man) if exp == EXP_MASK && man != 0 => "nan".to_string(),
//         (sign, exp, 0) if sign == SIGN_MASK && exp == EXP_MASK => "negative infinity".to_string(),
//         (0, exp, 0) if exp == EXP_MASK => "positive infinity".to_string(),
//         (0, 0, 0) => "positive zero".to_string(),
//         (sign, 0, 0) if sign == SIGN_MASK => "negative zero".to_string(),
//         (0, 0, man) => format!("positive subnormal 0.{:052b}", man),
//         (sign, 0, man) if sign == SIGN_MASK => format!("negative subnormal 0.{:052b}", man),
//         (0, exp, man) => format!("positive 2e{:011b} 1.{:052b}", unbias(exp), man),
//         (sign, exp, man) if sign == SIGN_MASK => {
//             format!("negative 2e{:011b} 1.{:052b}", unbias(exp), man)
//         }
//         _ => panic!(),
//     }
// }

// fn unbias(exp: u64) -> i64 {
//     (exp >> 52) as i64 - 1023
// }

// fn order(number: f64) -> u64 {
//     const SIGN_MASK: u64 = 0x8000_0000_0000_0000;
//     let bits = number.to_bits();
//     bits ^ SIGN_MASK
// }

// pub fn f64_to_bytes(self: f64) -> u64 {
//     const SIGN_MASK: u64 = 0x8000_0000_0000_0000;
//     let bits = self.to_bits();
//     if bits & SIGN_MASK == SIGN_MASK {
//         bits ^ !0
//     } else {
//         bits ^ SIGN_MASK
//     }
// }
