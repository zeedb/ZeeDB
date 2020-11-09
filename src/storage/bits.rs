use std::sync::atomic::{AtomicU8, Ordering};

#[derive(Clone)]
pub struct Bits {
    bytes: Vec<u8>,
}

impl Bits {
    pub fn zeros(len: usize) -> Self {
        Self {
            bytes: vec![0].repeat(len / 8),
        }
    }

    pub fn get(&self, i: usize) -> bool {
        (self.bytes[i >> 3] & BIT_MASK[i & 7]) != 0
    }

    pub fn set(&mut self, i: usize, value: bool) {
        if value {
            self.bytes[i >> 3] |= BIT_MASK[i & 7];
        } else {
            self.bytes[i >> 3] ^= BIT_MASK[i & 7];
        }
    }
}

pub static BIT_MASK: [u8; 8] = [1, 2, 4, 8, 16, 32, 64, 128];
