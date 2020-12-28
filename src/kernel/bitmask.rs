#[derive(Clone, Debug)]
pub struct Bitmask {
    values: Vec<u8>,
    len: usize,
}

impl Bitmask {
    pub fn new() -> Self {
        Self {
            values: vec![],
            len: 0,
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            values: Vec::with_capacity((capacity + 7) / 8),
            len: 0,
        }
    }

    pub fn from_slice(values: &[u8], len: usize) -> Self {
        Self {
            values: values.to_vec(),
            len,
        }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn falses(len: usize) -> Self {
        Self {
            values: vec![0].repeat((len + 7) / 8),
            len,
        }
    }

    pub fn trues(len: usize) -> Self {
        Self {
            values: vec![u8::MAX].repeat((len + 7) / 8),
            len,
        }
    }

    pub fn get(&self, index: usize) -> bool {
        get_bit(&self.values, index)
    }

    pub fn set(&mut self, index: usize, value: bool) {
        if value {
            set_bit(&mut self.values, index)
        } else {
            unset_bit(&mut self.values, index)
        }
    }

    pub fn push(&mut self, value: bool) {
        let index = self.len;
        self.len += 1;
        if self.values.len() * 8 < self.len {
            self.values.push(0)
        }
        self.set(index, value)
    }
}

pub fn get_bit(data: &[u8], i: usize) -> bool {
    (data[i >> 3] & BIT_MASK[i & 7]) != 0
}

unsafe fn get_bit_raw(data: *const u8, i: usize) -> bool {
    (*data.add(i >> 3) & BIT_MASK[i & 7]) != 0
}

pub fn set_bit(data: &mut [u8], i: usize) {
    data[i >> 3] |= BIT_MASK[i & 7];
}

unsafe fn set_bit_raw(data: *mut u8, i: usize) {
    *data.add(i >> 3) |= BIT_MASK[i & 7];
}

pub fn unset_bit(data: &mut [u8], i: usize) {
    data[i >> 3] &= !BIT_MASK[i & 7];
}

unsafe fn unset_bit_raw(data: *mut u8, i: usize) {
    *data.add(i >> 3) &= !BIT_MASK[i & 7];
}

static BIT_MASK: [u8; 8] = [1, 2, 4, 8, 16, 32, 64, 128];
