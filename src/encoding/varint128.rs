pub fn read(x: i128, buf: &mut [u8]) -> usize {
    // undo zigzag encoding
    let mut ux = (x as u128) << 1;
    if x < 0 {
        ux = !ux
    }
    // read from least-significant to most-significant byte
    let mut i = 0;
    // read first byte
    buf[i] = ux as u8 & 0b01111111;
    ux >>= 7;
    i += 1;
    // read remaining bytes
    while ux > 0 {
        buf[i] = ux as u8 & 0b01111111 | 0b10000000;
        ux >>= 7;
        i += 1;
    }
    // change little-endian to big-endian
    buf[..i].reverse();
    i
}

pub fn write(x: &mut i128, buf: &[u8]) -> usize {
    let mut ux = 0 as u128;
    let mut i = 0;
    // read continuation bytes
    while buf[i] & 0b10000000 != 0 {
        ux <<= 7;
        ux |= buf[i] as u128 & 0b01111111;
        i += 1;
    }
    // read last byte
    ux <<= 7;
    ux |= buf[i] as u128 & 0b01111111;
    i += 1;
    // undo zigzag encoding
    *x = (ux >> 1) as i128;
    if ux & 1 != 0 {
        *x = !*x;
    }
    i
}

pub const MAX_LEN: usize = 19;
