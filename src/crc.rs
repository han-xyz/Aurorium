/// The CRC-32 KingsIsle uses in its patch manifests: reflected polynomial `0xEDB88320`,
/// initial value 0 and no final XOR, so it differs from CRC-32/IEEE.
#[derive(Default)]
pub struct Crc32(u32);

const POLYNOMIAL: u32 = 0xEDB8_8320;

const TABLE: [u32; 256] = {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ POLYNOMIAL
            } else {
                crc >> 1
            };
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
};

impl Crc32 {
    pub fn update(&mut self, data: &[u8]) {
        for &byte in data {
            self.0 = TABLE[((self.0 ^ u32::from(byte)) & 0xFF) as usize] ^ (self.0 >> 8);
        }
    }

    pub fn value(&self) -> u32 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::Crc32;

    fn checksum(data: &[u8]) -> u32 {
        let mut crc = Crc32::default();
        crc.update(data);
        crc.value()
    }

    #[test]
    fn kingsisle_crc_vectors() {
        assert_eq!(checksum(b""), 0x0000_0000);
        assert_eq!(checksum(b"123456789"), 0x2dfd_2d88);
        assert_eq!(checksum(b"a"), 0x3ab5_51ce);
        assert_eq!(
            checksum(b"The quick brown fox jumps over the lazy dog"),
            0xb9c6_0808
        );
    }
}
