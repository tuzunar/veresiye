pub struct Footer {
   index_block_start_offset: u64
}

impl Footer {
    pub fn create(index_block_start_offset: u64) -> Self {
      Self {
         index_block_start_offset
      }
    }

   pub fn to_bytes(&self) -> [u8; 8] {
      self.index_block_start_offset.to_le_bytes()
    }

   pub fn from_bytes(bytes: &[u8]) -> Self {
      let mut byte_array = [0u8; 8];
      byte_array.copy_from_slice(&bytes[0..8]);
      let index_block_start_offset = u64::from_le_bytes(byte_array);
      println!("{:?}", bytes);
      Footer { index_block_start_offset }
    }
}