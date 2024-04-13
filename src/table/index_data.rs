pub struct IndexData {
   index_key: String,
   value_offset: u64,
   value_length: u64
}

impl IndexData {
   pub fn create(index_key: String, value_offset: u64, value_length:u64) -> Self {
      IndexData { index_key, value_offset, value_length }
   } 
}