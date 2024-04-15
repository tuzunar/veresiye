use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct IndexData {
    pub index_key: String,
    pub value_offset: u64,
    pub value_length: u64,
}

impl IndexData {
    pub fn create(index_key: String, value_offset: u64, value_length: u64) -> Self {
        IndexData {
            index_key,
            value_offset,
            value_length,
        }
    }

    pub fn get(self) -> Self {
        self
    }
}
