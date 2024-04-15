use serde::{Deserialize, Serialize};

use super::index_data::IndexData;

#[derive(Serialize, Deserialize, Debug)]
pub struct IndexBlock {
    pub index_block: Vec<IndexData>,
}

impl IndexBlock {
    pub fn create() -> Self {
        let index_data: Vec<IndexData> = vec![];
        Self {
            index_block: index_data,
        }
    }

    pub fn append(&mut self, index_data: IndexData) {
        self.index_block.push(index_data)
    }

    pub fn get_serialized(&self) -> Vec<u8> {
        let serialized_block = serde_json::to_vec(&self).unwrap();
        serialized_block
    }

    pub fn get_deserialized(iblock: &Vec<u8>) -> Self {
        let deserialized: IndexBlock = serde_json::from_slice(&iblock).expect("deserialized error");

        deserialized
    }
}
