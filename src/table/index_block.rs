use super::index_data::IndexData;

pub struct IndexBlock {
   index_block: Vec<IndexData>
}

impl IndexBlock {
   pub fn create() -> Self {
      let index_data: Vec<IndexData> = vec![];
      Self {
         index_block:  index_data
      }
   }

   pub fn append(&mut self, index_data: IndexData) {
      self.index_block.push(index_data)
   } 

   
}