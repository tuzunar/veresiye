use std::collections::HashMap;

pub struct memdb {
   data: HashMap<String, String>
}

impl memdb {
   pub fn new() -> memdb {
      memdb { data: HashMap::new() }
   }

   pub fn insert(&mut self, key: &str, value: &str) {
      self.data.insert(String::from(key), String::from(value));
   }

   pub fn get(&self, key: &str) -> Option<&String> {
      self.data.get(key)
   }

   pub fn get_hash_table(&self) -> &HashMap<String, String> {
      &self.data
   }

   pub fn delete(&mut self, key: &str) -> Option<String> {
      self.data.remove(key)
   }

   pub fn size(&self) -> usize {
      let key_size = std::mem::size_of::<String>();
      let value_size = std::mem::size_of::<String>();
      let hashmap_overhead = std::mem::size_of::<HashMap<String, String>>();
      let entry_overhead = std::mem::size_of::<(String, String)>();
      let num_entries = self.data.len();

      key_size * num_entries + value_size * num_entries + hashmap_overhead + entry_overhead * num_entries
   }

   pub fn clear(&mut self) {
      self.data.clear()
   }
}
