use std::collections::HashMap;

pub struct memdb {
    data: HashMap<String, String>,
    pub buffer: HashMap<String, String>,
}

impl memdb {
    pub fn new() -> memdb {
        memdb {
            data: HashMap::new(),
            buffer: HashMap::new(),
        }
    }

    pub fn insert(&mut self, key: &str, value: &str) {
        self.buffer.insert(String::from(key), String::from(value));
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.buffer.get(key)
    }

    pub fn get_hash_table(&self) -> &HashMap<String, String> {
        &self.data
    }

    pub fn delete(&mut self, key: &str) -> Option<String> {
        self.buffer.remove(key)
    }

    pub fn append(&mut self, data: HashMap<String, String>) {
        self.buffer.extend(data);
    }

    pub fn size(&self) -> usize {
        let key_size = std::mem::size_of::<String>();
        let value_size = std::mem::size_of::<String>();
        let hashmap_overhead = std::mem::size_of::<HashMap<String, String>>();
        let entry_overhead = std::mem::size_of::<(String, String)>();
        let num_entries = self.buffer.len();

        key_size * num_entries
            + value_size * num_entries
            + hashmap_overhead
            + entry_overhead * num_entries
    }

    fn clear(&mut self) {
        self.buffer.clear()
    }

    pub fn move_buffer_to_data(&mut self) {
        println!(
            "before move buffer to data new buffer size is {}",
            memdb::size(&self)
        );
        self.data = self.buffer.clone();
        self.buffer = HashMap::new();
        println!(
            "move buffer to data new buffer size is {}",
            memdb::size(&self)
        )
    }
}
