use std::collections::BTreeMap;

pub struct memdb {
    data: BTreeMap<String, String>,
    pub buffer: BTreeMap<String, String>,
}

impl memdb {
    pub fn new() -> memdb {
        memdb {
            data: BTreeMap::new(),
            buffer: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, key: &str, value: &str) {
        self.buffer.insert(String::from(key), String::from(value));
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.buffer.get(key)
    }

    pub fn get_hash_table(&self) -> &BTreeMap<String, String> {
        &self.data
    }

    pub fn delete(&mut self, key: &str) -> Option<String> {
        self.buffer.remove(key)
    }

    pub fn append(&mut self, data: BTreeMap<String, String>) {
        self.buffer.extend(data);
    }

    pub fn size(&self) -> usize {
        let key_size = std::mem::size_of::<String>();
        let value_size = std::mem::size_of::<String>();
        let btree_overhead = std::mem::size_of::<BTreeMap<String, String>>();
        let entry_overhead = std::mem::size_of::<(String, String)>();
        let num_entries = self.buffer.len();

        key_size * num_entries
            + value_size * num_entries
            + btree_overhead
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
        self.buffer = BTreeMap::new();
        println!(
            "move buffer to data new buffer size is {}",
            memdb::size(&self)
        )
    }
}

#[cfg(test)]
mod tests {
    use std::ptr::null;

    use super::*;

    #[test]
    fn test_insert() {
        let mut memdb = memdb::new();

        memdb.insert("test", "test_value");
        assert!(memdb.buffer.contains_key("test"));
    }

    #[test]
    fn test_get() {
        let mut memdb = memdb::new();

        memdb.insert("test", "test_value");
        assert_eq!(memdb.get("test").unwrap(), "test_value")
    }

    #[test]
    fn test_delete() {
        let mut memdb = memdb::new();

        memdb.insert("test", "test_value");
        memdb.insert("test1", "test_value1");
        memdb.delete("test");

        assert_eq!(
            match memdb.get("test") {
                Some(v) => v,
                None => "None",
            },
            "None"
        );
        assert_eq!(memdb.get("test1").unwrap(), "test_value1");
    }

    #[test]
    fn test_get_size() {
        let mut memdb = memdb::new();

        memdb.insert("key", "value");
        assert_eq!(memdb.size(), 120);
    }
}
