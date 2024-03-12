use std::{collections::BTreeMap, fs::{File, OpenOptions}, io::{self, Read, Seek, Write}};

pub struct Table {
    index: BTreeMap<String, u64>,
    file: File
}

impl Table {
    pub fn new(filename: &str) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(filename)?;

        Ok(Table {
            index: BTreeMap::new(),
            file
        })
    }

    pub fn insert(&mut self, key: &str, value: &str) -> io::Result<()> {
        let position = self.file.metadata()?.len();

        self.file.write_all(value.as_bytes())?;
        self.index.insert(key.to_string(), position);
        Ok(())
    }
    
    pub fn get(&mut self, key: &str) -> io::Result<Option<String>> {
        if let Some(&position) = self.index.get(key) {
            let mut buffer = String::new();

            self.file.seek(io::SeekFrom::Start(position))?;
            self.file.read_to_string(&mut buffer)?;
            Ok(Some(buffer))
        } else {
            Ok(None)
        }
    }

}

