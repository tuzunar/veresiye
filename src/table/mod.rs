use std::{collections::BTreeMap, fs::{File, OpenOptions}, io::{self, BufRead, BufReader, Read, Seek, Write, Result}};

use crate::{filter::BloomFilter, util};

mod index_block;
mod index_data;

pub struct Table {
    index: BTreeMap<String, u64>,
    file: File,
    bloom: BloomFilter
}

impl Table {
    pub fn new(filename: &str) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(filename)?;

         let bloom = BloomFilter::create(10000, 0.001f64);
         let filter = format!("{:?}", &bloom.get_filter());
         file.write(&bloom.get_filter()).expect("cannot write bloom filter to file");

         writeln!(file).unwrap();

        Ok(Table {
            index: BTreeMap::new(),
            file,
            bloom
        })
    }

    pub fn insert(&mut self, key: &str, value: &str) -> io::Result<()> {
        let position = self.file.metadata()?.len();

        

        let data_entry = format!("{}:{},", key, value).as_bytes().to_vec();

        self.file.write_all(&data_entry)?;
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

    pub fn get_filter(&mut self) -> Result<String> {
      let mut file = &self.file;
      file.seek(io::SeekFrom::Start(0)).expect("seek error");

      // let mut buffer = BufReader::new(file);

      let mut read: Vec<u8> = Vec::new();
      file.read_to_end(&mut read).unwrap();

      // if let Some(Ok(filter)) = buffer.lines().next() {
      //    if filter.starts_with("[") && filter.ends_with("]") {
      //       return Ok(filter)
      //    }
      // }

      if let Some(Ok(filter)) = read.lines().next() {
         // if filter.starts_with("[") && filter.ends_with("]") {
         //    return Ok(filter)
         // }
         println!("{:?}", util::convert_byte_to_str(filter.as_bytes()).unwrap());
         return Ok(filter)
      }



      Err(io::Error::new(io::ErrorKind::InvalidData, "Filter not found")) 
    }
}

