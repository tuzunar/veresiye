use std::{collections::{BTreeMap, HashMap}, fs::{File, OpenOptions}, io::{self, BufRead, BufReader, BufWriter, Read, Result, Seek, Write}};

use serde::Serialize;

use crate::{filter::BloomFilter, util};

use self::{footer::Footer, index_block::IndexBlock, index_data::IndexData};

mod index_block;
mod index_data;
mod footer;

pub struct Table {
    index: BTreeMap<String, u64>,
    file: File,
    bloom: BloomFilter
}

impl Table {
    pub fn new(filename: &str) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(filename)?;

         let bloom = BloomFilter::create(10000, 0.001f64);
         // let filter = format!("{:?}", &bloom.get_filter());
         // file.write(&bloom.get_filter()).expect("cannot write bloom filter to file");

         // writeln!(file).unwrap();

        Ok(Table {
            index: BTreeMap::new(),
            file,
            bloom
        })
    }

    pub fn insert(&self, data_block: &HashMap<String, String>) {
      let mut iblock = IndexBlock::create();
      let f = &self.file;
      let mut b_write = BufWriter::new(f);
      &f.seek(io::SeekFrom::Start(0)).expect("seek error");
      b_write.seek(io::SeekFrom::Start(0)).expect("seek error");
      let mut data = String::new();

      for (key, value) in data_block {
         let formatted_data = format!("{}:{},", key, value);
         data.push_str(&formatted_data);
         let value_offset = data.len() - value.len();
         let idata = IndexData::create(String::from(key), value_offset as u64, value.len() as u64);
         iblock.append(idata);
      }
      f.set_len(data.len() as u64).expect("set len error ");
      b_write.write_all(data.as_bytes()).expect("write error");
      let iblock_len = &iblock.get_serialized().len();
      f.set_len((data.len() +*iblock_len) as u64).expect("set len error");
       b_write.write_all(&iblock.get_serialized()).expect("index block write error");

      println!("iblock end offset {}", data.len() + iblock_len);

      let footer = Footer::create((data.len() + iblock_len) as u64);

      f.set_len((data.len() + iblock_len + footer.to_bytes().len()) as u64).expect("set len error");
      let footer_start_offset = b_write.write(&footer.to_bytes()).expect("footer write error");

      println!("footer start offset: {}", footer_start_offset);
      println!("{}", f.metadata().unwrap().len());
      b_write.flush().expect("flush error");
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

