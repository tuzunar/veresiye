use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, BufRead, Read, Result, Seek, Write},
};

use memmap2::MmapOptions;

use crate::{filter::BloomFilter, util};

use self::{
    footer::{Footer, FOOTER_SIZE},
    index_block::IndexBlock,
    index_data::IndexData,
};

mod footer;
mod index_block;
mod index_data;

pub struct Table {
    file: File,
    bloom: BloomFilter,
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

        Ok(Table { file, bloom })
    }

    pub fn insert(&self, data_block: &HashMap<String, String>) {
        let mut iblock = IndexBlock::create();
        let mut f = &self.file;
        &f.seek(io::SeekFrom::Start(0)).expect("seek error");
        let mut data = String::new();

        for (key, value) in data_block {
            let formatted_data = format!("{}:{},", key, value);
            data.push_str(&formatted_data);
            let value_offset = data.len() - value.len();
            let idata =
                IndexData::create(String::from(key), value_offset as u64, value.len() as u64);
            iblock.append(idata);
        }
        f.write_all(data.as_bytes()).expect("write error");
        let iblock_len = &iblock.get_serialized().len();
        f.write_all(&iblock.get_serialized())
            .expect("index block write error");

        println!("iblock end offset {}", data.len() + iblock_len);

        let footer = Footer::create((data.len()) as u64);

        let footer_start_offset = f.write(&footer.to_bytes()).expect("footer write error");

        println!("footer start offset: {}", footer_start_offset);
        println!("{}", f.metadata().unwrap().len());
    }

    pub fn get(&mut self, key: &str) {
        let mmap = unsafe {
            MmapOptions::new()
                .map_mut(&self.file)
                .expect("mmap file error")
        };
        println!("{}", mmap.len());
        let footer_buf = &mmap[(mmap.len() as usize - FOOTER_SIZE as usize) as usize..mmap.len()];

        let footer = Footer::from_bytes(footer_buf);

        let end_of_index_block = &mmap.len() - (FOOTER_SIZE as usize);

        let index_block_bytes =
            &mmap[(footer.get_index_block_start_offseet() as usize)..end_of_index_block];

        let iblocks = IndexBlock::get_deserialized(&index_block_bytes.to_vec());

        let iblock = iblocks
            .index_block
            .into_iter()
            .filter(|ib| ib.index_key == key)
            .next()
            .unwrap();
        let value_bytes = &mmap[iblock.value_offset as usize - 1..][..iblock.value_length as usize];
        println!("value is {}", String::from_utf8_lossy(value_bytes));
        // self.file.seek(io::SeekFrom::End(-(FOOTER_SIZE as i64)));
        // let mut footer_bytes = [0u8; 8];
        // self.file
        //     .read_exact(&mut footer_bytes)
        //     .expect("exact read error");
        // let footer = Footer::from_bytes(&footer_bytes);
        // let mut index_block_bytes;
        // self.file.seek()
        // let iblock = IndexBlock::get_deserialized(iblock);
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
            println!(
                "{:?}",
                util::convert_byte_to_str(filter.as_bytes()).unwrap()
            );
            return Ok(filter);
        }

        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Filter not found",
        ))
    }
}
