use core::str;
use std::{
    cmp::Ordering,
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{self, BufRead, Read, Result, Seek, Write},
    path::PathBuf,
};

use memmap2::{Mmap, MmapOptions};

use crate::{filter::BloomFilter, util};

use self::{
    footer::{Footer, FOOTER_SIZE},
    index_block::IndexBlock,
    index_data::IndexData,
};

mod footer;
mod index_block;
mod index_data;

#[derive(Debug)]
pub struct Table {
    file: File,
    path: PathBuf,
    level: usize,
    //  bloom: BloomFilter,
}

impl Table {
    pub fn new(filename: &str, level: usize) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(filename)?;

        let bloom = BloomFilter::create(10000, 0.001f64);
        let path = PathBuf::from(filename);
        // let filter = format!("{:?}", &bloom.get_filter());
        // file.write(&bloom.get_filter()).expect("cannot write bloom filter to file");

        // writeln!(file).unwrap();

        Ok(Table { file, level, path })
    }

    pub fn open(file_path: &str) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .open(file_path)
            .expect("cannot open table file");
        let sstable_dir: Vec<&str> = file_path.split("/").collect();
        let sstable_labels: Vec<&str> = sstable_dir[2].split("_").collect();
        let sstable_level: &str = sstable_labels[1];
        let path = PathBuf::from(file_path);
        Ok(Table {
            file,
            level: sstable_level.parse::<usize>().unwrap(),
            path,
        })
    }

    pub fn insert(&self, data_block: &BTreeMap<String, String>) {
        let mut iblock = IndexBlock::create();
        let mut f = &self.file;
        f.seek(io::SeekFrom::Start(0)).expect("seek error");
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
    }

    pub fn get_table_path(&self) -> &PathBuf {
        &self.path
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let mmap = unsafe { MmapOptions::new().map(&self.file).expect("mmap file error") };

        let iblocks = get_index_block(&mmap, FOOTER_SIZE);

        let iblock: Vec<IndexData> = iblocks
            .index_block
            .into_iter()
            .filter(|ib| ib.index_key == key)
            .collect();

        if iblock.len() > 0 {
            let value_bytes =
                &mmap[iblock[0].value_offset as usize - 1..][..iblock[0].value_length as usize];
            Some(String::from_utf8_lossy(value_bytes).to_string())
        } else {
            // println!("value not found");
            None
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

    pub fn get_table_level(&self) -> usize {
        self.level
    }
}

impl Clone for Table {
    fn clone(&self) -> Self {
        Self {
            file: self.file.try_clone().expect("failed to clone file"),
            path: self.path.clone(),
            level: self.level.clone(),
        }
    }
}

impl PartialOrd for Table {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Table {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.path).cmp(&(other.path))
    }
}

impl PartialEq for Table {
    fn eq(&self, other: &Self) -> bool {
        (self.path) == (other.path)
    }
}

impl Eq for Table {}

///
/// Reconstruct BTree from sstable and return it.
///
/// @return BTreeMap<String, String>
///
pub fn reconstruct_tree_from_sstable(file: File) -> BTreeMap<String, String> {
    let mut reconstructed_tree: BTreeMap<String, String> = BTreeMap::new();

    let mmap: Mmap = unsafe { MmapOptions::new().map(&file).expect("cannot map the file") };

    let footer: Footer = get_footer(&mmap, FOOTER_SIZE);

    let raw_data = &mmap[(..footer.get_index_block_start_offseet() as usize)];

    //parse raw data to &str
    let parsed_data = match str::from_utf8(raw_data) {
        Ok(v) => v,
        Err(e) => panic!("cannot parse raw data because: {}", e),
    };

    let mut key_value_pairs: Vec<&str> = parsed_data.split(",").collect();

    key_value_pairs.pop().unwrap();

    for pairs in key_value_pairs {
        let pair: Vec<&str> = pairs.split(":").collect();
        let key: &str = pair[0];
        let value: &str = pair[1];

        reconstructed_tree.insert(String::from(key), String::from(value));
    }

    reconstructed_tree
}

fn get_footer(mmap: &Mmap, footer_size: usize) -> Footer {
    let footer_bytes = &mmap[(mmap.len() - FOOTER_SIZE..mmap.len())];

    let footer: Footer = Footer::from_bytes(footer_bytes);

    footer
}

fn get_index_block(mmap: &Mmap, footer_size: usize) -> IndexBlock {
    let footer: Footer = get_footer(mmap, FOOTER_SIZE);

    let end_of_index_block = mmap.len() - FOOTER_SIZE;

    let index_block_bytes =
        &mmap[(footer.get_index_block_start_offseet() as usize)..end_of_index_block];

    let iblocks = IndexBlock::get_deserialized(&index_block_bytes.to_vec());

    iblocks
}
