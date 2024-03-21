use std::{
    fs::{ File, OpenOptions}, io::{self, BufRead, BufReader, Read, Result, Seek, SeekFrom, Write}, path::Path, sync::Mutex, time::{SystemTime, UNIX_EPOCH}, u8, usize
};

use crate::util;



const DEFAULT_ENTRY_LIMIT: usize = 10 << 10;

#[derive(Debug)]
pub struct Segment {
    file: Mutex<File>,

    entry_number: usize,
    entry_limit: usize,
}

impl Segment {
    pub fn new(path: String, limit: usize) -> Result<Segment> {
        let file = OpenOptions::new().write(true).read(true).open(&path)?;
        let path_parts: &Vec<&str> = &path.split("/").collect();

        if path_parts[2].len() != 20 {
            eprintln!("Wrong log file");
        }

        if Path::new(&path).is_dir() {
            eprintln!("Wrong log file");
        }

        let reader = BufReader::new(&file);
        let line_count = reader.lines().count();

        Ok(Segment {
            entry_limit: get_entry_limit(limit),
            entry_number: line_count,
            file: Mutex::new(file),
        })
    }

    pub fn open(dir: &str, sequence: u64, limit: usize, create: bool) -> Result<Segment> {
        let fname = Path::new(dir).join(util::segment_name(sequence));
        let file = OpenOptions::new()
            .create(create)
            .read(true)
            .write(true)
            .open(&fname)?;

        Ok(Segment {
            entry_number: 0,
            entry_limit: get_entry_limit(limit),
            file: Mutex::new(file),
        })
    }

    pub fn read(&self) -> Result<String> {
        let mut file = self.file.lock().expect("file lock error");
        let mut content:String = String::new();

        file.seek(SeekFrom::Start(0))?;
        file.read_to_string(&mut content).expect("read to string error");

        Ok(content)
    }

    pub fn write(&mut self, entry: &[u8]) -> io::Result<()> {
        let mut file = self.file.lock().unwrap();
        let timestamp = util::get_timestamp();
        let entry_str = match std::str::from_utf8(entry) {
            Ok(s) => s,
            Err(e) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Write error: {}", e),
                ));
            }
        };
        let checksum = util::calculate_checksum(entry_str);
        let log_entry = format!("{:?}#{}#{}#{:?}", timestamp, entry.len(), checksum, entry);
        writeln!(file, "{}", log_entry)?;

        file.flush()?;

        self.entry_number += 1;

        Ok(())
    }

    pub fn space(&self) -> usize {
        self.entry_limit - self.entry_number
    }

    pub fn get_segment_limit(&self) -> usize {
        self.entry_limit
    }

    pub fn get_segment_path(&self) {
      let segment = &self.file.lock().unwrap();

      segment.

      let path = match segment.metadata() {
         Ok(metadata) => metadata.can
      }
    }

    pub fn get_segment_created_time(&self) -> Result<SystemTime> {
      let segment = &self.file.lock().unwrap();

      let created_at = segment.metadata().unwrap().created().expect("cannot read create time of the segment");

      Ok(created_at)
    }

    pub fn check_log_integrity(file: &File) -> Result<bool> {
        let reader = BufReader::new(file);

        for line in reader.lines() {
            let entry = line?;
            let log_parts: Vec<&str> = entry.split('#').collect();

            if log_parts.len() != 4 {
                return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "Invalid WAL Entry"
                        ))
            }

            let log_checksum = log_parts[2];
            let log_data: Vec<u8> =  parse_byte(log_parts[3]);
            
            let control_checksum = util::calculate_checksum(convert_byte_to_str(&log_data).expect("convert error"));
            if log_checksum != control_checksum {
                return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "Corrupted WAL Entry"
                        ))
            }
        }
        Ok(true)
    }
}


fn get_entry_limit(limit: usize) -> usize {
    if limit <= 0 {
        println!("Entry limit must be greater than zero. Default entry limit is 1024");
        DEFAULT_ENTRY_LIMIT
    } else {
        limit
    }
}

fn convert_byte_to_str(entry: &[u8]) -> io::Result<&str> {
   let entry_str = match std::str::from_utf8(&entry) {
      Ok(s) => s,
      Err(e) => {
            return Err(io::Error::new(
               io::ErrorKind::InvalidData,
               format!("Write error: {}", e),
            ));
      }
   };
   Ok(entry_str)
}

fn parse_byte(value: &str) -> Vec<u8> {
   let bytes: Vec<u8> =  value.split(", ")
   .filter_map(|s| {
         if let Ok(byte) = s.trim_matches(|c| c == '[' || c == ']').parse::<u8>() {
            Some(byte)
         } else {
            None
         }
   })
   .collect();

   bytes
}