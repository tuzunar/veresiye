use std::{
    fs::{ File, OpenOptions}, io::{self, BufRead, BufReader, Read, Result, Seek, SeekFrom, Write}, path::Path, sync::Mutex, time::{SystemTime, UNIX_EPOCH}, u8, usize
};

use sha256::digest;

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
        let fname = Path::new(dir).join(segment_name(sequence));
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
        let timestamp = get_timestamp();
        let entry_str = match std::str::from_utf8(entry) {
            Ok(s) => s,
            Err(e) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Write error: {}", e),
                ));
            }
        };
        let checksum = calculate_checksum(entry_str);
        let log_entry = format!("{:?}#{}#{}#{:?}", timestamp, entry.len(), checksum, entry);
        writeln!(file, "{}", log_entry)?;

        file.flush()?;

        self.entry_number += 1;

        Ok(())
    }

    pub fn space(&self) -> usize {
        self.entry_limit - self.entry_number
    }

    pub fn get_segment_limit (&self) -> usize {
        self.entry_limit
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
            let log_data: Vec<u8> =  log_parts[3].split(", ")
            .filter_map(|s| {
                if let Ok(byte) = s.trim_matches(|c| c == '[' || c == ']').parse::<u8>() {
                    Some(byte)
                } else {
                    None
                }
            })
            .collect();
            println!("{:?}", &log_data); 
            let control_checksum = calculate_checksum(convert_byte_to_str(&log_data).expect("convert error"));
            println!("controls {}, {}", log_checksum, control_checksum); 
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

fn calculate_checksum(data: &str) -> String {
   println!("{}", data);
    format!("{:x?}", &digest(data))
}

fn get_timestamp() -> u128 {
    let time = SystemTime::now();
    time.duration_since(UNIX_EPOCH).expect("time error").as_millis() 
}


fn segment_name(index: u64) -> String {
    format!("{:020}", index)
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
   println!("byte to str {:?}", &entry);
   let entry_str = match std::str::from_utf8(&entry) {
      Ok(s) => s,
      Err(e) => {
            return Err(io::Error::new(
               io::ErrorKind::InvalidData,
               format!("Write error: {}", e),
            ));
      }
   };
   println!("value {}", entry_str);
   Ok(entry_str)
}