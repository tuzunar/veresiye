use std::{
    fs::{File, OpenOptions},
    io::{self, BufRead, BufReader, Result, Write},
    path::Path,
    sync::Mutex,
};

const DEFAULT_ENTRY_LIMIT: usize = 10 << 10;

pub struct Segment {
    file: Mutex<File>,

    entry_number: usize,
    entry_limit: usize,
}

impl Segment {
    pub fn new(path: String, limit: usize) -> Result<Segment> {
        let file = File::open(path)?;
        let reader = BufReader::new(&file);
        let line_count = reader.lines().count();

        Ok(Segment {
            entry_limit: get_entry_limit(limit),
            entry_number: line_count,
            file: Mutex::new(file),
        })
    }
    pub fn open(dir: &str, sequence: u64, mut limit: usize, create: bool) -> Result<Segment> {
        if limit == 0 {
            limit = DEFAULT_ENTRY_LIMIT;
        }
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

    pub fn write(&mut self, entry: &[u8]) -> io::Result<()> {
        let mut file = self.file.lock().unwrap();

        let entry_str = match std::str::from_utf8(entry) {
            Ok(s) => s,
            Err(e) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Write error: {}", e),
                ));
            }
        };

        writeln!(file, "{:?}", entry_str)?;

        file.flush()?;

        self.entry_number += 1;

        Ok(())
    }

    pub fn space(&self) -> usize {
        self.entry_limit - self.entry_number
    }
}

fn segment_name(index: u64) -> String {
    format!("{:020}", index)
}

fn get_entry_limit(limit: usize) -> usize {
    if limit <= 0 {
        println!("Entry limit must be greater than zero");
        DEFAULT_ENTRY_LIMIT
    } else {
        limit
    }
}
