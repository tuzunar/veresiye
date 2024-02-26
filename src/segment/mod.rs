use std::{ fs::{File, OpenOptions}, io::{self, Write}, path::{Path, PathBuf}, sync::Mutex};
use std::io::{Result};

const DEFAULT_ENTRY_LIMIT: usize = 10 << 10;

pub struct Segment {
    sequence: u64,

    file: Mutex<File>,

    entry_limit: usize,
}

impl Segment {
    pub fn open(dir: &str, sequence: u64, mut limit :usize, create: bool) -> Result<Segment> {
        if limit == 0 {
            limit = DEFAULT_ENTRY_LIMIT;
        }
        let fname = Path::new(dir).join(segment_name(sequence));
        let file = OpenOptions::new()
            .create(create)
            .read(true)
            .write(true)
            .open(&fname)?;

        Ok(Segment{
             sequence,
             entry_limit: limit,
             file: Mutex::new(file)
        })

    }

    pub fn write(&mut self, entry: &[u8]) -> io::Result<()> {
        let mut file = self.file.lock().unwrap();

        let entry_str = match std::str::from_utf8(entry) {
            Ok(s) => s,
            Err(e) => {
                return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Write error: {}", e)
                ));
            }
        };

        writeln!(file, "{:?}", entry_str)?;

        file.flush()?;

        Ok(())

    }
}

fn segment_name(index: u64) -> String {
    format!("{:020}", index)
}

