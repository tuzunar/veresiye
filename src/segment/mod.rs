use std::{
    fs::{read_to_string, File, OpenOptions}, io::{self, BufRead, BufReader, Read, Result, Seek, SeekFrom, Write}, path::Path, sync::Mutex, time::{SystemTime, UNIX_EPOCH}, u8, usize
};

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
        let created_at = format!(
            "{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
        );
        let fname = Path::new(dir).join(segment_name(sequence));
        let mut file = OpenOptions::new()
            .create(create)
            .read(true)
            .write(true)
            .open(&fname)?;
        let trademarks: [String; 2] = [String::from("segments"), String::from(created_at)];
        for value in trademarks {
            writeln!(file, "{:?}", value).unwrap();
        }
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
        limit + 2
    }
}
