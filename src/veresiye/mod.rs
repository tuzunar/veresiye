use std::{ fs::create_dir_all, io::{self, Error, Result}, path::Path, time::{SystemTime, UNIX_EPOCH}};

use crate::{table::{self, Table}, wal::{self, Log}};

pub struct Veresiye {
        wal: Log,
        path: String,
        sstable: Table 
}


/// new sstables always write third level 

impl Veresiye {
    pub fn new(path: String) -> Result<Veresiye> {
        let p = Path::new(&path);
        if !p.exists() {
            create_dir_all(&p)?;
        }

        if !p.is_dir() {
            return Err(Error::new(io::ErrorKind::Other, "path not a directory"));
        }

        let sstable_name = format!("table_third_{}", get_timestamp());

        let sstable_path = format!("./data/{}", sstable_name);

        let sstable = table::Table::new(&sstable_path).unwrap();
        let  wal = wal::Log::open("./log", 10000).unwrap();
        Ok(Veresiye {
            wal,
            path,
            sstable
        })
    }

    pub fn get(&mut self, key: &str) -> Result<String> {
        let result = self.sstable.get(key).expect("get error").unwrap();
        Ok(result)
    }

    pub fn set(&mut self, key: &str, value: &str) {
        let operation = format!("SET, {}, {}", key, value);
        self.wal.write(operation.as_bytes()).unwrap();
        self.sstable.insert(key, value).unwrap();
    }

    pub fn recover() {
        println!("recovered")
    }
}

fn get_timestamp() -> u128 {
   let time = SystemTime::now();
   time.duration_since(UNIX_EPOCH).expect("time error").as_millis() 
}