use std::{ fs::create_dir_all, io::{self, Error, Result}, path::Path};

use crate::wal::{self, Log};

pub struct Veresiye {
        wal: Log,
        path: String
}

impl Veresiye {
    pub fn new(path: String) -> Result<Veresiye> {
        let p = Path::new(&path);
        if !p.exists() {
            create_dir_all(&p)?;
        }

        if !p.is_dir() {
            return Err(Error::new(io::ErrorKind::Other, "path not a directory"));
        }

        let  wal = wal::Log::open("./log", 10000).unwrap();
        Ok(Veresiye {
            wal,
            path
        })
    }

    pub fn set(&mut self, key: &str, value: &str) {
        let operation = format!("SET, {}, {}", key, value);
        self.wal.write(operation.as_bytes()).unwrap();
    }

    pub fn recover() {
        println!("recovered")
    }
}

