use std::{fs::File, sync::Mutex};


#[derive(Debug)]
pub enum LogFormat {
    Binary,
    JSON
}


#[derive(Debug)]
pub struct Options {
    pub no_sync: bool,

    pub segment_size: usize,

    pub log_format: LogFormat,

    pub segment_cache_size: usize,

    pub no_copy: bool,

    pub dir_perms: u32,
    pub file_perms: u32,
}

#[derive(Debug)]
enum Error {
    Corrupt,
    Closed,
}

struct Log {
    mu: Mutex<()>,
    path: String,
    opts: Options,
    closed: bool,
    corrupt: bool,
    log_file: Option<File>,
    first_index: u64,
    last_index: u64
}

impl Options {
    fn default() -> Self {
        Self {
            no_sync: false,
            segment_size: 20,
            log_format: LogFormat::Binary,
            segment_cache_size: 1,
            no_copy: false,
            dir_perms: 0o755,
            file_perms: 0o644 
        }
    }
}

impl Log {
    pub fn write(&self, index: u64, data: Vec<u8>){
        let _loc = self.mu.lock().unwrap();        

        self.wb
    }

    pub fn open(path: &str, opt: Option<Options>){
        let mut opts = opt;

        match opts {
            Some(p) => {
                println!("Options: {:?}", p)
            }
            None => {
                opts = Some(Options::default());
                println!("{:?}", opts)
            }
        }; 

    }
}


