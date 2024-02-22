use std::{fs::{File, OpenOptions}, io::{self, Write}, sync::Mutex};

enum LogFormat {
    Binary,
    JSON
}

const BINARY: LogFormat = LogFormat::Binary;

const JSON: LogFormat = LogFormat::JSON; 

pub struct Log {
    corrupt: bool,
    file: Mutex<File>,
    log_format: LogFormat,
    first_index: u64,
    last_index: u64
}



impl Log {
    pub fn new(path: &str) -> io::Result<Self> {
        let file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(path)?;

        Ok(Log { file: Mutex::new(file), corrupt: false, first_index: 0, last_index: 0, log_format: BINARY })
    }

    pub fn append(&self, entry: &str) -> io::Result<()> { 
        let mut file = self.file.lock().unwrap();


        writeln!(file, "{}", entry)?;

        file.flush()?;

        Ok(())
    }


}

// I tried to create a WAL implementation from a golang package.
// but It's fail because Go and Rust has really different mechanism
// #[derive(Debug)]
// pub enum LogFormat {
//     Binary,
//     JSON
// }

// #[derive(Debug)]
// pub struct Options {
//     pub no_sync: bool,

//     pub segment_size: usize,

//     pub log_format: LogFormat,

//     pub segment_cache_size: usize,

//     pub no_copy: bool,

//     pub dir_perms: u32,
//     pub file_perms: u32,
// }

// #[derive(Debug)]
// enum Error {
//     Corrupt,
//     Closed,
// }

// pub struct Log {
//     pub mu: Mutex<()>,
//     pub path: String,
//     // opts: Options,
//     pub closed: bool,
//     pub corrupt: bool,
//     pub log_file: File,
//     pub first_index: u64,
//     pub last_index: u64,
// }

// impl Options {
//     fn default() -> Self {
//         Self {
//             no_sync: false,
//             segment_size: 20,
//             log_format: LogFormat::Binary,
//             segment_cache_size: 1,
//             no_copy: false,
//             dir_perms: 0o755,
//             file_perms: 0o644
//         }
//     }
// }

// impl Log {
//     pub fn write(&mut self) -> io::Result<()> {
//         let lock: Result<MutexGuard<()>, _> = self.mu.try_lock();
//         match lock {
//             Ok(_lock) => {
//                 // Get the current system time
//                 let current_time = SystemTime::now();

//                 // Calculate the duration since the Unix epoch
//                 let duration_since_epoch = current_time
//                     .duration_since(UNIX_EPOCH)
//                     .expect("Time went backwards");

//                 // Get the timestamp in seconds
//                 let timestamp_seconds = duration_since_epoch.as_secs();
//                 let timestamp_str = format!("Timestamp: {}\n", timestamp_seconds);
//                 self.log_file.write_all(timestamp_str.as_bytes())?;
//                 Ok(())
//             }
//             Err(_) => Err(io::Error::new(
//                 io::ErrorKind::WouldBlock,
//                 "Mutex is already locked",
//             )),
//         }
//     }

    // pub fn open(path: &str, opt: Option<Options>){
    //     let mut opts = opt;

    //     match opts {
    //         Some(p) => {
    //             println!("Options: {:?}", p)
    //         }
    //         None => {
    //             opts = Some(Options::default());
    //             println!("{:?}", opts)
    //         }
    //     };

    // }
// }
