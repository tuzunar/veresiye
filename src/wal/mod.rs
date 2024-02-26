use std::{fs::create_dir_all, io::{self, Error, ErrorKind, Result}, path::Path};

use crate::segment::Segment;


pub struct Log {
    segments: Vec<Segment>,
    next_segment: u64,
    path: String
}

impl Log {
    pub fn open( path: &str) -> io::Result<Log> {
        let mut read_segment: u64 = 1;
        let p = Path::new(path);
        if !p.exists() {
            create_dir_all(&p)?;
        }

        if !p.is_dir() {
            return Err(Error::new(io::ErrorKind::Other, "path not a directory"));
        }


        let mut segments:Vec<Segment> = Vec::with_capacity(10);

            match Segment::open(path, read_segment, 2, true) {
                Ok(s) => segments.push(s),
                //Err(ref e) if e.kind() == ErrorKind::NotFound => break,
                Err(e) => return Err(e)
            }
            read_segment += 1;



        
        Ok(Log { segments, next_segment: read_segment, path: String::from(path)})
    }

    pub fn write(&mut self, entry: &[u8]) -> io::Result<()> {
        self.allocate(1)?;
        let segment  = self.segments.last_mut().unwrap();
        segment.write(entry)?;
        Ok(())
    }

    fn allocate(&mut self, n: usize) -> Result<usize> {
        match self.segments.last_mut() {
            Some(ref s) if s.space() > 0 => {
                let space = s.space();

                return if space > n {Ok(n)} else {Ok(space)}
            },

            Some(ss) => {
                println!("{}", ss.space())
            },

            None => {}
        }

        let new_segment = Segment::open(&self.path, self.next_segment, 2, true)?;
        println!("code is here");
        let space = new_segment.space();

        self.next_segment += 1;
        self.segments.push(new_segment);

        if space > n {Ok(n)} else {Ok(space)}
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
