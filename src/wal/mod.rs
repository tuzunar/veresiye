use std::{
    fs::{self, create_dir_all, read_dir, remove_file, File},
    io::{self, Error, Result},
    path::{Path, PathBuf}, time::{Duration, SystemTime},
};

use crate::segment::Segment;

pub struct Log {
    segments: Vec<Segment>,
    next_segment: u64,
    path: String,
}


const LOG_RETENTION_MS: u64 = 60480000;

impl Log {
    pub fn open(path: &str, entry_limit: usize) -> io::Result<Log> {
        let mut read_segment: u64 = 1;
        let p = Path::new(path);
        if !p.exists() {
            create_dir_all(&p)?;
        }

        if !p.is_dir() {
            return Err(Error::new(io::ErrorKind::Other, "path not a directory"));
        }

        let mut segments: Vec<Segment> = Vec::with_capacity(10);

        let mut dir: Vec<_> = read_dir(path)?.map(|entry| entry.unwrap()).collect();
        dir.sort_by(|a, b| a.path().cmp(&b.path()));
    
        
        if dir.len() >= usize::try_from(1).unwrap() {
            for file in dir {
                let file_path = format!("{}", &file.path().display());
                let f = File::open(&file_path).unwrap();
                match Segment::check_log_integrity(&f){
                    Ok(_) => {},
                    Err(e) => eprintln!("Error: {}", e)
                };
                match Segment::new(String::from(&file_path), entry_limit) {
                    Ok(s) => segments.push(s),

                    Err(e) => return Err(e),
                }
                read_segment += 1;
            }
        } else {
            match Segment::open(path, read_segment, entry_limit, true) {
                Ok(s) => segments.push(s),
                //Err(ref e) if e.kind() == ErrorKind::NotFound => break,
                Err(e) => return Err(e),
            }
            read_segment += 1;
        }

        Ok(Log {
            segments,
            next_segment: read_segment,
            path: String::from(path),
        })
    }

    pub fn write(&mut self, entry: &[u8]) -> io::Result<()> {
        self.allocate(1)?;
        let segment = self.segments.last_mut().unwrap();
        segment.write(entry)?;
        Ok(())
    }

    fn allocate(&mut self, n: usize) -> Result<usize> {
        match self.segments.last_mut() {
            Some(ref s) if s.space() > 0 => {
                let space = s.space();

                return if space > n { Ok(n) } else { Ok(space) };
            }

            Some(_) => {}

            None => {}
        }

        let new_segment = Segment::open(&self.path, self.next_segment, self.segments.last_mut().unwrap().get_segment_limit(), true)?;
        let space = new_segment.space();
        self.next_segment += 1;
        self.segments.push(new_segment);

        if space > n {
            Ok(n)
        } else {
            Ok(space)
        }
    }


    pub fn remove_logs(&self) {
      for segment in &self.segments {
         let created_at = &segment.get_segment_created_time().unwrap();
         if is_older_than_one_week(*created_at) {
            remove_file(segment.f)
         }
      }
    }

    pub fn list_logs(&self) -> Vec<PathBuf> {
        let paths = fs::read_dir(&self.path).unwrap();
        let mut sorted_paths: Vec<PathBuf> = paths.map(|entry| entry.unwrap().path()).collect();
        sorted_paths.sort();
        sorted_paths

        // for path in sorted_paths {
        //     println!("{}", path.display());
        // }
    }

    pub fn read(&self, index: usize) -> Result<String> {
        self.segments.get(index).expect("Segment not found").read()
    }
}

fn is_older_than_one_week(time: SystemTime) -> bool {
   let current_time = SystemTime::now();

   if let Ok(duration) = current_time.duration_since(time) {
      let one_week = Duration::from_secs(60 * 60 * 24 * 7);

      duration > one_week
   } else { false }   

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
