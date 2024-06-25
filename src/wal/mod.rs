mod segment;

use std::{
    collections::HashMap,
    fs::{self, create_dir_all, read_dir, remove_file, File},
    io::{self, Error, Result},
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

use segment::WriteResult;

use self::segment::Segment;

pub struct Log {
    segments: Vec<Segment>,
    next_segment: u64,
    path: String,
    removable_segments: Vec<PathBuf>,
}

const LOG_RETENTION_MS: u64 = 60 * 60 * 24 * 7;

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
                match Segment::check_log_integrity(&f) {
                    Ok(_) => println!("{} integrity is okay", &file.path().display()),
                    Err(e) => panic!("Corrupted WAL Detected"),
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
            removable_segments: Vec::new(),
        })
    }

    pub fn write(&mut self, entry: &[u8]) -> io::Result<WriteResult> {
        self.allocate(1)?;
        let segment = self.segments.last_mut().unwrap();
        Ok(segment.write(entry).expect("log cannot write to segment"))
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

        let new_segment = Segment::open(
            &self.path,
            self.next_segment,
            self.segments.last_mut().unwrap().get_segment_limit(),
            true,
        )?;
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
        let removable_segments = &self.removable_segments;
        for segment in removable_segments {
            remove_file(segment).expect("cannot removed the segment file");
        }
    }

    pub fn list_segments(&self) -> Vec<PathBuf> {
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

    pub fn set_checkpoint_flag(&mut self) {
        let current_segment = self.segments.last_mut().expect("cannot reach last segment");
        let current_segment_path = current_segment.get_segment_path().clone();
        current_segment
            .set_checkpoint_flag()
            .expect("cannot set checkpoint flag to segment");
        Log::mark_segments_as_removable(self, current_segment_path)
    }

    fn replay() {}

    fn mark_segments_as_removable(&mut self, current_segment: PathBuf) {
        let segment_paths = Log::list_segments(self);
        let removable_segments: Vec<PathBuf> = segment_paths
            .iter()
            .filter(|path| path.to_str().unwrap() < current_segment.to_str().unwrap())
            .cloned()
            .collect();
        self.removable_segments = removable_segments
    }
}

fn is_older_than_one_week(time: SystemTime) -> bool {
    let current_time = SystemTime::now();

    if let Ok(duration) = current_time.duration_since(time) {
        let one_week = Duration::from_secs(LOG_RETENTION_MS);

        duration > one_week
    } else {
        false
    }
}
