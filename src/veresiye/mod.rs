use std::{ collections::BTreeMap, fs::{create_dir_all, read_dir, File, OpenOptions}, io::{self, Error, Read, Result, Seek, SeekFrom, Write}, path::{Path, PathBuf}, time::{SystemTime, UNIX_EPOCH}};

use crate::{memdb::memdb, table::{self, Table}, wal::{self, Log}};

pub struct Veresiye {
        wal: Log,
        path: String,
        sstable: Table,
        memdb: memdb
}

const MEMDB_SIZE_THRESHOLD: usize = 1024 * 1024 * 1;

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

        let sstable_name = format!("level_zero_{}", get_timestamp());

        let sstable_path = format!("./{}/{}", p.display(), sstable_name);

        let sstable = table::Table::new(&sstable_path).unwrap();
        let wal = wal::Log::open("./log", 10000).unwrap();
        let memdb = memdb::new();

        Ok(Veresiye {
            wal,
            path,
            sstable,
            memdb
        })
    }

    pub fn get(&mut self, key: &str) -> Result<String> {
        let result = self.sstable.get(key).expect("get error").unwrap();
        Ok(result)
    }

    pub fn set(&mut self, key: &str, value: &str) {
      let operation = format!("SET, {}, {}", key, value);
      self.wal.write(operation.as_bytes()).unwrap();
      self.memdb.insert(key, value);

      if self.memdb.size() >= MEMDB_SIZE_THRESHOLD {
         println!("{}", self.memdb.size());
      }
    }

    pub fn get_all_sstable_dir(&self) -> Vec<PathBuf> {
      let path = read_dir(&self.path).expect("cannot read sstable dir");
      let dirs: Vec<PathBuf> = path.map(|path| path.unwrap().path()).collect();
      dirs
    }

    pub fn compact(&self) -> Result<()> {
      let dirs = Veresiye::get_all_sstable_dir(&self);
      let mut merged_table: BTreeMap<String, String>= BTreeMap::new();

      for dir in dirs {
         let path_string: &String = &dir.clone().into_os_string().into_string().unwrap();
         let leveled_sstable: Vec<&str> = path_string.split("/").collect();
         let third_label: Vec<&str> = leveled_sstable[2].split("_").collect();

         if third_label[0] == "level" && third_label[1] == "zero" {
            let mut file = OpenOptions::new().read(true).open(&dir).unwrap();
            let mut content: String = String::new();
            file.seek(SeekFrom::Start(0)).unwrap();
            file.read_to_string(&mut content).expect("sstable read error");

            let data: Vec<&str> = content.split(",").collect();

            for entries in data {
                  if entries.is_empty() {
                     continue; 
                  }
                  let entry: Vec<&str> = entries.split(":").collect();
                  merged_table.insert(String::from(entry[0]), String::from(entry[1]));
               }
            }

            let mut output_file = File::create("./data/compaction")?;
            for (key, value) in &merged_table {
               writeln!(output_file, "{}:{},", key, value)?;
            }
         }

      Ok(())
    }

    pub fn cleanup_logs(self) {
      self.wal.remove_logs();
    }

    pub fn recover() {
        println!("recovered")
    }
}

fn get_timestamp() -> u128 {
   let time = SystemTime::now();
   time.duration_since(UNIX_EPOCH).expect("time error").as_millis() 
}