use std::{
    collections::BTreeMap,
    fs::{create_dir_all, read_dir, File, OpenOptions},
    io::{self, Error, Read, Result, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    manifest::Manifest,
    memdb::memdb,
    table::{self, Table},
    wal::{self, Log},
};

pub struct Veresiye {
    wal: Log,
    path: String,
    sstable: Vec<Table>,
    memdb: memdb,
    manifest: Manifest,
}

const MEMDB_SIZE_THRESHOLD: usize = 1024 * 1024 * 1;

/// new sstables always write third level

impl Veresiye {
    pub fn new(path: String) -> Result<Veresiye> {
        let p = Path::new(&path);

        if !p.exists() {
            create_dir_all(&p)?;

            let sstable = vec![];
            let wal = wal::Log::open("./log", 10000).unwrap();
            let memdb = memdb::new();
            let manifest = Manifest::create().expect("cannot create manifest file");

            Ok(Veresiye {
                wal,
                path,
                sstable,
                memdb,
                manifest,
            })
        } else {
            if !p.is_dir() {
                return Err(Error::new(io::ErrorKind::Other, "path not a directory"));
            };
            let mut manifest = Manifest::open().expect("cannot open the manifest file");
            let wal = wal::Log::open("./log", 10000).unwrap();
            let mut memdb = memdb::new();

            let manifest_data = &manifest.get_manifest();
            let unflushed_data = wal.replay(
                &manifest_data.last_flushed_sequence,
                &manifest_data.last_flushed_segment,
            );
            memdb.append(unflushed_data);

            let table_dirs = Veresiye::get_all_sstable_dir(path.clone());
            let mut sstable: Vec<Table> = vec![];
            for table_dir in table_dirs {
                let table =
                    Table::open(table_dir.to_str().unwrap()).expect("cannot load table from path");
                sstable.push(table);
            }

            Ok(Veresiye {
                wal,
                path,
                memdb,
                sstable,
                manifest,
            })
        }
    }

    pub fn get(&mut self, key: &str) -> Option<String> {
        if self.memdb.buffer.contains_key(key) {
            Some(String::from(
                self.memdb.get(key).expect("cannot get value from memdb"),
            ))
        } else {
            for table in self.sstable.iter() {
                if let Some(value) = &table.get(key) {
                    return Some(value.clone());
                }
            }
            None
        }
    }

    pub fn get_memdb_size(&self) -> usize {
        self.memdb.size()
    }

    pub fn set(&mut self, key: &str, value: &str) {
        let operation = format!("SET, {}, {}", key, value);
        match self.wal.write(operation.as_bytes()) {
            Ok(result) => {
                self.memdb.insert(key, value);

                if self.memdb.size() >= MEMDB_SIZE_THRESHOLD {
                    println!("{}", self.memdb.size());
                    self.memdb.move_buffer_to_data();
                    let sstable_name = format!("level_zero_{}", get_timestamp());
                    let sstable_path = format!("./{}/{}", self.path, sstable_name);

                    let new_table =
                        table::Table::new(&sstable_path).expect("cannot create new table");
                    self.sstable.push(new_table);

                    self.sstable
                        .last()
                        .unwrap()
                        .insert(self.memdb.get_hash_table());

                    let manifest_data = self
                        .manifest
                        .edit_manifest(result.entry_number, result.file_path);

                    self.manifest.save_manifest(manifest_data)
                }
            }
            Err(e) => {
                panic!("operation failed {}", e)
            }
        }
    }

    pub fn get_all_sstable_dir(path: String) -> Vec<PathBuf> {
        let path = read_dir(path).expect("cannot read sstable dir");
        let dirs: Vec<PathBuf> = path.map(|path| path.unwrap().path()).collect();
        dirs
    }

    pub fn compact(&self) -> Result<()> {
        let dirs = Veresiye::get_all_sstable_dir(String::from(&self.path));
        let mut merged_table: BTreeMap<String, String> = BTreeMap::new();

        for dir in dirs {
            let path_string: &String = &dir.clone().into_os_string().into_string().unwrap();
            let leveled_sstable: Vec<&str> = path_string.split("/").collect();
            let third_label: Vec<&str> = leveled_sstable[2].split("_").collect();

            if third_label[0] == "level" && third_label[1] == "zero" {
                let mut file = OpenOptions::new().read(true).open(&dir).unwrap();
                let mut content: String = String::new();
                file.seek(SeekFrom::Start(0)).unwrap();
                file.read_to_string(&mut content)
                    .expect("sstable read error");

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
    time.duration_since(UNIX_EPOCH)
        .expect("time error")
        .as_millis()
}
