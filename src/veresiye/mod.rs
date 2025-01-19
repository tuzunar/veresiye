use std::{
    cmp,
    collections::BTreeMap,
    fs::{create_dir, create_dir_all, read_dir, File, OpenOptions},
    io::{self, Error, Read, Result, Seek, SeekFrom, Write},
    os::fd::{AsFd, AsRawFd},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use compaction::Compaction;

use crate::{
    manifest::Manifest,
    memdb::memdb,
    table::{self, Table},
    wal::{self, Log},
};

mod compaction;

pub struct Veresiye {
    wal: Log,
    path: String,
    sstable: Vec<Table>,
    memdb: memdb,
    manifest: Manifest,
}

const MEMDB_SIZE_THRESHOLD: usize = 1024 * 1024 * 1;

impl Veresiye {
    pub fn new(path: String) -> Result<Veresiye> {
        let p = Path::new(&path);

        let log_path = format!("{}/log", path);
        let table_path = format!("{}/tables", path);

        if !p.exists() {
            create_dir_all(&p)?;
            create_dir(table_path)?;
            let sstable = vec![];
            let wal = wal::Log::open(&log_path, 10000).unwrap();
            let memdb = memdb::new();
            let manifest =
                Manifest::create(String::from(&path)).expect("cannot create manifest file");

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
            let mut manifest =
                Manifest::open(String::from(&path)).expect("cannot open the manifest file");
            let wal = wal::Log::open(&log_path, 10000).unwrap();
            let mut memdb = memdb::new();

            let manifest_data = &manifest.get_manifest();
            let unflushed_data = wal.replay(
                &manifest_data.last_flushed_sequence,
                &manifest_data.last_flushed_segment,
            );
            memdb.append(unflushed_data);

            let table_dirs = Veresiye::get_all_sstable_dir(table_path);
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
            let manifest = self.manifest.get_manifest();
            let removable_tables = manifest.get_removable_tables();

            let mut tables = self
                .sstable
                .iter()
                .filter(|table| !removable_tables.contains(table.get_table_path()))
                .collect::<Vec<&Table>>()
                .clone();

            &tables.sort();
            for table in &tables {
                if removable_tables.contains(table.get_table_path()) {
                    continue;
                }

                if let Some(value) = &table.get(key) {
                    return Some(value.clone());
                }
            }
            println!("{:?}", &tables);
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
                    self.memdb.move_buffer_to_data();
                    let sstable_name = format!("level_0_{}", get_timestamp());
                    let sstable_path = format!("./{}/tables/{}", self.path, sstable_name);

                    let new_table = table::Table::new(&sstable_path, 0 as usize)
                        .expect("cannot create new table");
                    self.sstable.push(new_table);

                    self.sstable
                        .last()
                        .unwrap()
                        .insert(self.memdb.get_hash_table());

                    let manifest_data = self
                        .manifest
                        .edit_manifest(result.entry_number, result.file_path);

                    self.manifest.save_manifest(&manifest_data);
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

    pub fn compact(&mut self) {
        let cmpct = Compaction::init(String::from(&self.path)).clone();
        let old_manifest_data = &self.manifest.get_manifest();
        match cmpct
            .level_zero_check(old_manifest_data.get_removable_tables().to_vec())
            .clone()
        {
            true => {
                let removable_tables: Vec<PathBuf> = cmpct.compact_level_zero().clone();

                let manifest_data = &self
                    .manifest
                    .edit_removable_tables(removable_tables.to_vec());

                &self.manifest.save_manifest(&manifest_data);
            }

            false => println!("not need compaction for level 0 tables"),
        }

        match cmpct
            .level_one_check(old_manifest_data.get_removable_tables().to_vec())
            .clone()
        {
            true => {
                let removable_tables: Vec<PathBuf> = cmpct.compact_level_zero().clone();

                let manifest_data = &self
                    .manifest
                    .edit_removable_tables(removable_tables.to_vec());

                &self.manifest.save_manifest(&manifest_data);
            }

            false => println!("not need compaction for level 1 tables"),
        }

        match cmpct
            .level_two_check(old_manifest_data.get_removable_tables().to_vec())
            .clone()
        {
            true => {
                let removable_tables: Vec<PathBuf> = cmpct.compact_level_zero().clone();

                let manifest_data = &self
                    .manifest
                    .edit_removable_tables(removable_tables.to_vec());

                &self.manifest.save_manifest(&manifest_data);
            }

            false => println!("not need compaction for level 2 tables"),
        }
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

#[cfg(test)]
mod tests {
    use std::fs::remove_dir_all;

    use tempdir::TempDir;

    use super::*;

    #[test]
    fn init_db_test() {
        let db_dir = String::from("./test");
        let mut db = Veresiye::new(String::from(&db_dir)).unwrap();

        let table_path = format!("{}/tables", String::from(&db_dir));
        let log_path = format!("{}/log", String::from(&db_dir));

        let table_dir = Path::new(&table_path);
        let log_dir = Path::new(&log_path);

        assert!(log_dir.exists());
        assert!(table_dir.exists());

        for n in 0..10925 {
            let key = format!("key{}", n);
            let value = format!("value{}", n);

            db.set(&key, &value);
        }

        let table_folder_path = Path::new(&table_dir);

        assert!(table_folder_path.metadata().unwrap().len() > 0);

        let log_folder_path = Path::new(&log_dir);

        assert!(log_folder_path.metadata().unwrap().len() > 0);

        remove_dir_all(&db_dir);
    }
}
