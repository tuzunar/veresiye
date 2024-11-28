use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::{Seek, SeekFrom},
    path::PathBuf,
};

use crate::{
    table::{reconstruct_tree_from_sstable, Table},
    util::get_timestamp,
};

#[derive(Clone)]
pub struct Compaction {
    files: Vec<PathBuf>,
    path: String,
}

const LEVEL_ZERO_THRESHOLD: usize = 8 as usize;
const LEVEL_ONE_THRESHOLD: usize = 4 as usize;
const LEVEL_TWO_THRESHOLD: usize = 1 as usize;

impl Compaction {
    pub fn init(path: String) -> Self {
        let mut paths: Vec<PathBuf> = vec![];
        let files = fs::read_dir(&path).expect("can not open table directory");

        for file in files {
            let p = file.unwrap().path();
            paths.push(p);
        }

        Self { files: paths, path }
    }

    pub fn level_zero_check(self) -> bool {
        let level_zero_table = get_files_by_level(self.files, 0);

        if level_zero_table.len() >= LEVEL_ZERO_THRESHOLD {
            true
        } else {
            false
        }
    }

    pub fn level_one_check(self) -> bool {
        let level_one_table = get_files_by_level(self.files, 1);

        if level_one_table.len() >= LEVEL_ONE_THRESHOLD {
            true
        } else {
            false
        }
    }

    pub fn level_two_check(self) -> bool {
        let level_two_table = get_files_by_level(self.files, 2);

        if level_two_table.len() >= LEVEL_TWO_THRESHOLD {
            true
        } else {
            false
        }
    }

    pub fn compact_level_zero(self) -> Vec<PathBuf> {
        const CURRENT_LEVEL: usize = 0;
        const TARGET_LEVEL: usize = CURRENT_LEVEL + 1;

        let mut compacted_tree: BTreeMap<String, String> = BTreeMap::new();

        let level_zero_tables: Vec<PathBuf> = get_files_by_level(self.files, CURRENT_LEVEL);

        for path in &level_zero_tables {
            let mut file = OpenOptions::new().read(true).open(path).unwrap();
            file.seek(SeekFrom::Start(0)).unwrap();
            let tree = reconstruct_tree_from_sstable(file);

            compacted_tree.extend(tree);
        }

        let sstable_name: String = format!("level_{}_{}", TARGET_LEVEL, get_timestamp());
        let sstable_path: String = format!("./{}/{}", self.path, sstable_name);

        let compacted_table: Table =
            Table::new(&sstable_path, TARGET_LEVEL).expect("cannot create sstable");
        compacted_table.insert(&compacted_tree);
        level_zero_tables
    }
}

fn get_files_by_level(files: Vec<PathBuf>, current_level: usize) -> Vec<PathBuf> {
    files
        .into_iter()
        .filter(|path| {
            let path_string = path.to_str().unwrap();
            let path_parts: Vec<&str> = path_string.split("_").collect::<Vec<&str>>();
            return path_parts[1].parse::<usize>().unwrap() == current_level;
        })
        .collect::<Vec<PathBuf>>()
}
