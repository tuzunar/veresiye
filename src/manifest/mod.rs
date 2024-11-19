use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom},
    path::PathBuf, vec,
};

use manifest_data::ManifestData;

mod manifest_data;

pub struct Manifest {
    file: File,
}

impl Manifest {
    pub fn create() -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open("./manifest")
            .expect("cannot create manifest file");

        Ok(Manifest { file })
    }

    pub fn open() -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open("./manifest")
            .expect("cannot create manifest file");

        Ok(Manifest { file })
    }

    // pub fn get_serialized(&self) {
    //     let serialized =
    // }

    pub fn get_manifest(&mut self) -> ManifestData {
        if self.file.metadata().unwrap().len() as i32 == i32::from(0) {
            let manifest_content = ManifestData::create(String::from("0"), String::from("./log/00000000000000000001"), vec![]);
            manifest_content
        } else {
            self.file.seek(SeekFrom::Start(0)).unwrap();

            let mut manifest_buffer = Vec::new();

            self.file
                .read_to_end(&mut manifest_buffer)
                .expect("cannot read manifest file");
            let manifest_content: ManifestData = ManifestData::get_deserialized(manifest_buffer);

            manifest_content
        }
    }

    // pub fn get_removable_tables(&mut self) -> Vec<PathBuf> {
    //     if(self.file.metadata().unwrap().len() == 0){
    //         let empty_vec: Vec<PathBuf> = vec![];
    //         empty_vec
    //     } else {
    //         let manifest = self.get_manifest();
    //         manifest.get_removable_tables().to_vec()
    //     }
    // }

    pub fn edit_manifest(
        &mut self,
        new_sequence_number: String,
        segment_name: String
    ) -> ManifestData {
        let manifest =
            ManifestData::create(new_sequence_number, segment_name, vec![]);

        manifest
    }

    pub fn edit_removable_tables(&mut self, tables: Vec<PathBuf>) -> ManifestData {
        let manifest_content = self.get_manifest();
        let removable_tables = tables;

        let manifest = ManifestData::create(
            manifest_content.last_flushed_sequence,
            manifest_content.last_flushed_segment,
            removable_tables,
        );
        manifest
    }

    pub fn save_manifest(&mut self, manifest: ManifestData) {
        self.file.set_len(0).unwrap();
        self.file.rewind().unwrap();

        let last_flushed_sequence = manifest.get_sequence_number();
        let last_flushed_segment = manifest.get_segment_name();
        let current_data = &ManifestData {
            last_flushed_sequence: String::from(last_flushed_sequence),
            last_flushed_segment: String::from(last_flushed_segment),
            removable_tables: if manifest.get_removable_tables().len() == 0 {
                manifest.get_removable_tables().to_vec()
            } else {
                vec![]
            },
        };
        serde_json::to_writer(&self.file, current_data);
    }
}
