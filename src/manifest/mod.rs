use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom},
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
        self.file.seek(SeekFrom::Start(0)).unwrap();

        let mut manifest_buffer = Vec::new();

        self.file
            .read_to_end(&mut manifest_buffer)
            .expect("cannot read manifest file");

        let manifest_content: ManifestData = ManifestData::get_deserialized(manifest_buffer);

        manifest_content
    }

    pub fn edit_manifest(
        &mut self,
        new_sequence_number: String,
        segment_name: String,
    ) -> ManifestData {
        let manifest = ManifestData::create(new_sequence_number, segment_name);

        manifest
    }

    pub fn save_manifest(&mut self, manifest: ManifestData) {
        self.file.set_len(0).unwrap();
        self.file.rewind().unwrap();
        serde_json::to_writer(
            &self.file,
            &ManifestData {
                last_flushed_sequence: String::from(manifest.get_sequence_number()),
                last_flushed_segment: String::from(manifest.get_segment_name()),
            },
        )
        .expect("cannot update manifest file");
    }
}
