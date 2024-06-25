use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct ManifestData {
    pub last_flushed_sequence: String,
    pub last_flushed_segment: String,
}

impl ManifestData {
    pub fn create(last_flushed_sequence: String, last_flushed_segment: String) -> Self {
        let last_flushed_sequence = ManifestData {
            last_flushed_sequence,
            last_flushed_segment,
        };

        last_flushed_sequence
    }

    pub fn get_serialized(&self) -> Vec<u8> {
        let serialized = serde_json::to_vec(&self).expect("manifest data serialized error");

        serialized
    }

    pub fn get_deserialized(serialized_manifest: Vec<u8>) -> Self {
        let deserialized: ManifestData =
            serde_json::from_slice(&serialized_manifest).expect("deserialized error");

        deserialized
    }

    pub fn get_sequence_number(&self) -> &str {
        &self.last_flushed_sequence
    }

    pub fn get_segment_name(&self) -> &str {
        &self.last_flushed_segment
    }

    pub fn set_sequence_number(&mut self, sequence_number: String, segment_name: String) {
        self.last_flushed_sequence = sequence_number;
        self.last_flushed_segment = segment_name;
    }
}
