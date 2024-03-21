use std::time::{SystemTime, UNIX_EPOCH};

use sha256::digest;


pub fn calculate_checksum(data: &str) -> String {
    format!("{:x?}", &digest(data))
}

pub fn get_timestamp() -> u128 {
    let time = SystemTime::now();
    time.duration_since(UNIX_EPOCH).expect("time error").as_millis() 
}

pub fn segment_name(index: u64) -> String {
    format!("{:020}", index)
}