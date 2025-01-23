use std::{
    io,
    time::{SystemTime, UNIX_EPOCH},
};

use sha256::digest;

pub fn calculate_checksum(data: &str) -> String {
    format!("{:x?}", &digest(data))
}

pub fn get_timestamp() -> u128 {
    let time = SystemTime::now();
    time.duration_since(UNIX_EPOCH)
        .expect("time error")
        .as_millis()
}

pub fn segment_name(index: u64) -> String {
    format!("{:020}", index)
}

pub fn convert_byte_to_str(entry: &[u8]) -> io::Result<&str> {
    let entry_str = match std::str::from_utf8(&entry) {
        Ok(s) => s,
        Err(e) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Write error: {}", e),
            ));
        }
    };
    Ok(entry_str)
}

fn parse_byte(value: &str) -> Vec<u8> {
    let bytes: Vec<u8> = value
        .split(", ")
        .filter_map(|s| {
            if let Ok(byte) = s.trim_matches(|c| c == '[' || c == ']').parse::<u8>() {
                Some(byte)
            } else {
                None
            }
        })
        .collect();

    bytes
}

#[derive(Debug)]
pub struct LogData {
    pub command: String,
    pub key: String,
    pub value: String,
}

pub fn parse_log_line(entry: &str) -> LogData {
    let log_parts: Vec<&str> = entry.split("#").collect();
    let log_data = log_parts[4];
    let parsed_data: Vec<u8> = parse_byte(&log_data);
    let converted_log_data = convert_byte_to_str(&parsed_data).expect("convert error");

    let log_data_parts: Vec<&str> = converted_log_data.split(",").collect();
    // println!("{:?}", converted_log_data);
    // println!("{:?} {:?}", log_data, log_data_parts);
    let command = log_data_parts[0].to_string();
    let key = log_data_parts[1].to_string();
    let value = log_data_parts[2..].join(",").to_string();

    LogData {
        command,
        key,
        value,
    }
}
