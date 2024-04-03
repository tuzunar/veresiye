use std::{io, time::{SystemTime, UNIX_EPOCH}};

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