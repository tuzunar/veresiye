use crate::wal::Log;

pub struct Veresiye {
        wal: Log
}

impl Veresiye {
    pub fn new() {}
    pub fn recover() {
        println!("recovered")
    }
}
