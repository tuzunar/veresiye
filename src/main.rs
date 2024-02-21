use std::sync::Arc;

mod wal;

fn main() {
    let wal = Arc::new(wal::Log::new("./log").unwrap());

    let entries = vec!["hello", "from", "wal", "implementation"];

    for entry in entries {
        if let Err(err) = wal.append(entry) {
            eprintln!("Error writing to log: {}", err)
        }
    }
}
