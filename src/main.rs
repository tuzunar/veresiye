mod wal;
mod segment;

fn main() {
    let mut wal = wal::Log::open(1024, "./log").unwrap();

    let entries = vec!["hello", "from", "wal", "implementation"];

    for entry in entries {
         wal.write(entry.as_bytes());
    }
}
