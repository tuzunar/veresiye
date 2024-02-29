mod segment;
mod wal;

fn main() {
    let mut wal = wal::Log::open("./log").unwrap();

    let entries = vec![
        "hello",
        "from",
        "wal",
        "implementation",
        "another",
        "segment",
        "file",
        "logs",
        "lmao",
        "limeyo",
    ];

    for n in 1..=110000 {
        let entry = format!("Entry number is {}", n);
        let _ = wal.write(entry.as_bytes());
    }

    /*for entry in entries {
         wal.write(entry.as_bytes());
    }*/

    // wal.read_all();
}
