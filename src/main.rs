use rand::seq::index;

mod segment;
mod wal;

fn main() {
    let mut wal = wal::Log::open("./log", 5).unwrap();

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

    // for n in 1..=10 {
    //     let entry = format!("Entry number is {}", n);
    //     let _ = wal.write(entry.as_bytes());
    // }

    // let res = wal.list_logs();
    // for (index, path) in res.iter().enumerate() {
    //     println!("Index: {} Path: {}", index, path.display())
    // }

    let result = wal.read(0);

    match result {
        Ok(c) => println!("{}", c),
        Err(e) => eprint!("{}", e),
    }

    /*for entry in entries {
         wal.write(entry.as_bytes());
    }*/

    // wal.read_all();
}
