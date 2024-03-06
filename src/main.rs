
mod segment;
mod wal;
mod veresiye;

fn main() {
    let mut wal = wal::Log::open("./log", 5).unwrap();

     for n in 1..=100 {
         let entry = format!("Entry number is {}", n);
         let _ = wal.write(entry.as_bytes());
    }

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
