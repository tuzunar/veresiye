use std::{thread, time::Duration};

mod filter;
mod memdb;
mod segment;
mod table;
mod util;
mod veresiye;
mod wal;

fn main() {
    let mut db = veresiye::Veresiye::new(String::from("./data")).unwrap();

    // println!("{:?}", db.get_all_sstable_dir());

    for n in 10924..21848 {
        let key = format!("key{}", n);
        let value = format!("value{}", n);

        db.set(&key, &value);
    }

    println!("{}", db.get_memdb_size());
    db.get("key13");

    // thread::sleep(Duration::from_millis(4000));

    // match db.get("key11") {
    //    Ok(v) => println!("{}", v),
    //    Err(e) => eprintln!("Error: {}", e)
    // };

    //  db.compact().unwrap();
    //  db.cleanup_logs();
}
