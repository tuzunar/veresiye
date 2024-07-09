mod filter;
mod manifest;
mod memdb;
mod table;
mod util;
mod veresiye;
mod wal;

fn main() {
    let mut db = veresiye::Veresiye::new(String::from("./data")).unwrap();

    // println!("{:?}", db.get_all_sstable_dir());

    // for n in 10..20 {
    //     let key = format!("key{}", n);
    //     let value = format!("value{}", n);

    //     db.set(&key, &value);
    // }

    // for n in 0..10925 {
    //     let key = format!("key{}", n);
    //     let value = format!("value{}", n);

    //     db.set(&key, &value);
    // }
    // println!("{}", db.get_memdb_size());

    // for n in 10924..21848 {
    //     let key = format!("key{}", n);
    //     let value = format!("value{}", n);

    //     db.set(&key, &value);
    // }

    // println!("{}", db.get_memdb_size());
    // db.get("key17566");

    // thread::sleep(Duration::from_millis(4000));

    match db.get("key10923") {
        Some(v) => println!("{}", v),
        None => eprintln!("Error: Value Not Found"),
    };

    //  db.compact().unwrap();
    //  db.cleanup_logs();
}
