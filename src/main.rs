
mod segment;
mod wal;
mod veresiye;
mod table;
mod util;

fn main() {
    
    let mut db = veresiye::Veresiye::new(String::from("./data")).unwrap();

    db.set("key1", "value1");

    println!("{:?}", db.get_all_sstable_dir());

   //  match db.get("key1") {
   //      Ok(v) => println!("{}", v),
   //      Err(e) => eprintln!("Error: {}", e)
   //  };

    db.compact().unwrap();
    db.cleanup_logs();
}
