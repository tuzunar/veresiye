
mod segment;
mod wal;
mod veresiye;
mod table;
mod util;

fn main() {
    
    let mut db = veresiye::Veresiye::new(String::from("./data")).unwrap();

    db.set("key3", "value3");

    println!("{:?}", db.get_all_sstable_dir());

   //  match db.get("key1") {
   //      Ok(v) => println!("{}", v),
   //      Err(e) => eprintln!("Error: {}", e)
   //  };

    db.compact().unwrap();
}
