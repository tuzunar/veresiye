use std::{thread, time::Duration};


mod segment;
mod wal;
mod veresiye;
mod table;
mod util;
mod filter;
mod memdb;

fn main() {
    
    let mut db = veresiye::Veresiye::new(String::from("./data")).unwrap();


    println!("{:?}", db.get_all_sstable_dir());


   for n in 0..10924 {

      let key = format!("key{}", n);
      let value = format!("value{}", n);


      db.set(&key, &value);

   }
   println!("{}", db.get_memdb_size());

   // thread::sleep(Duration::from_millis(4000));

   // match db.get("key11") {
   //    Ok(v) => println!("{}", v),
   //    Err(e) => eprintln!("Error: {}", e)
   // };

   //  db.compact().unwrap();
   //  db.cleanup_logs();
}
