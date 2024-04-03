use std::{thread, time::Duration};


mod segment;
mod wal;
mod veresiye;
mod table;
mod util;
mod filter;

fn main() {
    
    let mut db = veresiye::Veresiye::new(String::from("./data")).unwrap();


    println!("{:?}", db.get_all_sstable_dir());


   for n in 0..10 {

      let key = format!("key{}", n);
      let value = format!("value{}", n);

      db.set(&key, &value);

   }

   // thread::sleep(Duration::from_millis(4000));

   match db.get("key11") {
      Ok(v) => println!("{}", v),
      Err(e) => eprintln!("Error: {}", e)
   };

   //  db.compact().unwrap();
   //  db.cleanup_logs();
}
