
mod segment;
mod wal;
mod veresiye;
mod table;

fn main() {
    
    let mut db = veresiye::Veresiye::new(String::from("./data")).unwrap();

    db.set("key1", "value2");

    match db.get("key1") {
        Ok(v) => println!("{}", v),
        Err(e) => eprintln!("Error: {}", e)
    };

    // let res = wal.list_logs();
    // for (index, path) in res.iter().enumerate() {
    //     println!("Index: {} Path: {}", index, path.display())
    // }


    /*for entry in entries {
         wal.write(entry.as_bytes());
    }*/

    // wal.read_all();
}
