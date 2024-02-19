
mod wal;



fn main() {
    let mut log_instance = wal::Log{
        mu: std::sync::Mutex::new(()),
        path: String::from("log"),
        closed: false,
        corrupt: false,
        log_file: std::fs::File::create("log").expect("Error creating File"),
        first_index: 0,
        last_index: 0,
    };

    match log_instance.write() {
        Ok(_) => println!("Data writen without error"),
        Err(e) => eprintln!("Error: {}", e)
    }
}
