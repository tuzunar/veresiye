use std::{
    sync::{Arc, Mutex},
    thread,
};

mod wal;

fn main() {
    let log_instance = Arc::new(Mutex::new(wal::Log {
        mu: Mutex::new(()),
        path: String::from("log"),
        closed: false,
        corrupt: false,
        log_file: std::fs::File::create("log").expect("Error creating File"),
        first_index: 0,
        last_index: 0,
    }));

    let thread1 = thread::spawn({
        let log_instance = Arc::clone(&log_instance);
        move || {
            let mut log_instance = log_instance.lock().unwrap();
            match log_instance.write() {
                Ok(_) => println!("Data writen without error"),
                Err(e) => eprintln!("Error: {}", e),
            };
        }
    });
    let thread2 = thread::spawn({
        let log_instance = Arc::clone(&log_instance);
        move || {
            let mut log_instance = log_instance.lock().unwrap();
            match log_instance.write() {
                Ok(_) => println!("Data writen without error"),
                Err(e) => eprintln!("Error: {}", e),
            };
        }
    });

    thread1.join().unwrap();
    thread2.join().unwrap();
}
