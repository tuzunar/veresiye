#![allow(warnings)]
mod filter;
mod manifest;
mod memdb;
mod table;
mod util;
pub mod veresiye;
mod wal;

// #[tokio::main]
// pub async fn launch_server() -> Result<(), Box<dyn std::error::Error>> {
//     let listener = TokioTcpListener::bind("127.0.0.1:19230").await?;

//     loop {
//         let (socket, _) = listener.accept().await?;
//         tokio::spawn(async move {
//             let mut stream = socket;

//             match protocol::handle_connection(&mut stream).await {
//                 Ok(_) => (),
//                 Err(e) => {
//                     eprintln!("Error handling connection: {}", e);
//                 }
//             }
//         });
//     }
// }

// mod protocol {

//     use tokio::net::TcpStream;

//     pub async fn handle_connection(stream: &TcpStream) -> Result<(), Box<dyn std::error::Error>> {
//         // use tokio::io::{AsyncReadExt, AsyncWriteExt};

//         println!("{:?}", stream);
//         Ok(())
//     }
// }
