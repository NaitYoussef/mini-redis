use mini_redis::{Command, Connection, Frame};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::{result, thread};
use std::thread::sleep;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::task;

type DB = Arc<Mutex<HashMap<String,String>>>;

#[tokio::main]
async fn main() {
    // Bind the listener to the address
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let db = Arc::new(Mutex::new(HashMap::new()));
    loop {
        // The second item contains the IP and port of the new connection.
        let (socket, _) = listener.accept().await.unwrap();
        tokio::spawn(process(socket, db.clone()));
        println!("Loop {:?}", thread::current().id());
    }
}

async fn process(socket: TcpStream, db: DB) {
    // The `Connection` lets us read/write redis **frames** instead of
    // byte streams. The `Connection` type is defined by mini-redis.
    let mut connection = Connection::new(socket);
    println!("Process {:?} {:?}", task::id(), thread::current().id());
    if let Ok(Some(frame)) = connection.read_frame().await {
        // Result is not a Future OK
        let response = match Command::from_frame(frame).unwrap() {
            Command::Get(param) => {
                let result = db.lock().unwrap();
                if let Some(value) = result.get(param.key()) {
                    Frame::Bulk(value.to_string().into())
                } else {
                    Frame::Bulk("Empty".to_string().into())
                }
            }
            Command::Set(param) => {
                let mut map = db.lock().unwrap();
                let bytes = param.value();
                map.insert(param.key().to_string(), String::from_utf8(bytes.to_vec()).unwrap());
                Frame::Error("OK".to_string())
            }
            _ => {Frame::Error("Not Implemented".to_string())}
        };
        // Respond with an error
        connection.write_frame(&response).await.unwrap();
    } else {
        println!("Could not read frame");
    }
}
