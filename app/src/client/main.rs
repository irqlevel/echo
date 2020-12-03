use tokio::io::{AsyncWriteExt};

use std::env;
use std::error::Error;
use log::{error, info, LevelFilter};

extern crate common_lib;

use common_lib::frame::frame::{Request, Response};

use common_lib::socket::socket::write as socket_write;
use common_lib::socket::socket::read as socket_read;
use common_lib::error::error::ServerError;

use common_lib::logger::logger::SimpleLogger;

struct Client {
    socket: Option<tokio::net::TcpStream>
}

impl Client {
    async fn send(&mut self, req: &mut Request) -> Result<Response, ServerError> {
        req.header.size = req.body.len() as u32;
        let req_header_bytes = req.header.to_bytes()?;

        match self.socket.as_mut() {
            Some(e) => {
                socket_write(e, &req_header_bytes).await?;
            }
            None => {
                return Err(ServerError::new())
            }
        }

        match self.socket.as_mut() {
            Some(e) => {
                socket_write(e, &req.body).await?;
            }
            None => {
                return Err(ServerError::new())
            }
        }

        let mut resp = Response::new();
        let mut resp_header_bytes = resp.header.to_bytes()?;

        match self.socket.as_mut() {
            Some(e) => {
                socket_read(e, &mut resp_header_bytes).await?;
            }
            None => {
                return Err(ServerError::new())
            }
        }

        resp.header.from_bytes(&resp_header_bytes)?;

        resp.body.resize(resp.header.size as usize, 0);

        match self.socket.as_mut() {
            Some(e) => {
                socket_read(e, &mut resp.body).await?;
            }
            None => {
                return Err(ServerError::new())
            }
        }

        Ok(resp)
    }

    async fn send2<T: ?Sized, V: ?Sized>(&mut self, request: &T) -> Result<V, ServerError>
    where
        T: serde::Serialize,
        V: serde::de::DeserializeOwned
    {
        let mut req = Request::new();
        req.body = match bincode::serialize(request) {
            Ok(v) => v,
            Err(e) => {
                error!("serialize error {}", e);
                return Err(ServerError::new())
            }
        };

        let resp = self.send(&mut req).await?;
    
        let decoded: V = match bincode::deserialize(&resp.body[..]) {
            Ok(v) => v,
            Err(e) => {
                error!("deserialize error {}", e);
                return Err(ServerError::new())
            }
        };

        Ok(decoded)
    }

    fn new() -> Client {
        Client{socket: None}
    }

    async fn connect(&mut self, addr: &str) -> Result<(), ServerError> {
        let socket = match tokio::net::TcpStream::connect(addr).await {
            Ok(v) => {
                v},
            Err(e) => {
                error!("connect error {}", e);
                return Err(ServerError::new());
            }
        };

        self.socket = Some(socket);
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ServerError> {

        match self.socket.as_mut() {
            Some(e) => {
                match e.shutdown().await {
                    Ok(()) => Ok(()),
                    Err(e) => {
                        error!("socket shutdown error{}", e);
                        return Err(ServerError::new());
                    }
                }
            }
            None => {
                Ok(())
            }
        }
    }
}

use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Entity {
    x: f32,
    y: f32,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct World(Vec<Entity>);

async fn test_client() -> Result<(), ServerError> {
    let addr = env::args()
    .nth(1)
    .unwrap_or_else(|| "127.0.0.1:8080".to_string());

    let mut client = Client::new();
    
    client.connect(&addr).await?;

    let world = World(vec![Entity { x: 0.0, y: 4.0 }, Entity { x: 10.0, y: 20.5 }]);

    let decoded: World = client.send2(&world).await?;

    assert_eq!(world, decoded);

    client.close().await?;

    Ok(())
}

static LOGGER: SimpleLogger = SimpleLogger;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {

    match log::set_logger(&LOGGER)
        .map(|()| log::set_max_level(LevelFilter::Info)) {
        Ok(()) => {},
        Err(e) => {
            println!("set_logger error {}", e);
            return Err("set_logger error".into())
        }
    }

    info!("starting");

    let mut handles = vec![];
    for _ in 0..1000000 {
        handles.push(
        tokio::spawn(async move {
            match test_client().await {
                Ok(()) => {},
                Err(e) => {
                    error!("test_client error {}", e);
                }
            }
        }));
    }
    futures::future::join_all(handles).await;
    info!("completed");

    Ok(())
}