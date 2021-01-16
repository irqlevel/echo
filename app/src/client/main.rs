use tokio::io::{AsyncWriteExt};

use std::env;
use std::error::Error;
use log::{error, info, LevelFilter};

extern crate common_lib;

use common_lib::frame::frame::{Request, Response, EchoRequest, EchoResponse};

use common_lib::error::error::CommonError;

use common_lib::logger::logger::SimpleLogger;
use bincode;

struct Client {
    socket: Option<tokio::net::TcpStream>
}

impl Client {
    async fn send<T: ?Sized, V: ?Sized>(&mut self, path: &str, request: &T) -> Result<V, CommonError>
    where
        T: serde::Serialize,
        V: serde::de::DeserializeOwned
    {
        match self.socket.as_mut() {
            Some(sock) => {
                let mut req = Request::new(path);

                req.body = match bincode::serialize(&request) {
                    Ok(v) => v,
                    Err(e) => {
                        error!("serialize error {}", e);
                        return Err(CommonError::new(format!("serialize error {}", e)))
                    }
                };
                Request::send(sock, &req).await?;

                let resp = Response::recv(sock).await?;
                if resp.req_id != req.req_id {
                    return Err(CommonError::new(format!("unexpected response req_id {} vs {}", resp.req_id, req.req_id)))
                }
                if resp.error != "" {
                    return Err(CommonError::new(format!("response error {}", resp.error)))
                }

                let response: V = match bincode::deserialize(&resp.body) {
                    Ok(v) => v,
                    Err(e) => {
                        error!("deserialize error {}", e);
                        return Err(CommonError::new(format!("deserialize error {}", e)))
                    }
                };
        
                Ok(response)
            }
            None => {
                return Err(CommonError::new(format!("socket doesn't exists")))
            }
        }
    }

    fn new() -> Client {
        Client{socket: None}
    }

    async fn connect(&mut self, addr: &str) -> Result<(), CommonError> {
        let socket = match tokio::net::TcpStream::connect(addr).await {
            Ok(v) => {
                v},
            Err(e) => {
                error!("connect error {}", e);
                return Err(CommonError::new(format!("connect error {}", e)))
            }
        };

        self.socket = Some(socket);
        Ok(())
    }

    async fn close(&mut self) -> Result<(), CommonError> {

        match self.socket.as_mut() {
            Some(e) => {
                match e.shutdown().await {
                    Ok(()) => Ok(()),
                    Err(e) => {
                        error!("socket shutdown error{}", e);
                        return Err(CommonError::new(format!("socket shutdown error{}", e)))
                    }
                }
            }
            None => {
                Ok(())
            }
        }
    }
}

async fn test_client() -> Result<(), CommonError> {
    let addr = env::args()
    .nth(1)
    .unwrap_or_else(|| "127.0.0.1:8080".to_string());

    let mut client = Client::new();
    
    client.connect(&addr).await?;

    let mut req = EchoRequest::new();
    req.message = "Hello world!!!!".to_string();

    let resp: EchoResponse = client.send("echo", &req).await?;

    assert_eq!(req.message, resp.message);

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
    for _ in 0..100000 {
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