use tokio::io::{AsyncWriteExt};

use std::env;
use std::error::Error;

extern crate common_lib;

use common_lib::frame::frame::{Request, Response};

use common_lib::socket::socket::write as socket_write;
use common_lib::socket::socket::read as socket_read;
use common_lib::error::error::ServerError;


struct Client {
    socket: Option<tokio::net::TcpStream>
}

impl Client {
    async fn send(&mut self, req: &mut Request) -> Result<Response, ServerError> {
        req.header.size = req.body.len() as u32;
        let req_header_bytes = req.header.to_bytes()?;

        //println!("header {:?}", req_header_bytes);
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

    fn new() -> Client {
        Client{socket: None}
    }

    async fn connect(&mut self, addr: &str) -> Result<(), ServerError> {
        let socket = match tokio::net::TcpStream::connect(addr).await {
            Ok(v) => {
                //println!("connected");
                v},
            Err(e) => {
                println!("connect error {}", e);
                return Err(ServerError::new());
            }
        };

        self.socket = Some(socket);
        Ok(())
    }

    async fn close(&mut self) {

        match self.socket.as_mut() {
            Some(e) => {
                e.shutdown().await;
            }
            None => {
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut handles = vec![];
    for _ in 0..1000000 {
        handles.push(
        tokio::spawn(async move {
            let addr = env::args()
            .nth(1)
            .unwrap_or_else(|| "127.0.0.1:8080".to_string());

            let mut client = Client::new();
            
            match client.connect(&addr).await {
                Ok(v) => {
                    //println!("success");
                    v},
                Err(e) => {
                    println!("connect error {}", e);
                    return Err(e)
                }
            }

            let mut req = Request::new();
            let message = String::from("Hello!!! Hahahsa");
            req.body = message.clone().into_bytes();

            let resp = match client.send(&mut req).await {
                Ok(v) => {
                    //println!("success");
                    v},
                Err(e) => {
                    println!("send error {}", e);
                    return Err(e)
                }
            };
            let output = String::from_utf8_lossy(&resp.body);
            if message != output {
                println!("invalid output");
            }

            Ok(())
        }));
    }
    futures::future::join_all(handles).await;
    Ok(())
}