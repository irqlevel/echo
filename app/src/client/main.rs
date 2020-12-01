use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

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
    async fn send(&mut self, req: &Request) -> Result<Response, ServerError> {
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

    fn new() -> Client {
        Client{socket: None}
    }

    async fn connect(&mut self, addr: &str) -> Result<(), ServerError> {
        let socket = match tokio::net::TcpStream::connect(addr).await {
            Ok(v) => {
                println!("connected");
                v},
            Err(e) => {
                println!("connect error {}", e);
                return Err(ServerError::new());
            }
        };

        self.socket = Some(socket);
        Ok(())
    }

    fn close(&mut self) {

        match self.socket.as_mut() {
            Some(e) => {
                e.shutdown();
            }
            None => {
            }
        }
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        println!("dropping");
        self.close()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".to_string());

    let mut client = Client::new();
    
    match client.connect(&addr).await {
        Ok(v) => {
            println!("success");
            v},
        Err(e) => {
            println!("connect error {}", e);
            return Err("something went wrong".into())
        }
    }

    let req = Request::new();

    let resp = match client.send(&req).await {
        Ok(v) => {
            println!("success");
            v},
        Err(e) => {
            println!("send error {}", e);
            return Err("something went wrong".into())
        }
    };

    Ok(())
}