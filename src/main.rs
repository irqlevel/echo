

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use std::env;
use std::error::Error;

mod frame;
use frame::{Request, RequestHeader};
use frame::{Response};

mod error;
use error::ServerError;

struct Server {
}

impl Server {

    fn new() -> Server {
        Server{}
    }

    async fn read_header(&self, socket: &mut tokio::net::TcpStream) -> Result<RequestHeader, ServerError> {
        let mut header_bytes = [0; std::mem::size_of::<RequestHeader>()];
        let mut read = 0;
        while read < header_bytes.len() {
            let n  = match socket.read(&mut header_bytes[read..]).await {
                Ok(n) => n,
                Err(e) => {
                    println!("socker read error {}", e);
                    return Err(ServerError::new())
                }
            };

            if n == 0 {
                return Err(ServerError::new())
            }
            read += n;
        }

        let mut header = RequestHeader::new();
        match header.from_bytes(&header_bytes) {
            Ok(()) => {},
            Err(e) => {
                println!("from_bytes error {}", e);
                return Err(e)
            }
        }

        Ok(header)
    }

    async fn read_body(&self, size: usize, socket: &mut tokio::net::TcpStream) -> Result<Vec<u8>, ServerError> {
        let mut body = vec![0; size as usize];
        let mut read = 0;
        while read < size as usize {
            let n = match socket.read(&mut body[read..]).await {
                    Ok(n) => n,
                    Err(e) => {
                        println!("socker read error {}", e);
                        return Err(ServerError::new())    
                    }
                };

            if n == 0 {
                return Err(ServerError::new())
            }
            read += n;
        }
        return Ok(body)
    }

    async fn read_request(&self, socket: &mut tokio::net::TcpStream) -> Result<Request, ServerError> {

        let header: RequestHeader = match self.read_header(socket).await {
            Ok(v) => v,
            Err(e) => {
                return Err(e)
            }
        };

        let body: Vec<u8> = match self.read_body(header.size as usize, socket).await {
            Ok(v) => v,
            Err(e) => {
                return Err(e)
            }
        };

        return Ok(Request{header: header, body: body})
    }

    async fn handle_req(&self, req: &Request, resp: &mut Response) -> Result<(), ServerError> {
        //TODO
        return Ok(())
    }

    async fn write_response(&self, socket: &mut tokio::net::TcpStream, resp: &Response) -> Result<(), ServerError> {
        let mut written = 0;

        let header_bytes = match resp.header.to_bytes() {
            Ok(v) => v,
            Err(e) => return Err(e)
        };

        while written < header_bytes.len() {
            let n = match socket.write(&header_bytes[written..]).await {
                Ok(n) => n,
                Err(e) => {
                    println!("socket write error {}", e);
                    return Err(ServerError::new())
                }
            };
            written += n;
        }

        written = 0;
        while written < resp.body.len() {
            let n = match socket.write(&resp.body[written..]).await {
                Ok(n) => n,
                Err(e) => {
                    println!("socket write error {}", e);
                    return Err(ServerError::new())
                }
            };
            written += n;
        }
        Ok(())
    }

    async fn handle_connection(&self, socket: &mut tokio::net::TcpStream) {

        let req: Request = match self.read_request(socket).await {
            Ok(v) => v,
            Err(e) => {
                println!("read_request error{}", e);
                return
            }
        };

        let mut resp = Response::new();
        match self.handle_req(&req, &mut resp).await {
            Ok(()) => {},
            Err(e) => {
                println!("handle_req error{}", e);
                return
            }
        }

        match self.write_response(socket, &resp).await {
            Ok(()) => {},
            Err(e) => {
                println!("write_response error{}", e);
                return
            }
        }
    }

}

use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".to_string());

    let listener = match TcpListener::bind(&addr).await {
        Ok(v) => v,
        Err(e) => {
            println!("bind {} error {}", addr, e);
            return Err("something went wrong".into())
        }
    };
    println!("listening on {}", addr);

    let server = Arc::new(Server::new());
    loop {
        let (mut socket, _) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                println!("accept error {}", e);
                continue
            }
        };

        let server_ref = server.clone();
        tokio::spawn(async move {
            server_ref.handle_connection(&mut socket).await;
        });
    }
}