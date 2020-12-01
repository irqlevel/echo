

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use std::env;
use std::error::Error;

mod frame;
use frame::{Request, RequestHeader};
use frame::{Response};

mod error;
use error::ServerError;

use std::sync::Arc;

struct Server {
}

impl Server {

    fn new() -> Server {
        Server{}
    }

    async fn read(&self, socket: &mut tokio::net::TcpStream, buf: &mut [u8]) -> Result<(), ServerError> {
        let mut read = 0;
        while read < buf.len() {
            let n  = match socket.read(&mut buf[read..]).await {
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
        Ok(())
    }

    async fn write(&self, socket: &mut tokio::net::TcpStream, buf: &[u8]) -> Result<(), ServerError> {
        let mut written = 0;
        while written < buf.len() {
            let n  = match socket.write(&buf[written..]).await {
                Ok(n) => n,
                Err(e) => {
                    println!("socker write error {}", e);
                    return Err(ServerError::new())
                }
            };

            written += n;
        }
        Ok(())
    }

    async fn read_header(&self, socket: &mut tokio::net::TcpStream) -> Result<RequestHeader, ServerError> {
        let mut header_bytes = [0; std::mem::size_of::<RequestHeader>()];

        self.read(socket, &mut header_bytes).await?;

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

        self.read(socket, &mut body).await?;
        return Ok(body)
    }

    async fn read_request(&self, socket: &mut tokio::net::TcpStream) -> Result<Request, ServerError> {
        let header: RequestHeader = self.read_header(socket).await?;

        let body: Vec<u8> = self.read_body(header.size as usize, socket).await?;

        return Ok(Request{header: header, body: body})
    }

    async fn handle_req(&self, req: &Request, resp: &mut Response) -> Result<(), ServerError> {
        //TODO
        return Ok(())
    }

    async fn write_response(&self, socket: &mut tokio::net::TcpStream, resp: &Response) -> Result<(), ServerError> {

        let header_bytes = resp.header.to_bytes()?;

        self.write(socket, &header_bytes).await?;

        self.write(socket, &resp.body).await?;

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