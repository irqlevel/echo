

use tokio::net::TcpListener;

use std::env;
use std::error::Error;

extern crate common_lib;

use common_lib::error::error::ServerError;
use common_lib::frame::frame::{RequestHeader, Request, Response};

use common_lib::socket::socket::write as socket_write;
use common_lib::socket::socket::read as socket_read;

use std::sync::Arc;
use log::{error, info, LevelFilter};

use common_lib::logger::logger::SimpleLogger;

struct Server;

impl Server {

    fn new() -> Self {
        Server{}
    }

    async fn read_header(&self, socket: &mut tokio::net::TcpStream) -> Result<RequestHeader, ServerError> {
        let mut header_bytes = [0; std::mem::size_of::<RequestHeader>()];

        socket_read(socket, &mut header_bytes).await?;

        let mut header = RequestHeader::new();
        header.from_bytes(&header_bytes)?;

        Ok(header)
    }

    async fn read_body(&self, size: usize, socket: &mut tokio::net::TcpStream) -> Result<Vec<u8>, ServerError> {
        let mut body = vec![0; size as usize];

        socket_read(socket, &mut body).await?;
        return Ok(body)
    }

    async fn read_request(&self, socket: &mut tokio::net::TcpStream) -> Result<Request, ServerError> {
        let header: RequestHeader = self.read_header(socket).await?;

        let body: Vec<u8> = self.read_body(header.size as usize, socket).await?;

        return Ok(Request{header: header, body: body})
    }

    async fn handle_req(&self, req: &Request, resp: &mut Response) -> Result<(), ServerError> {
        resp.body.resize(req.body.len(), 0);
        resp.body.copy_from_slice(&req.body);
        return Ok(())
    }

    async fn write_response(&self, socket: &mut tokio::net::TcpStream, resp: &Response) -> Result<(), ServerError> {

        let header_bytes = resp.header.to_bytes()?;

        socket_write(socket, &header_bytes).await?;

        socket_write(socket, &resp.body).await?;

        Ok(())
    }

    async fn handle_connection(&self, socket: &mut tokio::net::TcpStream) -> Result<(), ServerError> {
        let req: Request = self.read_request(socket).await?;

        let mut resp = Response::new();
        self.handle_req(&req, &mut resp).await?;
        resp.header.size = resp.body.len() as u32;
        self.write_response(socket, &resp).await?;

        Ok(())
    }

}

static LOGGER: SimpleLogger = SimpleLogger;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {

    match log::set_logger(&LOGGER).map(|()| log::set_max_level(LevelFilter::Info)) {
        Ok(()) => {},
        Err(e) => {
            println!("set_logger error {}", e);
            return Err("set_logger error".into())
        }
    }

    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".to_string());

    let listener = match TcpListener::bind(&addr).await {
        Ok(v) => v,
        Err(e) => {
            error!("bind {} error {}", addr, e);
            return Err("something went wrong".into())
        }
    };
    info!("listening on {}", addr);

    let server = Arc::new(Server::new());
    loop {
        let (mut socket, _) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                error!("accept error {}", e);
                continue
            }
        };

        let server_ref = server.clone();
        tokio::spawn(async move {
            match server_ref.handle_connection(&mut socket).await {
                Ok(()) => {},
                Err(e) => {
                    error!("handle_connection error {}", e);
                }
            }
        });
    }
}