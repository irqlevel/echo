

use tokio::net::TcpListener;

use std::env;
use std::error::Error;

extern crate common_lib;

use common_lib::error::error::CommonError;
use common_lib::frame::frame::{Request, Response, EchoRequest, EchoResponse};

use std::sync::Arc;
use log::{error, info, LevelFilter};

use common_lib::logger::logger::SimpleLogger;

struct Server;

impl Server {

    fn new() -> Self {
        Server{}
    }

    async fn handle_echo(&self, request: &EchoRequest) -> Result<EchoResponse, CommonError> {
        let mut response = EchoResponse::new();
        response.message = request.message.clone();
        Ok(response)
    }

    async fn dispatch_request(&self, request: &Request) -> Result<Response, CommonError> {
        let mut response = Response::new(&request.req_id, "");

        if request.version.as_str() != "1.0.0" {
            response.error = format!("unsupported version {}", request.version);
            return Ok(response)
        }

        match request.path.as_str() {
            "echo" => {
                let req: EchoRequest = match bincode::deserialize(&request.body) {
                    Ok(v) => v,
                    Err(e) => {
                        error!("deserialize error {}", e);
                        response.error = format!("deserialize error {}", e);
                        return Ok(response)
                    }
                };
                let resp = self.handle_echo(&req).await?;
                response.body = match bincode::serialize(&resp) {
                    Ok(v) => v,
                    Err(e) => {
                        error!("serialize error {}", e);
                        response.error = format!("serialize error {}", e);
                        return Ok(response)
                    }
                };
                return Ok(response)
            }
            _ => {
                response.error = format!("unsupported path {}", request.path);
                return Ok(response)
            }
        }
    }

    async fn handle_connection(&self, socket: &mut tokio::net::TcpStream) -> Result<(), CommonError> {
        let request = Request::recv(socket).await?;

        let response = self.dispatch_request(&request).await?;

        Response::send(socket, &response).await?;

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
            return Err("bind error".into())
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