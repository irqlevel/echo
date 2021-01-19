

use tokio::net::TcpListener;

use std::env;
use std::error::Error;

extern crate common_lib;

use common_lib::error::error::CommonError;
use common_lib::frame::frame::{Request, Response, EchoRequest, EchoResponse};

use std::sync::Arc;
use log::{error, info, LevelFilter};

use common_lib::logger::logger::SimpleLogger;

use serde_derive::Deserialize;
use std::fs::File;
use std::io::Read;
use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Deserialize)]
struct Config {
    address: String,
    nodes: Vec<String>,
    cluster_id: String,
    storage_path: String
}

use tokio::sync::RwLock;
use tokio::signal::unix::{signal, SignalKind};
use tokio::time::{sleep, Duration};

struct Server {
    config: Config,
    task_count: AtomicUsize,
    shutdown: bool
}

impl Server {

    fn new(config: Config) -> Self {
        Server{config: config, shutdown: false, task_count: AtomicUsize::new(0)}
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

    fn set_shutdown(&mut self) {
        self.shutdown = true;
    }

    async fn heartbeat(&self) -> Result<(), CommonError> {
        Ok(())
    }

    async fn shutdown(server: &ServerRef) {
        info!("shutdowning");

        server.write().await.set_shutdown();

        loop {
            let pending_tasks = server.read().await.task_count.fetch_add(0, Ordering::SeqCst);
            if pending_tasks == 0 {
                break;
            }

            info!("pending tasks {}", pending_tasks);

            sleep(Duration::from_millis(1000)).await;
        }

        info!("shutdowned");

        std::process::exit(0);
    }

    async fn run(server: &ServerRef) -> Result<(), CommonError> {
        let mut sig_term_stream = match signal(SignalKind::terminate()) {
            Ok(v) => v,
            Err(e) => {
                error!("signal error {}", e);
                return Err(CommonError::new(format!("signal error")));
            }
        };

        let mut sig_int_stream = match signal(SignalKind::interrupt()) {
            Ok(v) => v,
            Err(e) => {
                error!("signal error {}", e);
                return Err(CommonError::new(format!("signal error")));
            }
        };

        let listener = match TcpListener::bind(server.read().await.config.address.clone()).await {
            Ok(v) => v,
            Err(e) => {
                error!("bind {} error {}", server.read().await.config.address, e);
                return Err(CommonError::new(format!("bind error")));
            }
        };

        let server_ref = server.clone();
        server.read().await.task_count.fetch_add(1, Ordering::SeqCst);
        tokio::spawn(async move {
            loop {
                match server_ref.read().await.heartbeat().await {
                    Ok(()) => {},
                    Err(e) => {
                        error!("heartbeat error {}", e);
                    }
                }
                if server_ref.read().await.shutdown {
                    break;
                }
            }
            server_ref.read().await.task_count.fetch_sub(1, Ordering::SeqCst);
        });

        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok(connection) => {
                            let mut socket = connection.0;
                            let server_ref = server.clone();
                            server.read().await.task_count.fetch_add(1, Ordering::SeqCst);
                            tokio::spawn(async move {
                                match server_ref.read().await.handle_connection(&mut socket).await {
                                    Ok(()) => {},
                                    Err(e) => {
                                        error!("handle_connection error {}", e);
                                    }
                                }
                                server_ref.read().await.task_count.fetch_sub(1, Ordering::SeqCst);
                            });        
                        }
                        Err(e) => {
                            error!("handle_connection error {}", e);
                        }
                    }    
                }

                _sig_term_result = sig_term_stream.recv() => {
                    info!("received sig term");
                    let server_ref = server.clone();
                    Server::shutdown(&server_ref).await;
                }

                _sig_int_result = sig_int_stream.recv() => {
                    info!("received sig int");
                    let server_ref = server.clone();
                    Server::shutdown(&server_ref).await;
                }
            }
        }
    }
}

static LOGGER: SimpleLogger = SimpleLogger;

type ServerRef = Arc<RwLock<Server>>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {

    match log::set_logger(&LOGGER).map(|()| log::set_max_level(LevelFilter::Info)) {
        Ok(()) => {},
        Err(e) => {
            println!("set_logger error {}", e);
            return Err("set_logger error".into())
        }
    }

    let config_path = env::args()
        .nth(1)
        .unwrap_or_else(|| "/etc/rserver/config.toml".to_string());

    let mut config_file = File::open(config_path)?;
    let mut config_data = Vec::new();
    config_file.read_to_end(&mut config_data)?;
    let config: Config = toml::from_slice(&config_data)?;

    info!("listening on {} clusterId {} nodes {:?} storage_path {}", config.address, config.cluster_id, config.nodes, config.storage_path);

    fs::create_dir_all(&config.storage_path)?;

    let server = Arc::new(RwLock::new(Server::new(config)));
    match Server::run(&server).await {
        Ok(()) => Ok(()),
        Err(e) => {
            error!("run error {}", e);
            return Err("run error".into())
        }
    }
}