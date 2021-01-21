

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
use common_lib::client::client::Client;

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

type ServerRef = Arc<RwLock<Server>>;

impl Server {

    fn new(config: Config) -> Self {
        Server{config: config, shutdown: false, task_count: AtomicUsize::new(0)}
    }

    async fn handle_echo(_server: &ServerRef, request: &EchoRequest) -> Result<EchoResponse, CommonError> {
        let mut response = EchoResponse::new();
        response.message = request.message.clone();
        Ok(response)
    }

    async fn dispatch_request(server: &ServerRef, request: &Request) -> Result<Response, CommonError> {
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
                let resp = Server::handle_echo(server, &req).await?;
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

    async fn handle_connection(server: &ServerRef, socket: &mut tokio::net::TcpStream) -> Result<(), CommonError> {
        let request = Request::recv(socket).await?;

        let response = Server::dispatch_request(server, &request).await?;

        Response::send(socket, &response).await?;

        Ok(())
    }

    async fn heartbeat(server: &ServerRef) -> Result<(), CommonError> {
        info!("heartbeat");

        let mut nodes : Vec<String> = Vec::new();

        {
            let rserver = server.read().await;
            for node in &rserver.config.nodes {
                nodes.push(node.to_string());
            }
        }

        for node in nodes {
            let mut client = Client::new();

            info!("connecting {}", node);
            client.connect(&node).await?;
            info!("connected {}", node);

            let mut req = EchoRequest::new();
            req.message = "Hello world!!!!".to_string();
            let resp: EchoResponse = client.send("echo", &req).await?;
            info!("response {}", resp.message);
            client.close().await?;
        }

        info!("heartbeat done");
        Ok(())
    }

    async fn shutdown(server: &ServerRef) {
        info!("shutdowning");

        {
            let mut wserver = server.write().await;
            wserver.shutdown = true;
        }

        loop {
            let pending_tasks = Server::get_pending_tasks(server).await;
            if pending_tasks == 0 {
                break;
            }

            info!("pending tasks {}", pending_tasks);
            sleep(Duration::from_millis(1000)).await;
        }

        info!("shutdowned");
        std::process::exit(0);
    }

    async fn get_pending_tasks(server: &ServerRef) -> usize {
        let rserver = server.read().await;
        return rserver.task_count.fetch_add(0, Ordering::SeqCst);
    }

    async fn inc_pending_tasks(server: &ServerRef) {
        let rserver = server.read().await;
        rserver.task_count.fetch_add(1, Ordering::SeqCst);
    }

    async fn dec_pending_tasks(server: &ServerRef) {
        let rserver = server.read().await;
        rserver.task_count.fetch_sub(1, Ordering::SeqCst);
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

        let address = {
            let rserver = server.read().await;
            rserver.config.address.clone()
        };

        let listener = match TcpListener::bind(&address).await {
            Ok(v) => v,
            Err(e) => {
                error!("bind {} error {}", address, e);
                return Err(CommonError::new(format!("bind error")));
            }
        };

        let server_ref = server.clone();
        Server::inc_pending_tasks(&server_ref).await;
        tokio::spawn(async move {
            loop {
                match Server::heartbeat(&server_ref).await {
                    Ok(()) => {},
                    Err(e) => {
                        error!("heartbeat error {}", e);
                    }
                }

                let shutdown = {
                    let rserver = server_ref.read().await;
                    info!("shutdown {}", rserver.shutdown);
                    rserver.shutdown
                };

                if shutdown {
                    break;
                }
                sleep(Duration::from_millis(1000)).await;

                let shutdown = {
                    let rserver = server_ref.read().await;
                    info!("shutdown {}", rserver.shutdown);
                    rserver.shutdown
                };

                if shutdown {
                    break;
                }
            }
            Server::dec_pending_tasks(&server_ref).await;
        });

        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok(connection) => {
                            let mut socket = connection.0;
                            let server_ref = server.clone();
                            Server::inc_pending_tasks(server).await;
                            tokio::spawn(async move {
                                match Server::handle_connection(&server_ref, &mut socket).await {
                                    Ok(()) => {},
                                    Err(e) => {
                                        error!("handle_connection error {}", e);
                                    }
                                }
                                Server::dec_pending_tasks(&server_ref).await;
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