

use tokio::net::TcpListener;

use std::env;
use std::error::Error;

extern crate common_lib;

use common_lib::error::error::CommonError;
use common_lib::frame::frame::{Request, Response, EchoRequest, EchoResponse, HeartbeatRequest, HeartbeatResponse};

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
    storage_path: String,
    node_id: String
}

use tokio::sync::RwLock;
use tokio::signal::unix::{signal, SignalKind};
use tokio::time::{sleep, Duration};
use std::collections::HashMap;

enum NeighbourState {
    Invalid,
    Active
}

struct Neighbour {
    node_id: String,
    address: String,
    state: NeighbourState
}

impl Neighbour {
    fn new(node_id: &str, state: NeighbourState, address: &str) -> Neighbour {
        Neighbour{node_id: node_id.to_string(), state: state, address: address.to_string()}
    }

    fn set_state(&mut self, state: NeighbourState) {
        self.state = state;
    }
}

type NeighbourRef = Arc<RwLock<Neighbour>>;

struct Server {
    config: Config,
    task_count: AtomicUsize,
    shutdown: bool,
    neighbour_map: Arc<RwLock<HashMap<String, NeighbourRef>>>
}

type ServerRef = Arc<RwLock<Server>>;

impl Server {

    fn new(config: Config) -> Self {
        Server{config: config, shutdown: false, task_count: AtomicUsize::new(0), neighbour_map: Arc::new(RwLock::new(HashMap::new()))}
    }

    async fn echo_handler(_server: &ServerRef, request: &EchoRequest) -> Result<EchoResponse, CommonError> {
        let mut response = EchoResponse::new();
        response.message = request.message.clone();
        Ok(response)
    }

    async fn heartbeat_handler(server: &ServerRef, request: &HeartbeatRequest) -> Result<HeartbeatResponse, CommonError> {
        {
            let rserver = server.read().await;
            if rserver.config.cluster_id != request.cluster_id {
                error!("unexpected cluster_id {} vs {}", request.cluster_id, rserver.config.cluster_id);
                return Err(CommonError::new(format!("unexpected cluster id")));
            }
        }
        let mut response = HeartbeatResponse::new();
        let (cluster_id, node_id) = {
            let rserver = server.read().await;
            (rserver.config.cluster_id.clone(), rserver.config.node_id.clone())
        };
        response.cluster_id = cluster_id;
        response.node_id = node_id;
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
                let resp = Server::echo_handler(server, &req).await?;
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
            "heartbeat" => {
                let req: HeartbeatRequest = match bincode::deserialize(&request.body) {
                    Ok(v) => v,
                    Err(e) => {
                        error!("deserialize error {}", e);
                        response.error = format!("deserialize error {}", e);
                        return Ok(response)
                    }
                };
                let resp = Server::heartbeat_handler(server, &req).await?;
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

    async fn heartbeat_to_node(server: &ServerRef, node_addr: &str) -> Result<(), CommonError> {
        let mut client = Client::new();

        info!("connecting {}", node_addr);
        client.connect(node_addr).await?;
        info!("connected {}", node_addr);

        let mut req = HeartbeatRequest::new();
        let (cluster_id, node_id) = {
            let rserver = server.read().await;
            (rserver.config.cluster_id.clone(), rserver.config.node_id.clone())
        };
        req.cluster_id = cluster_id;
        req.node_id = node_id;

        let resp: HeartbeatResponse = client.send("heartbeat", &req).await?;
        client.close().await?;
        info!("addr {} cluster_id {} node_id {}", node_addr, resp.cluster_id, resp.node_id);
        if req.cluster_id != resp.cluster_id {
            error!("unexpected cluster_id {} vs. {}", req.cluster_id, resp.cluster_id);
            return Err(CommonError::new(format!("unexpected cluster id")));
        }

        if req.node_id == resp.node_id {
            info!("detected self: node_id {} addr {}", req.node_id, node_addr);
            return Ok(())
        }

        let neighbour_map = {
            let rserver = server.read().await;
            rserver.neighbour_map.clone()
        };

        {
            let rneighbour_map = neighbour_map.read().await;
            match rneighbour_map.get(&resp.node_id) {
                Some(n) => {
                    let mut neighbour = n.write().await;
                    neighbour.set_state(NeighbourState::Active);
                    return Ok(())
                }
                None => {
                }
            }
        }

        {
            let mut wneighbour_map = neighbour_map.write().await;
            match wneighbour_map.get(&resp.node_id) {
                Some(n) => {
                    let mut neighbour = n.write().await;
                    neighbour.set_state(NeighbourState::Active);
                    return Ok(())
                }
                None => {
                    let neighbour = Arc::new(RwLock::new(Neighbour::new(&resp.node_id, NeighbourState::Active, node_addr)));
                    wneighbour_map.insert(resp.node_id, neighbour);
                    return Ok(())
                }
            }
        }

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
            match Server::heartbeat_to_node(server, &node).await {
                Ok(()) => {},
                Err(_e) => {},
            }
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

    async fn get_shutdown(server: &ServerRef) -> bool {
        let rserver = server.read().await;
        info!("shutdown {}", rserver.shutdown);
        rserver.shutdown
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

                if Server::get_shutdown(&server_ref).await {
                    break;
                }

                sleep(Duration::from_millis(1000)).await;

                if Server::get_shutdown(&server_ref).await {
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