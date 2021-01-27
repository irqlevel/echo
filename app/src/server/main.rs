

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


use tokio::sync::RwLock;
use tokio::signal::unix::{signal, SignalKind};
use tokio::time::{sleep, Duration};
use std::collections::{HashMap, HashSet};

#[derive(Deserialize)]
struct Config {
    nodes: Vec<String>,
    cluster_id: String,
    storage_path: String,
    node_id: String
}

impl Config {
    fn get_node_address(&self, node_id: &str) -> Option<String> {
        for node in &self.nodes {
            let words = node.split(";").collect::<Vec<&str>>();
            if words.len() != 2 {
                return None::<String>;
            }

            if words[1] == node_id {
                return Some(words[0].to_string());
            }
        }
        return None::<String>;
    }

    fn check(&self) -> Result<(), CommonError> {
        if self.node_id == "" {
            return Err(CommonError::new("empty node_id".into()));
        }

        if self.cluster_id == "" {
            return Err(CommonError::new("empty cluster_id".into()));
        }

        if self.storage_path == "" {
            return Err(CommonError::new("empty storage_path".into()));
        }

        let mut addr_set = HashSet::new();
        let mut node_id_set = HashSet::new();

        for node in &self.nodes {
            let words = node.split(";").collect::<Vec<&str>>();
            if words.len() != 2 {
                return Err(CommonError::new(format!("unexpected token count in string {}", node)));
            }
            let address = words[0];
            let node_id = words[1];

            if addr_set.contains(address) {
                return Err(CommonError::new(format!("address {} already exists", address)));
            }
            addr_set.insert(address);

            if node_id_set.contains(node_id) {
                return Err(CommonError::new(format!("node_id {} already exists", node_id)));
            }
            node_id_set.insert(node_id);
        }

        match self.get_node_address(&self.node_id) {
            Some(_n) => {}
            None => {
                return Err(CommonError::new(format!("can't find address of node_id {}", self.node_id)));
            }
        };

        Ok(())
    }
}


#[derive(Debug)]
enum NeighState {
    Invalid,
    Active
}

struct Neigh {
    node_id: String,
    address: String,
    state: NeighState
}

impl Neigh {
    fn new(node_id: &str, state: NeighState, address: &str) -> Self {
        Neigh{node_id: node_id.to_string(), state: state, address: address.to_string()}
    }

    fn set_state(&mut self, state: NeighState) {
        info!("node {} state {:?} -> {:?}", self.node_id, self.state, state);
        self.state = state;
    }
}

type NeighRef = Arc<RwLock<Neigh>>;

struct Server {
    config: Config,
    task_count: AtomicUsize,
    shutdown: bool,
    neigh_map: Arc<RwLock<HashMap<String, NeighRef>>>,
    address: String
}

type ServerRef = Arc<RwLock<Server>>;

impl Server {

    async fn new(config: Config) -> Result<Self, CommonError> {
        let address = match config.get_node_address(&config.node_id) {
            Some(address) => {
                address
            }
            None => {
                return Err(CommonError::new(format!("can't find self address in config")));
            }
        };

        let neigh_map = Arc::new(RwLock::new(HashMap::new()));
        {
            let mut wneigh_map = neigh_map.write().await;
            for node in &config.nodes {
                let words = node.split(";").collect::<Vec<&str>>();
                if words.len() != 2 {
                    return Err(CommonError::new(format!("unexpected number of node words")));
                }
                let address = words[0];
                let node_id = words[1];

                if node_id != config.node_id {
                    match wneigh_map.get(node_id) {
                        Some(_n) => {
                            return Err(CommonError::new(format!("neigh with node_id {} already exists", node_id)));
                        }
                        None => {
                            let neigh = Arc::new(RwLock::new(Neigh::new(node_id, NeighState::Invalid, address)));
                            wneigh_map.insert(node_id.to_string(), neigh);
                        }
                    }
                }
            }
        }

        Ok(Server{config: config, shutdown: false, task_count: AtomicUsize::new(0), neigh_map: neigh_map,
            address: address})
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
            return Err(CommonError::new(format!("unsupported version {}", request.version)));
        }

        match request.path.as_str() {
            "echo" => {
                let req: EchoRequest = match bincode::deserialize(&request.body) {
                    Ok(v) => v,
                    Err(e) => {
                        error!("deserialize error {}", e);
                        return Err(CommonError::new(format!("deserialize error {}", e)));
                    }
                };
                let resp = Server::echo_handler(server, &req).await?;
                response.body = match bincode::serialize(&resp) {
                    Ok(v) => v,
                    Err(e) => {
                        error!("serialize error {}", e);
                        return Err(CommonError::new(format!("serialize error {}", e)));
                    }
                };
            }
            "heartbeat" => {
                let req: HeartbeatRequest = match bincode::deserialize(&request.body) {
                    Ok(v) => v,
                    Err(e) => {
                        error!("deserialize error {}", e);
                        return Err(CommonError::new(format!("deserialize error {}", e)));
                    }
                };
                let resp = Server::heartbeat_handler(server, &req).await?;
                response.body = match bincode::serialize(&resp) {
                    Ok(v) => v,
                    Err(e) => {
                        error!("serialize error {}", e);
                        return Err(CommonError::new(format!("serialize error {}", e)));
                    }
                };
            }
            _ => {
                return Err(CommonError::new(format!("unsupported path {}", request.path)));
            }
        }
        Ok(response)
    }

    async fn handle_connection(server: &ServerRef, socket: &mut tokio::net::TcpStream) -> Result<(), CommonError> {
        let request = Request::recv(socket).await?;

        let response = match Server::dispatch_request(server, &request).await {
            Ok(v) => {
                v
            }
            Err(e) => {
                Response::new(&request.req_id, &e.message)
            }
        };
        Response::send(socket, &response).await?;
        Ok(())
    }

    async fn heartbeat_to_node(server: &ServerRef, neigh: &NeighRef) -> Result<(), CommonError> {
        let mut client = Client::new();

        let (neigh_addr, neigh_node_id) = {
            let rneigh = neigh.read().await;
            (rneigh.address.clone(), rneigh.node_id.clone())
        };

        info!("connecting {}", neigh_addr);
        client.connect(&neigh_addr).await?;
        info!("connected {}", neigh_addr);

        let mut req = HeartbeatRequest::new();
        let (cluster_id, self_node_id) = {
            let rserver = server.read().await;
            (rserver.config.cluster_id.clone(), rserver.config.node_id.clone())
        };
        req.cluster_id = cluster_id;
        req.node_id = self_node_id;

        let resp: HeartbeatResponse = client.send("heartbeat", &req).await?;
        client.close().await?;
        info!("addr {} cluster_id {} node_id {}", neigh_addr, resp.cluster_id, resp.node_id);
        if req.cluster_id != resp.cluster_id {
            error!("unexpected cluster_id {} vs. {}", req.cluster_id, resp.cluster_id);
            return Err(CommonError::new(format!("unexpected cluster id")));
        }

        if neigh_node_id != resp.node_id {
            error!("neigh_node_id {} vs. resp.node_id {}", neigh_node_id, resp.node_id);
            return Err(CommonError::new(format!("unexpected node id")));
        }

        Ok(())
    }

    async fn get_neighs(&self) -> Vec<NeighRef> {
        let mut neighs : Vec<NeighRef> = Vec::new();
        {
            let rneigh_map = self.neigh_map.read().await;
            for (node_id, node) in &*rneigh_map {
                if node_id != &self.config.node_id {
                    neighs.push(node.clone());
                }
            }
        }
        neighs
    }

    async fn heartbeat(server: &ServerRef) -> Result<(), CommonError> {
        info!("heartbeat");

        let neighs = {
            let rserver = server.read().await;
            rserver.get_neighs().await
        };

        for neigh in neighs {
            match Server::heartbeat_to_node(server, &neigh).await {
                Ok(()) => {
                    let mut wneigh = neigh.write().await;
                    wneigh.set_state(NeighState::Active);
                },
                Err(_e) => {
                    let mut wneigh = neigh.write().await;
                    wneigh.set_state(NeighState::Invalid);
                },
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
            rserver.address.clone()
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

    info!("listening node_id {} cluster_id {} nodes {:?} storage_path {}", config.node_id, config.cluster_id, config.nodes, config.storage_path);
    match config.check() {
        Ok(()) => {}
        Err(e) => {
            error!("config check error {}", e);
            return Err(format!("init error {}", e).into())
        }
    }

    match fs::create_dir_all(&config.storage_path) {
        Ok(()) => {}
        Err(e) => {
            error!("create_dir_all {} error {}", config.storage_path, e);
            return Err(format!("create_dir_all {} error {}", config.storage_path, e).into())
        }
    }

    let server = match Server::new(config).await {
        Ok(v) => v,
        Err(e) => {
            error!("init error {}", e);
            return Err(format!("init error {}", e).into())
        }
    };

    let server_ref = Arc::new(RwLock::new(server));
    match Server::run(&server_ref).await {
        Ok(()) => Ok(()),
        Err(e) => {
            error!("run error {}", e);
            return Err(format!("run error {}", e).into())
        }
    }
}