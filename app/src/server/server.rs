
pub mod server {

extern crate common_lib;

use tokio::net::TcpListener;

use common_lib::error::error::CommonError;
use common_lib::frame::frame::{Request, Response, EchoRequest, EchoResponse, HeartbeatRequest, HeartbeatResponse};

use std::sync::Arc;
use log::{error, info};


use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::RwLock;
use tokio::signal::unix::{signal, SignalKind};
use tokio::time::{sleep, Duration};
use std::collections::HashMap;

use crate::config::config::Config;
use crate::neigh::neigh::NeighState;
use crate::neigh::neigh::NeighRef;
use crate::neigh::neigh::Neigh;
use crate::raft_state::raft_state::RaftState;

pub struct Server {
    config: Config,
    task_count: AtomicUsize,
    shutdown: bool,
    neigh_map: Arc<RwLock<HashMap<String, NeighRef>>>,
    address: String,
    raft_state: RaftState
}

type ServerRef = Arc<RwLock<Server>>;

impl Server {

    const RAFT_STATE_FILE_NAME: &'static str = "raft_state";

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

    async fn save_state(&self) -> Result<(), CommonError> {
        let raft_state_file_path = self.config.get_sub_storage_path(&Server::RAFT_STATE_FILE_NAME)?;
        self.raft_state.to_file(&raft_state_file_path).await?;
        Ok(())
    }

    async fn load_state(&mut self) -> Result<(), CommonError> {
        let raft_state_file_path = self.config.get_sub_storage_path(&Server::RAFT_STATE_FILE_NAME)?;
        self.raft_state = RaftState::from_file(&raft_state_file_path).await?;
        Ok(())
    }

    pub async fn new(config: Config) -> Result<Self, CommonError> {
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

            match config.get_node_map() {
                Ok(node_map) => {
                    for (node_id, address) in node_map.into_iter() {
                        let neigh = Arc::new(RwLock::new(Neigh::new(&node_id, NeighState::Invalid, &address)));
                        wneigh_map.insert(node_id.to_string(), neigh);
                    }
                },
                Err(e) => {
                    return Err(e);
                }
            }
        }

        let mut server = Server{config: config, shutdown: false, task_count: AtomicUsize::new(0), neigh_map: neigh_map,
            address: address, raft_state: RaftState::new()};
        server.load_state().await?;
        Ok(server)
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

    fn deserialize_request<R>(bytes: &[u8]) -> Result<R, CommonError>
    where
        R: serde::de::DeserializeOwned + Sized
    {
        let req: R = match bincode::deserialize(bytes) {
            Ok(v) => v,
            Err(e) => {
                error!("deserialize error {}", e);
                return Err(CommonError::new(format!("deserialize error {}", e)));
            }
        };
        Ok(req)
    }

    fn serialize_response<R>(resp: &R) -> Result<Vec<u8>, CommonError>
    where
        R: serde::Serialize + Sized,
    {
       let bytes = match bincode::serialize(&resp) {
            Ok(v) => v,
            Err(e) => {
                error!("serialize error {}", e);
                return Err(CommonError::new(format!("serialize error {}", e)));
            }
        };
        Ok(bytes)
    }

    async fn dispatch_request(server: &ServerRef, request: &Request) -> Result<Response, CommonError> {
        if request.version.as_str() != "1.0.0" {
            return Err(CommonError::new(format!("unsupported version {}", request.version)));
        }

        let mut response = Response::new(&request.req_id, "");
        match request.path.as_str() {
            "echo" => {
                let req: EchoRequest = Server::deserialize_request(&request.body)?;
                let resp = Server::echo_handler(server, &req).await?;
                response.body = Server::serialize_response(&resp)?;
            }
            "heartbeat" => {
                let req: HeartbeatRequest = Server::deserialize_request(&request.body)?;
                let resp = Server::heartbeat_handler(server, &req).await?;
                response.body = Server::serialize_response(&resp)?;
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
        let mut req = HeartbeatRequest::new();
        let (cluster_id, self_node_id) = {
            let rserver = server.read().await;
            (rserver.config.cluster_id.clone(), rserver.config.node_id.clone())
        };
        req.cluster_id = cluster_id;
        req.node_id = self_node_id;

        let rneigh = neigh.read().await;
        let resp: HeartbeatResponse = rneigh.send("heartbeat", &req).await?;
        info!("addr {} cluster_id {} node_id {}", rneigh.address, resp.cluster_id, resp.node_id);
        if req.cluster_id != resp.cluster_id {
            error!("unexpected cluster_id {} vs. {}", req.cluster_id, resp.cluster_id);
            return Err(CommonError::new(format!("unexpected cluster id")));
        }

        if rneigh.node_id != resp.node_id {
            error!("neigh_node_id {} vs. resp.node_id {}", rneigh.node_id, resp.node_id);
            return Err(CommonError::new(format!("unexpected node id")));
        }

        Ok(())
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

        {
            let mut wserver = server.write().await;
            wserver.raft_state.term += 1;
        }

        {
            let rserver = server.read().await;
            match rserver.save_state().await {
                Ok(_) => {},
                Err(e) => {
                    error!("save_state error {}", e);
                }
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

        {
            let wserver = server.write().await;
            match wserver.save_state().await {
                Ok(_) => {},
                Err(e) => {
                    error!("save state error {}", e);
                }
            }
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

    pub async fn run(server: &ServerRef) -> Result<(), CommonError> {
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
}