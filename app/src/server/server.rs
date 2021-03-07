
pub mod server {

extern crate common_lib;

use tokio::net::TcpListener;

use common_lib::error::error::CommonError;
use common_lib::frame::frame::{Request, Response, EchoRequest, EchoResponse, VoteRequest, VoteResponse};

use std::sync::Arc;
use log::{error, info};


use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::RwLock;
use tokio::signal::unix::{signal, SignalKind};
use tokio::time::{sleep, Duration};
use std::collections::HashMap;
use chrono::{Utc};
use rand::Rng;

use crate::config::config::Config;
use crate::neigh::neigh::NeighRef;
use crate::neigh::neigh::Neigh;
use crate::raft_state::raft_state::{RaftState, RaftWho};

pub struct Server {
    config: Config,
    task_count: AtomicUsize,
    shutdown: RwLock<bool>,
    neigh_map: HashMap<String, NeighRef>,
    address: String,
    raft_state: RwLock<RaftState>
}

type ServerRef = Arc<Server>;

impl Server {

    const RAFT_STATE_FILE_NAME: &'static str = "raft_state";
    const TICK_TIMEOUT_MILLIS: u64 = 1000;
    const ELECTION_TIMEOUT_MILLIS: i64 = 10000;
    const ELECTION_TIMEOUT_RANDOM_PART_MILLIS: i64 = 300;

    fn get_neighs(&self) -> Vec<NeighRef> {
        let mut neighs : Vec<NeighRef> = Vec::new();
        {
            for (node_id, node) in &self.neigh_map {
                if *node_id != self.config.node_id {
                    neighs.push(node.clone());
                }
            }
        }
        neighs
    }

    async fn save_state(&self) -> Result<(), CommonError> {
        let raft_state_file_path = self.config.get_sub_storage_path(&Server::RAFT_STATE_FILE_NAME)?;
        let raft_state = self.raft_state.read().await;
        raft_state.to_file(&raft_state_file_path).await?;
        Ok(())
    }

    async fn load_state(&mut self) -> Result<(), CommonError> {
        let raft_state_file_path = self.config.get_sub_storage_path(&Server::RAFT_STATE_FILE_NAME)?;
        let mut raft_state = self.raft_state.write().await;
        let raft_state_from_file = RaftState::from_file(&raft_state_file_path).await?;
        *raft_state = raft_state_from_file;
        raft_state.who = RaftWho::Follower;
        raft_state.last_election = Utc::now().timestamp_millis();
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

        let mut neigh_map = HashMap::new();
        {
            match config.get_node_map() {
                Ok(node_map) => {
                    for (node_id, address) in node_map.into_iter() {
                        let neigh = Arc::new(Neigh::new(&node_id, &address));
                        neigh_map.insert(node_id.to_string(), neigh);
                    }
                },
                Err(e) => {
                    return Err(e);
                }
            }
        }

        let mut server = Server{config: config, shutdown: RwLock::new(false), task_count: AtomicUsize::new(0), neigh_map: neigh_map,
            address: address, raft_state: RwLock::new(RaftState::new())};
        server.load_state().await?;
        Ok(server)
    }

    async fn echo_handler(self: &Server, request: &EchoRequest) -> Result<EchoResponse, CommonError> {
        let mut response = EchoResponse::new();
        response.message = request.message.clone();
        Ok(response)
    }

    async fn vote_handler(self: &Server, request: &VoteRequest) -> Result<VoteResponse, CommonError> {
        if self.config.cluster_id != request.cluster_id {
            error!("unexpected cluster_id {} vs {}", request.cluster_id, self.config.cluster_id);
            return Err(CommonError::new(format!("unexpected cluster id")));
        }
        let mut response = VoteResponse::new();
        response.cluster_id = self.config.cluster_id.clone();
        response.node_id = self.config.node_id.clone();
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

    async fn dispatch_request(self: &Server, request: &Request) -> Result<Response, CommonError> {
        if request.version.as_str() != "1.0.0" {
            return Err(CommonError::new(format!("unsupported version {}", request.version)));
        }

        let mut response = Response::new(&request.req_id, "");
        match request.path.as_str() {
            "/echo" => {
                let req: EchoRequest = Server::deserialize_request(&request.body)?;
                let resp = self.echo_handler(&req).await?;
                response.body = Server::serialize_response(&resp)?;
            }
            "/vote" => {
                let req: VoteRequest = Server::deserialize_request(&request.body)?;
                let resp = self.vote_handler(&req).await?;
                response.body = Server::serialize_response(&resp)?;
            }
            _ => {
                return Err(CommonError::new(format!("unsupported path {}", request.path)));
            }
        }
        Ok(response)
    }

    async fn handle_connection(self: &Server, socket: &mut tokio::net::TcpStream) -> Result<(), CommonError> {
        let request = Request::recv(socket).await?;
        let response = match self.dispatch_request(&request).await {
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

    async fn tick(self: &Server) -> Result<(), CommonError> {
        info!("tick");
        {
            let mut raft_state = self.raft_state.write().await;
            match raft_state.who {
                RaftWho::Follower => {
                    let last_election = raft_state.last_election;
                    let now = Utc::now().timestamp_millis();

                    if now > last_election && ((now - last_election) > Server::ELECTION_TIMEOUT_MILLIS) {
                        let mut rng = rand::thread_rng();
                        raft_state.last_election = now + rng.gen_range(0..Server::ELECTION_TIMEOUT_RANDOM_PART_MILLIS);
                        raft_state.who = RaftWho::Candidate;
                        raft_state.current_term += 1;
                        raft_state.voted_for = Some(self.config.node_id.clone());
                    }
                }
                _ => {}
            }
        }

        let (who, current_term) = {
            let raft_state = self.raft_state.read().await;
            (raft_state.who, raft_state.current_term)
        };

        match who {
            RaftWho::Candidate => {

                for neigh in self.get_neighs() {

                    let mut req = VoteRequest::new();

                    req.cluster_id = self.config.cluster_id.clone();
                    req.node_id = self.config.node_id.clone();
                    req.term = current_term;

                    let _resp: VoteResponse = match neigh.send("/vote", &req).await {
                        Ok(v) => v,
                        Err(e) => {
                            error!("vote request failure {}", e);
                            return Err(e)
                        }
                    };

                }
            }
            _ => {}
        }

        match self.save_state().await {
            Ok(_) => {},
            Err(e) => {
                error!("save_state error {}", e);
            }
        }

        info!("tick done");
        Ok(())
    }

    async fn shutdown(self: &Server) {
        info!("shutdowning");

        self.set_shutdown().await;

        loop {
            let pending_tasks = self.get_pending_tasks().await;
            if pending_tasks == 0 {
                break;
            }

            info!("pending tasks {}", pending_tasks);
            sleep(Duration::from_millis(Server::TICK_TIMEOUT_MILLIS)).await;
        }

        match self.save_state().await {
            Ok(_) => {},
            Err(e) => {
                error!("save state error {}", e);
            }
        }

        info!("shutdowned");
        std::process::exit(0);
    }

    async fn get_pending_tasks(self: &Server) -> usize {
        return self.task_count.fetch_add(0, Ordering::SeqCst);
    }

    async fn inc_pending_tasks(self: &Server) {
        self.task_count.fetch_add(1, Ordering::SeqCst);
    }

    async fn dec_pending_tasks(self: &Server) {
        self.task_count.fetch_sub(1, Ordering::SeqCst);
    }

    async fn get_shutdown(self: &Server) -> bool {
        let rshutdown = self.shutdown.read().await;
        info!("shutdown {}", *rshutdown);
        *rshutdown
    }

    async fn set_shutdown(self: &Server) {
        let mut wshutdown = self.shutdown.write().await;
        *wshutdown = true;
    }

    pub async fn run(self: &Server, server: &ServerRef) -> Result<(), CommonError> {
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

        let listener = match TcpListener::bind(&self.address).await {
            Ok(v) => v,
            Err(e) => {
                error!("bind {} error {}", self.address, e);
                return Err(CommonError::new(format!("bind error")));
            }
        };

        let server_ref = server.clone();
        self.inc_pending_tasks().await;
        tokio::spawn(async move {
            loop {
                match server_ref.tick().await {
                    Ok(()) => {},
                    Err(e) => {
                        error!("tick error {}", e);
                    }
                }

                if server_ref.get_shutdown().await {
                    break;
                }

                sleep(Duration::from_millis(1000)).await;

                if server_ref.get_shutdown().await {
                    break;
                }
            }
            server_ref.dec_pending_tasks().await;
        });

        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok(connection) => {
                            let mut socket = connection.0;
                            let server_ref = server.clone();
                            self.inc_pending_tasks().await;
                            tokio::spawn(async move {
                                match server_ref.handle_connection(&mut socket).await {
                                    Ok(()) => {},
                                    Err(e) => {
                                        error!("handle_connection error {}", e);
                                    }
                                }
                                server_ref.dec_pending_tasks().await;
                            });        
                        }
                        Err(e) => {
                            error!("handle_connection error {}", e);
                        }
                    }    
                }

                _sig_term_result = sig_term_stream.recv() => {
                    info!("received sig term");
                    self.shutdown().await;
                }

                _sig_int_result = sig_int_stream.recv() => {
                    info!("received sig int");
                    self.shutdown().await;
                }
            }
        }
    }
}
}