
pub mod server {

extern crate common_lib;

use tokio::net::TcpListener;

use common_lib::error::error::CommonError;
use common_lib::frame::frame::{Request, Response, EchoRequest, EchoResponse, VoteRequest, VoteResponse, AppendRequest, AppendResponse};

use std::sync::Arc;
use log::{error, info};


use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::RwLock;
use tokio::signal::unix::{signal, SignalKind};
use tokio::time::{sleep, Duration};
use std::collections::HashMap;
use chrono::{Utc};

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
        raft_state.set_who(RaftWho::Follower);
        raft_state.last_election = Utc::now().timestamp_millis();
        raft_state.set_voted_for(None);
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

    async fn echo_handler(&self, request: &EchoRequest) -> Result<EchoResponse, CommonError> {
        let mut response = EchoResponse::new();
        response.message = request.message.clone();
        Ok(response)
    }

    async fn vote_handler(&self, request: &VoteRequest) -> Result<VoteResponse, CommonError> {
        if self.config.cluster_id != request.cluster_id {
            error!("unexpected cluster_id {} vs {}", request.cluster_id, self.config.cluster_id);
            return Err(CommonError::new(format!("unexpected cluster id")));
        }
        if self.config.node_id == request.node_id {
            error!("unexpected node_id {} vs {}", request.node_id, self.config.node_id);
            return Err(CommonError::new(format!("unexpected cluster node_id")));
        }

        info!("vote from {}", request.node_id);

        let mut response = VoteResponse::new();
        response.cluster_id = self.config.cluster_id.clone();
        response.node_id = self.config.node_id.clone();

        let mut raft_state = self.raft_state.write().await;

        if request.term < raft_state.get_term() {
            response.term = raft_state.get_term();
            response.vote_granted  = false;
        } else {
            if request.term > raft_state.get_term() {
                raft_state.set_who(RaftWho::Follower);
                raft_state.set_term(request.term);
            }
            response.term = raft_state.get_term();
            match &raft_state.get_voted_for() {
                Some(node_id) =>  {
                    if request.node_id == *node_id {
                        response.vote_granted = true;
                    } else {
                        response.vote_granted = false;
                    }
                }
                None => {
                    raft_state.set_voted_for(Some(request.node_id.clone()));
                    response.vote_granted = true;
                }
            }
        }
        Ok(response)
    }

    async fn append_handler(&self, request: &AppendRequest) -> Result<AppendResponse, CommonError> {
        if self.config.cluster_id != request.cluster_id {
            error!("unexpected cluster_id {} vs {}", request.cluster_id, self.config.cluster_id);
            return Err(CommonError::new(format!("unexpected cluster id")));
        }
        if self.config.node_id == request.node_id {
            error!("unexpected node_id {} vs {}", request.node_id, self.config.node_id);
            return Err(CommonError::new(format!("unexpected cluster node_id")));
        }

        let mut response = AppendResponse::new();
        response.cluster_id = self.config.cluster_id.clone();
        response.node_id = self.config.node_id.clone();

        info!("append from {}", request.node_id);

        let mut raft_state = self.raft_state.write().await;
        if request.term > raft_state.get_term() {
            raft_state.set_who(RaftWho::Follower);
            raft_state.set_term(request.term);
            response.term = raft_state.get_term();
        } else if request.term < raft_state.get_term() {
            error!("unexpected term {} vs {}", request.term, raft_state.get_term());
            return Err(CommonError::new(format!("unexpected term")));
        } else {
            raft_state.reset_election();
            response.term = raft_state.get_term();
        }
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

    async fn dispatch_request(&self, request: &Request) -> Result<Response, CommonError> {
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
            "/append" => {
                let req: AppendRequest = Server::deserialize_request(&request.body)?;
                let resp = self.append_handler(&req).await?;
                response.body = Server::serialize_response(&resp)?;
            }
            _ => {
                return Err(CommonError::new(format!("unsupported path {}", request.path)));
            }
        }
        Ok(response)
    }

    async fn handle_connection(&self, socket: &mut tokio::net::TcpStream) -> Result<(), CommonError> {
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

    async fn tick(&self) -> Result<(), CommonError> {
        info!("tick");

        let (who, current_term, last_election) = {
            let raft_state = self.raft_state.read().await;
            (raft_state.get_who(), raft_state.get_term(), raft_state.last_election)
        };

        match who {
            RaftWho::Follower => {
                let now = Utc::now().timestamp_millis();
                if now > last_election && ((now - last_election) > Server::ELECTION_TIMEOUT_MILLIS) {
                    let mut raft_state = self.raft_state.write().await;
                    let now = Utc::now().timestamp_millis();
                    if now > raft_state.last_election && ((now - raft_state.last_election) > Server::ELECTION_TIMEOUT_MILLIS) &&
                        raft_state.get_who() == RaftWho::Follower {
                        raft_state.set_who(RaftWho::Candidate);
                        let current_term = raft_state.get_term();
                        raft_state.set_term(current_term + 1);
                        raft_state.set_voted_for(Some(self.config.node_id.clone()));
                    }
                }
            }
            RaftWho::Candidate => {
                let mut votes: usize = 0;
                let votes_need = self.neigh_map.len()/2 + 1;

                for neigh in self.get_neighs() {

                    let mut req = VoteRequest::new();

                    req.cluster_id = self.config.cluster_id.clone();
                    req.node_id = self.config.node_id.clone();
                    req.term = current_term;

                    let resp: VoteResponse = match neigh.send("/vote", &req).await {
                        Ok(_resp) => {_resp},
                        Err(e) => {
                            error!("vote request failure {}", e);
                            continue;
                        }
                    };

                    if resp.cluster_id != self.config.cluster_id ||
                        resp.node_id != neigh.node_id {
                        error!("unexpected response cluster_id {} node_id{}", resp.cluster_id, resp.node_id);
                        continue;
                    }

                    let mut raft_state = self.raft_state.write().await;
                    if resp.term > raft_state.get_term() {
                        raft_state.set_term(resp.term);
                        raft_state.set_who(RaftWho::Follower);
                    } else {
                        if resp.vote_granted && resp.term == current_term {
                            info!("vote from {} granted for {}", resp.node_id, self.config.node_id);
                            votes+= 1;
                        }
                    }
                }

                info!("votes {} need {}", votes, votes_need);

                let mut raft_state = self.raft_state.write().await;
                if votes >= votes_need && raft_state.get_who() == RaftWho::Candidate && raft_state.get_term() == current_term {
                    raft_state.set_who(RaftWho::Leader);
                } else {
                    raft_state.set_who(RaftWho::Follower);
                }
            }
            RaftWho::Leader => {
                for neigh in self.get_neighs() {

                    let mut req = AppendRequest::new();

                    req.cluster_id = self.config.cluster_id.clone();
                    req.node_id = self.config.node_id.clone();
                    req.term = current_term;

                    let resp: AppendResponse = match neigh.send("/append", &req).await {
                        Ok(v) => v,
                        Err(e) => {
                            error!("append request failure {}", e);
                            continue;
                        }
                    };

                    if resp.cluster_id != self.config.cluster_id ||
                        resp.node_id != neigh.node_id {
                        error!("unexpected response cluster_id {} node_id{}", resp.cluster_id, resp.node_id);
                        continue;
                    }

                    let mut raft_state = self.raft_state.write().await;
                    if resp.term > raft_state.get_term() {
                        raft_state.set_term(resp.term);
                        raft_state.set_who(RaftWho::Follower);
                        break;
                    }
                }
            }
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

    async fn shutdown(&self) {
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

    async fn get_pending_tasks(&self) -> usize {
        return self.task_count.fetch_add(0, Ordering::SeqCst);
    }

    async fn inc_pending_tasks(&self) {
        self.task_count.fetch_add(1, Ordering::SeqCst);
    }

    async fn dec_pending_tasks(&self) {
        self.task_count.fetch_sub(1, Ordering::SeqCst);
    }

    async fn get_shutdown(&self) -> bool {
        let rshutdown = self.shutdown.read().await;
        info!("shutdown {}", *rshutdown);
        *rshutdown
    }

    async fn set_shutdown(&self) {
        let mut wshutdown = self.shutdown.write().await;
        *wshutdown = true;
    }

    pub async fn run(&self, server: &ServerRef) -> Result<(), CommonError> {
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