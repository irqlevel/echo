pub mod neigh {

extern crate common_lib;

use common_lib::error::error::CommonError;

use std::sync::Arc;
use log::info;
use tokio::sync::RwLock;
use common_lib::client::client::Client;

#[derive(Debug)]
pub enum NeighState {
    Invalid,
    Active
}

pub struct Neigh {
    pub node_id: String,
    pub address: String,
    pub state: NeighState
}

impl Neigh {
    pub fn new(node_id: &str, state: NeighState, address: &str) -> Self {
        Neigh{node_id: node_id.to_string(), state: state, address: address.to_string()}
    }

    pub fn set_state(&mut self, state: NeighState) {
        info!("node {} state {:?} -> {:?}", self.node_id, self.state, state);
        self.state = state;
    }

    pub async fn send<R, S>(&self, req_path: &str, req: &R) -> Result<S, CommonError>
    where
        R: serde::Serialize,
        S: serde::de::DeserializeOwned
    {
        let mut client = Client::new();

        info!("connecting {}", self.address);
        client.connect(&self.address).await?;
        info!("connected {}", self.address);

        let resp: S = client.send(req_path, req).await?;
        client.close().await?;
        Ok(resp)
    }
}

pub type NeighRef = Arc<RwLock<Neigh>>;

}