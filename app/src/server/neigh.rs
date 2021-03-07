pub mod neigh {

extern crate common_lib;

use common_lib::error::error::CommonError;

use std::sync::Arc;
use log::info;
use common_lib::client::client::Client;

pub struct Neigh {
    pub node_id: String,
    pub address: String,
}

impl Neigh {
    pub fn new(node_id: &str, address: &str) -> Self {
        Neigh{node_id: node_id.to_string(), address: address.to_string()}
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

pub type NeighRef = Arc<Neigh>;

}