pub mod config {

extern crate common_lib;

use common_lib::error::error::CommonError;
use serde_derive::Deserialize;
use std::collections::{HashSet, HashMap};
use std::path::Path;

#[derive(Deserialize)]
pub struct Config {
    pub nodes: Vec<String>,
    pub cluster_id: String,
    pub storage_path: String,
    pub node_id: String
}

impl Config {
    pub fn get_node_address(&self, node_id: &str) -> Option<String> {
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

    pub fn check(&self) -> Result<(), CommonError> {
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

    pub fn get_node_map(&self) -> Result<HashMap<String, String>, CommonError> {
        let mut node_map = HashMap::new();
        for node in &self.nodes {
            let words = node.split(";").collect::<Vec<&str>>();
            if words.len() != 2 {
                return Err(CommonError::new(format!("unexpected number of node words")));
            }
            let address = words[0];
            let node_id = words[1];

            match node_map.get(node_id) {
                Some(_n) => {
                    return Err(CommonError::new(format!("node with node_id {} already exists", node_id)));
                }
                None => {
                    node_map.insert(node_id.to_string(), address.to_string());
                }
            }
        }
        Ok(node_map)
    }

    pub fn get_sub_storage_path(&self, sub_file_path: &str) -> Result<String, CommonError> {
        let storage_path = Path::new(&self.storage_path);
        let file_path = storage_path.join(sub_file_path);
        match file_path.to_str() {
            Some(v) => {
                return Ok(v.to_string())
            }
            None => {
                return Err(CommonError::new("empty file path".to_string()));
            }
        }
    }
}

}