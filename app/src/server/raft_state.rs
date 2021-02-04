pub mod raft_state {

extern crate common_lib;


use common_lib::error::error::CommonError;

use log::error;

use serde_derive::{Serialize, Deserialize};
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use std::io::ErrorKind;

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct RaftState {
    pub term: u64
}

impl RaftState {
    pub fn new() -> Self {
        Self{term: 0}
    }

    pub async fn to_file(&self, file_path: &str) -> Result<(), CommonError> {
        let state_str = match serde_json::to_string(self) {
            Ok(v) => v,
            Err(e) => {
                error!("serialize error {}", e);
                return Err(CommonError::new(format!("serialize error {}", e)));
            }
        };

        let mut file = match tokio::fs::File::create(file_path).await {
            Ok(v) => v,
            Err(e) => {
                error!("open error {}", e);
                return Err(CommonError::new(format!("open error {}", e)));
            }
        };
        match file.write_all(state_str.as_bytes()).await {
            Ok(v) => v,
            Err(e) => {
                error!("write_all error {}", e);
                return Err(CommonError::new(format!("write_all error {}", e)));
            }
        }
        match file.sync_data().await {
            Ok(v) => v,
            Err(e) => {
                error!("sync_data error {}", e);
                return Err(CommonError::new(format!("sync_data error {}", e)));
            }
        }
        Ok(())
    }

    pub async fn from_file(file_path: &str) -> Result<Self, CommonError> {
        let mut file = match tokio::fs::File::open(file_path).await {
            Ok(v) => v,
            Err(e) => {
                match e.kind() {
                    ErrorKind::NotFound => {
                        return Ok(Self::new())
                    }
                    _ => {
                        error!("open {} error {}", file_path, e);
                        return Err(CommonError::new(format!("open error {}", e)));        
                    }
                }
            }
        };

        let mut contents = vec![];
        match file.read_to_end(&mut contents).await {
            Ok(v) => v,
            Err(e) => {
                error!("read_to_end error {}", e);
                return Err(CommonError::new(format!("read_to_end error {}", e)));
            }
        };
        let state_str = match std::str::from_utf8(&contents) {
            Ok(v) => v,
            Err(e) => {
                error!("from_utf8 {}", e);
                return Err(CommonError::new(format!("from_utf8 error {}", e)));
            }
        };
        let state = match serde_json::from_str(&state_str) {
            Ok(v) => v,
            Err(e) => {
                error!("deserialize error {}", e);
                return Err(CommonError::new(format!("deserialize error {}", e)));
            }
        };
        Ok(state)
    }
}
}