extern crate common_lib;


use std::env;
use std::error::Error;

use std::sync::Arc;
use log::{error, info, LevelFilter};

use common_lib::logger::logger::SimpleLogger;

use std::fs::File;
use std::io::Read;
use std::fs;

use tokio::sync::RwLock;

pub mod config;
pub mod server;
pub mod neigh;
pub mod raft_state;

use config::config::Config;
use server::server::Server;

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