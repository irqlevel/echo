use std::env;
use std::error::Error;
use log::{error, info, LevelFilter};

extern crate common_lib;

use common_lib::frame::frame::{EchoRequest, EchoResponse};

use common_lib::error::error::CommonError;

use common_lib::logger::logger::SimpleLogger;
use common_lib::client::client::Client;


async fn test_client() -> Result<(), CommonError> {
    let addr = env::args()
    .nth(1)
    .unwrap_or_else(|| "127.0.0.1:8080".to_string());

    let mut client = Client::new();
    
    client.connect(&addr).await?;

    let mut req = EchoRequest::new();
    req.message = "Hello world!!!!".to_string();

    let resp: EchoResponse = client.send("echo", &req).await?;

    assert_eq!(req.message, resp.message);

    client.close().await?;

    Ok(())
}

static LOGGER: SimpleLogger = SimpleLogger;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {

    match log::set_logger(&LOGGER)
        .map(|()| log::set_max_level(LevelFilter::Info)) {
        Ok(()) => {},
        Err(e) => {
            println!("set_logger error {}", e);
            return Err("set_logger error".into())
        }
    }

    info!("starting");

    let mut handles = vec![];
    for _ in 0..100000 {
        handles.push(
        tokio::spawn(async move {
            match test_client().await {
                Ok(()) => {},
                Err(e) => {
                    error!("test_client error {}", e);
                }
            }
        }));
    }
    futures::future::join_all(handles).await;
    info!("completed");

    Ok(())
}