use std::env;
use std::error::Error;
use log::{error, info, LevelFilter};

extern crate common_lib;

use common_lib::frame::frame::{EchoRequest, EchoResponse, InsertKeyRequest, InsertKeyResponse};

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

use clap::{Arg, App};

async fn do_insert_key(address: &str, key: &str, value: &str) -> Result<(), CommonError> {
    let mut client = Client::new();

    client.connect(address).await?;

    let req = InsertKeyRequest::new(key, value);
    let resp: InsertKeyResponse = client.send("/insert-key", &req).await?;
    if resp.redirect {
        client.close().await?;

        client = Client::new();
        client.connect(&resp.redirect_address).await?;
        let req = InsertKeyRequest::new(key, value);
        let resp: InsertKeyResponse = client.send("/insert-key", &req).await?;
        client.close().await?;
        if resp.redirect {
            return Err(CommonError::new("Too many redirects".to_string()));
        }
    } else {
        client.close().await?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    match log::set_logger(&LOGGER).map(|()| log::set_max_level(LevelFilter::Info)) {
        Ok(()) => {},
        Err(e) => {
            println!("set_logger error {}", e);
            return Err("set_logger error".into())
        }
    }

    let matches = App::new("Echo client")
        .version("1.0")
        .author("Andrey Smetanin <irqlevel@gmail.com>")
        .about("Does awesome things")
        .arg(Arg::new("command")
            .about("Sets the command to use")
            .required(true)
            .index(1))
        .arg(Arg::new("key")
            .short('k')
            .long("key")
            .value_name("key")
            .about("Sets key")
            .takes_value(true))
        .arg(Arg::new("value")
            .short('v')
            .long("value")
            .value_name("value")
            .about("Sets value")
            .takes_value(true))
        .arg(Arg::new("address")
            .short('a')
            .long("address")
            .value_name("address")
            .about("Sets a custom server address")
            .takes_value(true))
        .get_matches();

    let address = matches.value_of("address");
    let key = matches.value_of("key");
    let value = matches.value_of("value");

    if let Some(cmd) = matches.value_of("command") {
        match cmd {
        "insert-key" => {
            if address.is_some() && key.is_some() && value.is_some() {
                match do_insert_key(address.unwrap(), key.unwrap(), value.unwrap()).await {
                    Ok(_) => {
                    },
                    Err(e) => {
                        return Err(format!("error {}", e).into())
                    }
                }
            } else {
                return Err("unexpected options".into())
            }
        }
        _ => {
            return Err(format!("unexpected cmd {}", cmd).into())
        }
        }
    }

    Ok(())
}