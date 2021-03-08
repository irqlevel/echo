pub mod frame {

use std::io::Cursor;
use serde::{Serialize, Deserialize};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use uuid::Uuid;

use crate::error::error::CommonError;
use crate::socket::socket;

use log::{error};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Frame {
    pub magic: u32,
    pub id: u32,
    pub size: u32,
    pub padding: u32
}

impl Frame {
    const MAGIC: u32 = 0xCBDACBDA;

    pub fn new() -> Frame {
        Frame{magic: Frame::MAGIC, id: 0, size: 0, padding: 0}
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, CommonError> {
        let mut data = vec![];
        match data.write_u32::<BigEndian>(self.magic) {
            Ok(()) => {},
            Err(e) => {
                error!("write error {}", e);
                return Err(CommonError::new(format!("read error {}", e)))
            }
        }

        match data.write_u32::<BigEndian>(self.id) {
            Ok(()) => {},
            Err(e) => {
                error!("write error {}", e);
                return Err(CommonError::new(format!("read error {}", e)))
            }
        }
        match data.write_u32::<BigEndian>(self.size) {
            Ok(()) => {},
            Err(e) => {
                error!("write error {}", e);
                return Err(CommonError::new(format!("read error {}", e)))
            }
        }
        match data.write_u32::<BigEndian>(self.padding) {
            Ok(()) => {},
            Err(e) => {
                error!("write error {}", e);
                return Err(CommonError::new(format!("read error {}", e)))
            }
        }
        Ok(data)
    }

    pub fn from_bytes(&mut self, bytes : &[u8]) -> Result<(), CommonError> {
        let mut rdr = Cursor::new(bytes);

        self.magic = match rdr.read_u32::<BigEndian>() {
            Ok(v) => v,
            Err(e) => {
                error!("read error {}", e);
                return Err(CommonError::new(format!("read error {}", e)))
            }
        };

        if self.magic != Frame::MAGIC {
            error!("invalid magic {}", self.magic);
            return Err(CommonError::new(format!("invalid magic {}", self.magic)))
        }

        self.id = match rdr.read_u32::<BigEndian>() {
            Ok(v) => v,
            Err(e) => {
                error!("read error {}", e);
                return Err(CommonError::new(format!("read error {}", e)))
            }
        };

        self.size = match rdr.read_u32::<BigEndian>() {
            Ok(v) => v,
            Err(e) => {
                error!("read error {}", e);
                return Err(CommonError::new(format!("read error {}", e)))
            }
        };

        self.padding = match rdr.read_u32::<BigEndian>() {
            Ok(v) => v,
            Err(e) => {
                error!("read error {}", e);
                return Err(CommonError::new(format!("read error {}", e)))
            }
        };

        return Ok(())
    }

    pub async fn send(socket: &mut tokio::net::TcpStream, data: &[u8]) -> Result<(), CommonError> {
        let mut frame = Frame::new();

        frame.size = data.len() as u32;
        let frame_bytes = frame.to_bytes()?;

        socket::write(socket, &frame_bytes).await?;
        socket::write(socket, data).await?;

        Ok(())
    }

    pub async fn recv(socket: &mut tokio::net::TcpStream) -> Result<Vec<u8>, CommonError> {
        let mut frame_bytes = [0 as u8; std::mem::size_of::<Frame>()];

        socket::read(socket, &mut frame_bytes).await?;

        let mut frame = Frame::new();
        frame.from_bytes(&frame_bytes)?;

        let mut body = vec![0; frame.size as usize];
        socket::read(socket, &mut body).await?;

        Ok(body)
    }
}


#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Request {
    pub path: String,
    pub req_id: String,
    pub version: String,
    pub body: Vec<u8>
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Response {
    pub req_id: String,
    pub error: String,
    pub body: Vec<u8>
}

impl Request {
    pub fn new(path: &str) -> Self {
        let req_id = Uuid::new_v4();
        Request{req_id: req_id.to_simple().to_string(), path: String::from(path), body: vec![0; 0], version: String::from("1.0.0")}
    }

    pub async fn send(socket: &mut tokio::net::TcpStream, req: &Request) -> Result<(), CommonError> {
        let bytes = match bincode::serialize(req) {
            Ok(v) => v,
            Err(e) => {
                error!("serialize error {}", e);
                return Err(CommonError::new(format!("serialize error {}", e)))
            }
        };

        Frame::send(socket, &bytes).await?;
        Ok(())
    }

    pub async fn recv(socket: &mut tokio::net::TcpStream) -> Result<Request, CommonError> {
        let bytes = Frame::recv(socket).await?;

        let req: Request = match bincode::deserialize(&bytes) {
            Ok(v) => v,
            Err(e) => {
                error!("deserialize error {}", e);
                return Err(CommonError::new(format!("deserialize error {}", e)))
            }
        };

        Ok(req)
    }
}

impl Response {
    pub fn new(req_id: &str, error: &str) -> Self {
        Response{req_id: String::from(req_id), error: String::from(error), body: vec![0; 0]}
    }

    pub async fn send(socket: &mut tokio::net::TcpStream, resp: &Response) -> Result<(), CommonError> {
        let bytes = match bincode::serialize(resp) {
            Ok(v) => v,
            Err(e) => {
                error!("serialize error {}", e);
                return Err(CommonError::new(format!("serialize error {}", e)))
            }
        };

        Frame::send(socket, &bytes).await?;
        Ok(())
    }

    pub async fn recv(socket: &mut tokio::net::TcpStream) -> Result<Response, CommonError> {
        let bytes = Frame::recv(socket).await?;

        let resp: Response = match bincode::deserialize(&bytes) {
            Ok(v) => v,
            Err(e) => {
                error!("deserialize error {}", e);
                return Err(CommonError::new(format!("deserialize error {}", e)))
            }
        };

        Ok(resp)
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct EchoRequest {
    pub message: String
}

impl EchoRequest {
    pub fn new() -> Self {
        EchoRequest{message: String::from("unknown")}
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct EchoResponse {
    pub message: String
}

impl EchoResponse {
    pub fn new() -> Self {
        EchoResponse{message: String::from("unknown")}
    }
}


#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct HeartbeatRequest {
    pub cluster_id: String,
    pub node_id: String
}

impl HeartbeatRequest {
    pub fn new() -> Self {
        HeartbeatRequest{cluster_id: "".to_string(), node_id: "".to_string()}
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct HeartbeatResponse {
    pub cluster_id: String,
    pub node_id: String
}

impl HeartbeatResponse {
    pub fn new() -> Self {
        HeartbeatResponse{cluster_id: "".to_string(), node_id: "".to_string()}
    }
}


#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct VoteRequest {
    pub cluster_id: String,
    pub node_id: String,
    pub term: u64,
    pub last_log_index: usize,
    pub last_log_term: u64
}

impl VoteRequest {
    pub fn new() -> Self {
        VoteRequest{cluster_id: "".to_string(), node_id: "".to_string(), term: 0, last_log_index: 0, last_log_term: 0}
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct VoteResponse {
    pub cluster_id: String,
    pub node_id: String,
    pub term: u64,
    pub vote_granted: bool
}

impl VoteResponse {
    pub fn new() -> Self {
        VoteResponse{cluster_id: "".to_string(), node_id: "".to_string(), term: 0, vote_granted: false}
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct AppendLogRequest {
    pub cluster_id: String,
    pub node_id: String,
    pub term: u64
}

impl AppendLogRequest {
    pub fn new() -> Self {
        AppendLogRequest{cluster_id: "".to_string(), node_id: "".to_string(), term: 0}
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct AppendLogResponse {
    pub cluster_id: String,
    pub node_id: String,
    pub term: u64
}

impl AppendLogResponse {
    pub fn new() -> Self {
        AppendLogResponse{cluster_id: "".to_string(), node_id: "".to_string(), term: 0}
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct InsertKeyRequest {
    pub key: String,
    pub value: String
}

impl InsertKeyRequest {
    pub fn new(key: &str, value: &str) -> Self {
        InsertKeyRequest{key: key.to_string(), value: value.to_string()}
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct InsertKeyResponse {
    pub redirect: bool,
    pub redirect_address: String
}

impl InsertKeyResponse {
    pub fn new() -> Self {
        InsertKeyResponse{redirect: false, redirect_address: "".to_string()}
    }
}

}