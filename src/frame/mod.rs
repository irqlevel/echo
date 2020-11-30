use std::io::Cursor;
use serde::{Serialize, Deserialize};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use super::error::ServerError;

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct RequestHeader {
    pub magic: u32,
    pub id: u32,
    pub size: u32,
    pub padding: u32
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct ResponseHeader {
    pub magic: u32,
    pub id: u32,
    pub size: u32,
    pub status: u32
}

impl ResponseHeader {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ServerError> {
        let mut data = vec![];
        match data.write_u32::<BigEndian>(self.magic) {
            Ok(()) => {},
            Err(e) => {
                println!("write error {}", e);
                return Err(ServerError::new())
            }
        }
        match data.write_u32::<BigEndian>(self.id) {
            Ok(()) => {},
            Err(e) => {
                println!("write error {}", e);
                return Err(ServerError::new())
            }
        }
        match data.write_u32::<BigEndian>(self.size) {
            Ok(()) => {},
            Err(e) => {
                println!("write error {}", e);
                return Err(ServerError::new())
            }
        }
        match data.write_u32::<BigEndian>(self.status) {
            Ok(()) => {},
            Err(e) => {
                println!("write error {}", e);
                return Err(ServerError::new())
            }
        }
        Ok(data)
    }
}

impl RequestHeader {
    pub fn new() -> RequestHeader {
        RequestHeader{magic: 0, id: 0, size: 0, padding: 0}
    }

    pub fn from_bytes(&mut self, bytes : &[u8]) -> Result<(), ServerError> {
        let mut rdr = Cursor::new(bytes);

        self.magic = match rdr.read_u32::<BigEndian>() {
            Ok(v) => v,
            Err(e) => {
                println!("read error {}", e);
                return Err(ServerError::new())
            }
        };

        self.id = match rdr.read_u32::<BigEndian>() {
            Ok(v) => v,
            Err(e) => {
                println!("read error {}", e);
                return Err(ServerError::new())
            }
        };

        self.size = match rdr.read_u32::<BigEndian>() {
            Ok(v) => v,
            Err(e) => {
                println!("read error {}", e);
                return Err(ServerError::new())
            }
        };

        return Ok(())
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Request {
    pub header: RequestHeader,
    pub body: Vec<u8>
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Response {
    pub header: ResponseHeader,
    pub body: Vec<u8>
}

impl Response {
    pub fn new() -> Response {
        Response{header: ResponseHeader{magic: 0, id: 0, size: 0, status:0}, body: Vec::new()}
    }
}