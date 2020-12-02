pub mod frame {

use std::io::Cursor;
use serde::{Serialize, Deserialize};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use crate::error::error::ServerError;

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
    const MAGIC: u32 = 0xBDCABDCA;

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

    pub fn from_bytes(&mut self, bytes : &[u8]) -> Result<(), ServerError> {
        let mut rdr = Cursor::new(bytes);

        self.magic = match rdr.read_u32::<BigEndian>() {
            Ok(v) => v,
            Err(e) => {
                println!("read error {}", e);
                return Err(ServerError::new())
            }
        };

        if self.magic != ResponseHeader::MAGIC {
            println!("invalid request header magic {}", self.magic);
            return Err(ServerError::new())
        }

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

        self.status = match rdr.read_u32::<BigEndian>() {
            Ok(v) => v,
            Err(e) => {
                println!("read error {}", e);
                return Err(ServerError::new())
            }
        };

        return Ok(())
    }

    pub fn new() -> ResponseHeader {
        ResponseHeader{magic: ResponseHeader::MAGIC, id: 0, size: 0, status: 0}
    }
}

impl RequestHeader {
    const MAGIC: u32 = 0xCBDACBDA;

    pub fn new() -> RequestHeader {
        RequestHeader{magic: RequestHeader::MAGIC, id: 0, size: 0, padding: 0}
    }

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
        match data.write_u32::<BigEndian>(self.padding) {
            Ok(()) => {},
            Err(e) => {
                println!("write error {}", e);
                return Err(ServerError::new())
            }
        }
        Ok(data)
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

        if self.magic != RequestHeader::MAGIC {
            println!("invalid request header magic {}", self.magic);
            return Err(ServerError::new())
        }

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
        //println!("size {}", self.size);
        self.padding = match rdr.read_u32::<BigEndian>() {
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

impl Request {
    pub fn new() -> Request {
        Request{header: RequestHeader::new(), body: Vec::new()}
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Response {
    pub header: ResponseHeader,
    pub body: Vec<u8>
}

impl Response {
    pub fn new() -> Response {
        Response{header: ResponseHeader::new(), body: Vec::new()}
    }
}

}