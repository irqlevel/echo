pub mod client
{
    use tokio::io::{AsyncWriteExt};
    use crate::frame::frame::{Request, Response};
    use crate::error::error::CommonError;
    use log::{error};

    pub struct Client {
        socket: Option<tokio::net::TcpStream>
    }
    
    impl Client {
        pub async fn send<T: ?Sized, V: ?Sized>(&mut self, path: &str, request: &T) -> Result<V, CommonError>
        where
            T: serde::Serialize,
            V: serde::de::DeserializeOwned
        {
            match self.socket.as_mut() {
                Some(sock) => {
                    let mut req = Request::new(path);
    
                    req.body = match bincode::serialize(&request) {
                        Ok(v) => v,
                        Err(e) => {
                            error!("serialize error {}", e);
                            return Err(CommonError::new(format!("serialize error {}", e)))
                        }
                    };
                    Request::send(sock, &req).await?;
    
                    let resp = Response::recv(sock).await?;
                    if resp.req_id != req.req_id {
                        return Err(CommonError::new(format!("unexpected response req_id {} vs {}", resp.req_id, req.req_id)))
                    }
                    if resp.error != "" {
                        return Err(CommonError::new(format!("response error {}", resp.error)))
                    }
    
                    let response: V = match bincode::deserialize(&resp.body) {
                        Ok(v) => v,
                        Err(e) => {
                            error!("deserialize error {}", e);
                            return Err(CommonError::new(format!("deserialize error {}", e)))
                        }
                    };
            
                    Ok(response)
                }
                None => {
                    return Err(CommonError::new(format!("socket doesn't exists")))
                }
            }
        }
    
        pub fn new() -> Client {
            Client{socket: None}
        }
    
        pub async fn connect(&mut self, addr: &str) -> Result<(), CommonError> {
            let socket = match tokio::net::TcpStream::connect(addr).await {
                Ok(v) => {
                    v},
                Err(e) => {
                    error!("connect {} error {}", addr, e);
                    return Err(CommonError::new(format!("connect {} error {}", addr, e)))
                }
            };
    
            self.socket = Some(socket);
            Ok(())
        }
    
        pub async fn close(&mut self) -> Result<(), CommonError> {
    
            match self.socket.as_mut() {
                Some(e) => {
                    match e.shutdown().await {
                        Ok(()) => Ok(()),
                        Err(e) => {
                            error!("socket shutdown error{}", e);
                            return Err(CommonError::new(format!("socket shutdown error{}", e)))
                        }
                    }
                }
                None => {
                    Ok(())
                }
            }
        }
    }
    
}