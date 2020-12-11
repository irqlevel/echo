pub mod socket {
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;
    use log::{error};
    use crate::error::error::CommonError;

    pub async fn read(socket: &mut tokio::net::TcpStream, buf: &mut [u8]) -> Result<(), CommonError> {
        let mut read = 0;
        while read < buf.len() {
            let n  = match socket.read(&mut buf[read..]).await {
                Ok(n) => n,
                Err(e) => {
                    error!("socket read error {}", e);
                    return Err(CommonError::new(format!("socket read error {}", e)))
                }
            };
    
            if n == 0 {
                return Err(CommonError::new(format!("socket read nothing")))
            }
            read += n;
        }
        Ok(())
    }

    pub async fn write(socket: &mut tokio::net::TcpStream, buf: &[u8]) -> Result<(), CommonError> {
        let mut written = 0;
        while written < buf.len() {
            let n  = match socket.write(&buf[written..]).await {
                Ok(n) => n,
                Err(e) => {
                    error!("socket write error {}", e);
                    return Err(CommonError::new(format!("socket write error {}", e)))
                }
            };
    
            written += n;
        }
        Ok(())
    }
}