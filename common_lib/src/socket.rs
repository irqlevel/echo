pub mod socket {
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;
    use crate::error::error::ServerError;

    pub async fn read(socket: &mut tokio::net::TcpStream, buf: &mut [u8]) -> Result<(), ServerError> {
        let mut read = 0;
        while read < buf.len() {
            println!("reading {}", buf.len() - read);
            let n  = match socket.read(&mut buf[read..]).await {
                Ok(n) => n,
                Err(e) => {
                    println!("socker read error {}", e);
                    return Err(ServerError::new())
                }
            };
    
            if n == 0 {
                return Err(ServerError::new())
            }
            read += n;
        }
        Ok(())
    }

    pub async fn write(socket: &mut tokio::net::TcpStream, buf: &[u8]) -> Result<(), ServerError> {
        let mut written = 0;
        while written < buf.len() {
            println!("writing {}", buf.len() - written);
            let n  = match socket.write(&buf[written..]).await {
                Ok(n) => n,
                Err(e) => {
                    println!("socker write error {}", e);
                    return Err(ServerError::new())
                }
            };
    
            written += n;
        }
        Ok(())
    }
}