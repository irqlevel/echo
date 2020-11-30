use std::fmt;

pub struct ServerError {
    pub code: usize,
    pub message: String,
}

impl ServerError {
    pub fn new() -> ServerError {
        ServerError{code: 0, message: "something went wrong".to_string()}
    }
}

// Different error messages according to ServerError.code
impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let err_msg = match self.code {
            404 => "Sorry, Can not find the Page!",
            _ => "Sorry, something is wrong! Please Try Again!",
        };

        write!(f, "{}", err_msg)
    }
}

// A unique format for dubugging output
impl fmt::Debug for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ServerError {{ code: {}, message: {} }}",
            self.code, self.message
        )
    }
}