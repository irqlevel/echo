pub mod error {

use std::fmt;

pub struct CommonError {
    pub code: usize,
    pub message: String,
}

impl CommonError {
    pub fn new(message: String) -> CommonError {
        CommonError{code: 0, message: message}
    }
}

impl fmt::Display for CommonError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl fmt::Debug for CommonError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CommonError {{ code: {}, message: {} }}",
            self.code, self.message
        )
    }
}
}