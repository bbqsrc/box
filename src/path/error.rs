use core::fmt;

#[derive(Debug, Clone)]
pub enum IntoBoxPathError {
    UnrepresentableStr,
    NonCanonical,
    EmptyPath,
}

impl core::error::Error for IntoBoxPathError {}

impl fmt::Display for IntoBoxPathError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl IntoBoxPathError {
    pub fn as_str(&self) -> &str {
        match self {
            IntoBoxPathError::NonCanonical => "non-canonical path received as input",
            IntoBoxPathError::UnrepresentableStr => "unrepresentable string found in path",
            IntoBoxPathError::EmptyPath => "no path provided",
        }
    }

    #[cfg(feature = "std")]
    pub fn as_io_error(&self) -> std::io::Error {
        use std::io::{Error, ErrorKind};
        Error::new(ErrorKind::InvalidInput, self.as_str())
    }
}

#[cfg(feature = "std")]
impl From<IntoBoxPathError> for std::io::Error {
    fn from(err: IntoBoxPathError) -> Self {
        err.as_io_error()
    }
}
