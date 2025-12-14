//! Error handling and HRESULT mapping for ProjFS.

use windows::Win32::Foundation::{
    ERROR_FILE_NOT_FOUND, ERROR_INVALID_PARAMETER, ERROR_NOT_ENOUGH_MEMORY, ERROR_READ_FAULT,
};

pub use windows::core::HRESULT;

/// Common HRESULT values used in ProjFS callbacks.
pub mod hresult {
    use windows::core::HRESULT;

    pub const S_OK: HRESULT = HRESULT(0);

    /// Convert a Win32 error code to HRESULT.
    #[inline]
    pub const fn from_win32(err: u32) -> HRESULT {
        if err == 0 {
            HRESULT(0)
        } else {
            HRESULT(((err & 0x0000FFFF) | 0x80070000) as i32)
        }
    }
}

impl From<std::io::Error> for ProjFsError {
    fn from(err: std::io::Error) -> Self {
        ProjFsError::Io(err)
    }
}

/// Errors that can occur in the ProjFS provider.
#[derive(Debug)]
pub enum ProjFsError {
    /// I/O error
    Io(std::io::Error),
    /// Path not found in archive
    PathNotFound(String),
    /// Invalid path
    InvalidPath(String),
    /// Windows API error
    Windows(windows::core::Error),
}

impl std::fmt::Display for ProjFsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProjFsError::Io(e) => write!(f, "I/O error: {}", e),
            ProjFsError::PathNotFound(p) => write!(f, "Path not found: {}", p),
            ProjFsError::InvalidPath(p) => write!(f, "Invalid path: {}", p),
            ProjFsError::Windows(e) => write!(f, "Windows error: {}", e),
        }
    }
}

impl std::error::Error for ProjFsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ProjFsError::Io(e) => Some(e),
            ProjFsError::Windows(e) => Some(e),
            _ => None,
        }
    }
}

/// Convert a ProjFsError to an HRESULT for return from callbacks.
pub fn error_to_hresult(err: &ProjFsError) -> HRESULT {
    match err {
        ProjFsError::Io(io_err) => io_error_to_hresult(io_err),
        ProjFsError::PathNotFound(_) => hresult::from_win32(ERROR_FILE_NOT_FOUND.0),
        ProjFsError::InvalidPath(_) => hresult::from_win32(ERROR_INVALID_PARAMETER.0),
        ProjFsError::Windows(e) => HRESULT(e.code().0),
    }
}

/// Convert a std::io::Error to an HRESULT.
pub fn io_error_to_hresult(err: &std::io::Error) -> HRESULT {
    use std::io::ErrorKind;

    let win32_err = match err.kind() {
        ErrorKind::NotFound => ERROR_FILE_NOT_FOUND.0,
        ErrorKind::InvalidInput => ERROR_INVALID_PARAMETER.0,
        ErrorKind::OutOfMemory => ERROR_NOT_ENOUGH_MEMORY.0,
        _ => {
            // Try to get the raw OS error code
            if let Some(code) = err.raw_os_error() {
                code as u32
            } else {
                ERROR_READ_FAULT.0
            }
        }
    };

    hresult::from_win32(win32_err)
}
