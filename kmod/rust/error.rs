// SPDX-License-Identifier: GPL-2.0-only
//! Error types for boxfs kernel module

use core::ffi::c_int;

/// Kernel error codes
#[derive(Debug, Clone, Copy)]
pub enum KernelError {
    /// No such file or directory
    NotFound,
    /// I/O error
    Io,
    /// Out of memory
    NoMemory,
    /// Invalid argument
    Invalid,
    /// No such device
    NoDevice,
    /// Not a directory
    NotDir,
    /// Is a directory
    IsDir,
    /// Invalid data in archive
    BadData,
    /// Operation not permitted
    Permission,
    /// Name too long
    NameTooLong,
    /// No data available (for xattr)
    NoData,
    /// Result too large (buffer too small)
    Range,
}

impl KernelError {
    /// Convert to negative errno value for returning to C
    pub fn to_errno(self) -> c_int {
        match self {
            KernelError::NotFound => -2,    // ENOENT
            KernelError::Io => -5,          // EIO
            KernelError::NoMemory => -12,   // ENOMEM
            KernelError::Invalid => -22,    // EINVAL
            KernelError::NoDevice => -19,   // ENODEV
            KernelError::NotDir => -20,     // ENOTDIR
            KernelError::IsDir => -21,      // EISDIR
            KernelError::BadData => -74,    // EBADMSG
            KernelError::Permission => -1,  // EPERM
            KernelError::NameTooLong => -36, // ENAMETOOLONG
            KernelError::NoData => -61,     // ENODATA
            KernelError::Range => -34,      // ERANGE
        }
    }
}
