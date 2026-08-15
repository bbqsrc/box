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
    // [spec:box:req:kernel-abi.root.ownership-and-errors]
    pub fn to_errno(self) -> c_int {
        match self {
            KernelError::NotFound => -2,     // ENOENT
            KernelError::Io => -5,           // EIO
            KernelError::NoMemory => -12,    // ENOMEM
            KernelError::Invalid => -22,     // EINVAL
            KernelError::NoDevice => -19,    // ENODEV
            KernelError::NotDir => -20,      // ENOTDIR
            KernelError::IsDir => -21,       // EISDIR
            KernelError::BadData => -74,     // EBADMSG
            KernelError::Permission => -1,   // EPERM
            KernelError::NameTooLong => -36, // ENAMETOOLONG
            KernelError::NoData => -61,      // ENODATA
            KernelError::Range => -34,       // ERANGE
        }
    }

    /// Convert to the signed-size result used by read and xattr ABI entry points.
    #[inline]
    pub fn to_ssize(self) -> isize {
        self.to_errno() as isize
    }
}

#[cfg(test)]
mod tests {
    use super::KernelError;

    // [spec:box:req:kernel-abi.root.ownership-and-errors/test]
    #[test]
    fn ssize_error_results_remain_negative_errno_values() {
        let cases = [
            (KernelError::NotFound, -2),
            (KernelError::Io, -5),
            (KernelError::NoMemory, -12),
            (KernelError::Invalid, -22),
            (KernelError::NoDevice, -19),
            (KernelError::NotDir, -20),
            (KernelError::IsDir, -21),
            (KernelError::BadData, -74),
            (KernelError::Permission, -1),
            (KernelError::NameTooLong, -36),
            (KernelError::NoData, -61),
            (KernelError::Range, -34),
        ];

        for (error, expected) in cases {
            assert_eq!(error.to_ssize(), expected);
        }
    }
}
