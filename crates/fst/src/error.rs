use core::fmt;

#[derive(Debug)]
pub enum BuildError {
    OutOfOrder,
    DuplicateKey,
    Empty,
}

impl fmt::Display for BuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BuildError::OutOfOrder => write!(f, "keys must be inserted in lexicographic order"),
            BuildError::DuplicateKey => write!(f, "duplicate key"),
            BuildError::Empty => write!(f, "FST is empty (no keys inserted)"),
        }
    }
}

impl core::error::Error for BuildError {}

#[derive(Debug)]
pub enum FstError {
    InvalidMagic,
    UnsupportedVersion(u8),
    TooShort,
    Corrupted,
}

impl fmt::Display for FstError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FstError::InvalidMagic => write!(f, "invalid magic bytes"),
            FstError::UnsupportedVersion(v) => write!(f, "unsupported version: {}", v),
            FstError::TooShort => write!(f, "data too short"),
            FstError::Corrupted => write!(f, "corrupted data"),
        }
    }
}

impl core::error::Error for FstError {}
