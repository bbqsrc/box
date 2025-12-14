use thiserror::Error;

#[derive(Debug, Error)]
pub enum BuildError {
    #[error("keys must be inserted in lexicographic order")]
    OutOfOrder,

    #[error("duplicate key")]
    DuplicateKey,

    #[error("FST is empty (no keys inserted)")]
    Empty,

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Error)]
pub enum FstError {
    #[error("invalid magic bytes")]
    InvalidMagic,

    #[error("unsupported version: {0}")]
    UnsupportedVersion(u8),

    #[error("data too short")]
    TooShort,

    #[error("corrupted data")]
    Corrupted,
}
