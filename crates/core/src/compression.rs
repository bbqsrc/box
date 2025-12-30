//! Compression types and constants.

use core::fmt;

/// Compression algorithm identifiers.
pub mod constants {
    pub const COMPRESSION_STORED: u8 = 0x00;
    pub const COMPRESSION_DEFLATE: u8 = 0x10;
    pub const COMPRESSION_ZSTD: u8 = 0x20;
    pub const COMPRESSION_XZ: u8 = 0x30;
    pub const COMPRESSION_SNAPPY: u8 = 0x40;
    pub const COMPRESSION_BROTLI: u8 = 0x50;

    /// Minimum file size for compression to be worthwhile.
    /// Files smaller than this will be stored uncompressed.
    pub const MIN_COMPRESSIBLE_SIZE: u64 = 96;
}

use self::constants::*;

/// Compression algorithm type.
#[derive(Clone, Copy, Eq, PartialEq, Default)]
pub enum Compression {
    #[default]
    Stored,
    Deflate,
    Zstd,
    Xz,
    Snappy,
    Brotli,
    Unknown(u8),
}

impl Compression {
    /// List of available compression variants as strings.
    pub const fn available_variants() -> &'static [&'static str] {
        &["stored", "brotli", "deflate", "snappy", "xz", "zstd"]
    }

    /// Get the byte identifier for this compression type.
    pub const fn id(self) -> u8 {
        match self {
            Compression::Stored => COMPRESSION_STORED,
            Compression::Deflate => COMPRESSION_DEFLATE,
            Compression::Zstd => COMPRESSION_ZSTD,
            Compression::Xz => COMPRESSION_XZ,
            Compression::Snappy => COMPRESSION_SNAPPY,
            Compression::Brotli => COMPRESSION_BROTLI,
            Compression::Unknown(id) => id,
        }
    }

    /// Create a Compression from its byte identifier.
    pub const fn from_id(id: u8) -> Compression {
        match id {
            COMPRESSION_STORED => Compression::Stored,
            COMPRESSION_DEFLATE => Compression::Deflate,
            COMPRESSION_ZSTD => Compression::Zstd,
            COMPRESSION_XZ => Compression::Xz,
            COMPRESSION_SNAPPY => Compression::Snappy,
            COMPRESSION_BROTLI => Compression::Brotli,
            other => Compression::Unknown(other),
        }
    }

    /// Returns the effective compression for a given file size.
    /// Files smaller than `MIN_COMPRESSIBLE_SIZE` will use `Stored` instead.
    pub const fn for_size(self, size: u64) -> Self {
        if size < MIN_COMPRESSIBLE_SIZE {
            Compression::Stored
        } else {
            self
        }
    }

    /// Check if this compression type is supported by the Linux kernel.
    /// Returns true for Stored, Deflate, Zstd, and Xz.
    pub const fn is_kernel_supported(self) -> bool {
        matches!(
            self,
            Compression::Stored | Compression::Deflate | Compression::Zstd | Compression::Xz
        )
    }

    /// Get the name of this compression type as a string.
    pub const fn name(self) -> &'static str {
        match self {
            Compression::Stored => "stored",
            Compression::Deflate => "DEFLATE",
            Compression::Zstd => "Zstd",
            Compression::Xz => "xz",
            Compression::Snappy => "Snappy",
            Compression::Brotli => "Brotli",
            Compression::Unknown(_) => "unknown",
        }
    }
}

impl fmt::Display for Compression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Compression::Unknown(id) => write!(f, "?{:x}?", id),
            other => write!(f, "{}", other.name()),
        }
    }
}

impl fmt::Debug for Compression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

/// Tracks the number of bytes read and written during compression/decompression.
#[derive(Debug, Clone, Copy, Default)]
pub struct ByteCount {
    /// Bytes read (uncompressed size)
    pub read: u64,
    /// Bytes written (compressed size)
    pub write: u64,
}
