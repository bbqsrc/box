//! Compression support for the Box format.
//!
//! This module provides streaming state machines (sans-IO) for compression.
//!
//! # State Machines (Sans-IO)
//!
//! Use the streaming state machines directly:
//! - [`zstd::ZstdCompressor`] / [`zstd::ZstdDecompressor`]
//! - [`xz::XzCompressor`] / [`xz::XzDecompressor`]
//!
//! These work with raw byte buffers and can be driven by any I/O layer.
//!
//! # Buffer-based Functions
//!
//! For simple use cases with complete buffers:
//! - [`compress_bytes_sync`] / [`decompress_bytes_sync`]

use core::fmt;
#[cfg(feature = "std")]
use std::collections::HashMap;
#[cfg(feature = "std")]
use std::io::Result;

#[cfg(feature = "xz")]
pub mod xz;
#[cfg(feature = "zstd")]
pub mod zstd;

pub mod constants {
    /// Compression ID for stored (uncompressed) data.
    pub const COMPRESSION_STORED: u8 = 0x00;
    /// Compression ID for Zstd compressed data.
    pub const COMPRESSION_ZSTD: u8 = 0x10;
    /// Compression ID for XZ compressed data.
    pub const COMPRESSION_XZ: u8 = 0x20;

    /// Record type ID for directory.
    pub const RECORD_TYPE_DIRECTORY: u8 = 0x01;
    /// Record type ID for file.
    pub const RECORD_TYPE_FILE: u8 = 0x02;
    /// Record type ID for symlink.
    pub const RECORD_TYPE_SYMLINK: u8 = 0x03;
    /// Record type ID for chunked file.
    pub const RECORD_TYPE_CHUNKED_FILE: u8 = 0x0A;
    /// Record type ID for external symlink.
    pub const RECORD_TYPE_EXTERNAL_SYMLINK: u8 = 0x0B;

    /// Minimum file size for compression to be worthwhile.
    pub const MIN_COMPRESSIBLE_SIZE: u64 = 96;

    /// Default block size for chunked files: 2MB.
    pub const DEFAULT_BLOCK_SIZE: u32 = 2_097_152;
}

use self::constants::*;

// ============================================================================
// STREAMING STATUS (Sans-IO)
// ============================================================================

/// Status returned by streaming compression/decompression operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamStatus {
    /// Made progress, call again with updated buffers.
    Progress {
        bytes_consumed: usize,
        bytes_produced: usize,
    },
    /// Stream finished successfully.
    Done {
        bytes_consumed: usize,
        bytes_produced: usize,
    },
}

impl StreamStatus {
    /// Returns true if the stream is finished.
    pub fn is_done(&self) -> bool {
        matches!(self, StreamStatus::Done { .. })
    }

    /// Returns the number of bytes consumed from the input buffer.
    pub fn bytes_consumed(&self) -> usize {
        match self {
            StreamStatus::Progress { bytes_consumed, .. } => *bytes_consumed,
            StreamStatus::Done { bytes_consumed, .. } => *bytes_consumed,
        }
    }

    /// Returns the number of bytes produced to the output buffer.
    pub fn bytes_produced(&self) -> usize {
        match self {
            StreamStatus::Progress { bytes_produced, .. } => *bytes_produced,
            StreamStatus::Done { bytes_produced, .. } => *bytes_produced,
        }
    }
}

// ============================================================================
// COMPRESSION TYPES
// ============================================================================

/// Tracks the number of bytes read and written during compression/decompression.
#[derive(Debug, Clone, Copy, Default)]
pub struct ByteCount {
    /// Bytes read (uncompressed size)
    pub read: u64,
    /// Bytes written (compressed size)
    pub write: u64,
}

/// Compression algorithm identifier.
#[derive(Clone, Copy, Eq, PartialEq, Default)]
pub enum Compression {
    #[default]
    Stored,
    Zstd,
    Xz,
    Unknown(u8),
}

impl Compression {
    pub const fn available_variants() -> &'static [&'static str] {
        &["stored", "xz", "zstd"]
    }

    /// Returns the effective compression for a given file size.
    /// Files smaller than `MIN_COMPRESSIBLE_SIZE` will use `Stored` instead.
    pub fn for_size(self, size: u64) -> Self {
        if size < MIN_COMPRESSIBLE_SIZE {
            Compression::Stored
        } else {
            self
        }
    }

    pub const fn id(self) -> u8 {
        match self {
            Compression::Stored => COMPRESSION_STORED,
            Compression::Zstd => COMPRESSION_ZSTD,
            Compression::Xz => COMPRESSION_XZ,
            Compression::Unknown(id) => id,
        }
    }
}

impl fmt::Display for Compression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Compression::Stored => write!(f, "stored"),
            Compression::Zstd => write!(f, "Zstd"),
            Compression::Xz => write!(f, "xz"),
            Compression::Unknown(id) => write!(f, "?{:x}?", id),
        }
    }
}

impl fmt::Debug for Compression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

/// Configuration for compression algorithms with optional parameters.
#[cfg(feature = "std")]
#[derive(Clone, Debug, Default)]
pub struct CompressionConfig {
    pub compression: Compression,
    pub options: HashMap<String, String>,
    /// Zstd dictionary for compression.
    pub dictionary: Option<Vec<u8>>,
}

#[cfg(feature = "std")]
impl CompressionConfig {
    pub fn new(compression: Compression) -> Self {
        Self {
            compression,
            options: HashMap::new(),
            dictionary: None,
        }
    }

    /// Create a config with a Zstd dictionary.
    pub fn with_dictionary(compression: Compression, dictionary: Vec<u8>) -> Self {
        Self {
            compression,
            options: HashMap::new(),
            dictionary: Some(dictionary),
        }
    }

    /// Set the dictionary.
    pub fn set_dictionary(&mut self, dictionary: Vec<u8>) {
        self.dictionary = Some(dictionary);
    }

    pub fn set_option(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.options.insert(key.into(), value.into());
    }

    pub fn get_i32(&self, key: &str) -> Option<i32> {
        self.options.get(key).and_then(|v| v.parse().ok())
    }

    pub fn get_u32(&self, key: &str) -> Option<u32> {
        self.options.get(key).and_then(|v| v.parse().ok())
    }

    pub fn get_bool(&self, key: &str) -> Option<bool> {
        self.options.get(key).and_then(|v| match v.as_str() {
            "true" | "1" | "yes" => Some(true),
            "false" | "0" | "no" => Some(false),
            _ => None,
        })
    }

    /// Returns the effective config for a given file size.
    pub fn for_size(&self, size: u64) -> Self {
        if size < MIN_COMPRESSIBLE_SIZE {
            Self::new(Compression::Stored)
        } else {
            self.clone()
        }
    }
}

// ============================================================================
// BUFFER-BASED COMPRESSION (uses state machines internally)
// ============================================================================

/// Compress a byte slice synchronously.
#[cfg(feature = "std")]
pub fn compress_bytes_sync(data: &[u8], config: &CompressionConfig) -> Result<Vec<u8>> {
    match config.compression {
        Compression::Stored => Ok(data.to_vec()),
        #[cfg(feature = "zstd")]
        Compression::Zstd => {
            let level = config
                .get_i32("level")
                .unwrap_or(::zstd::DEFAULT_COMPRESSION_LEVEL);
            zstd::compress_buffer(data, level, config.dictionary.as_deref())
        }
        #[cfg(feature = "xz")]
        Compression::Xz => {
            let level = config.get_i32("level").unwrap_or(6) as u32;
            xz::compress_buffer(data, level)
        }
        Compression::Unknown(id) => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Cannot handle compression with id {}", id),
        )),
        #[allow(unreachable_patterns)]
        missing => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Compiled without support for {:?}", missing),
        )),
    }
}

/// Decompress a byte slice synchronously.
#[cfg(feature = "std")]
pub fn decompress_bytes_sync(
    data: &[u8],
    compression: Compression,
    dictionary: Option<&[u8]>,
) -> Result<Vec<u8>> {
    match compression {
        Compression::Stored => Ok(data.to_vec()),
        #[cfg(feature = "zstd")]
        Compression::Zstd => zstd::decompress_buffer(data, dictionary),
        #[cfg(feature = "xz")]
        Compression::Xz => xz::decompress_buffer(data),
        Compression::Unknown(id) => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Cannot handle decompression with id {}", id),
        )),
        #[allow(unreachable_patterns)]
        missing => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Compiled without support for {:?}", missing),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compress_stored_sync() {
        let data = b"hello world";
        let config = CompressionConfig::new(Compression::Stored);
        let compressed = compress_bytes_sync(data, &config).unwrap();
        assert_eq!(compressed, data);

        let decompressed = decompress_bytes_sync(&compressed, Compression::Stored, None).unwrap();
        assert_eq!(decompressed, data);
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn test_compress_zstd_sync() {
        let data = b"hello world hello world hello world";
        let config = CompressionConfig::new(Compression::Zstd);
        let compressed = compress_bytes_sync(data, &config).unwrap();
        assert!(compressed.len() < data.len());

        let decompressed = decompress_bytes_sync(&compressed, Compression::Zstd, None).unwrap();
        assert_eq!(decompressed, data);
    }

    #[cfg(feature = "xz")]
    #[test]
    fn test_compress_xz_sync() {
        let data = b"hello world hello world hello world";
        let config = CompressionConfig::new(Compression::Xz);
        let compressed = compress_bytes_sync(data, &config).unwrap();

        let decompressed = decompress_bytes_sync(&compressed, Compression::Xz, None).unwrap();
        assert_eq!(decompressed, data);
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn test_compress_zstd_with_dict_sync() {
        let data = b"hello world hello world hello world";
        let dict = ::zstd::dict::from_samples(&[data.as_slice(); 10], 1024).unwrap();

        let config = CompressionConfig::with_dictionary(Compression::Zstd, dict.clone());
        let compressed = compress_bytes_sync(data, &config).unwrap();

        let decompressed =
            decompress_bytes_sync(&compressed, Compression::Zstd, Some(&dict)).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_stream_status() {
        let progress = StreamStatus::Progress {
            bytes_consumed: 10,
            bytes_produced: 5,
        };
        assert!(!progress.is_done());
        assert_eq!(progress.bytes_consumed(), 10);
        assert_eq!(progress.bytes_produced(), 5);

        let done = StreamStatus::Done {
            bytes_consumed: 100,
            bytes_produced: 50,
        };
        assert!(done.is_done());
        assert_eq!(done.bytes_consumed(), 100);
        assert_eq!(done.bytes_produced(), 50);
    }
}
