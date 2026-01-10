use std::collections::HashMap;
use std::fmt;
use std::io::Result;

use tokio::io::{AsyncBufRead, AsyncWrite, AsyncWriteExt};

use crate::counting::CountingWriter;

#[cfg(feature = "xz")]
use async_compression::tokio::{bufread::XzDecoder, write::XzEncoder};
#[cfg(feature = "zstd")]
use async_compression::tokio::{bufread::ZstdDecoder, write::ZstdEncoder};

#[cfg(any(feature = "zstd", feature = "xz"))]
use async_compression::Level;

pub mod constants {
    // Compression IDs (stored in high nibble of type/compression byte)
    pub const COMPRESSION_STORED: u8 = 0x00;
    pub const COMPRESSION_ZSTD: u8 = 0x10;
    pub const COMPRESSION_XZ: u8 = 0x20;

    // Record type IDs (stored in low nibble of type/compression byte)
    pub const RECORD_TYPE_DIRECTORY: u8 = 0x01;
    pub const RECORD_TYPE_FILE: u8 = 0x02;
    pub const RECORD_TYPE_SYMLINK: u8 = 0x03;
    pub const RECORD_TYPE_CHUNKED_FILE: u8 = 0x0A;
    pub const RECORD_TYPE_EXTERNAL_SYMLINK: u8 = 0x0B;

    /// Minimum file size for compression to be worthwhile.
    /// Files smaller than this will be stored uncompressed.
    pub const MIN_COMPRESSIBLE_SIZE: u64 = 96;

    /// Default block size for chunked files: 2MB (2,097,152 bytes).
    /// Aligns with common hugepage sizes and provides good balance
    /// between compression ratio and random access granularity.
    pub const DEFAULT_BLOCK_SIZE: u32 = 2_097_152;
}

use self::constants::*;

/// Tracks the number of bytes read and written during compression/decompression.
#[derive(Debug, Clone, Copy, Default)]
pub struct ByteCount {
    /// Bytes read (uncompressed size)
    pub read: u64,
    /// Bytes written (compressed size)
    pub write: u64,
}

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
}

/// Configuration for compression algorithms with optional parameters.
#[derive(Clone, Debug, Default)]
pub struct CompressionConfig {
    pub compression: Compression,
    pub options: HashMap<String, String>,
    /// Zstd dictionary for compression. When set, Zstd compression will use this dictionary.
    pub dictionary: Option<Vec<u8>>,
}

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

    /// Set the dictionary (consumes and stores a copy).
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
    /// Files smaller than `MIN_COMPRESSIBLE_SIZE` will use `Stored` instead.
    pub fn for_size(&self, size: u64) -> Self {
        if size < MIN_COMPRESSIBLE_SIZE {
            Self::new(Compression::Stored)
        } else {
            self.clone()
        }
    }

    pub async fn compress<W, R>(&self, writer: W, mut reader: R) -> Result<ByteCount>
    where
        W: AsyncWrite + Unpin,
        R: AsyncBufRead + Unpin,
    {
        self.compress_ref(writer, &mut reader).await
    }

    /// Compress data from a reader reference, allowing caller to retain ownership.
    pub async fn compress_ref<W, R>(&self, writer: W, reader: &mut R) -> Result<ByteCount>
    where
        W: AsyncWrite + Unpin,
        R: AsyncBufRead + Unpin,
    {
        use Compression::*;

        match self.compression {
            Stored => compress_stored(writer, reader).await,
            #[cfg(feature = "zstd")]
            Zstd => compress_zstd(writer, reader, self).await,
            #[cfg(feature = "xz")]
            Xz => compress_xz(writer, reader, self.get_i32("level")).await,
            Unknown(id) => Err(std::io::Error::new(
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
}

impl fmt::Display for Compression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use Compression::*;

        let s = match self {
            Stored => "stored",
            Zstd => "Zstd",
            Xz => "xz",
            Unknown(id) => return write!(f, "?{:x}?", id),
        };

        write!(f, "{}", s)
    }
}

impl fmt::Debug for Compression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl Compression {
    pub const fn id(self) -> u8 {
        use Compression::*;

        match self {
            Stored => COMPRESSION_STORED,
            Zstd => COMPRESSION_ZSTD,
            Xz => COMPRESSION_XZ,
            Unknown(id) => id,
        }
    }

    pub async fn compress<W, R>(self, writer: W, mut reader: R) -> Result<ByteCount>
    where
        W: AsyncWrite + Unpin,
        R: AsyncBufRead + Unpin,
    {
        self.compress_ref(writer, &mut reader).await
    }

    /// Compress data from a reader reference, allowing caller to retain ownership.
    /// Uses default compression options.
    pub async fn compress_ref<W, R>(self, writer: W, reader: &mut R) -> Result<ByteCount>
    where
        W: AsyncWrite + Unpin,
        R: AsyncBufRead + Unpin,
    {
        let config = CompressionConfig::new(self);
        config.compress_ref(writer, reader).await
    }

    pub async fn decompress_write<R, W>(self, reader: R, writer: W) -> Result<()>
    where
        R: AsyncBufRead + Unpin,
        W: AsyncWrite + Unpin,
    {
        self.decompress_write_with_dict(reader, writer, None).await
    }

    /// Decompress with optional dictionary support.
    pub async fn decompress_write_with_dict<R, W>(
        self,
        reader: R,
        writer: W,
        dictionary: Option<&[u8]>,
    ) -> Result<()>
    where
        R: AsyncBufRead + Unpin,
        W: AsyncWrite + Unpin,
    {
        use Compression::*;

        match self {
            Stored => decompress_stored(reader, writer).await,
            #[cfg(feature = "zstd")]
            Zstd => decompress_zstd(reader, writer, dictionary).await,
            #[cfg(feature = "xz")]
            Xz => decompress_xz(reader, writer).await,
            Unknown(id) => Err(std::io::Error::new(
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
}

// Stored (no compression)
async fn compress_stored<W, R>(writer: W, reader: &mut R) -> Result<ByteCount>
where
    W: AsyncWrite + Unpin,
    R: AsyncBufRead + Unpin,
{
    let mut counting = CountingWriter::new(writer);
    let read = tokio::io::copy_buf(reader, &mut counting).await?;
    counting.flush().await?;
    Ok(ByteCount {
        read,
        write: counting.bytes_written(),
    })
}

async fn decompress_stored<R, W>(mut reader: R, mut writer: W) -> Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    tokio::io::copy_buf(&mut reader, &mut writer).await?;
    writer.flush().await?;
    Ok(())
}

// Zstd
#[cfg(feature = "zstd")]
async fn compress_zstd<W, R>(
    writer: W,
    reader: &mut R,
    config: &CompressionConfig,
) -> Result<ByteCount>
where
    W: AsyncWrite + Unpin,
    R: AsyncBufRead + Unpin,
{
    use async_compression::zstd::CParameter;

    let counting = CountingWriter::new(writer);
    let level = config
        .get_i32("level")
        .map(Level::Precise)
        .unwrap_or(Level::Default);

    // Build CParameter list from config options
    let mut params = Vec::new();
    if let Some(v) = config.get_u32("window_log") {
        params.push(CParameter::window_log(v));
    }
    if let Some(v) = config.get_u32("hash_log") {
        params.push(CParameter::hash_log(v));
    }
    if let Some(v) = config.get_u32("chain_log") {
        params.push(CParameter::chain_log(v));
    }
    if let Some(v) = config.get_u32("search_log") {
        params.push(CParameter::search_log(v));
    }
    if let Some(v) = config.get_u32("min_match") {
        params.push(CParameter::min_match(v));
    }
    if let Some(v) = config.get_u32("target_length") {
        params.push(CParameter::target_length(v));
    }
    if let Some(v) = config.get_bool("checksum") {
        params.push(CParameter::checksum_flag(v));
    }

    // Note: async_compression doesn't support dict + params together, so dict takes precedence
    let mut encoder = match &config.dictionary {
        Some(dict) => ZstdEncoder::with_dict(counting, level, dict)?,
        None if params.is_empty() => ZstdEncoder::with_quality(counting, level),
        None => ZstdEncoder::with_quality_and_params(counting, level, &params),
    };

    let read = tokio::io::copy_buf(reader, &mut encoder).await?;
    encoder.shutdown().await?;
    let counting = encoder.into_inner();
    Ok(ByteCount {
        read,
        write: counting.bytes_written(),
    })
}

#[cfg(feature = "zstd")]
async fn decompress_zstd<R, W>(reader: R, mut writer: W, dictionary: Option<&[u8]>) -> Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut decoder = match dictionary {
        Some(dict) => ZstdDecoder::with_dict(reader, dict)?,
        None => ZstdDecoder::new(reader),
    };
    tokio::io::copy(&mut decoder, &mut writer).await?;
    writer.flush().await?;
    Ok(())
}

// XZ
#[cfg(feature = "xz")]
async fn compress_xz<W, R>(writer: W, reader: &mut R, level: Option<i32>) -> Result<ByteCount>
where
    W: AsyncWrite + Unpin,
    R: AsyncBufRead + Unpin,
{
    let counting = CountingWriter::new(writer);
    let mut encoder = match level {
        Some(l) => XzEncoder::with_quality(counting, Level::Precise(l)),
        None => XzEncoder::new(counting),
    };
    let read = tokio::io::copy_buf(reader, &mut encoder).await?;
    encoder.shutdown().await?;
    let counting = encoder.into_inner();
    Ok(ByteCount {
        read,
        write: counting.bytes_written(),
    })
}

#[cfg(feature = "xz")]
async fn decompress_xz<R, W>(reader: R, mut writer: W) -> Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut decoder = XzDecoder::new(reader);
    tokio::io::copy(&mut decoder, &mut writer).await?;
    writer.flush().await?;
    Ok(())
}
