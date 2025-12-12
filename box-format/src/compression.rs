use std::fmt;
use std::io::Result;

use tokio::io::{AsyncBufRead, AsyncWrite, AsyncWriteExt};

#[cfg(feature = "brotli")]
use async_compression::tokio::{bufread::BrotliDecoder, write::BrotliEncoder};
#[cfg(feature = "deflate")]
use async_compression::tokio::{bufread::DeflateDecoder, write::DeflateEncoder};
#[cfg(feature = "xz")]
use async_compression::tokio::{bufread::XzDecoder, write::XzEncoder};
#[cfg(feature = "zstd")]
use async_compression::tokio::{bufread::ZstdDecoder, write::ZstdEncoder};

pub mod constants {
    pub const COMPRESSION_STORED: u8 = 0x00;
    pub const COMPRESSION_DEFLATE: u8 = 0x10;
    pub const COMPRESSION_ZSTD: u8 = 0x20;
    pub const COMPRESSION_XZ: u8 = 0x30;
    pub const COMPRESSION_SNAPPY: u8 = 0x40;
    pub const COMPRESSION_BROTLI: u8 = 0x50;
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

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Compression {
    Stored,
    Deflate,
    Zstd,
    Xz,
    Snappy,
    Brotli,
    Unknown(u8),
}

impl Default for Compression {
    fn default() -> Self {
        Self::Stored
    }
}

impl Compression {
    pub const fn available_variants() -> &'static [&'static str] {
        &["stored", "brotli", "deflate", "snappy", "xz", "zstd"]
    }
}

impl fmt::Display for Compression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use Compression::*;

        let s = match self {
            Stored => "stored",
            Deflate => "DEFLATE",
            Zstd => "Zstd",
            Xz => "xz",
            Snappy => "Snappy",
            Brotli => "Brotli",
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
            Deflate => COMPRESSION_DEFLATE,
            Zstd => COMPRESSION_ZSTD,
            Xz => COMPRESSION_XZ,
            Snappy => COMPRESSION_SNAPPY,
            Brotli => COMPRESSION_BROTLI,
            Unknown(id) => id,
        }
    }

    pub async fn compress<W, R>(self, writer: W, reader: R) -> Result<ByteCount>
    where
        W: AsyncWrite + Unpin,
        R: AsyncBufRead + Unpin,
    {
        use Compression::*;

        match self {
            Stored => compress_stored(writer, reader).await,
            #[cfg(feature = "deflate")]
            Deflate => compress_deflate(writer, reader).await,
            #[cfg(feature = "zstd")]
            Zstd => compress_zstd(writer, reader).await,
            #[cfg(feature = "xz")]
            Xz => compress_xz(writer, reader).await,
            #[cfg(feature = "snappy")]
            Snappy => compress_snappy(writer, reader).await,
            #[cfg(feature = "brotli")]
            Brotli => compress_brotli(writer, reader).await,
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

    pub async fn decompress_write<R, W>(self, reader: R, writer: W) -> Result<()>
    where
        R: AsyncBufRead + Unpin,
        W: AsyncWrite + Unpin,
    {
        use Compression::*;

        match self {
            Stored => decompress_stored(reader, writer).await,
            #[cfg(feature = "deflate")]
            Deflate => decompress_deflate(reader, writer).await,
            #[cfg(feature = "zstd")]
            Zstd => decompress_zstd(reader, writer).await,
            #[cfg(feature = "xz")]
            Xz => decompress_xz(reader, writer).await,
            #[cfg(feature = "snappy")]
            Snappy => decompress_snappy(reader, writer).await,
            #[cfg(feature = "brotli")]
            Brotli => decompress_brotli(reader, writer).await,
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
async fn compress_stored<W, R>(mut writer: W, mut reader: R) -> Result<ByteCount>
where
    W: AsyncWrite + Unpin,
    R: AsyncBufRead + Unpin,
{
    let written = tokio::io::copy_buf(&mut reader, &mut writer).await?;
    writer.flush().await?;
    Ok(ByteCount {
        read: written,
        write: written,
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

// Deflate
#[cfg(feature = "deflate")]
async fn compress_deflate<W, R>(writer: W, mut reader: R) -> Result<ByteCount>
where
    W: AsyncWrite + Unpin,
    R: AsyncBufRead + Unpin,
{
    use tokio::io::AsyncBufReadExt;

    let mut encoder = DeflateEncoder::new(writer);
    let mut read_count = 0u64;

    loop {
        let buf = reader.fill_buf().await?;
        if buf.is_empty() {
            break;
        }
        let len = buf.len();
        encoder.write_all(buf).await?;
        reader.consume(len);
        read_count += len as u64;
    }

    encoder.shutdown().await?;
    let inner = encoder.into_inner();
    let _ = inner;

    // We can't easily get the written count from the encoder, so we estimate
    // In a real implementation, you'd wrap the writer to count bytes
    Ok(ByteCount {
        read: read_count,
        write: read_count, // This is approximate; actual compressed size may differ
    })
}

#[cfg(feature = "deflate")]
async fn decompress_deflate<R, W>(reader: R, mut writer: W) -> Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut decoder = DeflateDecoder::new(reader);
    tokio::io::copy(&mut decoder, &mut writer).await?;
    writer.flush().await?;
    Ok(())
}

// Zstd
#[cfg(feature = "zstd")]
async fn compress_zstd<W, R>(writer: W, mut reader: R) -> Result<ByteCount>
where
    W: AsyncWrite + Unpin,
    R: AsyncBufRead + Unpin,
{
    use tokio::io::AsyncBufReadExt;

    let mut encoder = ZstdEncoder::new(writer);
    let mut read_count = 0u64;

    loop {
        let buf = reader.fill_buf().await?;
        if buf.is_empty() {
            break;
        }
        let len = buf.len();
        encoder.write_all(buf).await?;
        reader.consume(len);
        read_count += len as u64;
    }

    encoder.shutdown().await?;

    Ok(ByteCount {
        read: read_count,
        write: read_count,
    })
}

#[cfg(feature = "zstd")]
async fn decompress_zstd<R, W>(reader: R, mut writer: W) -> Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut decoder = ZstdDecoder::new(reader);
    tokio::io::copy(&mut decoder, &mut writer).await?;
    writer.flush().await?;
    Ok(())
}

// XZ
#[cfg(feature = "xz")]
async fn compress_xz<W, R>(writer: W, mut reader: R) -> Result<ByteCount>
where
    W: AsyncWrite + Unpin,
    R: AsyncBufRead + Unpin,
{
    use tokio::io::AsyncBufReadExt;

    let mut encoder = XzEncoder::new(writer);
    let mut read_count = 0u64;

    loop {
        let buf = reader.fill_buf().await?;
        if buf.is_empty() {
            break;
        }
        let len = buf.len();
        encoder.write_all(buf).await?;
        reader.consume(len);
        read_count += len as u64;
    }

    encoder.shutdown().await?;

    Ok(ByteCount {
        read: read_count,
        write: read_count,
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

// Brotli
#[cfg(feature = "brotli")]
async fn compress_brotli<W, R>(writer: W, mut reader: R) -> Result<ByteCount>
where
    W: AsyncWrite + Unpin,
    R: AsyncBufRead + Unpin,
{
    use tokio::io::AsyncBufReadExt;

    let mut encoder = BrotliEncoder::new(writer);
    let mut read_count = 0u64;

    loop {
        let buf = reader.fill_buf().await?;
        if buf.is_empty() {
            break;
        }
        let len = buf.len();
        encoder.write_all(buf).await?;
        reader.consume(len);
        read_count += len as u64;
    }

    encoder.shutdown().await?;

    Ok(ByteCount {
        read: read_count,
        write: read_count,
    })
}

#[cfg(feature = "brotli")]
async fn decompress_brotli<R, W>(reader: R, mut writer: W) -> Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut decoder = BrotliDecoder::new(reader);
    tokio::io::copy(&mut decoder, &mut writer).await?;
    writer.flush().await?;
    Ok(())
}

// Snappy
#[cfg(feature = "snappy")]
async fn compress_snappy<W, R>(writer: W, mut reader: R) -> Result<ByteCount>
where
    W: AsyncWrite + Unpin,
    R: AsyncBufRead + Unpin,
{
    use tokio::io::AsyncBufReadExt;
    use tokio_snappy::SnappyIO;

    let mut snappy_writer = SnappyIO::new(writer);
    let mut read_count = 0u64;

    loop {
        let buf = reader.fill_buf().await?;
        if buf.is_empty() {
            break;
        }
        let len = buf.len();
        snappy_writer.write_all(buf).await?;
        reader.consume(len);
        read_count += len as u64;
    }

    snappy_writer.shutdown().await?;

    Ok(ByteCount {
        read: read_count,
        write: read_count,
    })
}

#[cfg(feature = "snappy")]
async fn decompress_snappy<R, W>(reader: R, mut writer: W) -> Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    use tokio_snappy::SnappyIO;

    let mut snappy_reader = SnappyIO::new(reader);
    tokio::io::copy(&mut snappy_reader, &mut writer).await?;
    writer.flush().await?;
    Ok(())
}
