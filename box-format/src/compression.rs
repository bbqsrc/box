use std::fmt;
use std::io::{Read, Result, Seek, Write};

#[cfg(feature = "brotli")]
use comde::brotli::{BrotliCompressor, BrotliDecompressor};
#[cfg(feature = "deflate")]
use comde::deflate::{DeflateCompressor, DeflateDecompressor};
#[cfg(feature = "snappy")]
use comde::snappy::{SnappyCompressor, SnappyDecompressor};
#[cfg(feature = "xz")]
use comde::xz::{XzCompressor, XzDecompressor};
#[cfg(feature = "zstd")]
use comde::zstd::{ZstdCompressor, ZstdDecompressor};
use comde::{
    stored::{StoredCompressor, StoredDecompressor},
    ByteCount, Compressor, Decompress, Decompressor,
};

pub mod constants {
    pub const COMPRESSION_STORED: u8 = 0x00;
    pub const COMPRESSION_DEFLATE: u8 = 0x10;
    pub const COMPRESSION_ZSTD: u8 = 0x20;
    pub const COMPRESSION_XZ: u8 = 0x30;
    pub const COMPRESSION_SNAPPY: u8 = 0x40;
    pub const COMPRESSION_BROTLI: u8 = 0x50;
}

use self::constants::*;

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
            Zstd => "Zstandard",
            Xz => "xz",
            Snappy => "Snappy",
            Brotli => "Brotli",
            Unknown(id) => return write!(f, "Unknown(id: {:x})", id),
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

    pub fn compress<W: Write + Seek, R: Read>(
        self,
        mut writer: W,
        reader: &mut R,
    ) -> Result<ByteCount> {
        use Compression::*;

        match self {
            Stored => StoredCompressor.compress(&mut writer, reader),
            #[cfg(feature = "deflate")]
            Deflate => DeflateCompressor.compress(&mut writer, reader),
            #[cfg(feature = "zstd")]
            Zstd => ZstdCompressor.compress(&mut writer, reader),
            #[cfg(feature = "xz")]
            Xz => XzCompressor.compress(&mut writer, reader),
            #[cfg(feature = "snappy")]
            Snappy => SnappyCompressor.compress(&mut writer, reader),
            #[cfg(feature = "brotli")]
            Brotli => BrotliCompressor.compress(&mut writer, reader),
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

    pub fn decompress<R: Read, V: Decompress>(self, reader: R) -> Result<V> {
        use Compression::*;

        match self {
            Stored => StoredDecompressor.from_reader(reader),
            #[cfg(feature = "deflate")]
            Deflate => DeflateDecompressor.from_reader(reader),
            #[cfg(feature = "zstd")]
            Zstd => ZstdDecompressor.from_reader(reader),
            #[cfg(feature = "xz")]
            Xz => XzDecompressor.from_reader(reader),
            #[cfg(feature = "snappy")]
            Snappy => SnappyDecompressor.from_reader(reader),
            #[cfg(feature = "brotli")]
            Brotli => BrotliDecompressor.from_reader(reader),
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

    pub fn decompress_write<R: Read, W: Write>(self, reader: R, writer: W) -> Result<()> {
        use Compression::*;

        match self {
            Stored => StoredDecompressor.copy(reader, writer),
            #[cfg(feature = "deflate")]
            Deflate => DeflateDecompressor.copy(reader, writer),
            #[cfg(feature = "zstd")]
            Zstd => ZstdDecompressor.copy(reader, writer),
            #[cfg(feature = "xz")]
            Xz => XzDecompressor.copy(reader, writer),
            #[cfg(feature = "snappy")]
            Snappy => SnappyDecompressor.copy(reader, writer),
            #[cfg(feature = "brotli")]
            Brotli => BrotliDecompressor.copy(reader, writer),
            Unknown(id) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Cannot handle decompression with id {}", id),
            )),
            #[allow(unreachable_patterns)]
            missing => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Compiled without support for {:?}", missing),
            )),
        }?;

        Ok(())
    }
}
