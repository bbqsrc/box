use std::fmt;
use std::io::{Read, Result, Seek, Write};

use comde::{
    deflate::{DeflateCompressor, DeflateDecompressor},
    snappy::{SnappyCompressor, SnappyDecompressor},
    stored::{StoredCompressor, StoredDecompressor},
    xz::{XzCompressor, XzDecompressor},
    zstd::{ZstdCompressor, ZstdDecompressor},
    ByteCount, Compress, Compressor, Decompress, Decompressor,
};

pub mod constants {
    pub const COMPRESSION_STORED: u8 = 0x00;
    pub const COMPRESSION_DEFLATE: u8 = 0x10;
    pub const COMPRESSION_ZSTD: u8 = 0x20;
    pub const COMPRESSION_XZ: u8 = 0x30;
    pub const COMPRESSION_SNAPPY: u8 = 0x40;
}

use self::constants::*;

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Compression {
    Stored,
    Deflate,
    Zstd,
    Xz,
    Snappy,
    Unknown(u8),
}

impl Compression {
    // FIXME(killercup): Replace with strum-macros when new release is available
    pub fn available_variants() -> &'static [&'static str] {
        &["stored", "deflate", "zstd", "xz", "snappy"]
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
    pub fn id(self) -> u8 {
        use Compression::*;

        match self {
            Stored => COMPRESSION_STORED,
            Deflate => COMPRESSION_DEFLATE,
            Zstd => COMPRESSION_ZSTD,
            Xz => COMPRESSION_XZ,
            Snappy => COMPRESSION_SNAPPY,
            Unknown(id) => id,
        }
    }

    pub fn compress<W: Write + Seek, R: Read>(self, mut writer: W, reader: &mut R) -> Result<ByteCount> {
        use Compression::*;

        match self {
            Stored => StoredCompressor.compress(&mut writer, reader),
            Deflate => DeflateCompressor.compress(&mut writer, reader),
            Zstd => ZstdCompressor.compress(&mut writer, reader),
            Xz => XzCompressor.compress(&mut writer, reader),
            Snappy => SnappyCompressor.compress(&mut writer, reader),
            Unknown(id) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Cannot handle compression with id {}", id),
            )),
        }
    }

    pub fn decompress<R: Read, V: Decompress>(self, reader: R) -> Result<V> {
        use Compression::*;

        match self {
            Stored => StoredDecompressor.from_reader(reader),
            Deflate => DeflateDecompressor.from_reader(reader),
            Zstd => ZstdDecompressor.from_reader(reader),
            Xz => XzDecompressor.from_reader(reader),
            Snappy => SnappyDecompressor.from_reader(reader),
            Unknown(id) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Cannot handle decompression with id {}", id),
            )),
        }
    }

    pub fn decompress_write<R: Read, W: Write>(self, reader: R, writer: W) -> Result<()> {
        use Compression::*;

        match self {
            Stored => StoredDecompressor.copy(reader, writer),
            Deflate => DeflateDecompressor.copy(reader, writer),
            Zstd => ZstdDecompressor.copy(reader, writer),
            Xz => XzDecompressor.copy(reader, writer),
            Snappy => SnappyDecompressor.copy(reader, writer),
            Unknown(id) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Cannot handle decompression with id {}", id),
            )),
        }?;

        Ok(())
    }
}
