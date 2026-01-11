//! Herein lies the brains of the `box` file format.
//!
//! Use [BoxFileReader][BoxFileReader] to read files, and [BoxFileWriter][BoxFileWriter] to write files.

/// Box epoch: 2026-01-01 00:00:00 UTC (seconds since Unix epoch)
pub const BOX_EPOCH_UNIX: i64 = 1767225600;

#[cfg(any(feature = "reader", feature = "writer"))]
pub mod aio;
pub mod checksum;
mod compression;
pub mod core;
mod counting;
#[cfg(feature = "reader")]
mod de;
#[cfg(feature = "zstd")]
pub mod dict;
pub mod encode;
pub mod fs;
pub mod hashing;
pub mod header;
pub mod parse;
pub mod path;
mod record;
#[cfg(feature = "writer")]
mod ser;
#[cfg(any(feature = "reader", feature = "writer"))]
pub mod sync;

pub use self::core::RecordIndex;
#[cfg(feature = "writer")]
pub use aio::{
    AddOptions, AddStats, BoxFileWriter, CompressedData, CompressedFile, FileJob, ParallelProgress,
    calculate_memory_threshold, compress_file,
};
#[cfg(feature = "reader")]
pub use aio::{
    BoxFileReader, ExtractError, ExtractOptions, ExtractProgress, ExtractStats, ExtractTiming,
    OpenError, ValidateProgress, ValidateStats,
};
pub use checksum::{Checksum, NullChecksum};
pub use compression::{
    ByteCount, Compression, CompressionConfig, StreamStatus, compress_bytes_sync,
    decompress_bytes_sync,
};
pub use core::{AttrKey, AttrMap, AttrType, AttrValue, BoxMetadata};
pub use parse::{HeaderData, ParseError, ParseResult, RecordHeader, RecordType};
pub use path::BoxPath;
pub use record::{
    ChunkedFileRecord, DirectoryRecord, ExternalLinkRecord, FileRecord, LinkRecord, Record,
};
