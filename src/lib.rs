//! Herein lies the brains of the `box` file format.
//!
//! Use [BoxFileReader][BoxFileReader] to read files, and [BoxFileWriter][BoxFileWriter] to write files.

#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(feature = "alloc")]
extern crate alloc;

mod compat;

/// Box epoch: 2026-01-01 00:00:00 UTC (seconds since Unix epoch)
pub const BOX_EPOCH_UNIX: i64 = 1767225600;

pub mod attrs;

#[cfg(any(feature = "reader", feature = "writer"))]
pub mod aio;
#[cfg(feature = "std")]
pub mod checksum;
mod compression;
pub mod core;
#[cfg(feature = "std")]
mod counting;
#[cfg(feature = "reader")]
mod de;
#[cfg(feature = "zstd")]
pub mod dict;
#[cfg(feature = "std")]
pub mod encode;
#[cfg(feature = "std")]
pub mod fs;
#[cfg(feature = "std")]
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
#[cfg(feature = "std")]
pub use checksum::{Checksum, NullChecksum};
// Core compression types (no_std compatible)
pub use compression::{ByteCount, Compression, StreamStatus};
// Std-only compression types and functions
pub use compat::HashMap;
#[cfg(feature = "std")]
pub use compression::{CompressionConfig, compress_bytes_sync, decompress_bytes_sync};
pub use core::{AttrKey, AttrMap, AttrType, AttrValue, BoxMetadata};
pub use parse::{HeaderData, ParseError, ParseResult, RecordHeader, RecordType};
pub use path::BoxPath;
pub use record::{
    ChunkedFileRecord, DirectoryRecord, ExternalLinkRecord, FileRecord, LinkRecord, Record,
};
