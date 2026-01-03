//! Herein lies the brains of the `box` file format.
//!
//! Use [BoxFileReader][BoxFileReader] to read files, and [BoxFileWriter][BoxFileWriter] to write files.

/// Box epoch: 2026-01-01 00:00:00 UTC (seconds since Unix epoch)
pub const BOX_EPOCH_UNIX: i64 = 1767225600;

pub mod checksum;
mod compression;
mod counting;
#[cfg(feature = "reader")]
mod de;
mod file;
pub mod fs;
pub mod hashing;
mod header;
pub mod path;
mod record;
#[cfg(feature = "writer")]
mod ser;

pub use self::file::RecordIndex;
pub use checksum::{Checksum, NullChecksum};
pub use compression::{ByteCount, Compression, CompressionConfig};
#[cfg(feature = "reader")]
pub use file::reader::{
    BoxFileReader, ExtractError, ExtractOptions, ExtractProgress, ExtractStats, ExtractTiming,
    OpenError, ValidateProgress, ValidateStats,
};
#[cfg(feature = "writer")]
pub use file::writer::{
    AddOptions, AddStats, BoxFileWriter, CompressedData, CompressedFile, FileJob, ParallelProgress,
    calculate_memory_threshold, compress_file,
};
pub use file::{
    AttrMap, BoxMetadata,
    meta::{AttrKey, AttrType, AttrValue},
};
use header::BoxHeader;
pub use path::BoxPath;
pub use record::{DirectoryRecord, ExternalLinkRecord, FileRecord, LinkRecord, Record};
