//! Herein lies the brains of the `box` file format.
//!
//! Use [BoxFileReader][BoxFileReader] to read files, and [BoxFileWriter][BoxFileWriter] to write files.

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

#[cfg(feature = "ffi")]
pub mod ffi;

pub use self::file::RecordIndex;
pub use checksum::{Checksum, NullChecksum};
pub use compression::{ByteCount, Compression, CompressionConfig};
#[cfg(feature = "reader")]
pub use file::reader::{
    BoxFileReader, ExtractError, ExtractOptions, ExtractProgress, ExtractStats, OpenError,
    ValidateProgress, ValidateStats,
};
#[cfg(feature = "writer")]
pub use file::writer::{
    AddOptions, AddStats, BoxFileWriter, CompressedData, CompressedFile, FileJob, ParallelProgress,
    calculate_memory_threshold, compress_file,
};
pub use file::{AttrMap, BoxMetadata, meta::AttrValue};
use header::BoxHeader;
pub use path::BoxPath;
pub use record::{DirectoryRecord, FileRecord, LinkRecord, Record};
