use std::path::PathBuf;

use crate::compat::HashMap;
#[cfg(feature = "zstd")]
use crate::compression::zstd::ZstdCompressor;
use crate::{
    compression::{Compression, CompressionConfig},
    path::BoxPath,
};

/// Where compressed data is stored.
pub enum CompressedData {
    /// Small files: compressed data in memory.
    Memory(Vec<u8>),
    /// Large files: compressed data in a temp file.
    TempFile(tempfile::NamedTempFile),
}

/// A file job for parallel compression.
///
/// This struct specifies a file to be compressed with its own compression setting,
/// allowing different files to use different compression algorithms.
pub struct FileJob {
    /// Path to the file on the filesystem.
    pub fs_path: PathBuf,
    /// Path within the archive.
    pub box_path: BoxPath<'static>,
    /// Compression configuration for this file.
    pub config: CompressionConfig,
    /// Additional attributes to set on this file (merged with metadata-derived attrs).
    pub attrs: HashMap<String, Vec<u8>>,
}

/// Compressed file data ready to be written to the archive.
///
/// This struct holds all the data needed to write a file to the archive,
/// but does NOT contain the file offset - that's determined at write time
/// to prevent data races when compressing in parallel.
pub struct CompressedFile {
    /// The path within the archive.
    pub box_path: BoxPath<'static>,
    /// The compressed data (in memory or temp file).
    pub data: CompressedData,
    /// The compression algorithm used.
    pub compression: Compression,
    /// Size of the compressed data in bytes.
    pub compressed_length: u64,
    /// Size of the original uncompressed data in bytes.
    pub decompressed_length: u64,
    /// File attributes (will be converted to internal keys at write time).
    pub attrs: HashMap<String, Vec<u8>>,
    /// Optional checksum: (attribute_name, hash_bytes).
    pub checksum: Option<(&'static str, Vec<u8>)>,
}

/// Independently-compressed blocks ready for one sequential archive write.
pub(super) struct CompressedChunkedFile {
    pub(super) box_path: BoxPath<'static>,
    pub(super) data: tempfile::NamedTempFile,
    pub(super) compression: Compression,
    pub(super) block_size: u32,
    pub(super) block_offsets: Vec<u64>,
    pub(super) compressed_length: u64,
    pub(super) decompressed_length: u64,
    pub(super) attrs: HashMap<String, Vec<u8>>,
    pub(super) checksum: Option<(&'static str, Vec<u8>)>,
    pub(super) dictionary: Option<Vec<u8>>,
}

pub(super) enum PreparedFile {
    Regular {
        file: CompressedFile,
        dictionary: Option<Vec<u8>>,
    },
    Chunked(CompressedChunkedFile),
}

impl PreparedFile {
    pub(super) fn box_path(&self) -> &BoxPath<'static> {
        match self {
            Self::Regular { file, .. } => &file.box_path,
            Self::Chunked(file) => &file.box_path,
        }
    }

    pub(super) fn dictionary(&self) -> Option<&[u8]> {
        match self {
            Self::Regular { dictionary, .. } => dictionary.as_deref(),
            Self::Chunked(file) => file.dictionary.as_deref(),
        }
    }

    pub(super) fn lengths(&self) -> (u64, u64) {
        match self {
            Self::Regular { file, .. } => (file.decompressed_length, file.compressed_length),
            Self::Chunked(file) => (file.decompressed_length, file.compressed_length),
        }
    }
}

/// Reuses zstd contexts across files compressed by one parallel archive job.
///
/// Initializing a context is noticeable for package trees made up of many small
/// files. Dictionaries are deliberately excluded: callers may mix dictionary
/// contents, while the overwhelmingly common package path uses dictionary-free
/// zstd with a small set of compression levels.
#[cfg(feature = "zstd")]
#[derive(Default)]
pub(super) struct ZstdCompressorPool {
    compressors: std::sync::Mutex<HashMap<i32, Vec<ZstdCompressor<'static>>>>,
}

#[cfg(feature = "zstd")]
impl ZstdCompressorPool {
    pub(super) fn take(&self, level: i32) -> std::io::Result<ZstdCompressor<'static>> {
        let cached = self
            .compressors
            .lock()
            .map_err(|_| std::io::Error::other("zstd compressor pool poisoned"))?
            .get_mut(&level)
            .and_then(Vec::pop);

        cached.map_or_else(|| ZstdCompressor::new(level), Ok)
    }

    pub(super) fn put(&self, level: i32, mut compressor: ZstdCompressor<'static>) {
        if compressor.reset().is_ok()
            && let Ok(mut compressors) = self.compressors.lock()
        {
            compressors.entry(level).or_default().push(compressor);
        }
    }
}
