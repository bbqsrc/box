use std::io::SeekFrom;
use std::num::NonZeroU64;
use std::ops::{AddAssign, Deref};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use lru::LruCache;

use mmap_io::MemoryMappedFile;
use mmap_io::segment::Segment;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, BufReader};

#[cfg(feature = "xz")]
use crate::compression::xz::XzDecompressor;
#[cfg(feature = "zstd")]
use crate::compression::zstd::ZstdDecompressor;
use crate::core::{ArchiveReader, AttrValue, BoxMetadata, RecordIndex, Records, RecordsItem};
use crate::path::IntoBoxPathError;
use crate::{
    compression::{Compression, constants::DEFAULT_BLOCK_SIZE},
    de::deserialize_metadata_borrowed,
    header::BoxHeader,
    path::BoxPath,
    record::{ChunkedFileRecord, FileRecord, LinkRecord, Record},
};

/// Async reader for Box archives.
///
/// This is a frontend that wraps the sans-IO [`ArchiveReader`] core,
/// providing async I/O operations for reading archives.
pub struct BoxFileReader {
    /// The sans-IO core that manages archive state
    pub(crate) core: ArchiveReader<'static>,
    /// Path to the archive file
    pub(crate) path: PathBuf,
    /// Holds the mmapped trailer data. The Arc inside keeps the data alive.
    /// This must not be dropped before `core.meta` is dropped.
    #[allow(dead_code)]
    pub(crate) trailer_segment: Segment,
}

impl std::fmt::Debug for BoxFileReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoxFileReader")
            .field("path", &self.path)
            .field("header", &self.core.header)
            .field("meta", &self.core.meta)
            .field("offset", &self.core.offset)
            .finish_non_exhaustive()
    }
}

pub(super) async fn read_header<R: tokio::io::AsyncRead + tokio::io::AsyncSeek + Unpin + Send>(
    file: &mut R,
    offset: u64,
) -> std::io::Result<BoxHeader> {
    use tokio::io::AsyncReadExt;
    file.seek(SeekFrom::Start(offset)).await?;

    // Read header bytes and parse using sans-IO parser
    let mut buf = [0u8; 32];
    file.read_exact(&mut buf).await?;
    let (header_data, _) = crate::parse::parse_header(&buf)?;

    Ok(BoxHeader {
        version: header_data.version,
        allow_external_symlinks: header_data.allow_external_symlinks,
        allow_escapes: header_data.allow_escapes,
        alignment: header_data.alignment,
        trailer: std::num::NonZeroU64::new(header_data.trailer_offset),
    })
}

pub(super) async fn read_trailer<R: tokio::io::AsyncRead + tokio::io::AsyncSeek + Unpin + Send>(
    reader: &mut R,
    ptr: NonZeroU64,
    offset: u64,
    version: u8,
) -> std::io::Result<BoxMetadata<'static>> {
    use tokio::io::AsyncReadExt;
    reader.seek(SeekFrom::Start(offset + ptr.get())).await?;

    // Read all remaining data and parse using sans-IO parser
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await?;

    let meta = match version {
        0 => {
            // v0 uses different format
            let mut pos = 0;
            crate::de::v0::deserialize_metadata_borrowed(&buf, &mut pos)?.into_owned()
        }
        _ => {
            let (meta, _) = crate::parse::parse_metadata_v1(&buf)?;
            meta.into_owned()
        }
    };

    Ok(meta)
}

#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    #[error("Could not find trailer (the end of the file is missing).")]
    MissingTrailer,

    #[error("Invalid trailer data (the data that describes where all the files are is invalid).")]
    InvalidTrailer(#[source] std::io::Error),

    #[error("Could not read header. Is this a valid Box archive?")]
    MissingHeader(#[source] std::io::Error),

    #[error("Invalid path to Box file. Path: '{}'", .1.display())]
    InvalidPath(#[source] std::io::Error, PathBuf),

    #[error("Failed to read Box file. Path: '{}'", .1.display())]
    ReadFailed(#[source] std::io::Error, PathBuf),
}

#[derive(Debug, thiserror::Error)]
pub enum ExtractError {
    #[error("Creating directory failed. Path: '{}'", .1.display())]
    CreateDirFailed(#[source] std::io::Error, PathBuf),

    #[error("Creating file failed. Path: '{}'", .1.display())]
    CreateFileFailed(#[source] std::io::Error, PathBuf),

    #[error("Path not found in archive. Path: '{}'", .0.display())]
    NotFoundInArchive(PathBuf),

    #[error("Decompressing file failed. Path: '{}'", .1.display())]
    DecompressionFailed(#[source] std::io::Error, PathBuf),

    #[error("Creating link failed. Path: '{}' -> '{}'", .1.display(), .2.display())]
    CreateLinkFailed(#[source] std::io::Error, PathBuf, PathBuf),

    #[error("Resolving link failed: Path: '{}' -> index {}", .1.name, .1.target.get())]
    ResolveLinkFailed(#[source] std::io::Error, LinkRecord<'static>),

    #[error("Could not convert to a valid Box path. Path suffix: '{}'", .1)]
    ResolveBoxPathFailed(#[source] IntoBoxPathError, String),

    #[error("Verification failed. Path: '{}'", .1.display())]
    VerificationFailed(#[source] std::io::Error, PathBuf),

    #[error("Archive has escaped paths but allow_escapes was not set in ExtractOptions")]
    AllowEscapesRequired,

    #[error(
        "Archive has external symlinks but allow_external_symlinks was not set in ExtractOptions"
    )]
    ExternalSymlinksRequired,
}

/// Timing breakdown for extraction phases.
#[derive(Debug, Clone, Default)]
pub struct ExtractTiming {
    /// Time spent collecting entries from metadata.
    pub collect: Duration,
    /// Time spent creating directories.
    pub directories: Duration,
    /// Time spent decompressing and writing files.
    pub decompress: Duration,
    /// Time spent creating symlinks.
    pub symlinks: Duration,
}

/// Statistics from extracting files from an archive.
#[derive(Debug, Clone, Default)]
pub struct ExtractStats {
    /// Number of files extracted.
    pub files_extracted: u64,
    /// Number of directories created.
    pub dirs_created: u64,
    /// Number of symlinks created.
    pub links_created: u64,
    /// Total bytes written to disk.
    pub bytes_written: u64,
    /// Number of files that failed checksum verification.
    pub checksum_failures: u64,
    /// Timing breakdown for extraction phases.
    pub timing: ExtractTiming,
}

impl AddAssign for ExtractStats {
    fn add_assign(&mut self, other: Self) {
        self.files_extracted += other.files_extracted;
        self.dirs_created += other.dirs_created;
        self.links_created += other.links_created;
        self.bytes_written += other.bytes_written;
        self.checksum_failures += other.checksum_failures;
        // Note: timing is not added - it's only meaningful at the top level
    }
}

/// Options for extraction.
#[derive(Debug, Clone)]
pub struct ExtractOptions {
    /// Verify blake3 checksums during extraction.
    pub verify_checksums: bool,
    /// Allow extracting archives with `\xNN` escape sequences in paths.
    pub allow_escapes: bool,
    /// Allow extracting archives with external symlinks (pointing outside the archive).
    pub allow_external_symlinks: bool,
    /// Restore extended attributes (Linux only).
    pub xattrs: bool,
}

impl Default for ExtractOptions {
    fn default() -> Self {
        Self {
            verify_checksums: true,
            allow_escapes: false,
            allow_external_symlinks: false,
            xattrs: false,
        }
    }
}

/// Statistics from validating files in an archive.
#[derive(Debug, Clone, Default)]
pub struct ValidateStats {
    /// Number of files checked.
    pub files_checked: u64,
    /// Number of files without a checksum attribute.
    pub files_without_checksum: u64,
    /// Number of files that failed checksum verification.
    pub checksum_failures: u64,
}

/// Progress updates from parallel extraction.
#[derive(Debug, Clone)]
pub enum ExtractProgress {
    /// Extraction started.
    Started {
        total_files: u64,
        total_dirs: u64,
        total_links: u64,
    },
    /// A directory was created.
    DirectoryCreated { path: BoxPath<'static> },
    /// A file is being extracted.
    Extracting { path: BoxPath<'static> },
    /// A file was extracted.
    Extracted {
        path: BoxPath<'static>,
        files_extracted: u64,
        total_files: u64,
    },
    /// A symlink was created.
    LinkCreated { path: BoxPath<'static> },
    /// All files have been extracted.
    Finished,
}

/// Progress updates from parallel validation.
#[derive(Debug, Clone)]
pub enum ValidateProgress {
    /// Validation started.
    Started { total_files: u64 },
    /// A file is being validated.
    Validating { path: BoxPath<'static> },
    /// A file was validated.
    Validated {
        path: BoxPath<'static>,
        files_checked: u64,
        total_files: u64,
        success: bool,
    },
    /// All files have been validated.
    Finished,
}

impl BoxFileReader {
    /// This will open an existing `.box` file for reading and error if the file is not valid.
    pub async fn open_at_offset<P: AsRef<Path>>(
        path: P,
        offset: u64,
    ) -> Result<BoxFileReader, OpenError> {
        let path = path.as_ref().to_path_buf();
        let path = tokio::fs::canonicalize(&path)
            .await
            .map_err(|e| OpenError::InvalidPath(e, path.to_path_buf()))?;

        let file = OpenOptions::new()
            .read(true)
            .open(&path)
            .await
            .map_err(|e| OpenError::ReadFailed(e, path.clone()))?;

        // Read the header to get the trailer pointer
        let header = {
            let mut reader = BufReader::new(file);
            read_header(&mut reader, offset)
                .await
                .map_err(OpenError::MissingHeader)?
        };

        let trailer_ptr = header.trailer.ok_or(OpenError::MissingTrailer)?;

        // Memory-map the file and use zero-copy deserialization for the trailer
        let mmap = MemoryMappedFile::builder(&path)
            .huge_pages(true)
            .open()
            .map_err(|e| OpenError::ReadFailed(std::io::Error::other(e), path.clone()))?;

        // Get file size to calculate trailer segment bounds
        let file_size = std::fs::metadata(&path)
            .map_err(|e| OpenError::ReadFailed(e, path.clone()))?
            .len();

        let trailer_offset = offset + trailer_ptr.get();
        let trailer_len = file_size - trailer_offset;

        let trailer_segment = Segment::new(mmap.into(), trailer_offset, trailer_len)
            .map_err(|e| OpenError::InvalidTrailer(std::io::Error::other(e)))?;

        let trailer_data = trailer_segment
            .as_slice()
            .map_err(|e| OpenError::InvalidTrailer(std::io::Error::other(e)))?;

        // Deserialize with borrowed data from the mmap
        let mut pos = 0;
        let meta = deserialize_metadata_borrowed(trailer_data, &mut pos, header.version)
            .map_err(OpenError::InvalidTrailer)?;

        // Safety: The trailer_segment holds an Arc<MemoryMappedFile> which keeps the
        // underlying memory alive. As long as BoxFileReader exists, the segment exists,
        // and the borrowed references in meta remain valid. We transmute to 'static
        // to express this in the type system.
        let meta: BoxMetadata<'static> = unsafe { std::mem::transmute(meta) };

        // Create the sans-IO core reader
        let core = ArchiveReader::new(header, meta, offset);

        let f = BoxFileReader {
            core,
            path,
            trailer_segment,
        };

        Ok(f)
    }

    /// This will open an existing `.box` file for reading and error if the file is not valid.
    #[inline]
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<BoxFileReader, OpenError> {
        Self::open_at_offset(path, 0).await
    }

    #[inline(always)]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[inline(always)]
    pub fn alignment(&self) -> u32 {
        self.core.alignment()
    }

    #[inline(always)]
    pub fn version(&self) -> u8 {
        self.core.version()
    }

    /// Returns true if this archive allows `\xNN` escape sequences in paths.
    #[inline(always)]
    pub fn allow_escapes(&self) -> bool {
        self.core.allow_escapes()
    }

    /// Returns true if this archive contains external symlinks (pointing outside the archive).
    #[inline(always)]
    pub fn allow_external_symlinks(&self) -> bool {
        self.core.allow_external_symlinks()
    }

    #[inline(always)]
    pub fn metadata(&self) -> &BoxMetadata<'static> {
        self.core.metadata()
    }

    /// Get file-level attributes with type-aware parsing.
    pub fn file_attrs(&self) -> std::collections::BTreeMap<&str, AttrValue<'_>> {
        self.core.file_attrs()
    }

    #[inline(always)]
    pub fn trailer_size(&self) -> u64 {
        self.trailer_segment.len()
    }

    /// Get an attribute value with fallback to archive-level attributes.
    ///
    /// Checks: record attr -> archive attr -> None
    pub fn get_attr<'a>(&'a self, record: &'a Record<'_>, key: &str) -> Option<&'a [u8]> {
        // Try record-level attr first
        if let Some(value) = record.attr(self.core.metadata(), key) {
            return Some(value);
        }
        // Fall back to archive-level attr
        self.core.metadata().file_attr(key)
    }

    /// Get the unix mode for a record, with fallback to defaults.
    ///
    /// Checks: record attr -> archive attr -> default (0o644 for files, 0o755 for dirs)
    #[cfg(unix)]
    pub fn get_mode(&self, record: &Record<'_>) -> u32 {
        self.core.get_mode(record)
    }

    pub async fn decompress<W: tokio::io::AsyncWrite + Unpin>(
        &self,
        record: &FileRecord<'_>,
        mut dest: W,
    ) -> std::io::Result<()> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let segment = self.memory_map(record)?;
        let data = segment.as_slice().map_err(std::io::Error::other)?;
        let cursor = std::io::Cursor::new(data);
        let mut buf_reader = tokio::io::BufReader::new(cursor);

        match record.compression {
            Compression::Stored => {
                tokio::io::copy(&mut buf_reader, &mut dest).await?;
            }
            #[cfg(feature = "zstd")]
            Compression::Zstd => {
                let dict = self.core.dictionary();
                let mut decompressor = match dict {
                    Some(d) => ZstdDecompressor::with_dictionary(d)?,
                    None => ZstdDecompressor::new()?,
                };
                let mut read_buf = vec![0u8; DEFAULT_BLOCK_SIZE as usize];
                let mut out_buf = vec![0u8; DEFAULT_BLOCK_SIZE as usize];

                loop {
                    let n = buf_reader.read(&mut read_buf).await?;
                    if n == 0 {
                        break;
                    }

                    let mut in_pos = 0;
                    while in_pos < n {
                        let status = decompressor.decompress(&read_buf[in_pos..n], &mut out_buf)?;
                        let consumed = status.bytes_consumed();
                        let produced = status.bytes_produced();
                        if produced > 0 {
                            dest.write_all(&out_buf[..produced]).await?;
                        }
                        in_pos += consumed;
                        if status.is_done() {
                            break;
                        }
                    }
                }
            }
            #[cfg(feature = "xz")]
            Compression::Xz => {
                let mut decompressor = XzDecompressor::new()?;
                let mut read_buf = vec![0u8; DEFAULT_BLOCK_SIZE as usize];
                let mut out_buf = vec![0u8; DEFAULT_BLOCK_SIZE as usize];

                loop {
                    let n = buf_reader.read(&mut read_buf).await?;
                    if n == 0 {
                        break;
                    }

                    let mut in_pos = 0;
                    while in_pos < n {
                        let status = decompressor.decompress(&read_buf[in_pos..n], &mut out_buf)?;
                        let consumed = status.bytes_consumed();
                        let produced = status.bytes_produced();
                        if produced > 0 {
                            dest.write_all(&out_buf[..produced]).await?;
                        }
                        in_pos += consumed;
                        if status.is_done() {
                            break;
                        }
                    }
                }
            }
            Compression::Unknown(id) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Unknown compression ID: {}", id),
                ));
            }
        }
        Ok(())
    }

    /// Decompress a chunked file by decompressing each block separately.
    ///
    /// Each block is independently compressed, so we must decompress them
    /// one at a time and concatenate the output.
    pub async fn decompress_chunked<W: tokio::io::AsyncWrite + Unpin>(
        &self,
        record: &ChunkedFileRecord<'_>,
        record_index: RecordIndex,
        mut dest: W,
    ) -> std::io::Result<()> {
        use tokio::io::AsyncWriteExt;

        // Get all block entries for this record
        let blocks = self.core.blocks_for_record(record_index);

        if blocks.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "chunked file has no block FST entries",
            ));
        }

        // Memory-map the entire chunked file data region
        let segment = self.memory_map_chunked(record)?;
        let all_data = segment.as_slice().map_err(std::io::Error::other)?;

        // Decompress each block
        for i in 0..blocks.len() {
            let (_logical_offset, physical_offset) = blocks[i];

            // Determine block's compressed size from next block's offset (or end of data)
            let compressed_end = if i + 1 < blocks.len() {
                blocks[i + 1].1 // Next block's physical offset
            } else {
                record.data.get() + record.length // End of all compressed data
            };
            let compressed_size = (compressed_end - physical_offset) as usize;

            // Get slice of compressed block data (relative to start of chunked file data)
            let block_offset = (physical_offset - record.data.get()) as usize;
            let block_data = &all_data[block_offset..block_offset + compressed_size];

            // Decompress this block using the core's decompress method
            let block_output = self.core.decompress_chunked_block(record, block_data)?;

            // Write decompressed block to destination
            dest.write_all(&block_output).await?;
        }

        dest.flush().await?;
        Ok(())
    }

    pub fn find(&self, path: &BoxPath<'_>) -> Result<&Record<'static>, ExtractError> {
        self.core
            .find(path)
            .ok_or_else(|| ExtractError::NotFoundInArchive(path.to_path_buf()))
    }

    pub async fn extract<P: AsRef<Path>>(
        &self,
        path: &BoxPath<'_>,
        output_path: P,
    ) -> Result<(), ExtractError> {
        if self.core.allow_escapes() {
            return Err(ExtractError::AllowEscapesRequired);
        }
        if self.core.allow_external_symlinks() {
            return Err(ExtractError::ExternalSymlinksRequired);
        }
        let output_path = output_path.as_ref();
        let record_index = self
            .core
            .metadata()
            .index(path)
            .ok_or_else(|| ExtractError::NotFoundInArchive(path.to_path_buf()))?;
        let record = self
            .core
            .record(record_index)
            .ok_or_else(|| ExtractError::NotFoundInArchive(path.to_path_buf()))?;
        self.extract_inner(path, record, record_index, output_path)
            .await
    }

    pub async fn extract_recursive<P: AsRef<Path>>(
        &self,
        path: &BoxPath<'_>,
        output_path: P,
    ) -> Result<(), ExtractError> {
        self.extract_recursive_with_options(path, output_path, ExtractOptions::default())
            .await
            .map(|_| ())
    }

    pub async fn extract_all<P: AsRef<Path>>(&self, output_path: P) -> Result<(), ExtractError> {
        self.extract_all_with_options(output_path, ExtractOptions::default())
            .await
            .map(|_| ())
    }

    /// Extract all files with options, returning extraction statistics.
    pub async fn extract_all_with_options<P: AsRef<Path>>(
        &self,
        output_path: P,
        options: ExtractOptions,
    ) -> Result<ExtractStats, ExtractError> {
        if self.core.allow_escapes() && !options.allow_escapes {
            return Err(ExtractError::AllowEscapesRequired);
        }
        if self.core.allow_external_symlinks() && !options.allow_external_symlinks {
            return Err(ExtractError::ExternalSymlinksRequired);
        }
        let output_path = output_path.as_ref();
        let mut stats = ExtractStats::default();
        let start = Instant::now();
        for item in self.core.iter() {
            self.extract_inner_with_options(
                &item.path,
                item.record,
                item.index,
                output_path,
                &options,
                &mut stats,
            )
            .await?;
        }
        // Serial extraction doesn't have separate phases, report all as decompress
        stats.timing.decompress = start.elapsed();
        Ok(stats)
    }

    /// Extract a path and all children with options, returning extraction statistics.
    pub async fn extract_recursive_with_options<P: AsRef<Path>>(
        &self,
        path: &BoxPath<'_>,
        output_path: P,
        options: ExtractOptions,
    ) -> Result<ExtractStats, ExtractError> {
        if self.core.allow_escapes() && !options.allow_escapes {
            return Err(ExtractError::AllowEscapesRequired);
        }
        if self.core.allow_external_symlinks() && !options.allow_external_symlinks {
            return Err(ExtractError::ExternalSymlinksRequired);
        }
        let output_path = output_path.as_ref();
        let mut stats = ExtractStats::default();

        let index = self
            .core
            .metadata()
            .index(path)
            .ok_or_else(|| ExtractError::NotFoundInArchive(path.to_path_buf()))?;

        for item in Records::new(self.core.metadata(), &[index], None) {
            self.extract_inner_with_options(
                &item.path,
                item.record,
                item.index,
                output_path,
                &options,
                &mut stats,
            )
            .await?;
        }
        Ok(stats)
    }

    /// Extract all files with parallel decompression.
    ///
    /// This method extracts files in three phases:
    /// 1. Create directories (sequential - parents must exist before children)
    /// 2. Extract files (parallel - bounded by concurrency)
    /// 3. Create symlinks (sequential - targets must exist first)
    pub async fn extract_all_parallel<P: AsRef<Path>>(
        &self,
        output_path: P,
        options: ExtractOptions,
        concurrency: usize,
    ) -> Result<ExtractStats, ExtractError> {
        self.extract_all_parallel_with_progress(output_path, options, concurrency, None)
            .await
    }

    /// Extract all files with parallel decompression and progress reporting.
    pub async fn extract_all_parallel_with_progress<P: AsRef<Path>>(
        &self,
        output_path: P,
        options: ExtractOptions,
        concurrency: usize,
        progress: Option<tokio::sync::mpsc::UnboundedSender<ExtractProgress>>,
    ) -> Result<ExtractStats, ExtractError> {
        if self.core.allow_escapes() && !options.allow_escapes {
            return Err(ExtractError::AllowEscapesRequired);
        }
        if self.core.allow_external_symlinks() && !options.allow_external_symlinks {
            return Err(ExtractError::ExternalSymlinksRequired);
        }
        let output_path = output_path.as_ref();
        let mut timing = ExtractTiming::default();

        // Collect entries by type
        let collect_start = Instant::now();
        let mut directories = Vec::new();
        let mut files = Vec::new();
        let mut chunked_files = Vec::new();
        let mut symlinks = Vec::new();

        for item in self.core.iter() {
            match item.record {
                Record::Directory(_) => directories.push((item.path.clone(), item.record.clone())),
                Record::File(f) => {
                    let expected_hash: Option<[u8; 32]> = match item
                        .record
                        .attr_value(self.metadata(), crate::attrs::BLAKE3)
                    {
                        Some(AttrValue::U256(h)) => Some(*h),
                        _ => None,
                    };
                    #[cfg(unix)]
                    let mode = self.get_mode(item.record);
                    #[cfg(not(unix))]
                    let mode = 0u32;

                    // Collect xattrs if option enabled
                    let xattrs: Vec<(String, Vec<u8>)> = if options.xattrs {
                        item.record
                            .attrs_iter(self.metadata())
                            .filter(|(k, _)| k.starts_with(crate::attrs::LINUX_XATTR_PREFIX))
                            .map(|(k, v)| (k.to_string(), v.to_vec()))
                            .collect()
                    } else {
                        Vec::new()
                    };

                    files.push((item.path.clone(), f.clone(), expected_hash, mode, xattrs));
                }
                Record::ChunkedFile(f) => {
                    let expected_hash: Option<[u8; 32]> = match item
                        .record
                        .attr_value(self.metadata(), crate::attrs::BLAKE3)
                    {
                        Some(AttrValue::U256(h)) => Some(*h),
                        _ => None,
                    };
                    #[cfg(unix)]
                    let mode = self.get_mode(item.record);
                    #[cfg(not(unix))]
                    let mode = 0u32;

                    let xattrs: Vec<(String, Vec<u8>)> = if options.xattrs {
                        item.record
                            .attrs_iter(self.metadata())
                            .filter(|(k, _)| k.starts_with(crate::attrs::LINUX_XATTR_PREFIX))
                            .map(|(k, v)| (k.to_string(), v.to_vec()))
                            .collect()
                    } else {
                        Vec::new()
                    };

                    // Get block entries for this chunked file
                    let blocks = self.core.blocks_for_record(item.index);

                    chunked_files.push((
                        item.path.clone(),
                        f.clone(),
                        expected_hash,
                        mode,
                        xattrs,
                        blocks,
                    ));
                }
                Record::Link(_) | Record::ExternalLink(_) => {
                    symlinks.push((item.path.clone(), item.record.clone()))
                }
            }
        }

        let total_files = (files.len() + chunked_files.len()) as u64;
        let total_dirs = directories.len() as u64;
        let total_links = symlinks.len() as u64;
        timing.collect = collect_start.elapsed();

        if let Some(ref p) = progress {
            let _ = p.send(ExtractProgress::Started {
                total_files,
                total_dirs,
                total_links,
            });
        }

        let mut stats = ExtractStats::default();

        // Phase 1: Create directories (sequential)
        let dirs_start = Instant::now();
        for (path, record) in directories {
            fs::create_dir_all(output_path)
                .await
                .map_err(|e| ExtractError::CreateDirFailed(e, output_path.to_path_buf()))?;
            let new_dir = output_path.join(path.to_path_buf());
            fs::create_dir_all(&new_dir)
                .await
                .map_err(|e| ExtractError::CreateDirFailed(e, new_dir.clone()))?;

            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let mode = self.get_mode(&record);
                let permissions = std::fs::Permissions::from_mode(mode);
                fs::set_permissions(&new_dir, permissions).await.ok();
            }

            #[cfg(windows)]
            {
                // Windows does not have unix-style permissions
                let _ = &record;
            }

            // Restore extended attributes if requested
            if options.xattrs {
                let xattr_iter = record
                    .attrs_iter(self.metadata())
                    .filter(|(k, _)| k.starts_with(crate::attrs::LINUX_XATTR_PREFIX));
                crate::fs::write_xattrs(&new_dir, xattr_iter);
            }

            stats.dirs_created += 1;

            if let Some(ref p) = progress {
                let _ = p.send(ExtractProgress::DirectoryCreated { path });
            }
        }
        timing.directories = dirs_start.elapsed();

        // Phase 2: Extract files (parallel) with pipelined validation
        let decompress_start = Instant::now();
        // Open mmap once and share across all tasks
        let mmap: Arc<MemoryMappedFile> = MemoryMappedFile::builder(&self.path)
            .huge_pages(true)
            .open()
            .map_err(|e| {
                ExtractError::DecompressionFailed(std::io::Error::other(e), self.path.clone())
            })?
            .into();

        let archive_offset = self.core.offset;
        let verify_checksums = options.verify_checksums;
        let dictionary: Option<Arc<[u8]>> =
            self.core.meta.dictionary.as_ref().map(|d| d.clone().into());

        // Two JoinSets: extraction (async I/O) and validation (blocking mmap hash)
        let mut extract_set = tokio::task::JoinSet::new();
        let mut validate_set = tokio::task::JoinSet::new();
        let mut files_iter = files.into_iter();
        let mut chunked_files_iter = chunked_files.into_iter();
        let mut files_extracted = 0u64;

        // Helper to spawn an extraction task for regular files
        let spawn_extract =
            |extract_set: &mut tokio::task::JoinSet<_>,
             box_path: BoxPath<'static>,
             record: FileRecord<'static>,
             expected_hash: Option<[u8; 32]>,
             mode: u32,
             xattrs: Vec<(String, Vec<u8>)>,
             mmap: Arc<MemoryMappedFile>,
             out_base: PathBuf,
             progress: Option<tokio::sync::mpsc::UnboundedSender<ExtractProgress>>,
             dictionary: Option<Arc<[u8]>>| {
                extract_set.spawn(async move {
                    if let Some(ref p) = progress {
                        let _ = p.send(ExtractProgress::Extracting {
                            path: box_path.clone(),
                        });
                    }

                    let result = extract_single_file_from_mmap(
                        mmap,
                        archive_offset,
                        &out_base,
                        &box_path,
                        &record,
                        mode,
                        xattrs,
                        dictionary,
                    )
                    .await;

                    result.map(|r| (box_path, r, expected_hash))
                });
            };

        // Helper to spawn an extraction task for chunked files
        let spawn_extract_chunked =
            |extract_set: &mut tokio::task::JoinSet<_>,
             box_path: BoxPath<'static>,
             record: ChunkedFileRecord<'static>,
             expected_hash: Option<[u8; 32]>,
             mode: u32,
             xattrs: Vec<(String, Vec<u8>)>,
             blocks: Vec<(u64, u64)>,
             mmap: Arc<MemoryMappedFile>,
             out_base: PathBuf,
             progress: Option<tokio::sync::mpsc::UnboundedSender<ExtractProgress>>,
             dictionary: Option<Arc<[u8]>>| {
                extract_set.spawn(async move {
                    if let Some(ref p) = progress {
                        let _ = p.send(ExtractProgress::Extracting {
                            path: box_path.clone(),
                        });
                    }

                    let result = extract_single_chunked_file_from_mmap(
                        mmap,
                        archive_offset,
                        &out_base,
                        &box_path,
                        &record,
                        blocks,
                        mode,
                        xattrs,
                        dictionary,
                    )
                    .await;

                    result.map(|r| (box_path, r, expected_hash))
                });
            };

        // Helper to spawn next extraction task (regular or chunked)
        let spawn_next = |extract_set: &mut tokio::task::JoinSet<_>,
                          files_iter: &mut std::vec::IntoIter<_>,
                          chunked_files_iter: &mut std::vec::IntoIter<_>,
                          mmap: Arc<MemoryMappedFile>,
                          out_base: PathBuf,
                          progress: Option<tokio::sync::mpsc::UnboundedSender<ExtractProgress>>,
                          dictionary: Option<Arc<[u8]>>|
         -> bool {
            if let Some((box_path, record, expected_hash, mode, xattrs)) = files_iter.next() {
                spawn_extract(
                    extract_set,
                    box_path,
                    record,
                    expected_hash,
                    mode,
                    xattrs,
                    mmap,
                    out_base,
                    progress,
                    dictionary,
                );
                true
            } else if let Some((box_path, record, expected_hash, mode, xattrs, blocks)) =
                chunked_files_iter.next()
            {
                spawn_extract_chunked(
                    extract_set,
                    box_path,
                    record,
                    expected_hash,
                    mode,
                    xattrs,
                    blocks,
                    mmap,
                    out_base,
                    progress,
                    dictionary,
                );
                true
            } else {
                false
            }
        };

        // Seed initial extraction tasks up to concurrency limit
        for _ in 0..concurrency {
            if !spawn_next(
                &mut extract_set,
                &mut files_iter,
                &mut chunked_files_iter,
                mmap.clone(),
                output_path.to_path_buf(),
                progress.clone(),
                dictionary.clone(),
            ) {
                break;
            }
        }

        // Process both extraction and validation results as they complete
        loop {
            // Check if we're done
            if extract_set.is_empty() && validate_set.is_empty() {
                break;
            }

            tokio::select! {
                // Handle extraction completion
                Some(result) = extract_set.join_next() => {
                    let result = result.map_err(|e| {
                        ExtractError::DecompressionFailed(
                            std::io::Error::other(e),
                            output_path.to_path_buf(),
                        )
                    })?;

                    let (path, extract_result, expected_hash) = result?;
                    stats += extract_result.stats;
                    files_extracted += 1;

                    if let Some(ref p) = progress {
                        let _ = p.send(ExtractProgress::Extracted {
                            path: path.clone(),
                            files_extracted,
                            total_files,
                        });
                    }

                    // Spawn validation task if checksum verification requested
                    if verify_checksums {
                        if let Some(expected) = expected_hash {
                            let out_path = extract_result.out_path;
                            validate_set.spawn_blocking(move || {
                                validate_file_checksum(&out_path, &expected)
                            });
                        }
                    }

                    // Spawn next extraction task if more files remain
                    spawn_next(
                        &mut extract_set,
                        &mut files_iter,
                        &mut chunked_files_iter,
                        mmap.clone(),
                        output_path.to_path_buf(),
                        progress.clone(),
                        dictionary.clone(),
                    );
                }

                // Handle validation completion
                Some(result) = validate_set.join_next(), if !validate_set.is_empty() => {
                    let result = result.map_err(|e| {
                        ExtractError::DecompressionFailed(
                            std::io::Error::other(e),
                            output_path.to_path_buf(),
                        )
                    })?;

                    if let Ok(matches) = result {
                        if !matches {
                            stats.checksum_failures += 1;
                        }
                    } else if let Err(e) = result {
                        // Log but don't fail extraction for validation errors
                        tracing::error!("Validation error: {}", e);
                        stats.checksum_failures += 1;
                    }
                }
            }
        }
        timing.decompress = decompress_start.elapsed();

        // Phase 3: Create symlinks (sequential)
        let symlinks_start = Instant::now();
        for (path, record) in symlinks {
            if let Record::Link(link) = &record {
                let link_path = output_path.join(path.to_path_buf());

                // Create parent directory if needed
                if let Some(parent) = link_path.parent() {
                    fs::create_dir_all(parent)
                        .await
                        .map_err(|e| ExtractError::CreateDirFailed(e, parent.to_path_buf()))?;
                }

                // Resolve target index to path and compute relative symlink target
                let target_path = self.core.path_for_index(link.target).ok_or_else(|| {
                    ExtractError::ResolveLinkFailed(
                        std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            format!("No path for target index: {}", link.target.get()),
                        ),
                        link.clone().into_owned(),
                    )
                })?;
                let target = self.compute_relative_symlink_target(&path, &target_path);

                #[cfg(unix)]
                {
                    tokio::fs::symlink(&target, &link_path).await.map_err(|e| {
                        ExtractError::CreateLinkFailed(e, link_path.clone(), target)
                    })?;
                }

                #[cfg(windows)]
                {
                    // On Windows, we need to know if it's a dir or file symlink
                    let is_dir = self
                        .resolve_link(link)
                        .map(|r| r.record.as_directory().is_some())
                        .unwrap_or(false);

                    if is_dir {
                        tokio::fs::symlink_dir(&target, &link_path)
                            .await
                            .map_err(|e| {
                                ExtractError::CreateLinkFailed(e, link_path.clone(), target)
                            })?;
                    } else {
                        tokio::fs::symlink_file(&target, &link_path)
                            .await
                            .map_err(|e| {
                                ExtractError::CreateLinkFailed(e, link_path.clone(), target)
                            })?;
                    }
                }

                stats.links_created += 1;

                if let Some(ref p) = progress {
                    let _ = p.send(ExtractProgress::LinkCreated { path });
                }
            }
        }
        timing.symlinks = symlinks_start.elapsed();

        if let Some(ref p) = progress {
            let _ = p.send(ExtractProgress::Finished);
        }

        stats.timing = timing;
        Ok(stats)
    }

    /// Validate all file checksums without extracting.
    pub async fn validate_all(&self) -> Result<ValidateStats, ExtractError> {
        let mut stats = ValidateStats::default();

        for item in self.core.iter() {
            if let Record::File(file) = item.record {
                stats.files_checked += 1;

                let expected_hash: [u8; 32] = match item
                    .record
                    .attr_value(self.metadata(), crate::attrs::BLAKE3)
                {
                    Some(AttrValue::U256(h)) => *h,
                    _ => {
                        stats.files_without_checksum += 1;
                        continue;
                    }
                };

                // Decompress to compute hash
                let mut hasher = blake3::Hasher::new();
                let segment = self
                    .memory_map(file)
                    .map_err(|e| ExtractError::VerificationFailed(e, item.path.to_path_buf()))?;
                let data = segment.as_slice().map_err(|e| {
                    ExtractError::VerificationFailed(
                        std::io::Error::other(e),
                        item.path.to_path_buf(),
                    )
                })?;
                // Decompress and hash using sans-IO
                let dict = self.core.dictionary();
                let decompressed =
                    crate::compression::decompress_bytes_sync(data, file.compression, dict)
                        .map_err(|e| {
                            ExtractError::VerificationFailed(e, item.path.to_path_buf())
                        })?;

                // Feed decompressed data to hasher
                hasher.update(&decompressed);

                let actual_hash = hasher.finalize();
                if actual_hash.as_bytes() != &expected_hash {
                    tracing::warn!(
                        "Checksum mismatch for {}: expected {}, got {}",
                        item.path,
                        hex::encode(expected_hash),
                        hex::encode(actual_hash.as_bytes())
                    );
                    stats.checksum_failures += 1;
                }
            }
        }

        Ok(stats)
    }

    /// Validate all file checksums in parallel without extracting.
    pub async fn validate_all_parallel(
        &self,
        concurrency: usize,
    ) -> Result<ValidateStats, ExtractError> {
        self.validate_all_parallel_with_progress(concurrency, None)
            .await
    }

    /// Validate all file checksums in parallel with progress reporting.
    pub async fn validate_all_parallel_with_progress(
        &self,
        concurrency: usize,
        progress: Option<tokio::sync::mpsc::UnboundedSender<ValidateProgress>>,
    ) -> Result<ValidateStats, ExtractError> {
        use std::sync::Arc;
        use tokio::sync::{Semaphore, mpsc};

        // Collect files with checksums
        let mut files = Vec::new();
        let mut files_without_checksum = 0u64;

        for item in self.core.iter() {
            if let Record::File(f) = item.record {
                match item
                    .record
                    .attr_value(self.metadata(), crate::attrs::BLAKE3)
                {
                    Some(AttrValue::U256(h)) => {
                        files.push((item.path.clone(), f.clone(), *h));
                    }
                    _ => {
                        files_without_checksum += 1;
                    }
                }
            }
        }

        let total_files = files.len() as u64;

        if let Some(ref p) = progress {
            let _ = p.send(ValidateProgress::Started { total_files });
        }

        let semaphore = Arc::new(Semaphore::new(concurrency));
        let (tx, mut rx) = mpsc::channel::<Result<(BoxPath, bool), ExtractError>>(concurrency * 2);

        // Open mmap once and share across all tasks
        let mmap: Arc<MemoryMappedFile> = MemoryMappedFile::builder(&self.path)
            .huge_pages(true)
            .open()
            .map_err(|e| {
                ExtractError::DecompressionFailed(std::io::Error::other(e), self.path.clone())
            })?
            .into();

        let archive_offset = self.core.offset;
        let dictionary: Option<Arc<[u8]>> =
            self.core.meta.dictionary.as_ref().map(|d| d.clone().into());

        for (box_path, record, expected_hash) in files {
            let tx = tx.clone();
            let progress = progress.clone();
            let semaphore = semaphore.clone();
            let mmap = mmap.clone();
            let dictionary = dictionary.clone();

            tokio::spawn(async move {
                let _permit = semaphore.acquire_owned().await.unwrap();

                if let Some(ref p) = progress {
                    let _ = p.send(ValidateProgress::Validating {
                        path: box_path.clone(),
                    });
                }

                let result = validate_single_file_from_mmap(
                    mmap,
                    archive_offset,
                    &box_path,
                    &record,
                    &expected_hash,
                    dictionary,
                )
                .await
                .map(|success| (box_path, success));

                let _ = tx.send(result).await;
            });
        }

        drop(tx);

        // Collect results
        let mut stats = ValidateStats {
            files_checked: 0,
            files_without_checksum,
            checksum_failures: 0,
        };

        while let Some(result) = rx.recv().await {
            let (path, success) = result?;
            stats.files_checked += 1;

            if !success {
                stats.checksum_failures += 1;
            }

            if let Some(ref p) = progress {
                let _ = p.send(ValidateProgress::Validated {
                    path,
                    files_checked: stats.files_checked,
                    total_files,
                    success,
                });
            }
        }

        if let Some(ref p) = progress {
            let _ = p.send(ValidateProgress::Finished);
        }

        Ok(stats)
    }

    pub fn resolve_link(&self, link: &LinkRecord<'_>) -> std::io::Result<RecordsItem<'_, 'static>> {
        let index = link.target;
        let record = self.core.record(index).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("No record for link target index: {}", index.get()),
            )
        })?;
        let path = self.core.path_for_index(index).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Could not find path for link target index: {}", index.get()),
            )
        })?;
        Ok(RecordsItem {
            index,
            path,
            record,
        })
    }

    /// Compute the relative path from a link's location to its target.
    ///
    /// Given the link's path and target's path, computes the relative symlink target
    /// (e.g., "../x86_64-unknown-linux-musl/libclang_rt.builtins.a").
    fn compute_relative_symlink_target(
        &self,
        link_path: &BoxPath<'_>,
        target_path: &BoxPath<'_>,
    ) -> PathBuf {
        let link_parent = link_path
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_default();
        let target = target_path.to_path_buf();

        // Use pathdiff to compute relative path, or fall back to target if it fails
        pathdiff::diff_paths(&target, &link_parent).unwrap_or(target)
    }

    pub async fn read_bytes(
        &self,
        record: &FileRecord<'_>,
    ) -> std::io::Result<tokio::io::Take<File>> {
        let mut file = OpenOptions::new().read(true).open(&self.path).await?;

        file.seek(SeekFrom::Start(self.core.offset + record.data.get()))
            .await?;
        Ok(file.take(record.length))
    }

    /// Memory-map the file and return a segment for the record's data.
    pub fn memory_map(&self, record: &FileRecord<'_>) -> std::io::Result<Segment> {
        let mmap = MemoryMappedFile::builder(&self.path)
            .huge_pages(true)
            .open()
            .map_err(std::io::Error::other)?;
        let offset = self.core.offset + record.data.get();
        Segment::new(mmap.into(), offset, record.length).map_err(std::io::Error::other)
    }

    /// Memory-map the file and return a segment for a chunked file record's data.
    pub fn memory_map_chunked(&self, record: &ChunkedFileRecord<'_>) -> std::io::Result<Segment> {
        let mmap = MemoryMappedFile::builder(&self.path)
            .huge_pages(true)
            .open()
            .map_err(std::io::Error::other)?;
        let offset = self.core.offset + record.data.get();
        Segment::new(mmap.into(), offset, record.length).map_err(std::io::Error::other)
    }

    /// Read a byte range from a chunked file.
    ///
    /// Returns decompressed bytes from `[offset..offset+len)`.
    /// This is the core random access method for chunked files.
    ///
    /// # Arguments
    /// * `record` - The chunked file record
    /// * `record_index` - The record's index in the archive
    /// * `offset` - Starting byte offset within the decompressed file
    /// * `len` - Number of bytes to read
    ///
    /// # Errors
    /// Returns an error if the range exceeds the file size or if decompression fails.
    pub async fn read_chunked_range(
        &self,
        record: &ChunkedFileRecord<'_>,
        record_index: RecordIndex,
        offset: u64,
        len: usize,
    ) -> std::io::Result<Vec<u8>> {
        // Validate range
        if offset + len as u64 > record.decompressed_length {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "read range [{}, {}) exceeds file size {}",
                    offset,
                    offset + len as u64,
                    record.decompressed_length
                ),
            ));
        }

        if len == 0 {
            return Ok(Vec::new());
        }

        // Find the starting block
        let Some((block_physical_offset, block_logical_offset)) =
            self.core.find_block(record_index, offset)
        else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "chunked file has no block FST entries",
            ));
        };

        // Memory-map the archive
        let segment = self.memory_map_chunked(record)?;
        let all_data = segment.as_slice().map_err(std::io::Error::other)?;

        let mut result = Vec::with_capacity(len);
        let mut remaining = len;
        let mut current_offset = offset;
        let mut current_block_logical = block_logical_offset;
        let mut current_block_physical = block_physical_offset;

        while remaining > 0 {
            // Calculate compressed block size from next block's offset or end of data
            let compressed_end = if let Some((_, next_physical)) =
                self.core.next_block(record_index, current_block_logical)
            {
                next_physical
            } else {
                record.data.get() + record.length
            };
            let compressed_size = (compressed_end - current_block_physical) as usize;

            // Get slice of compressed block data
            let block_data_offset = (current_block_physical - record.data.get()) as usize;
            let block_data = &all_data[block_data_offset..block_data_offset + compressed_size];

            // Decompress the block using sans-IO
            let dict = self.core.dictionary();
            let decompressed =
                crate::compression::decompress_bytes_sync(block_data, record.compression, dict)?;

            // Calculate slice within this block
            let start_in_block = (current_offset - current_block_logical) as usize;
            let available = decompressed.len() - start_in_block;
            let to_copy = remaining.min(available);

            result.extend_from_slice(&decompressed[start_in_block..start_in_block + to_copy]);
            remaining -= to_copy;
            current_offset += to_copy as u64;

            // Move to next block if needed
            if remaining > 0 {
                if let Some((next_logical, next_physical)) =
                    self.core.next_block(record_index, current_block_logical)
                {
                    current_block_logical = next_logical;
                    current_block_physical = next_physical;
                } else {
                    // No more blocks but still have bytes to read - shouldn't happen
                    // if the range validation was correct
                    break;
                }
            }
        }

        Ok(result)
    }

    /// Create a chunked file reader with seek support.
    ///
    /// Returns a reader that implements `AsyncRead` and `AsyncSeek` for
    /// random access to a chunked file's contents.
    pub fn chunked_reader<'a>(
        &'a self,
        record: &'a ChunkedFileRecord<'a>,
        record_index: RecordIndex,
    ) -> std::io::Result<ChunkedReader<'a>> {
        ChunkedReader::new(self, record, record_index)
    }

    /// Load a chunked file's entire contents into memory for slice access.
    ///
    /// This decompresses the entire file and returns a wrapper that implements
    /// `Deref<Target = [u8]>` for transparent slice access.
    pub async fn chunked_slice(
        &self,
        record: &ChunkedFileRecord<'_>,
        record_index: RecordIndex,
    ) -> std::io::Result<ChunkedSlice> {
        ChunkedSlice::new(self, record, record_index).await
    }

    async fn extract_inner(
        &self,
        path: &BoxPath<'_>,
        record: &Record<'_>,
        record_index: RecordIndex,
        output_path: &Path,
    ) -> Result<(), ExtractError> {
        match record {
            Record::File(file) => {
                fs::create_dir_all(output_path)
                    .await
                    .map_err(|e| ExtractError::CreateDirFailed(e, output_path.to_path_buf()))?;
                let out_path = output_path.join(path.to_path_buf());

                let out_file = fs::File::create(&out_path)
                    .await
                    .map_err(|e| ExtractError::CreateFileFailed(e, out_path.to_path_buf()))?;

                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let mode = self.get_mode(record);
                    let permissions = std::fs::Permissions::from_mode(mode);
                    fs::set_permissions(&out_path, permissions).await.ok();
                }

                let out_file = tokio::io::BufWriter::new(out_file);
                self.decompress(file, out_file)
                    .await
                    .map_err(|e| ExtractError::DecompressionFailed(e, path.to_path_buf()))?;

                Ok(())
            }
            Record::Directory(_dir) => {
                fs::create_dir_all(output_path)
                    .await
                    .map_err(|e| ExtractError::CreateDirFailed(e, output_path.to_path_buf()))?;
                let new_dir = output_path.join(path.to_path_buf());
                fs::create_dir(&new_dir)
                    .await
                    .map_err(|e| ExtractError::CreateDirFailed(e, new_dir))
            }
            #[cfg(unix)]
            Record::Link(link) => {
                let link_path = output_path.join(path.to_path_buf());

                // Create parent directory if needed
                if let Some(parent) = link_path.parent() {
                    fs::create_dir_all(parent)
                        .await
                        .map_err(|e| ExtractError::CreateDirFailed(e, parent.to_path_buf()))?;
                }

                // Resolve target index to path and compute relative symlink target
                let target_path = self.core.path_for_index(link.target).ok_or_else(|| {
                    ExtractError::ResolveLinkFailed(
                        std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            format!("No path for target index: {}", link.target.get()),
                        ),
                        link.clone().into_owned(),
                    )
                })?;
                let target = self.compute_relative_symlink_target(path, &target_path);

                tokio::fs::symlink(&target, &link_path)
                    .await
                    .map_err(|e| ExtractError::CreateLinkFailed(e, link_path, target))
            }
            #[cfg(windows)]
            Record::Link(link) => {
                let link_path = output_path.join(path.to_path_buf());

                // Create parent directory if needed
                if let Some(parent) = link_path.parent() {
                    fs::create_dir_all(parent)
                        .await
                        .map_err(|e| ExtractError::CreateDirFailed(e, parent.to_path_buf()))?;
                }

                // Resolve target index to path and compute relative symlink target
                let target_path = self.core.path_for_index(link.target).ok_or_else(|| {
                    ExtractError::ResolveLinkFailed(
                        std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            format!("No path for target index: {}", link.target.get()),
                        ),
                        link.clone().into_owned(),
                    )
                })?;
                let target = self.compute_relative_symlink_target(path, &target_path);

                // On Windows, we need to know if it's a dir or file symlink
                let is_dir = self
                    .resolve_link(link)
                    .map(|r| r.record.as_directory().is_some())
                    .unwrap_or(false);

                if is_dir {
                    tokio::fs::symlink_dir(&target, &link_path)
                        .await
                        .map_err(|e| ExtractError::CreateLinkFailed(e, link_path, target))
                } else {
                    tokio::fs::symlink_file(&target, &link_path)
                        .await
                        .map_err(|e| ExtractError::CreateLinkFailed(e, link_path, target))
                }
            }
            #[cfg(unix)]
            Record::ExternalLink(link) => {
                let link_path = output_path.join(path.to_path_buf());

                // Create parent directory if needed
                if let Some(parent) = link_path.parent() {
                    fs::create_dir_all(parent)
                        .await
                        .map_err(|e| ExtractError::CreateDirFailed(e, parent.to_path_buf()))?;
                }

                // External symlinks use the target path directly
                let target = PathBuf::from(link.target.as_ref());

                tokio::fs::symlink(&target, &link_path)
                    .await
                    .map_err(|e| ExtractError::CreateLinkFailed(e, link_path, target))
            }
            #[cfg(windows)]
            Record::ExternalLink(link) => {
                let link_path = output_path.join(path.to_path_buf());

                // Create parent directory if needed
                if let Some(parent) = link_path.parent() {
                    fs::create_dir_all(parent)
                        .await
                        .map_err(|e| ExtractError::CreateDirFailed(e, parent.to_path_buf()))?;
                }

                // External symlinks use the target path directly
                // On Windows, we default to file symlinks for external targets
                let target = PathBuf::from(link.target.as_ref());

                tokio::fs::symlink_file(&target, &link_path)
                    .await
                    .map_err(|e| ExtractError::CreateLinkFailed(e, link_path, target))
            }
            Record::ChunkedFile(file) => {
                fs::create_dir_all(output_path)
                    .await
                    .map_err(|e| ExtractError::CreateDirFailed(e, output_path.to_path_buf()))?;
                let out_path = output_path.join(path.to_path_buf());

                let out_file = fs::File::create(&out_path)
                    .await
                    .map_err(|e| ExtractError::CreateFileFailed(e, out_path.to_path_buf()))?;

                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let mode = self.get_mode(record);
                    let permissions = std::fs::Permissions::from_mode(mode);
                    fs::set_permissions(&out_path, permissions).await.ok();
                }

                let out_file = tokio::io::BufWriter::new(out_file);
                self.decompress_chunked(file, record_index, out_file)
                    .await
                    .map_err(|e| ExtractError::DecompressionFailed(e, path.to_path_buf()))?;

                Ok(())
            }
        }
    }

    async fn extract_inner_with_options(
        &self,
        path: &BoxPath<'_>,
        record: &Record<'_>,
        record_index: RecordIndex,
        output_path: &Path,
        options: &ExtractOptions,
        stats: &mut ExtractStats,
    ) -> Result<(), ExtractError> {
        match record {
            Record::File(file) => {
                fs::create_dir_all(output_path)
                    .await
                    .map_err(|e| ExtractError::CreateDirFailed(e, output_path.to_path_buf()))?;
                let out_path = output_path.join(path.to_path_buf());

                let out_file = fs::File::create(&out_path)
                    .await
                    .map_err(|e| ExtractError::CreateFileFailed(e, out_path.to_path_buf()))?;

                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let mode = self.get_mode(record);
                    let permissions = std::fs::Permissions::from_mode(mode);
                    fs::set_permissions(&out_path, permissions).await.ok();
                }

                let out_file = tokio::io::BufWriter::new(out_file);
                self.decompress(file, out_file)
                    .await
                    .map_err(|e| ExtractError::DecompressionFailed(e, path.to_path_buf()))?;

                stats.files_extracted += 1;
                stats.bytes_written += file.decompressed_length;

                // Verify checksum if requested
                if options.verify_checksums {
                    if let Some(AttrValue::U256(expected_hash)) =
                        record.attr_value(self.metadata(), crate::attrs::BLAKE3)
                    {
                        let actual_hash = compute_file_blake3(&out_path).await.map_err(|e| {
                            ExtractError::VerificationFailed(e, out_path.to_path_buf())
                        })?;

                        if actual_hash.as_bytes() != &*expected_hash {
                            tracing::warn!(
                                "Checksum mismatch for {}: expected {}, got {}",
                                path,
                                hex::encode(&*expected_hash),
                                hex::encode(actual_hash.as_bytes())
                            );
                            stats.checksum_failures += 1;
                        }
                    }
                }

                // Restore extended attributes if requested
                if options.xattrs {
                    let xattr_iter = record
                        .attrs_iter(self.metadata())
                        .filter(|(k, _)| k.starts_with(crate::attrs::LINUX_XATTR_PREFIX));
                    crate::fs::write_xattrs(&out_path, xattr_iter);
                }

                Ok(())
            }
            Record::Directory(_dir) => {
                fs::create_dir_all(output_path)
                    .await
                    .map_err(|e| ExtractError::CreateDirFailed(e, output_path.to_path_buf()))?;
                let new_dir = output_path.join(path.to_path_buf());
                fs::create_dir(&new_dir)
                    .await
                    .map_err(|e| ExtractError::CreateDirFailed(e, new_dir))?;
                stats.dirs_created += 1;
                Ok(())
            }
            #[cfg(unix)]
            Record::Link(link) => {
                let link_path = output_path.join(path.to_path_buf());

                // Create parent directory if needed
                if let Some(parent) = link_path.parent() {
                    fs::create_dir_all(parent)
                        .await
                        .map_err(|e| ExtractError::CreateDirFailed(e, parent.to_path_buf()))?;
                }

                // Resolve target index to path and compute relative symlink target
                let target_path = self.core.path_for_index(link.target).ok_or_else(|| {
                    ExtractError::ResolveLinkFailed(
                        std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            format!("No path for target index: {}", link.target.get()),
                        ),
                        link.clone().into_owned(),
                    )
                })?;
                let target = self.compute_relative_symlink_target(path, &target_path);

                tokio::fs::symlink(&target, &link_path)
                    .await
                    .map_err(|e| ExtractError::CreateLinkFailed(e, link_path, target))?;
                stats.links_created += 1;
                Ok(())
            }
            #[cfg(windows)]
            Record::Link(link) => {
                let link_path = output_path.join(path.to_path_buf());

                // Create parent directory if needed
                if let Some(parent) = link_path.parent() {
                    fs::create_dir_all(parent)
                        .await
                        .map_err(|e| ExtractError::CreateDirFailed(e, parent.to_path_buf()))?;
                }

                // Resolve target index to path and compute relative symlink target
                let target_path = self.core.path_for_index(link.target).ok_or_else(|| {
                    ExtractError::ResolveLinkFailed(
                        std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            format!("No path for target index: {}", link.target.get()),
                        ),
                        link.clone().into_owned(),
                    )
                })?;
                let target = self.compute_relative_symlink_target(path, &target_path);

                // On Windows, we need to know if it's a dir or file symlink
                let is_dir = self
                    .resolve_link(link)
                    .map(|r| r.record.as_directory().is_some())
                    .unwrap_or(false);

                if is_dir {
                    tokio::fs::symlink_dir(&target, &link_path)
                        .await
                        .map_err(|e| ExtractError::CreateLinkFailed(e, link_path, target))?;
                } else {
                    tokio::fs::symlink_file(&target, &link_path)
                        .await
                        .map_err(|e| ExtractError::CreateLinkFailed(e, link_path, target))?;
                }
                stats.links_created += 1;
                Ok(())
            }
            #[cfg(unix)]
            Record::ExternalLink(link) => {
                let link_path = output_path.join(path.to_path_buf());

                // Create parent directory if needed
                if let Some(parent) = link_path.parent() {
                    fs::create_dir_all(parent)
                        .await
                        .map_err(|e| ExtractError::CreateDirFailed(e, parent.to_path_buf()))?;
                }

                // External symlinks use the target path directly
                let target = PathBuf::from(link.target.as_ref());

                tokio::fs::symlink(&target, &link_path)
                    .await
                    .map_err(|e| ExtractError::CreateLinkFailed(e, link_path, target))?;
                stats.links_created += 1;
                Ok(())
            }
            #[cfg(windows)]
            Record::ExternalLink(link) => {
                let link_path = output_path.join(path.to_path_buf());

                // Create parent directory if needed
                if let Some(parent) = link_path.parent() {
                    fs::create_dir_all(parent)
                        .await
                        .map_err(|e| ExtractError::CreateDirFailed(e, parent.to_path_buf()))?;
                }

                // External symlinks use the target path directly
                // On Windows, we default to file symlinks for external targets
                let target = PathBuf::from(link.target.as_ref());

                tokio::fs::symlink_file(&target, &link_path)
                    .await
                    .map_err(|e| ExtractError::CreateLinkFailed(e, link_path, target))?;
                stats.links_created += 1;
                Ok(())
            }
            Record::ChunkedFile(file) => {
                fs::create_dir_all(output_path)
                    .await
                    .map_err(|e| ExtractError::CreateDirFailed(e, output_path.to_path_buf()))?;
                let out_path = output_path.join(path.to_path_buf());

                let out_file = fs::File::create(&out_path)
                    .await
                    .map_err(|e| ExtractError::CreateFileFailed(e, out_path.to_path_buf()))?;

                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let mode = self.get_mode(record);
                    let permissions = std::fs::Permissions::from_mode(mode);
                    fs::set_permissions(&out_path, permissions).await.ok();
                }

                let out_file = tokio::io::BufWriter::new(out_file);
                self.decompress_chunked(file, record_index, out_file)
                    .await
                    .map_err(|e| ExtractError::DecompressionFailed(e, path.to_path_buf()))?;

                stats.files_extracted += 1;
                stats.bytes_written += file.decompressed_length;

                // Verify checksum if requested
                if options.verify_checksums {
                    if let Some(AttrValue::U256(expected_hash)) =
                        record.attr_value(self.metadata(), crate::attrs::BLAKE3)
                    {
                        let actual_hash = compute_file_blake3(&out_path).await.map_err(|e| {
                            ExtractError::VerificationFailed(e, out_path.to_path_buf())
                        })?;

                        if actual_hash.as_bytes() != &*expected_hash {
                            tracing::warn!(
                                "Checksum mismatch for {}: expected {}, got {}",
                                path,
                                hex::encode(&*expected_hash),
                                hex::encode(actual_hash.as_bytes())
                            );
                            stats.checksum_failures += 1;
                        }
                    }
                }

                // Restore extended attributes if requested
                if options.xattrs {
                    let xattr_iter = record
                        .attrs_iter(self.metadata())
                        .filter(|(k, _)| k.starts_with(crate::attrs::LINUX_XATTR_PREFIX));
                    crate::fs::write_xattrs(&out_path, xattr_iter);
                }

                Ok(())
            }
        }
    }
}

// ============================================================================
// BLOCK CACHE
// ============================================================================

/// LRU cache for decompressed blocks.
///
/// Caches decompressed block data keyed by (record_index, block_logical_offset).
/// This significantly speeds up sequential reads and repeated access patterns.
pub struct BlockCache {
    cache: LruCache<(u64, u64), Box<[u8]>>,
}

impl BlockCache {
    /// Create a new block cache with the specified capacity.
    ///
    /// Capacity is the number of blocks to cache, not bytes.
    /// With 2MB blocks, 8 blocks = 16MB cache.
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: LruCache::new(
                std::num::NonZeroUsize::new(capacity).expect("capacity must be > 0"),
            ),
        }
    }

    /// Get a cached block if present.
    pub fn get(&mut self, record_index: u64, block_offset: u64) -> Option<&[u8]> {
        self.cache.get(&(record_index, block_offset)).map(|b| &**b)
    }

    /// Insert a decompressed block into the cache.
    pub fn insert(&mut self, record_index: u64, block_offset: u64, data: Box<[u8]>) {
        self.cache.put((record_index, block_offset), data);
    }

    /// Check if a block is in the cache without updating LRU order.
    #[allow(dead_code)]
    pub fn contains(&self, record_index: u64, block_offset: u64) -> bool {
        self.cache.contains(&(record_index, block_offset))
    }

    /// Clear all cached blocks.
    #[allow(dead_code)]
    pub fn clear(&mut self) {
        self.cache.clear();
    }

    /// Number of blocks currently cached.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Check if cache is empty.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }
}

impl Default for BlockCache {
    fn default() -> Self {
        // Default: 8 blocks (16MB with 2MB blocks)
        Self::new(8)
    }
}

// ============================================================================
// CHUNKED READER (AsyncRead + AsyncSeek)
// ============================================================================

/// Currently loaded block for the chunked reader.
struct CurrentBlock {
    /// Logical offset where this block starts
    logical_offset: u64,
    /// Decompressed block data
    data: Vec<u8>,
}

/// Async reader for chunked files with seek support.
///
/// Implements `AsyncRead` and `AsyncSeek` for random access to chunked file contents.
/// Includes an LRU block cache for efficient sequential and repeated access patterns.
///
/// Uses synchronous decompression (sans-IO) internally, making it suitable for
/// contexts where async runtimes are not available or blocking is acceptable.
///
/// # Example
/// ```ignore
/// let mut reader = bf.chunked_reader(&record, record_index)?;
/// let mut buf = vec![0u8; 1024];
/// reader.read(&mut buf).await?;
/// reader.seek(SeekFrom::Start(1000)).await?;
/// ```
pub struct ChunkedReader<'a> {
    reader: &'a BoxFileReader,
    record: &'a ChunkedFileRecord<'a>,
    record_index: RecordIndex,
    position: u64,
    cache: BlockCache,
    segment: Segment,
    blocks: Vec<(u64, u64)>,
    current_block: Option<CurrentBlock>,
}

impl<'a> ChunkedReader<'a> {
    /// Create a new chunked file reader.
    pub fn new(
        reader: &'a BoxFileReader,
        record: &'a ChunkedFileRecord<'a>,
        record_index: RecordIndex,
    ) -> std::io::Result<Self> {
        let segment = reader.memory_map_chunked(record)?;
        let blocks = reader.core.blocks_for_record(record_index);

        if blocks.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "chunked file has no block FST entries",
            ));
        }

        Ok(Self {
            reader,
            record,
            record_index,
            position: 0,
            cache: BlockCache::default(),
            segment,
            blocks,
            current_block: None,
        })
    }

    /// Create a new chunked file reader with a custom cache capacity.
    pub fn with_cache_capacity(
        reader: &'a BoxFileReader,
        record: &'a ChunkedFileRecord<'a>,
        record_index: RecordIndex,
        cache_capacity: usize,
    ) -> std::io::Result<Self> {
        let mut r = Self::new(reader, record, record_index)?;
        r.cache = BlockCache::new(cache_capacity);
        Ok(r)
    }

    /// Get the current position within the file.
    pub fn position(&self) -> u64 {
        self.position
    }

    /// Get the total decompressed file size.
    pub fn len(&self) -> u64 {
        self.record.decompressed_length
    }

    /// Check if the file is empty.
    pub fn is_empty(&self) -> bool {
        self.record.decompressed_length == 0
    }

    /// Get the block size used for this chunked file.
    pub fn block_size(&self) -> u32 {
        self.record.block_size
    }

    /// Read bytes at a specific offset without changing the reader's position.
    ///
    /// This is the primary random access method - like `pread(2)` or indexing a memory map.
    /// Uses the block cache for efficiency on repeated/nearby accesses.
    ///
    /// # Arguments
    /// * `offset` - Byte offset within the decompressed file
    /// * `buf` - Buffer to read into
    ///
    /// # Returns
    /// Number of bytes read (may be less than buf.len() at EOF)
    pub async fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() || offset >= self.record.decompressed_length {
            return Ok(0);
        }

        // Clamp read to file size
        let available = (self.record.decompressed_length - offset) as usize;
        let to_read = buf.len().min(available);
        let mut total_read = 0;
        let mut current_offset = offset;

        while total_read < to_read {
            // Find block containing current_offset
            let Some(block_idx) = find_block_index(&self.blocks, current_offset) else {
                break;
            };

            let (block_logical, block_physical) = self.blocks[block_idx];

            // Get decompressed block (from cache or decompress)
            let block_data = self
                .get_block(block_idx, block_logical, block_physical)
                .await?;

            // Calculate how much to copy from this block
            let offset_in_block = (current_offset - block_logical) as usize;
            let block_remaining = block_data.len() - offset_in_block;
            let copy_len = (to_read - total_read).min(block_remaining);

            buf[total_read..total_read + copy_len]
                .copy_from_slice(&block_data[offset_in_block..offset_in_block + copy_len]);

            total_read += copy_len;
            current_offset += copy_len as u64;
        }

        Ok(total_read)
    }

    /// Get a decompressed block, using cache if available.
    ///
    /// Uses synchronous decompression (sans-IO) for simplicity and portability.
    fn get_block_sync(
        &mut self,
        block_idx: usize,
        block_logical: u64,
        block_physical: u64,
    ) -> std::io::Result<Vec<u8>> {
        // Check cache first
        if let Some(cached) = self.cache.get(self.record_index.get(), block_logical) {
            return Ok(cached.to_vec());
        }

        // Extract compressed block data
        let all_data = self.segment.as_slice().map_err(std::io::Error::other)?;

        let compressed_end = if block_idx + 1 < self.blocks.len() {
            self.blocks[block_idx + 1].1
        } else {
            self.record.data.get() + self.record.length
        };

        let block_start = (block_physical - self.record.data.get()) as usize;
        let block_end = (compressed_end - self.record.data.get()) as usize;
        let block_data = &all_data[block_start..block_end];

        // Use sync decompression (sans-IO)
        let decompressed = crate::compression::decompress_bytes_sync(
            block_data,
            self.record.compression,
            self.reader.core.dictionary(),
        )?;

        // Cache it
        self.cache.insert(
            self.record_index.get(),
            block_logical,
            decompressed.clone().into_boxed_slice(),
        );

        Ok(decompressed)
    }

    /// Get a decompressed block, using cache if available (async wrapper).
    async fn get_block(
        &mut self,
        block_idx: usize,
        block_logical: u64,
        block_physical: u64,
    ) -> std::io::Result<Vec<u8>> {
        self.get_block_sync(block_idx, block_logical, block_physical)
    }
}

/// Find the block index that contains the given logical offset.
fn find_block_index(blocks: &[(u64, u64)], offset: u64) -> Option<usize> {
    // Binary search for the block containing this offset
    match blocks.binary_search_by(|(logical, _)| logical.cmp(&offset)) {
        Ok(idx) => Some(idx),
        Err(0) => None, // offset is before first block
        Err(idx) => Some(idx - 1),
    }
}

/// Read bytes from current block at the given position.
fn read_from_block(current_block: &Option<CurrentBlock>, position: u64, buf: &mut [u8]) -> usize {
    let Some(block) = current_block else {
        return 0;
    };

    // Check if current position is within this block
    if position < block.logical_offset {
        return 0;
    }

    let offset_in_block = (position - block.logical_offset) as usize;
    if offset_in_block >= block.data.len() {
        return 0;
    }

    let available = block.data.len() - offset_in_block;
    let to_copy = buf.len().min(available);

    buf[..to_copy].copy_from_slice(&block.data[offset_in_block..offset_in_block + to_copy]);
    to_copy
}

impl tokio::io::AsyncRead for ChunkedReader<'_> {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();

        // Check if we're at EOF
        if this.position >= this.record.decompressed_length {
            return Poll::Ready(Ok(()));
        }

        // Try to read from current block
        if this.current_block.is_some() {
            let slice = buf.initialize_unfilled();
            let n = read_from_block(&this.current_block, this.position, slice);
            if n > 0 {
                buf.advance(n);
                this.position += n as u64;
                return Poll::Ready(Ok(()));
            }
        }

        // Need new block
        let Some(block_idx) = find_block_index(&this.blocks, this.position) else {
            return Poll::Ready(Ok(())); // EOF
        };

        let (logical_offset, physical_offset) = this.blocks[block_idx];

        // Get decompressed block (from cache or decompress synchronously)
        let data = match this.get_block_sync(block_idx, logical_offset, physical_offset) {
            Ok(d) => d,
            Err(e) => return Poll::Ready(Err(e)),
        };

        this.current_block = Some(CurrentBlock {
            logical_offset,
            data,
        });

        // Read from the newly loaded block
        let slice = buf.initialize_unfilled();
        let n = read_from_block(&this.current_block, this.position, slice);
        buf.advance(n);
        this.position += n as u64;
        Poll::Ready(Ok(()))
    }
}

impl tokio::io::AsyncSeek for ChunkedReader<'_> {
    fn start_seek(self: Pin<&mut Self>, position: SeekFrom) -> std::io::Result<()> {
        let this = self.get_mut();

        let new_pos = match position {
            SeekFrom::Start(pos) => pos as i64,
            SeekFrom::End(offset) => this.record.decompressed_length as i64 + offset,
            SeekFrom::Current(offset) => this.position as i64 + offset,
        };

        if new_pos < 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "cannot seek to negative position",
            ));
        }

        let new_pos = new_pos as u64;
        if new_pos > this.record.decompressed_length {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "cannot seek past end of file ({} > {})",
                    new_pos, this.record.decompressed_length
                ),
            ));
        }

        this.position = new_pos;

        // Invalidate current block if position is outside it
        if let Some(block) = &this.current_block {
            let block_end = block.logical_offset + block.data.len() as u64;
            if new_pos < block.logical_offset || new_pos >= block_end {
                this.current_block = None;
            }
        }

        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<u64>> {
        Poll::Ready(Ok(self.get_mut().position))
    }
}

// ============================================================================
// CHUNKED SLICE (Deref to &[u8])
// ============================================================================

/// Transparent slice access to chunked file data.
///
/// This struct decompresses the entire chunked file into memory and provides
/// direct `&[u8]` access via `Deref`. Useful when you need to access the file
/// contents as a contiguous slice.
///
/// # Example
/// ```ignore
/// let slice = bf.chunked_slice(&record, record_index).await?;
/// let data: &[u8] = &*slice;
/// println!("First byte: {}", data[0]);
/// ```
pub struct ChunkedSlice {
    data: Box<[u8]>,
}

impl ChunkedSlice {
    /// Create a new ChunkedSlice by decompressing the entire chunked file.
    pub async fn new(
        reader: &BoxFileReader,
        record: &ChunkedFileRecord<'_>,
        record_index: RecordIndex,
    ) -> std::io::Result<Self> {
        let mut data = Vec::with_capacity(record.decompressed_length as usize);
        reader
            .decompress_chunked(record, record_index, &mut data)
            .await?;
        Ok(Self {
            data: data.into_boxed_slice(),
        })
    }

    /// Get the length of the decompressed data.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if the data is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Consume self and return the underlying boxed slice.
    pub fn into_boxed_slice(self) -> Box<[u8]> {
        self.data
    }

    /// Consume self and return the data as a Vec.
    pub fn into_vec(self) -> Vec<u8> {
        self.data.into_vec()
    }
}

impl Deref for ChunkedSlice {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl AsRef<[u8]> for ChunkedSlice {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl std::borrow::Borrow<[u8]> for ChunkedSlice {
    fn borrow(&self) -> &[u8] {
        &self.data
    }
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

/// Compute blake3 hash of a file on disk using mmap for better performance.
async fn compute_file_blake3(path: &Path) -> std::io::Result<blake3::Hash> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let mut hasher = blake3::Hasher::new();
        hasher.update_mmap(&path)?;
        Ok(hasher.finalize())
    })
    .await
    .map_err(|e| std::io::Error::other(e))?
}

/// Result of extracting a single file (without checksum verification).
struct ExtractFileResult {
    stats: ExtractStats,
    out_path: PathBuf,
}

/// Extract a single file from the archive using a shared mmap.
///
/// This is a standalone function so it can be spawned as a task.
/// Does NOT perform checksum verification - that happens separately in the validation pipeline.
#[allow(clippy::too_many_arguments)]
async fn extract_single_file_from_mmap(
    mmap: Arc<MemoryMappedFile>,
    archive_offset: u64,
    output_base: &Path,
    box_path: &BoxPath<'_>,
    record: &FileRecord<'_>,
    mode: u32,
    xattrs: Vec<(String, Vec<u8>)>,
    dictionary: Option<Arc<[u8]>>,
) -> Result<ExtractFileResult, ExtractError> {
    use tokio::io::AsyncWriteExt;

    let out_path = output_base.join(box_path.to_path_buf());

    // Ensure parent directory exists (may race with other tasks, but create_dir_all is safe)
    if let Some(parent) = out_path.parent() {
        fs::create_dir_all(parent)
            .await
            .map_err(|e| ExtractError::CreateDirFailed(e, parent.to_path_buf()))?;
    }

    // Create segment from shared mmap
    let offset = archive_offset + record.data.get();
    let segment = Segment::new(mmap, offset, record.length).map_err(|e| {
        ExtractError::DecompressionFailed(std::io::Error::other(e), box_path.to_path_buf())
    })?;
    let data = segment.as_slice().map_err(|e| {
        ExtractError::DecompressionFailed(std::io::Error::other(e), box_path.to_path_buf())
    })?;

    // Create output file
    let out_file = fs::File::create(&out_path)
        .await
        .map_err(|e| ExtractError::CreateFileFailed(e, out_path.to_path_buf()))?;

    // Size buffers based on file size, capped at 8MB
    const MAX_BUFFER_SIZE: usize = 8 * 1024 * 1024;
    let write_buf_size = (record.decompressed_length as usize).min(MAX_BUFFER_SIZE);

    let mut out_file = tokio::io::BufWriter::with_capacity(write_buf_size, out_file);

    // Decompress using sans-IO state machine with inline I/O
    match record.compression {
        Compression::Stored => {
            out_file
                .write_all(data)
                .await
                .map_err(|e| ExtractError::DecompressionFailed(e, box_path.to_path_buf()))?;
        }
        #[cfg(feature = "zstd")]
        Compression::Zstd => {
            use tokio::io::AsyncReadExt;
            let cursor = std::io::Cursor::new(data);
            let mut buf_reader = tokio::io::BufReader::new(cursor);
            let dict = dictionary.as_deref();
            let mut decompressor = match dict {
                Some(d) => ZstdDecompressor::with_dictionary(d)
                    .map_err(|e| ExtractError::DecompressionFailed(e, box_path.to_path_buf()))?,
                None => ZstdDecompressor::new()
                    .map_err(|e| ExtractError::DecompressionFailed(e, box_path.to_path_buf()))?,
            };
            let mut read_buf = vec![0u8; DEFAULT_BLOCK_SIZE as usize];
            let mut out_buf = vec![0u8; DEFAULT_BLOCK_SIZE as usize];

            loop {
                let n = buf_reader
                    .read(&mut read_buf)
                    .await
                    .map_err(|e| ExtractError::DecompressionFailed(e, box_path.to_path_buf()))?;
                if n == 0 {
                    break;
                }

                let mut in_pos = 0;
                while in_pos < n {
                    let status = decompressor
                        .decompress(&read_buf[in_pos..n], &mut out_buf)
                        .map_err(|e| {
                            ExtractError::DecompressionFailed(e, box_path.to_path_buf())
                        })?;
                    let consumed = status.bytes_consumed();
                    let produced = status.bytes_produced();
                    if produced > 0 {
                        out_file
                            .write_all(&out_buf[..produced])
                            .await
                            .map_err(|e| {
                                ExtractError::DecompressionFailed(e, box_path.to_path_buf())
                            })?;
                    }
                    in_pos += consumed;
                    if status.is_done() {
                        break;
                    }
                }
            }
        }
        #[cfg(feature = "xz")]
        Compression::Xz => {
            use tokio::io::AsyncReadExt;
            let cursor = std::io::Cursor::new(data);
            let mut buf_reader = tokio::io::BufReader::new(cursor);
            let mut decompressor = XzDecompressor::new()
                .map_err(|e| ExtractError::DecompressionFailed(e, box_path.to_path_buf()))?;
            let mut read_buf = vec![0u8; DEFAULT_BLOCK_SIZE as usize];
            let mut out_buf = vec![0u8; DEFAULT_BLOCK_SIZE as usize];

            loop {
                let n = buf_reader
                    .read(&mut read_buf)
                    .await
                    .map_err(|e| ExtractError::DecompressionFailed(e, box_path.to_path_buf()))?;
                if n == 0 {
                    break;
                }

                let mut in_pos = 0;
                while in_pos < n {
                    let status = decompressor
                        .decompress(&read_buf[in_pos..n], &mut out_buf)
                        .map_err(|e| {
                            ExtractError::DecompressionFailed(e, box_path.to_path_buf())
                        })?;
                    let consumed = status.bytes_consumed();
                    let produced = status.bytes_produced();
                    if produced > 0 {
                        out_file
                            .write_all(&out_buf[..produced])
                            .await
                            .map_err(|e| {
                                ExtractError::DecompressionFailed(e, box_path.to_path_buf())
                            })?;
                    }
                    in_pos += consumed;
                    if status.is_done() {
                        break;
                    }
                }
            }
        }
        Compression::Unknown(id) => {
            return Err(ExtractError::DecompressionFailed(
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Unknown compression ID: {}", id),
                ),
                box_path.to_path_buf(),
            ));
        }
    }

    out_file
        .flush()
        .await
        .map_err(|e| ExtractError::DecompressionFailed(e, box_path.to_path_buf()))?;

    // Set file permissions
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let permissions = std::fs::Permissions::from_mode(mode);
        fs::set_permissions(&out_path, permissions).await.ok();
    }
    #[cfg(not(unix))]
    let _ = mode;

    // Restore extended attributes
    if !xattrs.is_empty() {
        let xattr_iter = xattrs.iter().map(|(k, v)| (k.as_str(), v.as_slice()));
        crate::fs::write_xattrs(&out_path, xattr_iter);
    }

    let stats = ExtractStats {
        files_extracted: 1,
        bytes_written: record.decompressed_length,
        ..Default::default()
    };

    Ok(ExtractFileResult { stats, out_path })
}

/// Extract a single chunked file from memory-mapped archive data.
///
/// This is a standalone function so it can be spawned as a task.
/// Chunked files contain independently-compressed blocks that decompress sequentially.
/// Does NOT perform checksum verification - that happens separately in the validation pipeline.
#[allow(clippy::too_many_arguments)]
async fn extract_single_chunked_file_from_mmap(
    mmap: Arc<MemoryMappedFile>,
    archive_offset: u64,
    output_base: &Path,
    box_path: &BoxPath<'_>,
    record: &ChunkedFileRecord<'_>,
    blocks: Vec<(u64, u64)>,
    mode: u32,
    xattrs: Vec<(String, Vec<u8>)>,
    dictionary: Option<Arc<[u8]>>,
) -> Result<ExtractFileResult, ExtractError> {
    use tokio::io::AsyncWriteExt;

    if blocks.is_empty() {
        return Err(ExtractError::DecompressionFailed(
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "chunked file has no block FST entries",
            ),
            box_path.to_path_buf(),
        ));
    }

    let out_path = output_base.join(box_path.to_path_buf());

    // Ensure parent directory exists (may race with other tasks, but create_dir_all is safe)
    if let Some(parent) = out_path.parent() {
        fs::create_dir_all(parent)
            .await
            .map_err(|e| ExtractError::CreateDirFailed(e, parent.to_path_buf()))?;
    }

    // Create output file
    let out_file = fs::File::create(&out_path)
        .await
        .map_err(|e| ExtractError::CreateFileFailed(e, out_path.to_path_buf()))?;

    // Create segment for the entire chunked file data
    let offset = archive_offset + record.data.get();
    let segment = Segment::new(mmap, offset, record.length).map_err(|e| {
        ExtractError::DecompressionFailed(std::io::Error::other(e), box_path.to_path_buf())
    })?;
    let all_data = segment.as_slice().map_err(|e| {
        ExtractError::DecompressionFailed(std::io::Error::other(e), box_path.to_path_buf())
    })?;

    // Size buffer based on file size, capped at 8MB
    const MAX_BUFFER_SIZE: usize = 8 * 1024 * 1024;
    let write_buf_size = (record.decompressed_length as usize).min(MAX_BUFFER_SIZE);
    let mut out_file = tokio::io::BufWriter::with_capacity(write_buf_size, out_file);

    // Decompress each block separately
    for i in 0..blocks.len() {
        let (_logical_offset, physical_offset) = blocks[i];

        // Determine block's compressed size from next block's offset (or end of data)
        let compressed_end = if i + 1 < blocks.len() {
            blocks[i + 1].1 // Next block's physical offset
        } else {
            record.data.get() + record.length // End of all compressed data
        };
        let compressed_size = (compressed_end - physical_offset) as usize;

        // Get slice of compressed block data (relative to start of chunked file data)
        let block_offset = (physical_offset - record.data.get()) as usize;
        let block_data = &all_data[block_offset..block_offset + compressed_size];

        // Decompress this block using sans-IO
        let dict = dictionary.as_deref();
        let block_output =
            crate::compression::decompress_bytes_sync(block_data, record.compression, dict)
                .map_err(|e| ExtractError::DecompressionFailed(e, box_path.to_path_buf()))?;

        out_file
            .write_all(&block_output)
            .await
            .map_err(|e| ExtractError::DecompressionFailed(e, box_path.to_path_buf()))?;
    }

    out_file
        .flush()
        .await
        .map_err(|e| ExtractError::DecompressionFailed(e, box_path.to_path_buf()))?;

    // Set file permissions
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let permissions = std::fs::Permissions::from_mode(mode);
        fs::set_permissions(&out_path, permissions).await.ok();
    }
    #[cfg(not(unix))]
    let _ = mode;

    // Restore extended attributes
    if !xattrs.is_empty() {
        let xattr_iter = xattrs.iter().map(|(k, v)| (k.as_str(), v.as_slice()));
        crate::fs::write_xattrs(&out_path, xattr_iter);
    }

    let stats = ExtractStats {
        files_extracted: 1,
        bytes_written: record.decompressed_length,
        ..Default::default()
    };

    Ok(ExtractFileResult { stats, out_path })
}

/// Validate a file's checksum by reading it from disk and computing blake3 hash.
///
/// Uses blake3's mmap support for optimized hashing.
/// Returns true if checksum matches, false if mismatch.
fn validate_file_checksum(path: &Path, expected_hash: &[u8; 32]) -> Result<bool, ExtractError> {
    let mut hasher = blake3::Hasher::new();
    hasher
        .update_mmap(path)
        .map_err(|e| ExtractError::VerificationFailed(e, path.to_path_buf()))?;

    let actual_hash = hasher.finalize();
    let matches = actual_hash.as_bytes() == expected_hash;

    if !matches {
        tracing::warn!(
            "Checksum mismatch for {}: expected {}, got {}",
            path.display(),
            hex::encode(expected_hash),
            hex::encode(actual_hash.as_bytes())
        );
    }

    Ok(matches)
}

/// Validate a single file's checksum using a shared mmap.
///
/// Returns `true` if checksum matches, `false` if mismatch.
async fn validate_single_file_from_mmap(
    mmap: Arc<MemoryMappedFile>,
    archive_offset: u64,
    box_path: &BoxPath<'_>,
    record: &FileRecord<'_>,
    expected_hash: &[u8; 32],
    dictionary: Option<Arc<[u8]>>,
) -> Result<bool, ExtractError> {
    // Create segment from shared mmap
    let offset = archive_offset + record.data.get();
    let segment = Segment::new(mmap, offset, record.length).map_err(|e| {
        ExtractError::VerificationFailed(std::io::Error::other(e), box_path.to_path_buf())
    })?;
    let data = segment.as_slice().map_err(|e| {
        ExtractError::VerificationFailed(std::io::Error::other(e), box_path.to_path_buf())
    })?;

    // Decompress using sans-IO and hash the result
    let dict = dictionary.as_deref();
    let decompressed = crate::compression::decompress_bytes_sync(data, record.compression, dict)
        .map_err(|e| ExtractError::VerificationFailed(e, box_path.to_path_buf()))?;

    let mut hasher = blake3::Hasher::new();
    hasher.update(&decompressed);

    let actual_hash = hasher.finalize();
    let matches = actual_hash.as_bytes() == expected_hash;

    if !matches {
        tracing::warn!(
            "Checksum mismatch for {}: expected {}, got {}",
            box_path,
            hex::encode(expected_hash),
            hex::encode(actual_hash.as_bytes())
        );
    }

    Ok(matches)
}
