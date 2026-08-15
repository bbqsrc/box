use std::collections::HashSet;
use std::io::SeekFrom;
use std::num::NonZeroU64;
use std::ops::{AddAssign, Range};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mmap_io::MemoryMappedFile;
use mmap_io::segment::Segment;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, BufReader};

#[cfg(feature = "xz")]
use crate::compression::xz::XzDecompressor;
#[cfg(feature = "zstd")]
use crate::compression::zstd::ZstdDecompressor;
use crate::core::{ArchiveReader, AttrValue, BoxMetadata, RecordIndex, RecordsItem};
use crate::path::IntoBoxPathError;
use crate::{
    compression::{Compression, constants::DEFAULT_BLOCK_SIZE},
    de::deserialize_metadata_borrowed,
    header::BoxHeader,
    path::BoxPath,
    record::{ChunkedFileRecord, FileRecord, LinkRecord, Record},
};

#[cfg(test)]
use super::chunked::checked_seek_position;
use super::chunked::{ChunkedReader, ChunkedSlice};

/// Async reader for Box archives.
///
/// This is a frontend that wraps the sans-IO [`ArchiveReader`] core,
/// providing async I/O operations for reading archives.
// [spec:box:sem:async-io.root]
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

pub(super) fn invalid_chunked_data(message: impl Into<String>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message.into())
}

fn checked_archive_data_offset(archive_offset: u64, record_offset: u64) -> std::io::Result<u64> {
    archive_offset
        .checked_add(record_offset)
        .ok_or_else(|| invalid_chunked_data("archive and record data offsets overflow u64"))
}

pub(super) fn logical_file_buffer(length: u64) -> std::io::Result<Vec<u8>> {
    let capacity = usize::try_from(length).map_err(|_| {
        invalid_chunked_data(format!(
            "logical file length {length} does not fit in memory"
        ))
    })?;
    let mut buffer = Vec::new();
    buffer.try_reserve_exact(capacity).map_err(|error| {
        std::io::Error::new(
            std::io::ErrorKind::OutOfMemory,
            format!("cannot reserve {capacity} bytes for logical file data: {error}"),
        )
    })?;
    Ok(buffer)
}

pub(super) fn chunked_data_end(record: &ChunkedFileRecord<'_>) -> std::io::Result<u64> {
    record
        .data
        .get()
        .checked_add(record.length)
        .ok_or_else(|| invalid_chunked_data("chunked file data range overflows u64"))
}

/// Convert a pair of absolute block-FST offsets into a checked range within
/// the mapped record payload.
pub(super) fn chunked_block_data_range(
    record: &ChunkedFileRecord<'_>,
    mapped_len: usize,
    block_index: usize,
    physical_start: u64,
    physical_end: u64,
) -> std::io::Result<Range<usize>> {
    let data_start = record.data.get();
    let data_end = chunked_data_end(record)?;

    let relative_start = physical_start.checked_sub(data_start).ok_or_else(|| {
        invalid_chunked_data(format!(
            "chunked file block {block_index} starts before the record data"
        ))
    })?;
    let relative_end = physical_end.checked_sub(data_start).ok_or_else(|| {
        invalid_chunked_data(format!(
            "chunked file block {block_index} ends before the record data"
        ))
    })?;

    if physical_end > data_end || relative_end <= relative_start {
        return Err(invalid_chunked_data(format!(
            "chunked file block {block_index} has an invalid compressed-data range"
        )));
    }

    let relative_start = usize::try_from(relative_start).map_err(|_| {
        invalid_chunked_data(format!(
            "chunked file block {block_index} start does not fit in memory"
        ))
    })?;
    let relative_end = usize::try_from(relative_end).map_err(|_| {
        invalid_chunked_data(format!(
            "chunked file block {block_index} end does not fit in memory"
        ))
    })?;

    if relative_end > mapped_len {
        return Err(invalid_chunked_data(format!(
            "chunked file block {block_index} is outside the mapped record data"
        )));
    }

    Ok(relative_start..relative_end)
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
    let trailer_offset = checked_archive_data_offset(offset, ptr.get())?;
    reader.seek(SeekFrom::Start(trailer_offset)).await?;

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
    #[error("The Box header is valid, but it does not contain a metadata-trailer offset.")]
    MissingTrailer,

    #[error("The Box header is valid, but its metadata trailer could not be read.")]
    InvalidTrailer(#[source] std::io::Error),

    #[error("Could not read header. Is this a valid Box archive?")]
    MissingHeader(#[source] std::io::Error),

    #[error("Invalid path to Box file. Path: '{}'", .1.display())]
    InvalidPath(#[source] std::io::Error, PathBuf),

    #[error("Failed to read Box file. Path: '{}'", .1.display())]
    ReadFailed(#[source] std::io::Error, PathBuf),
}

impl OpenError {
    pub(crate) fn invalid_trailer_at(
        source: std::io::Error,
        version: u8,
        trailer_offset: u64,
        file_size: u64,
        parser_position: Option<usize>,
    ) -> Self {
        let trailer_len = file_size.saturating_sub(trailer_offset);
        let location = match parser_position {
            Some(position) => {
                let absolute_position = trailer_offset.saturating_add(position as u64);
                format!(
                    "Box format version {version} metadata trailer starts at file byte \
                     {trailer_offset} (0x{trailer_offset:x}) and has {trailer_len} bytes \
                     through EOF at byte {file_size}; parsing failed at trailer byte \
                     {position} (0x{position:x}), absolute file byte {absolute_position} \
                     (0x{absolute_position:x})"
                )
            }
            None if trailer_offset <= file_size => format!(
                "Box format version {version} metadata trailer starts at file byte \
                 {trailer_offset} (0x{trailer_offset:x}) and has {trailer_len} bytes \
                 through EOF at byte {file_size}"
            ),
            None => format!(
                "Box format version {version} header points to a metadata trailer at file byte \
                 {trailer_offset} (0x{trailer_offset:x}), beyond EOF at byte {file_size}"
            ),
        };

        Self::InvalidTrailer(crate::de::wrap_io_error(source, location))
    }

    pub(crate) fn invalid_trailer_pointer(
        source: std::io::Error,
        version: u8,
        archive_offset: u64,
        trailer_pointer: u64,
        file_size: u64,
    ) -> Self {
        Self::InvalidTrailer(crate::de::wrap_io_error(
            source,
            format!(
                "Box format version {version} header declares trailer offset {trailer_pointer} \
                 relative to archive base {archive_offset}; archive file size is {file_size} bytes"
            ),
        ))
    }

    /// Actionable help for the stage at which opening the archive failed.
    pub fn diagnostic_help(&self) -> &'static str {
        match self {
            Self::InvalidTrailer(_) => {
                "The Box header is valid. The archive is likely truncated or its metadata \
                 trailer is corrupt; use the record, field, and byte offsets above to locate \
                 the failure."
            }
            Self::MissingTrailer => {
                "The Box header is valid, but its trailer offset is zero. The archive may not \
                 have been finalized."
            }
            Self::MissingHeader(_) => {
                "No valid Box header was found. Check that this is a Box archive and that its \
                 32-byte header is complete."
            }
            Self::InvalidPath(_, _) => "Check that the archive path exists and can be resolved.",
            Self::ReadFailed(_, _) => {
                "Check that the archive file is readable and was not moved or replaced."
            }
        }
    }
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

    #[error("Archive hierarchy is invalid. Path: '{}'", .1.display())]
    InvalidArchiveHierarchy(#[source] std::io::Error, PathBuf),

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

enum ValidationRecord {
    File(FileRecord<'static>),
    Chunked {
        record: ChunkedFileRecord<'static>,
        blocks: Vec<(u64, u64)>,
    },
}

impl BoxFileReader {
    /// This will open an existing `.box` file for reading and error if the file is not valid.
    // [spec:box:sem:async-io.root.open]
    // [spec:box:req:wire.root.bounds]
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

        let trailer_offset = offset.checked_add(trailer_ptr.get()).ok_or_else(|| {
            OpenError::invalid_trailer_pointer(
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "archive base plus trailer offset overflows a 64-bit file position",
                ),
                header.version,
                offset,
                trailer_ptr.get(),
                file_size,
            )
        })?;
        if trailer_offset > file_size {
            return Err(OpenError::invalid_trailer_at(
                std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!(
                        "metadata trailer starts {} bytes beyond the end of the archive",
                        trailer_offset - file_size
                    ),
                ),
                header.version,
                trailer_offset,
                file_size,
                None,
            ));
        }
        let trailer_len = file_size - trailer_offset;

        let trailer_segment =
            Segment::new(mmap.into(), trailer_offset, trailer_len).map_err(|e| {
                OpenError::invalid_trailer_at(
                    std::io::Error::other(e),
                    header.version,
                    trailer_offset,
                    file_size,
                    None,
                )
            })?;

        let trailer_data = trailer_segment.as_slice().map_err(|e| {
            OpenError::invalid_trailer_at(
                std::io::Error::other(e),
                header.version,
                trailer_offset,
                file_size,
                None,
            )
        })?;

        // Deserialize with borrowed data from the mmap
        let mut pos = 0;
        let meta = deserialize_metadata_borrowed(trailer_data, &mut pos, header.version).map_err(
            |source| {
                OpenError::invalid_trailer_at(
                    source,
                    header.version,
                    trailer_offset,
                    file_size,
                    Some(pos),
                )
            },
        )?;

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

    // [spec:box:sem:async-io.root.read]
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
        dest.flush().await?;
        Ok(())
    }

    /// Decompress a chunked file by decompressing each block separately.
    ///
    /// Each block is independently compressed, so we must decompress them
    /// one at a time and concatenate the output.
    // [spec:box:sem:chunked-io.root.block-decompression]
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
        let data_end = chunked_data_end(record)?;
        let mut decompressed_length = 0u64;

        // Decompress each block
        for (i, &(logical_offset, physical_offset)) in blocks.iter().enumerate() {
            if logical_offset != decompressed_length {
                return Err(invalid_chunked_data(format!(
                    "chunked file block {i} starts at logical offset {logical_offset}, expected {decompressed_length}"
                )));
            }

            // Determine block's compressed size from next block's offset (or end of data)
            let compressed_end = if i + 1 < blocks.len() {
                blocks[i + 1].1 // Next block's physical offset
            } else {
                data_end
            };

            // Get slice of compressed block data (relative to start of chunked file data)
            let block_range = chunked_block_data_range(
                record,
                all_data.len(),
                i,
                physical_offset,
                compressed_end,
            )?;
            let block_data = all_data.get(block_range).ok_or_else(|| {
                invalid_chunked_data(format!(
                    "chunked file block {i} is outside the mapped record data"
                ))
            })?;

            // Decompress this block using the core's decompress method
            let block_output = self.core.decompress_chunked_block(record, block_data)?;
            let block_output_len = u64::try_from(block_output.len()).map_err(|_| {
                invalid_chunked_data(format!(
                    "chunked file block {i} decompressed length does not fit in u64"
                ))
            })?;
            decompressed_length = decompressed_length
                .checked_add(block_output_len)
                .ok_or_else(|| {
                    invalid_chunked_data("chunked file decompressed length overflows u64")
                })?;

            let expected_end = blocks
                .get(i + 1)
                .map(|(next_logical, _)| *next_logical)
                .unwrap_or(record.decompressed_length);
            if decompressed_length != expected_end {
                return Err(invalid_chunked_data(format!(
                    "chunked file block {i} ends at logical offset {decompressed_length}, expected {expected_end}"
                )));
            }

            // Write decompressed block to destination
            dest.write_all(&block_output).await?;
        }

        if decompressed_length != record.decompressed_length {
            return Err(invalid_chunked_data(format!(
                "chunked file decompressed to {decompressed_length} bytes, expected {}",
                record.decompressed_length
            )));
        }

        dest.flush().await?;
        Ok(())
    }

    pub fn find(&self, path: &BoxPath<'_>) -> Result<&Record<'static>, ExtractError> {
        self.core
            .find(path)
            .ok_or_else(|| ExtractError::NotFoundInArchive(path.to_path_buf()))
    }

    fn validate_materialization_path(
        &self,
        path: &BoxPath<'_>,
        index: RecordIndex,
    ) -> Result<(), ExtractError> {
        self.core
            .validate_extraction_path(path, index)
            .map_err(|error| ExtractError::InvalidArchiveHierarchy(error, path.to_path_buf()))
    }

    fn checked_link_target(&self, link: &LinkRecord<'_>) -> Result<BoxPath<'static>, ExtractError> {
        self.core
            .extraction_path_for_index(link.target)
            .map_err(|error| ExtractError::ResolveLinkFailed(error, link.clone().into_owned()))
    }

    // [spec:box:req:paths.root.extraction-gates+1]
    // [spec:box:req:extraction.root.safety-options]
    // [spec:box:sem:extraction.root.selection+2]
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
    // [spec:box:req:paths.root.extraction-gates+1]
    // [spec:box:req:extraction.root]
    // [spec:box:req:extraction.root.safety-options]
    // [spec:box:sem:extraction.root.selection+2]
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
    // [spec:box:req:paths.root.extraction-gates+1]
    // [spec:box:req:extraction.root.safety-options]
    // [spec:box:sem:extraction.root.selection+2]
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

        let mut pending = vec![(path.clone().into_owned(), index)];
        let mut visited = HashSet::new();
        while let Some((item_path, item_index)) = pending.pop() {
            if !visited.insert(item_index) {
                return Err(ExtractError::InvalidArchiveHierarchy(
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "record index {} appears more than once in the recursive hierarchy",
                            item_index.get()
                        ),
                    ),
                    item_path.to_path_buf(),
                ));
            }
            let record = self
                .core
                .record(item_index)
                .ok_or_else(|| ExtractError::NotFoundInArchive(item_path.to_path_buf()))?;
            let children = if matches!(record, Record::Directory(_)) {
                self.core
                    .extraction_children_by_index(item_index, &item_path)
                    .map_err(|error| {
                        ExtractError::InvalidArchiveHierarchy(error, item_path.to_path_buf())
                    })?
                    .into_iter()
                    .rev()
                    .collect()
            } else {
                Vec::new()
            };

            self.extract_inner_with_options(
                &item_path,
                record,
                item_index,
                output_path,
                &options,
                &mut stats,
            )
            .await?;
            pending.extend(children);
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
    // [spec:box:req:paths.root.extraction-gates+1]
    // [spec:box:req:checksums.root.verification]
    // [spec:box:sem:checksums.root.verification.extraction-statistics]
    // [spec:box:req:extraction.root.safety-options]
    // [spec:box:req:extraction.root.internal-symlink]
    // [spec:box:req:extraction.root.external-symlink]
    // [spec:box:sem:extraction.root.parallel-ordering]
    // [spec:box:req:extraction.root.checksum-verification]
    // [spec:box:sem:extraction.root.progress]
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
            self.validate_materialization_path(&item.path, item.index)?;
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

        let work_items = files.len().saturating_add(chunked_files.len());
        let concurrency = concurrency.max(1).min(work_items.max(1));
        let total_files = work_items as u64;
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

        // Create directories sequentially before dependent entries.
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

        // Extract files in parallel with pipelined validation.
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

                    if !result? {
                        stats.checksum_failures += 1;
                    }
                }
            }
        }
        timing.decompress = decompress_start.elapsed();

        // Create symlinks sequentially after their targets.
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
                let target_path = self.checked_link_target(link)?;
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
            } else if let Record::ExternalLink(link) = &record {
                // An external link points outside the archive — a package that
                // references another package's file. It is created with its
                // target as written, even when that target does not exist yet:
                // a dangling symlink is a valid filesystem object, and it
                // resolves once the package it points at is installed. Skipping
                // it here is how a package silently loses files it declared it
                // owned.
                let link_path = output_path.join(path.to_path_buf());

                if let Some(parent) = link_path.parent() {
                    fs::create_dir_all(parent)
                        .await
                        .map_err(|e| ExtractError::CreateDirFailed(e, parent.to_path_buf()))?;
                }

                let target = PathBuf::from(link.target.as_ref());
                #[cfg(unix)]
                tokio::fs::symlink(&target, &link_path)
                    .await
                    .map_err(|e| ExtractError::CreateLinkFailed(e, link_path.clone(), target))?;
                #[cfg(windows)]
                tokio::fs::symlink_file(&target, &link_path)
                    .await
                    .map_err(|e| ExtractError::CreateLinkFailed(e, link_path.clone(), target))?;

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
    // [spec:box:req:checksums.root.verification]
    // [spec:box:sem:checksums.root.verification.checksum-less]
    // [spec:box:req:validation.root]
    // [spec:box:sem:validation.root.payload-hash]
    // [spec:box:sem:validation.root.results]
    pub async fn validate_all(&self) -> Result<ValidateStats, ExtractError> {
        let mut stats = ValidateStats::default();

        for item in self.core.iter() {
            match item.record {
                Record::File(file) => {
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
                    let segment = self.memory_map(file).map_err(|e| {
                        ExtractError::VerificationFailed(e, item.path.to_path_buf())
                    })?;
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
                Record::ChunkedFile(file) => {
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

                    let segment = self.memory_map_chunked(file).map_err(|e| {
                        ExtractError::VerificationFailed(e, item.path.to_path_buf())
                    })?;
                    let data = segment.as_slice().map_err(|e| {
                        ExtractError::VerificationFailed(
                            std::io::Error::other(e),
                            item.path.to_path_buf(),
                        )
                    })?;
                    let blocks = self.core.blocks_for_record(item.index);
                    let matches = validate_chunked_file_data(
                        data,
                        &item.path,
                        file,
                        &blocks,
                        &expected_hash,
                        self.core.dictionary(),
                    )?;

                    if !matches {
                        stats.checksum_failures += 1;
                    }
                }
                _ => {}
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
    // [spec:box:req:checksums.root.verification]
    // [spec:box:sem:checksums.root.verification.checksum-less]
    // [spec:box:req:validation.root]
    // [spec:box:sem:validation.root.results]
    // [spec:box:sem:validation.root.parallel]
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
            let validation_record = match item.record {
                Record::File(record) => ValidationRecord::File(record.clone()),
                Record::ChunkedFile(record) => ValidationRecord::Chunked {
                    record: record.clone(),
                    blocks: self.core.blocks_for_record(item.index),
                },
                _ => continue,
            };

            match item
                .record
                .attr_value(self.metadata(), crate::attrs::BLAKE3)
            {
                Some(AttrValue::U256(h)) => {
                    files.push((item.path.clone(), validation_record, *h));
                }
                _ => {
                    files_without_checksum += 1;
                }
            }
        }

        let total_files = files.len() as u64;
        // Keep the library API total for computed or hostile worker counts:
        // Tokio rejects zero-capacity channels and semaphores above its limit.
        let concurrency = concurrency
            .max(1)
            .min(files.len().max(1))
            .min(Semaphore::MAX_PERMITS);

        if let Some(ref p) = progress {
            let _ = p.send(ValidateProgress::Started { total_files });
        }

        let semaphore = Arc::new(Semaphore::new(concurrency));
        let channel_capacity = concurrency.saturating_mul(2);
        let (tx, mut rx) = mpsc::channel::<Result<(BoxPath, bool), ExtractError>>(channel_capacity);

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

                let result = match record {
                    ValidationRecord::File(record) => {
                        validate_single_file_from_mmap(
                            mmap,
                            archive_offset,
                            &box_path,
                            &record,
                            &expected_hash,
                            dictionary,
                        )
                        .await
                    }
                    ValidationRecord::Chunked { record, blocks } => {
                        validate_single_chunked_file_from_mmap(
                            mmap,
                            archive_offset,
                            &box_path,
                            &record,
                            &blocks,
                            &expected_hash,
                            dictionary,
                        )
                        .await
                    }
                }
                .map(|success| (box_path, success));

                let _ = tx.send(result).await;
            });
        }

        drop(tx);

        // Collect results
        let mut stats = ValidateStats {
            // Keep the public counter consistent with sequential validation: it
            // includes file records skipped because they have no checksum.
            files_checked: files_without_checksum,
            files_without_checksum,
            checksum_failures: 0,
        };
        // Progress only covers checksum-bearing validation jobs, matching the
        // `total_files` announced above.
        let mut validations_completed = 0u64;

        while let Some(result) = rx.recv().await {
            let (path, success) = result?;
            stats.files_checked += 1;
            validations_completed += 1;

            if !success {
                stats.checksum_failures += 1;
            }

            if let Some(ref p) = progress {
                let _ = p.send(ValidateProgress::Validated {
                    path,
                    files_checked: validations_completed,
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

    // [spec:box:req:records.root.references.resolution]
    pub fn resolve_link(&self, link: &LinkRecord<'_>) -> std::io::Result<RecordsItem<'_, 'static>> {
        let index = link.target;
        let record = self.core.record(index).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("No record for link target index: {}", index.get()),
            )
        })?;
        let path = self.core.extraction_path_for_index(index)?;
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

    // [spec:box:sem:async-io.root.read]
    pub async fn read_bytes(
        &self,
        record: &FileRecord<'_>,
    ) -> std::io::Result<tokio::io::Take<File>> {
        let mut file = OpenOptions::new().read(true).open(&self.path).await?;

        let offset = checked_archive_data_offset(self.core.offset, record.data.get())?;
        file.seek(SeekFrom::Start(offset)).await?;
        Ok(file.take(record.length))
    }

    /// Memory-map the file and return a segment for the record's data.
    // [spec:box:sem:async-io.root.read]
    pub fn memory_map(&self, record: &FileRecord<'_>) -> std::io::Result<Segment> {
        let mmap = MemoryMappedFile::builder(&self.path)
            .huge_pages(true)
            .open()
            .map_err(std::io::Error::other)?;
        let offset = checked_archive_data_offset(self.core.offset, record.data.get())?;
        Segment::new(mmap.into(), offset, record.length).map_err(std::io::Error::other)
    }

    /// Memory-map the file and return a segment for a chunked file record's data.
    pub fn memory_map_chunked(&self, record: &ChunkedFileRecord<'_>) -> std::io::Result<Segment> {
        let mmap = MemoryMappedFile::builder(&self.path)
            .huge_pages(true)
            .open()
            .map_err(std::io::Error::other)?;
        let offset = checked_archive_data_offset(self.core.offset, record.data.get())?;
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
    // [spec:box:sem:chunked-io.root.async-range]
    pub async fn read_chunked_range(
        &self,
        record: &ChunkedFileRecord<'_>,
        record_index: RecordIndex,
        offset: u64,
        len: usize,
    ) -> std::io::Result<Vec<u8>> {
        // Validate range
        let len_u64 = u64::try_from(len).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "requested chunked-file range length does not fit in u64",
            )
        })?;
        let range_end = offset.checked_add(len_u64).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "requested chunked-file range overflows u64",
            )
        })?;
        if range_end > record.decompressed_length {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "read range [{}, {}) exceeds file size {}",
                    offset, range_end, record.decompressed_length
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
        let data_end = chunked_data_end(record)?;

        let mut result = Vec::new();
        result.try_reserve_exact(len).map_err(|error| {
            std::io::Error::new(
                std::io::ErrorKind::OutOfMemory,
                format!("cannot allocate chunked-file range buffer: {error}"),
            )
        })?;
        let mut remaining = len;
        let mut current_offset = offset;
        let mut current_block_logical = block_logical_offset;
        let mut current_block_physical = block_physical_offset;
        let mut block_index = self
            .core
            .blocks_for_record(record_index)
            .iter()
            .position(|entry| *entry == (block_logical_offset, block_physical_offset))
            .ok_or_else(|| invalid_chunked_data("starting block is not in the block FST"))?;

        while remaining > 0 {
            // Calculate compressed block size from next block's offset or end of data
            let next_block = self.core.next_block(record_index, current_block_logical);
            let compressed_end = next_block
                .map(|(_, next_physical)| next_physical)
                .unwrap_or(data_end);

            // Get slice of compressed block data
            let block_range = chunked_block_data_range(
                record,
                all_data.len(),
                block_index,
                current_block_physical,
                compressed_end,
            )?;
            let block_data = all_data.get(block_range).ok_or_else(|| {
                invalid_chunked_data(format!(
                    "chunked file block {block_index} is outside the mapped record data"
                ))
            })?;

            // Decompress the block using sans-IO
            let dict = self.core.dictionary();
            let decompressed =
                crate::compression::decompress_bytes_sync(block_data, record.compression, dict)?;

            // Calculate slice within this block
            let start_in_block = current_offset
                .checked_sub(current_block_logical)
                .and_then(|value| usize::try_from(value).ok())
                .ok_or_else(|| {
                    invalid_chunked_data(format!(
                        "requested offset is before chunked file block {block_index}"
                    ))
                })?;
            let available = decompressed
                .len()
                .checked_sub(start_in_block)
                .ok_or_else(|| {
                    invalid_chunked_data(format!(
                        "requested offset is outside chunked file block {block_index}"
                    ))
                })?;
            if available == 0 {
                return Err(invalid_chunked_data(format!(
                    "chunked file block {block_index} does not cover the requested range"
                )));
            }
            let to_copy = remaining.min(available);

            let block_slice = decompressed
                .get(start_in_block..start_in_block + to_copy)
                .ok_or_else(|| {
                    invalid_chunked_data(format!(
                        "requested range is outside chunked file block {block_index}"
                    ))
                })?;
            result.extend_from_slice(block_slice);
            remaining -= to_copy;
            current_offset = current_offset
                .checked_add(to_copy as u64)
                .ok_or_else(|| invalid_chunked_data("chunked-file read offset overflows u64"))?;

            // Move to next block if needed
            if remaining > 0 {
                if let Some((next_logical, next_physical)) = next_block {
                    if next_logical <= current_block_logical || next_logical != current_offset {
                        return Err(invalid_chunked_data(format!(
                            "chunked file block {} starts at logical offset {next_logical}, expected {current_offset}",
                            block_index + 1
                        )));
                    }
                    current_block_logical = next_logical;
                    current_block_physical = next_physical;
                    block_index += 1;
                } else {
                    return Err(invalid_chunked_data(
                        "chunked file ends before the requested logical range",
                    ));
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
        self.validate_materialization_path(path, record_index)?;
        match record {
            Record::File(file) => {
                let out_path = output_path.join(path.to_path_buf());
                if let Some(parent) = out_path.parent() {
                    fs::create_dir_all(parent)
                        .await
                        .map_err(|e| ExtractError::CreateDirFailed(e, parent.to_path_buf()))?;
                }

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
                let new_dir = output_path.join(path.to_path_buf());
                fs::create_dir_all(&new_dir)
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
                let target_path = self.checked_link_target(link)?;
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
                let target_path = self.checked_link_target(link)?;
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
                let out_path = output_path.join(path.to_path_buf());
                if let Some(parent) = out_path.parent() {
                    fs::create_dir_all(parent)
                        .await
                        .map_err(|e| ExtractError::CreateDirFailed(e, parent.to_path_buf()))?;
                }

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

    // [spec:box:req:checksums.root.verification]
    // [spec:box:sem:checksums.root.verification.extraction-statistics]
    // [spec:box:req:records.root.references.resolution]
    // [spec:box:req:extraction.root.materialization]
    // [spec:box:req:extraction.root.internal-symlink]
    // [spec:box:req:extraction.root.external-symlink]
    // [spec:box:req:extraction.root.checksum-verification]
    // [spec:box:sem:chunked-io.root.slice-extraction]
    async fn extract_inner_with_options(
        &self,
        path: &BoxPath<'_>,
        record: &Record<'_>,
        record_index: RecordIndex,
        output_path: &Path,
        options: &ExtractOptions,
        stats: &mut ExtractStats,
    ) -> Result<(), ExtractError> {
        self.validate_materialization_path(path, record_index)?;
        match record {
            Record::File(file) => {
                let out_path = output_path.join(path.to_path_buf());
                if let Some(parent) = out_path.parent() {
                    fs::create_dir_all(parent)
                        .await
                        .map_err(|e| ExtractError::CreateDirFailed(e, parent.to_path_buf()))?;
                }

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
                let new_dir = output_path.join(path.to_path_buf());
                fs::create_dir_all(&new_dir)
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
                let target_path = self.checked_link_target(link)?;
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
                let target_path = self.checked_link_target(link)?;
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
                let out_path = output_path.join(path.to_path_buf());
                if let Some(parent) = out_path.parent() {
                    fs::create_dir_all(parent)
                        .await
                        .map_err(|e| ExtractError::CreateDirFailed(e, parent.to_path_buf()))?;
                }

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
// [spec:box:req:extraction.root.materialization]
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
    let offset = archive_offset
        .checked_add(record.data.get())
        .ok_or_else(|| {
            ExtractError::DecompressionFailed(
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "file archive offset overflows u64",
                ),
                box_path.to_path_buf(),
            )
        })?;
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
// [spec:box:req:extraction.root.materialization]
// [spec:box:sem:chunked-io.root.slice-extraction]
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
    let offset = archive_offset
        .checked_add(record.data.get())
        .ok_or_else(|| {
            ExtractError::DecompressionFailed(
                invalid_chunked_data("chunked file archive offset overflows u64"),
                box_path.to_path_buf(),
            )
        })?;
    let segment = Segment::new(mmap, offset, record.length).map_err(|e| {
        ExtractError::DecompressionFailed(std::io::Error::other(e), box_path.to_path_buf())
    })?;
    let all_data = segment.as_slice().map_err(|e| {
        ExtractError::DecompressionFailed(std::io::Error::other(e), box_path.to_path_buf())
    })?;

    // Size buffer based on file size, capped at 8MB
    const MAX_BUFFER_SIZE: usize = 8 * 1024 * 1024;
    let write_buf_size = usize::try_from(record.decompressed_length)
        .unwrap_or(usize::MAX)
        .min(MAX_BUFFER_SIZE);
    let mut out_file = tokio::io::BufWriter::with_capacity(write_buf_size, out_file);
    let data_end = chunked_data_end(record)
        .map_err(|error| ExtractError::DecompressionFailed(error, box_path.to_path_buf()))?;
    let mut decompressed_length = 0u64;

    // Decompress each block separately
    for (i, &(logical_offset, physical_offset)) in blocks.iter().enumerate() {
        if logical_offset != decompressed_length {
            return Err(ExtractError::DecompressionFailed(
                invalid_chunked_data(format!(
                    "chunked file block {i} starts at logical offset {logical_offset}, expected {decompressed_length}"
                )),
                box_path.to_path_buf(),
            ));
        }

        // Determine block's compressed size from next block's offset (or end of data)
        let compressed_end = if i + 1 < blocks.len() {
            blocks[i + 1].1 // Next block's physical offset
        } else {
            data_end
        };

        // Get slice of compressed block data (relative to start of chunked file data)
        let block_range =
            chunked_block_data_range(record, all_data.len(), i, physical_offset, compressed_end)
                .map_err(|error| {
                    ExtractError::DecompressionFailed(error, box_path.to_path_buf())
                })?;
        let block_data = all_data.get(block_range).ok_or_else(|| {
            ExtractError::DecompressionFailed(
                invalid_chunked_data(format!(
                    "chunked file block {i} is outside the mapped record data"
                )),
                box_path.to_path_buf(),
            )
        })?;

        // Decompress this block using sans-IO
        let dict = dictionary.as_deref();
        let block_output =
            crate::compression::decompress_bytes_sync(block_data, record.compression, dict)
                .map_err(|e| ExtractError::DecompressionFailed(e, box_path.to_path_buf()))?;

        decompressed_length = decompressed_length
            .checked_add(block_output.len() as u64)
            .ok_or_else(|| {
                ExtractError::DecompressionFailed(
                    invalid_chunked_data("chunked file decompressed length overflows u64"),
                    box_path.to_path_buf(),
                )
            })?;
        let expected_end = blocks
            .get(i + 1)
            .map(|(next_logical, _)| *next_logical)
            .unwrap_or(record.decompressed_length);
        if decompressed_length != expected_end {
            return Err(ExtractError::DecompressionFailed(
                invalid_chunked_data(format!(
                    "chunked file block {i} ends at logical offset {decompressed_length}, expected {expected_end}"
                )),
                box_path.to_path_buf(),
            ));
        }

        out_file
            .write_all(&block_output)
            .await
            .map_err(|e| ExtractError::DecompressionFailed(e, box_path.to_path_buf()))?;
    }

    if decompressed_length != record.decompressed_length {
        return Err(ExtractError::DecompressionFailed(
            invalid_chunked_data(format!(
                "chunked file decompressed to {decompressed_length} bytes, expected {}",
                record.decompressed_length
            )),
            box_path.to_path_buf(),
        ));
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
// [spec:box:sem:validation.root.payload-hash]
// [spec:box:def:checksums.root.logical-content-domain]
async fn validate_single_file_from_mmap(
    mmap: Arc<MemoryMappedFile>,
    archive_offset: u64,
    box_path: &BoxPath<'_>,
    record: &FileRecord<'_>,
    expected_hash: &[u8; 32],
    dictionary: Option<Arc<[u8]>>,
) -> Result<bool, ExtractError> {
    // Create segment from shared mmap
    let offset = checked_archive_data_offset(archive_offset, record.data.get())
        .map_err(|error| ExtractError::VerificationFailed(error, box_path.to_path_buf()))?;
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

/// Validate a chunked file's checksum using a shared mmap.
///
/// Each independently-compressed block is fed to one hasher in logical order,
/// so the checksum covers the same bytes as the fully materialized file.
// [spec:box:req:checksums.root.verification]
// [spec:box:def:checksums.root.logical-content-domain]
// [spec:box:sem:validation.root.payload-hash]
async fn validate_single_chunked_file_from_mmap(
    mmap: Arc<MemoryMappedFile>,
    archive_offset: u64,
    box_path: &BoxPath<'_>,
    record: &ChunkedFileRecord<'_>,
    blocks: &[(u64, u64)],
    expected_hash: &[u8; 32],
    dictionary: Option<Arc<[u8]>>,
) -> Result<bool, ExtractError> {
    let offset = archive_offset
        .checked_add(record.data.get())
        .ok_or_else(|| {
            ExtractError::VerificationFailed(
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "chunked file data offset overflows u64",
                ),
                box_path.to_path_buf(),
            )
        })?;
    let segment = Segment::new(mmap, offset, record.length).map_err(|e| {
        ExtractError::VerificationFailed(std::io::Error::other(e), box_path.to_path_buf())
    })?;
    let data = segment.as_slice().map_err(|e| {
        ExtractError::VerificationFailed(std::io::Error::other(e), box_path.to_path_buf())
    })?;

    validate_chunked_file_data(
        data,
        box_path,
        record,
        blocks,
        expected_hash,
        dictionary.as_deref(),
    )
}

fn validate_chunked_file_data(
    data: &[u8],
    box_path: &BoxPath<'_>,
    record: &ChunkedFileRecord<'_>,
    blocks: &[(u64, u64)],
    expected_hash: &[u8; 32],
    dictionary: Option<&[u8]>,
) -> Result<bool, ExtractError> {
    let invalid_data = |message: String| {
        ExtractError::VerificationFailed(
            std::io::Error::new(std::io::ErrorKind::InvalidData, message),
            box_path.to_path_buf(),
        )
    };

    if blocks.is_empty() {
        return Err(invalid_data(
            "chunked file has no block FST entries".to_string(),
        ));
    }

    let data_start = record.data.get();
    let data_end = data_start
        .checked_add(record.length)
        .ok_or_else(|| invalid_data("chunked file data range overflows u64".to_string()))?;
    let mut hasher = blake3::Hasher::new();
    let mut decompressed_length = 0u64;

    for (index, &(logical_offset, physical_offset)) in blocks.iter().enumerate() {
        if logical_offset != decompressed_length {
            return Err(invalid_data(format!(
                "chunked file block {index} starts at logical offset {logical_offset}, expected {decompressed_length}"
            )));
        }

        let compressed_end = blocks
            .get(index + 1)
            .map(|(_, next_physical_offset)| *next_physical_offset)
            .unwrap_or(data_end);
        let block_range =
            chunked_block_data_range(record, data.len(), index, physical_offset, compressed_end)
                .map_err(|error| ExtractError::VerificationFailed(error, box_path.to_path_buf()))?;
        let block_data = data.get(block_range).ok_or_else(|| {
            invalid_data(format!(
                "chunked file block {index} is outside the mapped record data"
            ))
        })?;
        let block_output =
            crate::compression::decompress_bytes_sync(block_data, record.compression, dictionary)
                .map_err(|e| ExtractError::VerificationFailed(e, box_path.to_path_buf()))?;

        hasher.update(&block_output);
        decompressed_length = decompressed_length
            .checked_add(block_output.len() as u64)
            .ok_or_else(|| {
                invalid_data("chunked file decompressed length overflows u64".to_string())
            })?;
    }

    if decompressed_length != record.decompressed_length {
        return Err(invalid_data(format!(
            "chunked file decompressed to {decompressed_length} bytes, expected {}",
            record.decompressed_length
        )));
    }

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

#[cfg(test)]
mod chunked_metadata_tests {
    use std::borrow::Cow;

    use super::*;

    fn record(data_start: u64, length: u64) -> ChunkedFileRecord<'static> {
        ChunkedFileRecord {
            compression: Compression::Stored,
            block_size: 8,
            length,
            decompressed_length: 8,
            data: NonZeroU64::new(data_start).expect("test data offset is nonzero"),
            name: Cow::Borrowed("hostile.bin"),
            attrs: Default::default(),
        }
    }

    // [spec:box:sem:chunked-io.root.block-decompression/test/unit]
    #[test]
    fn hostile_block_ranges_return_errors() {
        let valid_record = record(100, 20);
        assert_eq!(
            chunked_block_data_range(&valid_record, 20, 0, 100, 110).unwrap(),
            0..10
        );

        for result in [
            chunked_block_data_range(&valid_record, 20, 0, 99, 105),
            chunked_block_data_range(&valid_record, 20, 0, 110, 105),
            chunked_block_data_range(&valid_record, 20, 0, 110, 121),
            chunked_block_data_range(&valid_record, 19, 0, 100, 120),
            chunked_block_data_range(&valid_record, 20, 0, 110, 110),
        ] {
            assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidData);
        }

        let overflowing_record = record(u64::MAX - 4, 10);
        assert_eq!(
            chunked_block_data_range(&overflowing_record, 10, 0, u64::MAX - 4, u64::MAX)
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::InvalidData
        );

        assert!(matches!(
            logical_file_buffer(u64::MAX).unwrap_err().kind(),
            std::io::ErrorKind::InvalidData | std::io::ErrorKind::OutOfMemory
        ));
    }

    // [spec:box:sem:async-io.root.read/test/unit]
    // [spec:box:sem:validation.root.payload-hash/test/unit]
    #[test]
    fn regular_and_chunked_offsets_reject_hostile_bounds() {
        assert_eq!(checked_archive_data_offset(7, 11).unwrap(), 18);
        assert_eq!(
            checked_archive_data_offset(1, u64::MAX).unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
        );

        let path = BoxPath::new("hostile.bin").unwrap();
        let expected = *blake3::hash(b"").as_bytes();
        let mut empty_record = record(100, 0);
        empty_record.decompressed_length = 0;

        for error in [
            validate_chunked_file_data(&[], &path, &empty_record, &[], &expected, None)
                .unwrap_err(),
            validate_chunked_file_data(&[], &path, &empty_record, &[(0, 100)], &expected, None)
                .unwrap_err(),
        ] {
            match error {
                ExtractError::VerificationFailed(source, _) => {
                    assert_eq!(source.kind(), std::io::ErrorKind::InvalidData)
                }
                other => panic!("expected verification error, got {other:?}"),
            }
        }

        let valid_record = record(100, 20);
        let error = validate_chunked_file_data(
            &[0; 20],
            &path,
            &valid_record,
            &[(0, 120)],
            &expected,
            None,
        )
        .unwrap_err();
        assert!(matches!(error, ExtractError::VerificationFailed(_, _)));
    }

    // [spec:box:sem:chunked-io.root.seek-reader/test/unit]
    #[test]
    fn seek_arithmetic_handles_u64_boundaries() {
        assert_eq!(
            checked_seek_position(SeekFrom::Start(u64::MAX), 0, u64::MAX).unwrap(),
            u64::MAX
        );
        assert_eq!(
            checked_seek_position(SeekFrom::End(-1), 0, u64::MAX).unwrap(),
            u64::MAX - 1
        );

        for error in [
            checked_seek_position(SeekFrom::End(1), 0, u64::MAX).unwrap_err(),
            checked_seek_position(SeekFrom::Current(i64::MAX), u64::MAX, u64::MAX).unwrap_err(),
            checked_seek_position(SeekFrom::Start(i64::MAX as u64 + 1), 0, i64::MAX as u64)
                .unwrap_err(),
            checked_seek_position(SeekFrom::End(i64::MIN), 0, 0).unwrap_err(),
        ] {
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        }
    }
}
