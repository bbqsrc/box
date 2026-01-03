use std::io::SeekFrom;
use std::num::NonZeroU64;
use std::ops::AddAssign;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mmap_io::MemoryMappedFile;
use mmap_io::segment::Segment;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, BufReader};

use super::meta::{AttrValue, Records};
use super::{BoxMetadata, meta::RecordsItem};
use crate::path::IntoBoxPathError;
use crate::{
    de::{DeserializeOwned, deserialize_metadata_borrowed},
    header::BoxHeader,
    path::BoxPath,
    record::{FileRecord, LinkRecord, Record},
};

pub struct BoxFileReader {
    pub(crate) path: PathBuf,
    pub(crate) header: BoxHeader,
    pub(crate) meta: BoxMetadata<'static>,
    pub(crate) offset: u64,
    /// Holds the mmapped trailer data. The Arc inside keeps the data alive.
    /// This must not be dropped before `meta` is dropped.
    #[allow(dead_code)]
    pub(crate) trailer_segment: Segment,
}

impl std::fmt::Debug for BoxFileReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoxFileReader")
            .field("path", &self.path)
            .field("header", &self.header)
            .field("meta", &self.meta)
            .field("offset", &self.offset)
            .finish_non_exhaustive()
    }
}

pub(super) async fn read_header<R: tokio::io::AsyncRead + tokio::io::AsyncSeek + Unpin + Send>(
    file: &mut R,
    offset: u64,
) -> std::io::Result<BoxHeader> {
    file.seek(SeekFrom::Start(offset)).await?;
    BoxHeader::deserialize_owned(file).await
}

pub(super) async fn read_trailer<R: tokio::io::AsyncRead + tokio::io::AsyncSeek + Unpin + Send>(
    reader: &mut R,
    ptr: NonZeroU64,
    offset: u64,
    version: u8,
) -> std::io::Result<BoxMetadata<'static>> {
    reader.seek(SeekFrom::Start(offset + ptr.get())).await?;
    crate::de::deserialize_metadata_owned(reader, version).await
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
}

impl Default for ExtractOptions {
    fn default() -> Self {
        Self {
            verify_checksums: true,
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
        let mmap = MemoryMappedFile::open_ro(&path)
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

        let f = BoxFileReader {
            path,
            header,
            meta,
            offset,
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
        self.header.alignment
    }

    #[inline(always)]
    pub fn version(&self) -> u8 {
        self.header.version
    }

    #[inline(always)]
    pub fn metadata(&self) -> &BoxMetadata<'static> {
        &self.meta
    }

    /// Get file-level attributes with type-aware parsing.
    pub fn file_attrs(&self) -> std::collections::BTreeMap<&str, AttrValue<'_>> {
        self.meta.file_attrs()
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
        if let Some(value) = record.attr(&self.meta, key) {
            return Some(value);
        }
        // Fall back to archive-level attr
        self.meta.file_attr(key).map(|v| v.as_slice())
    }

    /// Get the unix mode for a record, with fallback to defaults.
    ///
    /// Checks: record attr -> archive attr -> default (0o644 for files, 0o755 for dirs)
    #[cfg(unix)]
    pub fn get_mode(&self, record: &Record<'_>) -> u32 {
        if let Some(mode_bytes) = self.get_attr(record, "unix.mode") {
            let (mode, len) = fastvint::decode_vu32_slice(mode_bytes);
            if len > 0 {
                return mode;
            }
        }
        // Default based on record type
        match record {
            Record::Directory(_) => crate::fs::DEFAULT_DIR_MODE,
            _ => crate::fs::DEFAULT_FILE_MODE,
        }
    }

    pub async fn decompress<W: tokio::io::AsyncWrite + Unpin>(
        &self,
        record: &FileRecord<'_>,
        dest: W,
    ) -> std::io::Result<()> {
        let segment = self.memory_map(record)?;
        let data = segment.as_slice().map_err(std::io::Error::other)?;
        let cursor = std::io::Cursor::new(data);
        let buf_reader = tokio::io::BufReader::new(cursor);
        record.compression.decompress_write(buf_reader, dest).await
    }

    pub fn find(&self, path: &BoxPath<'_>) -> Result<&Record<'static>, ExtractError> {
        let record = self
            .meta
            .index(path)
            .and_then(|x| self.meta.record(x))
            .ok_or_else(|| ExtractError::NotFoundInArchive(path.to_path_buf()))?;
        Ok(record)
    }

    pub async fn extract<P: AsRef<Path>>(
        &self,
        path: &BoxPath<'_>,
        output_path: P,
    ) -> Result<(), ExtractError> {
        let output_path = output_path.as_ref();
        let record = self
            .meta
            .index(path)
            .and_then(|x| self.meta.record(x))
            .ok_or_else(|| ExtractError::NotFoundInArchive(path.to_path_buf()))?;
        self.extract_inner(path, record, output_path).await
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
        let output_path = output_path.as_ref();
        let mut stats = ExtractStats::default();
        let start = Instant::now();
        for item in self.meta.iter() {
            self.extract_inner_with_options(
                &item.path,
                item.record,
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
        let output_path = output_path.as_ref();
        let mut stats = ExtractStats::default();

        let index = self
            .meta
            .index(path)
            .ok_or_else(|| ExtractError::NotFoundInArchive(path.to_path_buf()))?;

        for item in Records::new(&self.meta, &[index], None) {
            self.extract_inner_with_options(
                &item.path,
                item.record,
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
        let output_path = output_path.as_ref();
        let mut timing = ExtractTiming::default();

        // Collect entries by type
        let collect_start = Instant::now();
        let mut directories = Vec::new();
        let mut files = Vec::new();
        let mut symlinks = Vec::new();

        for item in self.meta.iter() {
            match item.record {
                Record::Directory(_) => directories.push((item.path.clone(), item.record.clone())),
                Record::File(f) => {
                    let expected_hash = item
                        .record
                        .attr(self.metadata(), "blake3")
                        .map(|h| h.to_vec());
                    #[cfg(unix)]
                    let mode = self.get_mode(item.record);
                    #[cfg(not(unix))]
                    let mode = 0u32;
                    files.push((item.path.clone(), f.clone(), expected_hash, mode));
                }
                Record::Link(_) => symlinks.push((item.path.clone(), item.record.clone())),
            }
        }

        let total_files = files.len() as u64;
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
                let _ = record;
            }

            stats.dirs_created += 1;

            if let Some(ref p) = progress {
                let _ = p.send(ExtractProgress::DirectoryCreated { path });
            }
        }
        timing.directories = dirs_start.elapsed();

        // Phase 2: Extract files (parallel)
        let decompress_start = Instant::now();
        // Open mmap once and share across all tasks
        let mmap: Arc<MemoryMappedFile> = MemoryMappedFile::open_ro(&self.path)
            .map_err(|e| {
                ExtractError::DecompressionFailed(std::io::Error::other(e), self.path.clone())
            })?
            .into();

        let archive_offset = self.offset;
        let verify_checksums = options.verify_checksums;

        let mut join_set = tokio::task::JoinSet::new();
        let mut files_iter = files.into_iter();
        let mut files_extracted = 0u64;

        // Seed initial tasks up to concurrency limit
        for _ in 0..concurrency {
            if let Some((box_path, record, expected_hash, mode)) = files_iter.next() {
                let mmap = mmap.clone();
                let out_base = output_path.to_path_buf();
                let progress = progress.clone();

                join_set.spawn(async move {
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
                        verify_checksums,
                        expected_hash.as_deref(),
                        mode,
                    )
                    .await;

                    result.map(|s| (box_path, s))
                });
            }
        }

        // Process results as they complete, spawning new tasks to maintain concurrency
        while let Some(result) = join_set.join_next().await {
            let result = result.map_err(|e| {
                ExtractError::DecompressionFailed(
                    std::io::Error::other(e),
                    output_path.to_path_buf(),
                )
            })?;

            let (path, file_stats) = result?;
            stats += file_stats;
            files_extracted += 1;

            if let Some(ref p) = progress {
                let _ = p.send(ExtractProgress::Extracted {
                    path,
                    files_extracted,
                    total_files,
                });
            }

            // Spawn next task if more files remain
            if let Some((box_path, record, expected_hash, mode)) = files_iter.next() {
                let mmap = mmap.clone();
                let out_base = output_path.to_path_buf();
                let progress = progress.clone();

                join_set.spawn(async move {
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
                        verify_checksums,
                        expected_hash.as_deref(),
                        mode,
                    )
                    .await;

                    result.map(|s| (box_path, s))
                });
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
                let target_path = self.meta.path_for_index(link.target).ok_or_else(|| {
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

        for item in self.meta.iter() {
            if let Record::File(file) = item.record {
                stats.files_checked += 1;

                let expected_hash = item.record.attr(self.metadata(), "blake3");
                if expected_hash.is_none() {
                    stats.files_without_checksum += 1;
                    continue;
                }
                let expected_hash = expected_hash.unwrap();

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
                let cursor = std::io::Cursor::new(data);
                let buf_reader = tokio::io::BufReader::new(cursor);

                // Create a hashing writer that wraps a sink
                let mut hashing_sink = HashingSink::new(&mut hasher);
                file.compression
                    .decompress_write(buf_reader, &mut hashing_sink)
                    .await
                    .map_err(|e| ExtractError::VerificationFailed(e, item.path.to_path_buf()))?;

                let actual_hash = hasher.finalize();
                if actual_hash.as_bytes() != expected_hash {
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

        for item in self.meta.iter() {
            if let Record::File(f) = item.record {
                if let Some(expected_hash) = item.record.attr(self.metadata(), "blake3") {
                    files.push((item.path.clone(), f.clone(), expected_hash.to_vec()));
                } else {
                    files_without_checksum += 1;
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
        let mmap: Arc<MemoryMappedFile> = MemoryMappedFile::open_ro(&self.path)
            .map_err(|e| {
                ExtractError::DecompressionFailed(std::io::Error::other(e), self.path.clone())
            })?
            .into();

        let archive_offset = self.offset;

        for (box_path, record, expected_hash) in files {
            let tx = tx.clone();
            let progress = progress.clone();
            let semaphore = semaphore.clone();
            let mmap = mmap.clone();

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
        let record = self.meta.record(index).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("No record for link target index: {}", index.get()),
            )
        })?;
        let path = self.meta.path_for_index(index).ok_or_else(|| {
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

        file.seek(SeekFrom::Start(self.offset + record.data.get()))
            .await?;
        Ok(file.take(record.length))
    }

    /// Memory-map the file and return a segment for the record's data.
    pub fn memory_map(&self, record: &FileRecord<'_>) -> std::io::Result<Segment> {
        let mmap = MemoryMappedFile::open_ro(&self.path).map_err(std::io::Error::other)?;
        let offset = self.offset + record.data.get();
        Segment::new(mmap.into(), offset, record.length).map_err(std::io::Error::other)
    }

    async fn extract_inner(
        &self,
        path: &BoxPath<'_>,
        record: &Record<'_>,
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
                let target_path = self.meta.path_for_index(link.target).ok_or_else(|| {
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
                let target_path = self.meta.path_for_index(link.target).ok_or_else(|| {
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
        }
    }

    async fn extract_inner_with_options(
        &self,
        path: &BoxPath<'_>,
        record: &Record<'_>,
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
                if options.verify_checksums
                    && let Some(expected_hash) = record.attr(self.metadata(), "blake3")
                {
                    let actual_hash = compute_file_blake3(&out_path)
                        .await
                        .map_err(|e| ExtractError::VerificationFailed(e, out_path.to_path_buf()))?;

                    if actual_hash.as_bytes() != expected_hash {
                        tracing::warn!(
                            "Checksum mismatch for {}: expected {}, got {}",
                            path,
                            hex::encode(expected_hash),
                            hex::encode(actual_hash.as_bytes())
                        );
                        stats.checksum_failures += 1;
                    }
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
                let target_path = self.meta.path_for_index(link.target).ok_or_else(|| {
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
                let target_path = self.meta.path_for_index(link.target).ok_or_else(|| {
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
        }
    }
}

/// Compute blake3 hash of a file on disk.
async fn compute_file_blake3(path: &Path) -> std::io::Result<blake3::Hash> {
    let mut file = fs::File::open(path).await?;
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0u8; 64 * 1024];

    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }

    Ok(hasher.finalize())
}

/// A writer that computes a hash while writing to a sink.
struct HashingSink<'a> {
    hasher: &'a mut blake3::Hasher,
}

impl<'a> HashingSink<'a> {
    fn new(hasher: &'a mut blake3::Hasher) -> Self {
        Self { hasher }
    }
}

impl tokio::io::AsyncWrite for HashingSink<'_> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        self.hasher.update(buf);
        std::task::Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

/// Extract a single file from the archive using a shared mmap.
///
/// This is a standalone function so it can be spawned as a task.
#[allow(clippy::too_many_arguments)]
async fn extract_single_file_from_mmap(
    mmap: Arc<MemoryMappedFile>,
    archive_offset: u64,
    output_base: &Path,
    box_path: &BoxPath<'_>,
    record: &FileRecord<'_>,
    verify_checksum: bool,
    expected_hash: Option<&[u8]>,
    mode: u32,
) -> Result<ExtractStats, ExtractError> {
    use crate::hashing::HashingWriter;
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

    let cursor = std::io::Cursor::new(data);
    let buf_reader = tokio::io::BufReader::new(cursor);

    let mut stats = ExtractStats {
        files_extracted: 1,
        ..Default::default()
    };

    // Decompress with optional inline checksum verification
    if verify_checksum && expected_hash.is_some() {
        let out_file = tokio::io::BufWriter::new(out_file);
        let mut hashing_writer = HashingWriter::<_, blake3::Hasher>::new(out_file);

        record
            .compression
            .decompress_write(buf_reader, &mut hashing_writer)
            .await
            .map_err(|e| ExtractError::DecompressionFailed(e, box_path.to_path_buf()))?;

        // Flush the writer
        hashing_writer
            .flush()
            .await
            .map_err(|e| ExtractError::DecompressionFailed(e, box_path.to_path_buf()))?;

        stats.bytes_written = hashing_writer.bytes_written();

        // Verify checksum
        let actual_hash = hashing_writer.finalize_bytes();
        if let Some(expected) = expected_hash
            && actual_hash != expected
        {
            tracing::warn!(
                "Checksum mismatch for {}: expected {}, got {}",
                box_path,
                hex::encode(expected),
                hex::encode(&actual_hash)
            );
            stats.checksum_failures = 1;
        }
    } else {
        let mut out_file = tokio::io::BufWriter::new(out_file);

        record
            .compression
            .decompress_write(buf_reader, &mut out_file)
            .await
            .map_err(|e| ExtractError::DecompressionFailed(e, box_path.to_path_buf()))?;

        out_file
            .flush()
            .await
            .map_err(|e| ExtractError::DecompressionFailed(e, box_path.to_path_buf()))?;

        stats.bytes_written = record.decompressed_length;
    }

    // Set file permissions
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let permissions = std::fs::Permissions::from_mode(mode);
        fs::set_permissions(&out_path, permissions).await.ok();
    }

    #[cfg(not(unix))]
    let _ = mode;

    Ok(stats)
}

/// Validate a single file's checksum using a shared mmap.
///
/// Returns `true` if checksum matches, `false` if mismatch.
async fn validate_single_file_from_mmap(
    mmap: Arc<MemoryMappedFile>,
    archive_offset: u64,
    box_path: &BoxPath<'_>,
    record: &FileRecord<'_>,
    expected_hash: &[u8],
) -> Result<bool, ExtractError> {
    // Create segment from shared mmap
    let offset = archive_offset + record.data.get();
    let segment = Segment::new(mmap, offset, record.length).map_err(|e| {
        ExtractError::VerificationFailed(std::io::Error::other(e), box_path.to_path_buf())
    })?;
    let data = segment.as_slice().map_err(|e| {
        ExtractError::VerificationFailed(std::io::Error::other(e), box_path.to_path_buf())
    })?;

    let cursor = std::io::Cursor::new(data);
    let buf_reader = tokio::io::BufReader::new(cursor);

    // Decompress to a hashing sink (no disk writes)
    let mut hasher = blake3::Hasher::new();
    let mut hashing_sink = HashingSink::new(&mut hasher);

    record
        .compression
        .decompress_write(buf_reader, &mut hashing_sink)
        .await
        .map_err(|e| ExtractError::VerificationFailed(e, box_path.to_path_buf()))?;

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
