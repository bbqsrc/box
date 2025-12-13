use std::io::SeekFrom;
use std::num::NonZeroU64;
use std::ops::AddAssign;
use std::path::{Path, PathBuf};

use mmap_io::MemoryMappedFile;
use mmap_io::segment::Segment;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, BufReader};

use super::meta::Records;
use super::{BoxMetadata, meta::RecordsItem};
use crate::path::IntoBoxPathError;
use crate::{
    de::DeserializeOwned,
    header::BoxHeader,
    path::BoxPath,
    record::{FileRecord, LinkRecord, Record},
};

#[derive(Debug)]
pub struct BoxFileReader {
    pub(crate) path: PathBuf,
    pub(crate) header: BoxHeader,
    pub(crate) meta: BoxMetadata<'static>,
    pub(crate) offset: u64,
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
) -> std::io::Result<BoxMetadata<'static>> {
    reader.seek(SeekFrom::Start(offset + ptr.get())).await?;
    BoxMetadata::deserialize_owned(reader).await
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

    #[error("Resolving link failed: Path: '{}' -> '{}'", .1.name, .1.target)]
    ResolveLinkFailed(#[source] std::io::Error, LinkRecord<'static>),

    #[error("Could not convert to a valid Box path. Path suffix: '{}'", .1)]
    ResolveBoxPathFailed(#[source] IntoBoxPathError, String),

    #[error("Verification failed. Path: '{}'", .1.display())]
    VerificationFailed(#[source] std::io::Error, PathBuf),
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
}

impl AddAssign for ExtractStats {
    fn add_assign(&mut self, other: Self) {
        self.files_extracted += other.files_extracted;
        self.dirs_created += other.dirs_created;
        self.links_created += other.links_created;
        self.bytes_written += other.bytes_written;
        self.checksum_failures += other.checksum_failures;
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

        // Try to load the header so we can easily rewrite it when saving.
        // If header is invalid, we're not even loading a .box file.
        let (header, meta) = {
            let mut reader = BufReader::new(file);
            let header = read_header(&mut reader, offset)
                .await
                .map_err(OpenError::MissingHeader)?;
            let ptr = header.trailer.ok_or(OpenError::MissingTrailer)?;
            let meta = read_trailer(&mut reader, ptr, offset)
                .await
                .map_err(OpenError::InvalidTrailer)?;

            (header, meta)
        };

        let f = BoxFileReader {
            path,
            header,
            meta,
            offset,
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
    pub fn metadata(&self) -> &BoxMetadata {
        &self.meta
    }

    pub async fn decompress<W: tokio::io::AsyncWrite + Unpin>(
        &self,
        record: &FileRecord,
        dest: W,
    ) -> std::io::Result<()> {
        let segment = self.memory_map(record)?;
        let data = segment.as_slice().map_err(std::io::Error::other)?;
        let cursor = std::io::Cursor::new(data);
        let buf_reader = tokio::io::BufReader::new(cursor);
        record.compression.decompress_write(buf_reader, dest).await
    }

    pub fn find(&self, path: &BoxPath) -> Result<&Record, ExtractError> {
        let record = self
            .meta
            .index(path)
            .and_then(|x| self.meta.record(x))
            .ok_or_else(|| ExtractError::NotFoundInArchive(path.to_path_buf()))?;
        Ok(record)
    }

    pub async fn extract<P: AsRef<Path>>(
        &self,
        path: &BoxPath,
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
        path: &BoxPath,
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
        Ok(stats)
    }

    /// Extract a path and all children with options, returning extraction statistics.
    pub async fn extract_recursive_with_options<P: AsRef<Path>>(
        &self,
        path: &BoxPath,
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
        use std::sync::Arc;
        use tokio::sync::{Semaphore, mpsc};

        let output_path = output_path.as_ref();

        // Collect entries by type
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
                    files.push((item.path.clone(), f.clone(), expected_hash));
                }
                Record::Link(_) => symlinks.push((item.path.clone(), item.record.clone())),
            }
        }

        let total_files = files.len() as u64;
        let total_dirs = directories.len() as u64;
        let total_links = symlinks.len() as u64;

        if let Some(ref p) = progress {
            let _ = p.send(ExtractProgress::Started {
                total_files,
                total_dirs,
                total_links,
            });
        }

        let mut stats = ExtractStats::default();

        // Phase 1: Create directories (sequential)
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
                if let Some(mode_bytes) = record.attr(self.metadata(), "unix.mode") {
                    if mode_bytes.len() == 4 {
                        let mode = u32::from_le_bytes([
                            mode_bytes[0],
                            mode_bytes[1],
                            mode_bytes[2],
                            mode_bytes[3],
                        ]);
                        let permissions = std::fs::Permissions::from_mode(mode);
                        fs::set_permissions(&new_dir, permissions).await.ok();
                    }
                }
            }

            stats.dirs_created += 1;

            if let Some(ref p) = progress {
                let _ = p.send(ExtractProgress::DirectoryCreated { path });
            }
        }

        // Phase 2: Extract files (parallel)
        let semaphore = Arc::new(Semaphore::new(concurrency));
        let (tx, mut rx) =
            mpsc::channel::<Result<(BoxPath, ExtractStats), ExtractError>>(concurrency * 2);

        let archive_path = self.path.clone();
        let archive_offset = self.offset;
        let verify_checksums = options.verify_checksums;

        for (box_path, record, expected_hash) in files {
            let tx = tx.clone();
            let progress = progress.clone();
            let permit = semaphore.clone().acquire_owned().await.map_err(|e| {
                ExtractError::DecompressionFailed(
                    std::io::Error::new(std::io::ErrorKind::Other, e),
                    box_path.to_path_buf(),
                )
            })?;

            let archive_path = archive_path.clone();
            let out_base = output_path.to_path_buf();

            tokio::spawn(async move {
                let _permit = permit;

                if let Some(ref p) = progress {
                    let _ = p.send(ExtractProgress::Extracting {
                        path: box_path.clone(),
                    });
                }

                let result = extract_single_file(
                    &archive_path,
                    archive_offset,
                    &out_base,
                    &box_path,
                    &record,
                    verify_checksums,
                    expected_hash.as_deref(),
                )
                .await
                .map(|s| (box_path, s));

                let _ = tx.send(result).await;
            });
        }

        drop(tx);

        // Collect results
        let mut files_extracted = 0u64;
        while let Some(result) = rx.recv().await {
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
        }

        // Phase 3: Create symlinks (sequential)
        for (path, record) in symlinks {
            if let Record::Link(link) = &record {
                let link_target = self
                    .resolve_link(link)
                    .map_err(|e| ExtractError::ResolveLinkFailed(e, link.clone()))?;

                let source = output_path.join(path.to_path_buf());
                let destination = output_path.join(link_target.path.to_path_buf());

                #[cfg(unix)]
                {
                    tokio::fs::symlink(&destination, &source)
                        .await
                        .map_err(|e| {
                            ExtractError::CreateLinkFailed(e, source.clone(), destination)
                        })?;
                }

                #[cfg(windows)]
                {
                    if link_target.record.as_directory().is_some() {
                        tokio::fs::symlink_dir(&destination, &source)
                            .await
                            .map_err(|e| {
                                ExtractError::CreateLinkFailed(e, source.clone(), destination)
                            })?;
                    } else {
                        tokio::fs::symlink_file(&destination, &source)
                            .await
                            .map_err(|e| {
                                ExtractError::CreateLinkFailed(e, source.clone(), destination)
                            })?;
                    }
                }

                stats.links_created += 1;

                if let Some(ref p) = progress {
                    let _ = p.send(ExtractProgress::LinkCreated { path });
                }
            }
        }

        if let Some(ref p) = progress {
            let _ = p.send(ExtractProgress::Finished);
        }

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
                let expected_hash = item.record.attr(self.metadata(), "blake3");
                if expected_hash.is_none() {
                    files_without_checksum += 1;
                } else {
                    files.push((
                        item.path.clone(),
                        f.clone(),
                        expected_hash.unwrap().to_vec(),
                    ));
                }
            }
        }

        let total_files = files.len() as u64;

        if let Some(ref p) = progress {
            let _ = p.send(ValidateProgress::Started { total_files });
        }

        let semaphore = Arc::new(Semaphore::new(concurrency));
        let (tx, mut rx) = mpsc::channel::<Result<(BoxPath, bool), ExtractError>>(concurrency * 2);

        let archive_path = self.path.clone();
        let archive_offset = self.offset;

        for (box_path, record, expected_hash) in files {
            let tx = tx.clone();
            let progress = progress.clone();
            let semaphore = semaphore.clone();
            let archive_path = archive_path.clone();

            tokio::spawn(async move {
                let _permit = semaphore.acquire_owned().await.unwrap();

                if let Some(ref p) = progress {
                    let _ = p.send(ValidateProgress::Validating {
                        path: box_path.clone(),
                    });
                }

                let result = validate_single_file(
                    &archive_path,
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

    pub fn resolve_link(&self, link: &LinkRecord) -> std::io::Result<RecordsItem<'_>> {
        match self.meta.index(&link.target) {
            Some(index) => Ok(RecordsItem {
                index,
                path: link.target.to_owned(),
                record: self.meta.record(index).unwrap(),
            }),
            None => Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("No record for link target: {}", link.target),
            )),
        }
    }

    pub async fn read_bytes(&self, record: &FileRecord) -> std::io::Result<tokio::io::Take<File>> {
        let mut file = OpenOptions::new().read(true).open(&self.path).await?;

        file.seek(SeekFrom::Start(self.offset + record.data.get()))
            .await?;
        Ok(file.take(record.length))
    }

    /// Memory-map the file and return a segment for the record's data.
    pub fn memory_map(&self, record: &FileRecord) -> std::io::Result<Segment> {
        let mmap = MemoryMappedFile::open_ro(&self.path).map_err(std::io::Error::other)?;
        let offset = self.offset + record.data.get();
        Segment::new(mmap.into(), offset, record.length).map_err(std::io::Error::other)
    }

    async fn extract_inner(
        &self,
        path: &BoxPath,
        record: &Record,
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

                    let mode: Option<u32> = record
                        .attr(self.metadata(), "unix.mode")
                        .filter(|x| x.len() == 4)
                        .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]));

                    if let Some(mode) = mode {
                        let permissions = std::fs::Permissions::from_mode(mode);
                        fs::set_permissions(&out_path, permissions).await.ok();
                    }
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
                let link_target = self
                    .resolve_link(link)
                    .map_err(|e| ExtractError::ResolveLinkFailed(e, link.clone()))?;

                let source = output_path.join(path.to_path_buf());
                let destination = output_path.join(link_target.path.to_path_buf());

                tokio::fs::symlink(&source, &destination)
                    .await
                    .map_err(|e| ExtractError::CreateLinkFailed(e, source, destination))
            }
            #[cfg(windows)]
            Record::Link(link) => {
                let link_target = self
                    .resolve_link(link)
                    .map_err(|e| ExtractError::ResolveLinkFailed(e, link.clone()))?;

                let source = output_path.join(path.to_path_buf());
                let destination = output_path.join(link_target.path.to_path_buf());

                if link_target.record.as_directory().is_some() {
                    tokio::fs::symlink_dir(&source, &destination)
                        .await
                        .map_err(|e| ExtractError::CreateLinkFailed(e, source, destination))
                } else {
                    tokio::fs::symlink_file(&source, &destination)
                        .await
                        .map_err(|e| ExtractError::CreateLinkFailed(e, source, destination))
                }
            }
        }
    }

    async fn extract_inner_with_options(
        &self,
        path: &BoxPath,
        record: &Record,
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

                    let mode: Option<u32> = record
                        .attr(self.metadata(), "unix.mode")
                        .filter(|x| x.len() == 4)
                        .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]));

                    if let Some(mode) = mode {
                        let permissions = std::fs::Permissions::from_mode(mode);
                        fs::set_permissions(&out_path, permissions).await.ok();
                    }
                }

                let out_file = tokio::io::BufWriter::new(out_file);
                self.decompress(file, out_file)
                    .await
                    .map_err(|e| ExtractError::DecompressionFailed(e, path.to_path_buf()))?;

                stats.files_extracted += 1;
                stats.bytes_written += file.decompressed_length;

                // Verify checksum if requested
                if options.verify_checksums {
                    if let Some(expected_hash) = record.attr(self.metadata(), "blake3") {
                        let actual_hash = compute_file_blake3(&out_path).await.map_err(|e| {
                            ExtractError::VerificationFailed(e, out_path.to_path_buf())
                        })?;

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
                let link_target = self
                    .resolve_link(link)
                    .map_err(|e| ExtractError::ResolveLinkFailed(e, link.clone()))?;

                let source = output_path.join(path.to_path_buf());
                let destination = output_path.join(link_target.path.to_path_buf());

                tokio::fs::symlink(&source, &destination)
                    .await
                    .map_err(|e| ExtractError::CreateLinkFailed(e, source, destination))?;
                stats.links_created += 1;
                Ok(())
            }
            #[cfg(windows)]
            Record::Link(link) => {
                let link_target = self
                    .resolve_link(link)
                    .map_err(|e| ExtractError::ResolveLinkFailed(e, link.clone()))?;

                let source = output_path.join(path.to_path_buf());
                let destination = output_path.join(link_target.path.to_path_buf());

                if link_target.record.as_directory().is_some() {
                    tokio::fs::symlink_dir(&source, &destination)
                        .await
                        .map_err(|e| ExtractError::CreateLinkFailed(e, source, destination))?;
                } else {
                    tokio::fs::symlink_file(&source, &destination)
                        .await
                        .map_err(|e| ExtractError::CreateLinkFailed(e, source, destination))?;
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

/// Extract a single file from the archive.
///
/// This is a standalone function so it can be spawned as a task.
async fn extract_single_file(
    archive_path: &Path,
    archive_offset: u64,
    output_base: &Path,
    box_path: &BoxPath,
    record: &FileRecord,
    verify_checksum: bool,
    expected_hash: Option<&[u8]>,
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

    // Memory map the archive for this file's data
    let mmap = MemoryMappedFile::open_ro(archive_path).map_err(|e| {
        ExtractError::DecompressionFailed(std::io::Error::other(e), box_path.to_path_buf())
    })?;
    let offset = archive_offset + record.data.get();
    let segment = Segment::new(mmap.into(), offset, record.length).map_err(|e| {
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

    let mut stats = ExtractStats::default();
    stats.files_extracted = 1;

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
        if let Some(expected) = expected_hash {
            if actual_hash != expected {
                tracing::warn!(
                    "Checksum mismatch for {}: expected {}, got {}",
                    box_path,
                    hex::encode(expected),
                    hex::encode(&actual_hash)
                );
                stats.checksum_failures = 1;
            }
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

    Ok(stats)
}

/// Validate a single file's checksum without extracting.
///
/// Returns `true` if checksum matches, `false` if mismatch.
async fn validate_single_file(
    archive_path: &Path,
    archive_offset: u64,
    box_path: &BoxPath,
    record: &FileRecord,
    expected_hash: &[u8],
) -> Result<bool, ExtractError> {
    // Memory map the archive for this file's data
    let mmap = MemoryMappedFile::open_ro(archive_path).map_err(|e| {
        ExtractError::VerificationFailed(std::io::Error::other(e), box_path.to_path_buf())
    })?;
    let offset = archive_offset + record.data.get();
    let segment = Segment::new(mmap.into(), offset, record.length).map_err(|e| {
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
