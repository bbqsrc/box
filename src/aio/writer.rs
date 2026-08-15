use crate::compat::HashMap;
use std::borrow::Cow;
use std::default::Default;
use std::io::SeekFrom;
use std::num::NonZeroU64;
use std::ops::AddAssign;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::checksum::Checksum;
use async_walkdir::WalkDir;
use futures::StreamExt;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter};

#[cfg(feature = "xz")]
use crate::compression::xz::XzCompressor;
#[cfg(feature = "zstd")]
use crate::compression::zstd::ZstdCompressor;
use crate::{
    compression::{
        ByteCount, Compression, CompressionConfig, StreamStatus, constants::DEFAULT_BLOCK_SIZE,
    },
    core::{ArchiveWriter, AttrType, BoxMetadata, RecordIndex},
    hashing::HashingReader,
    header::BoxHeader,
    path::BoxPath,
    record::{
        ChunkedFileRecord, DirectoryRecord, ExternalLinkRecord, FileRecord, LinkRecord, Record,
    },
};

use super::reader::{read_header, read_trailer};

#[path = "prepared.rs"]
mod prepared;
#[cfg(feature = "zstd")]
use prepared::ZstdCompressorPool;
use prepared::{CompressedChunkedFile, PreparedFile};
pub use prepared::{CompressedData, CompressedFile, FileJob};

#[cfg(test)]
#[path = "writer_test_probe.rs"]
mod writer_test_probe;
#[cfg(test)]
use writer_test_probe::CompressionTestProbe;

/// 8MB buffer for efficient sequential writes
const WRITE_BUFFER_SIZE: usize = 8 * 1024 * 1024;

/// Async writer for Box archives.
///
/// This is a frontend that wraps the sans-IO [`ArchiveWriter`] core,
/// providing async I/O operations for writing archives.
// [spec:box:sem:async-io.root]
pub struct BoxFileWriter {
    /// The sans-IO core that manages archive metadata
    pub(crate) core: ArchiveWriter,
    /// File handle for writing
    pub(crate) file: BufWriter<File>,
    /// Path to the archive file
    pub(crate) path: PathBuf,
    /// Current file position (to avoid seek-induced buffer flushes)
    file_pos: u64,
    finished: bool,
    #[cfg(test)]
    compression_test_probe: Option<Arc<CompressionTestProbe>>,
}

// [spec:box:sem:async-io.root.writer-lifecycle]
impl Drop for BoxFileWriter {
    fn drop(&mut self) {
        if !self.finished {
            // Can't do async in Drop, so we warn if not finished
            tracing::warn!(
                "BoxFileWriter dropped without calling finish(). \
                 Archive at {:?} may be incomplete.",
                self.path
            );
        }
    }
}

impl BoxFileWriter {
    async fn write_header(&mut self) -> std::io::Result<()> {
        self.file.seek(SeekFrom::Start(0)).await?;
        // Encode header using core's encoder and write
        let buf = self.core.encode_header();
        self.file.write_all(&buf).await?;
        self.file_pos = BoxHeader::SIZE as u64;
        Ok(())
    }

    // [spec:box:sem:async-io.root.writer-lifecycle]
    async fn finish_inner(&mut self) -> std::io::Result<u64> {
        // Flush any buffered file data before seeking
        self.file.flush().await?;

        // Finalize the core (builds FSTs and encodes metadata)
        let (trailer_offset, meta_bytes) = self.core.finish()?;

        // Write the header (now includes trailer offset)
        self.write_header().await?;

        // write_header left us at header end, seek to trailer position
        self.file.seek(SeekFrom::Start(trailer_offset)).await?;
        self.file_pos = trailer_offset;

        // Write metadata bytes from core
        self.file.write_all(&meta_bytes).await?;

        self.file.flush().await?;

        let new_pos = self.file.get_ref().metadata().await?.len();
        self.file.get_mut().set_len(new_pos).await?;
        self.finished = true;
        Ok(new_pos)
    }

    pub async fn finish(mut self) -> std::io::Result<u64> {
        self.finish_inner().await
    }

    #[inline]
    fn next_write_addr(&self) -> NonZeroU64 {
        self.core.next_write_addr()
    }

    /// This will open an existing `.box` file for writing, and error if the file is not valid.
    // [spec:box:sem:async-io.root.writer-lifecycle]
    pub async fn open<P: AsRef<Path>>(path: P) -> std::io::Result<BoxFileWriter> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.as_ref())
            .await?;
        let file_len = file.metadata().await?.len();

        // Try to load the header so we can easily rewrite it when saving.
        // If header is invalid, we're not even loading a .box file.
        let mut reader = tokio::io::BufReader::new(file);
        let header = read_header(&mut reader, 0).await?;
        let ptr = header
            .trailer
            .ok_or_else(|| std::io::Error::other("no trailer found"))?;
        let meta = read_trailer(&mut reader, ptr, 0, header.version).await?;

        // Get the file back from the BufReader
        let mut file = reader.into_inner();

        // Compute next write position from existing records
        let next_write_pos = ArchiveWriter::existing_data_end_for_append(&header, &meta, file_len)?;

        // Create the core writer from existing header and metadata
        let core = ArchiveWriter::from_existing(header, meta, next_write_pos)?;
        let file_pos = core.next_write_addr().get();
        file.seek(SeekFrom::Start(file_pos)).await?;

        let f = BoxFileWriter {
            core,
            file: BufWriter::with_capacity(WRITE_BUFFER_SIZE, file),
            path: tokio::fs::canonicalize(path.as_ref()).await?,
            file_pos,
            finished: false,
            #[cfg(test)]
            compression_test_probe: None,
        };

        Ok(f)
    }

    /// This will create a new `.box` file for writing, and error if the file already exists.
    pub async fn create<P: AsRef<Path>>(path: P) -> std::io::Result<BoxFileWriter> {
        Self::create_inner(path, BoxHeader::default()).await
    }

    /// This will create a new `.box` file for reading and writing, and error if the file already exists.
    /// Will insert byte-aligned values based on provided `alignment` value. For best results, consider a power of 2.
    pub async fn create_with_alignment<P: AsRef<Path>>(
        path: P,
        alignment: u32,
    ) -> std::io::Result<BoxFileWriter> {
        Self::create_inner(path, BoxHeader::with_alignment(alignment)).await
    }

    /// This will create a new `.box` file that allows `\xNN` escape sequences in paths.
    /// Use this for archives that need to store systemd-style filenames.
    pub async fn create_with_escapes<P: AsRef<Path>>(path: P) -> std::io::Result<BoxFileWriter> {
        Self::create_inner(path, BoxHeader::with_escapes(true)).await
    }

    /// This will create a new `.box` file with custom alignment and escape settings.
    pub async fn create_with_options<P: AsRef<Path>>(
        path: P,
        alignment: u32,
        allow_escapes: bool,
        allow_external_symlinks: bool,
    ) -> std::io::Result<BoxFileWriter> {
        Self::create_inner(
            path,
            BoxHeader::with_options(alignment, allow_escapes, allow_external_symlinks),
        )
        .await
    }

    // [spec:box:sem:async-io.root.writer-lifecycle]
    async fn create_inner<P: AsRef<Path>>(
        path: P,
        header: BoxHeader,
    ) -> std::io::Result<BoxFileWriter> {
        let file = OpenOptions::new()
            .write(true)
            .read(true)
            .create_new(true)
            .open(path.as_ref())
            .await?;

        // Create core writer from header with empty metadata
        // For new archives, next write position is right after the header
        let core =
            ArchiveWriter::from_existing(header, BoxMetadata::default(), BoxHeader::SIZE as u64)?;

        let mut boxfile = BoxFileWriter {
            core,
            file: BufWriter::with_capacity(WRITE_BUFFER_SIZE, file),
            path: tokio::fs::canonicalize(path.as_ref()).await?,
            file_pos: 0, // Will be set by write_header
            finished: false,
            #[cfg(test)]
            compression_test_probe: None,
        };

        boxfile.write_header().await?;
        // file_pos is now header_size after write_header

        Ok(boxfile)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn alignment(&self) -> u32 {
        self.core.alignment()
    }

    pub fn version(&self) -> u8 {
        self.core.version()
    }

    /// Returns true if this archive allows `\xNN` escape sequences in paths.
    pub fn allow_escapes(&self) -> bool {
        self.core.allow_escapes()
    }

    /// Create a BoxPath using the appropriate sanitization for this archive.
    fn make_box_path<P: AsRef<Path>>(
        &self,
        path: P,
    ) -> std::result::Result<BoxPath<'static>, crate::path::IntoBoxPathError> {
        if self.core.allow_escapes() {
            BoxPath::new_with_escapes(path)
        } else {
            BoxPath::new(path)
        }
    }

    /// Will return the metadata for the `.box` if it has been provided.
    pub fn metadata(&self) -> &BoxMetadata<'static> {
        self.core.metadata()
    }

    fn iter(&self) -> crate::core::Records<'_, 'static> {
        crate::core::Records::new(self.metadata(), &self.metadata().root, None)
    }

    fn convert_attrs(
        &mut self,
        attrs_map: HashMap<String, Vec<u8>>,
    ) -> std::io::Result<HashMap<usize, Box<[u8]>>> {
        use crate::attrs;

        // Set archive-level uid/gid defaults from first file if not already set
        if let Some(uid) = attrs_map.get(attrs::UNIX_UID) {
            let uid_key = self
                .core
                .meta
                .attr_key_or_create(attrs::UNIX_UID, AttrType::Vu32)?;
            self.core
                .meta
                .attrs
                .entry(uid_key)
                .or_insert_with(|| uid.clone().into_boxed_slice());
        }
        if let Some(gid) = attrs_map.get(attrs::UNIX_GID) {
            let gid_key = self
                .core
                .meta
                .attr_key_or_create(attrs::UNIX_GID, AttrType::Vu32)?;
            self.core
                .meta
                .attrs
                .entry(gid_key)
                .or_insert_with(|| gid.clone().into_boxed_slice());
        }

        // Filter out uid/gid that match archive defaults (scoped to release borrows)
        let filtered: Vec<_> = {
            let default_uid = self
                .core
                .meta
                .attr_key(attrs::UNIX_UID)
                .and_then(|k| self.core.meta.attrs.get(&k).map(|v| &**v));
            let default_gid = self
                .core
                .meta
                .attr_key(attrs::UNIX_GID)
                .and_then(|k| self.core.meta.attrs.get(&k).map(|v| &**v));

            attrs_map
                .into_iter()
                .filter(|(k, v)| {
                    if k == attrs::UNIX_UID && default_uid.is_some_and(|d| v.as_slice() == d) {
                        return false;
                    }
                    if k == attrs::UNIX_GID && default_gid.is_some_and(|d| v.as_slice() == d) {
                        return false;
                    }
                    true
                })
                .collect()
        };

        // Convert keys (now safe to mutate self.meta)
        let mut result = HashMap::new();
        for (k, v) in filtered {
            let attr_type = match k.as_str() {
                attrs::UNIX_MODE | attrs::UNIX_UID | attrs::UNIX_GID => AttrType::Vu32,
                attrs::CREATED | attrs::MODIFIED | attrs::ACCESSED => AttrType::DateTime,
                attrs::CREATED_SECONDS | attrs::MODIFIED_SECONDS | attrs::ACCESSED_SECONDS => {
                    AttrType::U8
                }
                attrs::CREATED_NANOSECONDS
                | attrs::MODIFIED_NANOSECONDS
                | attrs::ACCESSED_NANOSECONDS => AttrType::Vu64,
                attrs::BLAKE3 => AttrType::U256,
                _ => AttrType::Bytes,
            };
            let key = self.core.meta.attr_key_or_create(&k, attr_type)?;
            result.insert(key, v.into_boxed_slice());
        }
        Ok(result)
    }

    fn insert_inner(
        &mut self,
        path: BoxPath<'_>,
        record: Record<'static>,
    ) -> std::io::Result<RecordIndex> {
        self.insert_inner_with_parent(path, record, None)
    }

    fn validate_parent(
        &self,
        path: &BoxPath<'_>,
        cached_parent: Option<RecordIndex>,
    ) -> std::io::Result<Option<RecordIndex>> {
        let Some(parent_path) = path.parent() else {
            return Ok(None);
        };

        let parent_index = match cached_parent {
            Some(index) => index,
            None => self.core.meta.index(&parent_path).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("No record found for path: {:?}", parent_path),
                )
            })?,
        };
        let parent_record = self.core.meta.record(parent_index).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Parent record index {} for path {:?} does not exist",
                    parent_index.get(),
                    parent_path
                ),
            )
        })?;
        if parent_record.as_directory().is_none() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Parent path {:?} is not a directory", parent_path),
            ));
        }

        Ok(Some(parent_index))
    }

    /// Insert a record with optional pre-computed parent index for O(1) lookup.
    // [spec:box:req:sans-io.root.hierarchy]
    fn insert_inner_with_parent(
        &mut self,
        path: BoxPath<'_>,
        record: Record<'static>,
        cached_parent: Option<RecordIndex>,
    ) -> std::io::Result<RecordIndex> {
        tracing::trace!("insert_inner path: {:?}", path);
        match self.validate_parent(&path, cached_parent)? {
            Some(parent_index) => {
                tracing::trace!(
                    "Inserting record into parent {:?}: {:?}",
                    &parent_index,
                    &record
                );

                let new_index = self.core.meta.insert_record(record);
                tracing::trace!("Inserted with index: {:?}", &new_index);
                let Some(parent) = self
                    .core
                    .meta
                    .record_mut(parent_index)
                    .and_then(Record::as_directory_mut)
                else {
                    self.core.meta.records.pop();
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "validated parent record changed during insertion",
                    ));
                };
                parent.entries.push(new_index);
                Ok(new_index)
            }
            None => {
                tracing::trace!("Inserting record into root: {:?}", &record);
                let new_index = self.core.meta.insert_record(record);
                self.core.meta.root.push(new_index);
                Ok(new_index)
            }
        }
    }

    pub fn mkdir(
        &mut self,
        path: BoxPath<'_>,
        attrs: HashMap<String, Vec<u8>>,
    ) -> std::io::Result<()> {
        tracing::trace!("mkdir: {}", path);

        let record = DirectoryRecord {
            name: Cow::Owned(path.filename().to_string()),
            entries: vec![],
            attrs: self.convert_attrs(attrs)?,
        };

        self.insert_inner(path, record.into())?;
        Ok(())
    }

    /// Create a directory and all its parent directories if they don't exist.
    pub fn mkdir_all(
        &mut self,
        path: BoxPath<'_>,
        attrs: HashMap<String, Vec<u8>>,
    ) -> std::io::Result<()> {
        // First ensure all parent directories exist
        if let Some(parent) = path.parent()
            && self.core.meta.index(&parent).is_none()
        {
            self.mkdir_all(parent.into_owned(), HashMap::new())?;
        }

        // Now create this directory if it doesn't exist. An existing
        // non-directory cannot satisfy mkdir_all.
        if let Some(index) = self.core.meta.index(&path) {
            if self
                .core
                .meta
                .record(index)
                .is_none_or(|record| record.as_directory().is_none())
            {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Path {:?} exists and is not a directory", path),
                ));
            }
        } else {
            self.mkdir(path, attrs)?;
        }

        Ok(())
    }

    // [spec:box:req:records.root.references.insertion-target]
    pub fn link(
        &mut self,
        path: BoxPath<'_>,
        target: RecordIndex,
        attrs: HashMap<String, Vec<u8>>,
    ) -> std::io::Result<RecordIndex> {
        // Validate that the target index exists
        if self.core.meta.record(target).is_none() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Symlink target index {} does not exist in archive",
                    target.get()
                ),
            ));
        }

        let record = LinkRecord {
            name: Cow::Owned(path.filename().to_string()),
            target,
            attrs: self.convert_attrs(attrs)?,
        };

        self.insert_inner(path, record.into())
    }

    /// Add an external symlink pointing outside the archive.
    ///
    /// The target path should be a relative path (e.g., "../../../etc/environment").
    /// This will set the `allow_external_symlinks` flag in the header.
    // [spec:box:req:records.root.references.external-header-flag]
    pub fn external_link(
        &mut self,
        path: BoxPath<'_>,
        target: &str,
        attrs: HashMap<String, Vec<u8>>,
    ) -> std::io::Result<RecordIndex> {
        // Mark that this archive contains external symlinks
        self.core.header.allow_external_symlinks = true;

        let record = ExternalLinkRecord {
            name: Cow::Owned(path.filename().to_string()),
            target: Cow::Owned(target.to_string()),
            attrs: self.convert_attrs(attrs)?,
        };

        self.insert_inner(path, record.into())
    }

    pub async fn insert<R: tokio::io::AsyncBufRead + Unpin>(
        &mut self,
        config: &CompressionConfig,
        path: BoxPath<'_>,
        value: R,
        attrs: HashMap<String, Vec<u8>>,
    ) -> std::io::Result<&FileRecord<'static>> {
        let attrs = self.convert_attrs(attrs)?;
        let next_addr = self.next_write_addr();
        let byte_count = self.write_data(config, next_addr.get(), value).await?;

        // Update cached write position
        self.core.advance_position(byte_count.write)?;

        let record = FileRecord {
            compression: config.compression,
            length: byte_count.write,
            decompressed_length: byte_count.read,
            name: Cow::Owned(path.filename().to_string()),
            data: next_addr,
            attrs,
        };

        let index = self.insert_inner(path, record.into())?;

        Ok(self.core.meta.record(index).unwrap().as_file().unwrap())
    }

    /// Insert a file with streaming compression and inline checksum computation.
    ///
    /// This method computes a hash of the uncompressed data while it streams through
    /// the compression pipeline, avoiding the need to buffer the entire file or
    /// read it twice.
    ///
    /// The checksum type `C` determines both the hash algorithm and the attribute name
    /// where the checksum is stored (via `C::NAME`). Use `NullChecksum` to skip checksums.
    // [spec:box:sem:async-io.root.streaming-insert]
    // [spec:box:req:checksums.root]
    // [spec:box:req:checksums.root.attachment]
    // [spec:box:req:checksums.root.disabled]
    pub async fn insert_streaming<R, C>(
        &mut self,
        config: &CompressionConfig,
        path: BoxPath<'static>,
        reader: R,
        attrs: HashMap<String, Vec<u8>>,
    ) -> std::io::Result<&FileRecord<'static>>
    where
        R: tokio::io::AsyncRead + Unpin,
        C: Checksum,
    {
        let attrs = self.convert_attrs(attrs)?;
        let next_addr = self.next_write_addr();

        // Wrap the reader in HashingReader to compute checksum while reading
        let hashing_reader = HashingReader::<_, C>::new(reader);
        // Wrap in BufReader so copy_buf will use poll_read (which does hashing)
        let buf_reader = BufReader::new(hashing_reader);

        let (byte_count, hash_bytes) = self
            .write_data_hashing::<_, C>(config, next_addr.get(), buf_reader)
            .await?;

        // Update cached write position
        self.core.advance_position(byte_count.write)?;

        let record = FileRecord {
            compression: config.compression,
            length: byte_count.write,
            decompressed_length: byte_count.read,
            name: Cow::Owned(path.filename().to_string()),
            data: next_addr,
            attrs,
        };

        let index = self.insert_inner(path.clone(), record.into())?;

        // Set checksum attribute if NAME is not empty
        if !C::NAME.is_empty() {
            let key = self.core.meta.attr_key_or_create(C::NAME, AttrType::U256)?;
            self.core
                .meta
                .record_mut(index)
                .unwrap()
                .attrs_mut()
                .insert(key, hash_bytes.into_boxed_slice());
        }

        Ok(self.core.meta.record(index).unwrap().as_file().unwrap())
    }

    async fn write_data<R: tokio::io::AsyncBufRead + Unpin>(
        &mut self,
        config: &CompressionConfig,
        pos: u64,
        mut reader: R,
    ) -> std::io::Result<ByteCount> {
        // Only seek if we're not already at the right position
        if self.file_pos != pos {
            self.file.seek(SeekFrom::Start(pos)).await?;
            self.file_pos = pos;
        }

        let byte_count = match config.compression {
            Compression::Stored => {
                // Direct copy
                let mut buf = vec![0u8; DEFAULT_BLOCK_SIZE as usize];
                let mut total_read = 0u64;
                let mut total_write = 0u64;
                loop {
                    let n = reader.read(&mut buf).await?;
                    if n == 0 {
                        break;
                    }
                    total_read += n as u64;
                    self.file.write_all(&buf[..n]).await?;
                    total_write += n as u64;
                }
                ByteCount {
                    read: total_read,
                    write: total_write,
                }
            }
            #[cfg(feature = "zstd")]
            Compression::Zstd => {
                let level = config
                    .get_i32("level")
                    .unwrap_or(zstd::DEFAULT_COMPRESSION_LEVEL);
                let mut compressor = match &config.dictionary {
                    Some(dict) => ZstdCompressor::with_dictionary(level, dict)?,
                    None => ZstdCompressor::new(level)?,
                };

                let mut in_buf = vec![0u8; DEFAULT_BLOCK_SIZE as usize];
                let mut out_buf = vec![0u8; zstd_safe::compress_bound(DEFAULT_BLOCK_SIZE as usize)];
                let mut total_read = 0u64;
                let mut total_write = 0u64;

                // Compress loop
                loop {
                    let n = reader.read(&mut in_buf).await?;
                    if n == 0 {
                        break;
                    }
                    total_read += n as u64;

                    let mut in_pos = 0;
                    while in_pos < n {
                        let status = compressor.compress(&in_buf[in_pos..n], &mut out_buf)?;
                        in_pos += status.bytes_consumed();
                        if status.bytes_produced() > 0 {
                            self.file
                                .write_all(&out_buf[..status.bytes_produced()])
                                .await?;
                            total_write += status.bytes_produced() as u64;
                        }
                    }
                }

                // Finish loop
                loop {
                    match compressor.finish(&mut out_buf)? {
                        StreamStatus::Done { bytes_produced, .. } => {
                            if bytes_produced > 0 {
                                self.file.write_all(&out_buf[..bytes_produced]).await?;
                                total_write += bytes_produced as u64;
                            }
                            break;
                        }
                        StreamStatus::Progress { bytes_produced, .. } => {
                            if bytes_produced > 0 {
                                self.file.write_all(&out_buf[..bytes_produced]).await?;
                                total_write += bytes_produced as u64;
                            }
                        }
                    }
                }

                ByteCount {
                    read: total_read,
                    write: total_write,
                }
            }
            #[cfg(feature = "xz")]
            Compression::Xz => {
                let level = config.get_i32("level").unwrap_or(6) as u32;
                let mut compressor = XzCompressor::new(level)?;

                let mut in_buf = vec![0u8; DEFAULT_BLOCK_SIZE as usize];
                let mut out_buf = vec![0u8; DEFAULT_BLOCK_SIZE as usize + 1024];
                let mut total_read = 0u64;
                let mut total_write = 0u64;

                // Compress loop
                loop {
                    let n = reader.read(&mut in_buf).await?;
                    if n == 0 {
                        break;
                    }
                    total_read += n as u64;

                    let mut in_pos = 0;
                    while in_pos < n {
                        let status = compressor.compress(&in_buf[in_pos..n], &mut out_buf)?;
                        in_pos += status.bytes_consumed();
                        if status.bytes_produced() > 0 {
                            self.file
                                .write_all(&out_buf[..status.bytes_produced()])
                                .await?;
                            total_write += status.bytes_produced() as u64;
                        }
                        if status.bytes_consumed() == 0 && status.bytes_produced() == 0 {
                            break;
                        }
                    }
                }

                // Finish loop
                loop {
                    match compressor.finish(&mut out_buf)? {
                        StreamStatus::Done { bytes_produced, .. } => {
                            if bytes_produced > 0 {
                                self.file.write_all(&out_buf[..bytes_produced]).await?;
                                total_write += bytes_produced as u64;
                            }
                            break;
                        }
                        StreamStatus::Progress { bytes_produced, .. } => {
                            if bytes_produced > 0 {
                                self.file.write_all(&out_buf[..bytes_produced]).await?;
                                total_write += bytes_produced as u64;
                            }
                        }
                    }
                }

                ByteCount {
                    read: total_read,
                    write: total_write,
                }
            }
            Compression::Unknown(id) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Unknown compression id: {}", id),
                ));
            }
        };

        self.file_pos += byte_count.write;
        Ok(byte_count)
    }

    async fn write_data_hashing<R, C>(
        &mut self,
        config: &CompressionConfig,
        pos: u64,
        mut reader: BufReader<HashingReader<R, C>>,
    ) -> std::io::Result<(ByteCount, Vec<u8>)>
    where
        R: tokio::io::AsyncRead + Unpin,
        C: Checksum,
    {
        // Only seek if we're not already at the right position
        if self.file_pos != pos {
            self.file.seek(SeekFrom::Start(pos)).await?;
            self.file_pos = pos;
        }

        let byte_count = match config.compression {
            Compression::Stored => {
                let mut buf = vec![0u8; DEFAULT_BLOCK_SIZE as usize];
                let mut total_read = 0u64;
                let mut total_write = 0u64;
                loop {
                    let n = reader.read(&mut buf).await?;
                    if n == 0 {
                        break;
                    }
                    total_read += n as u64;
                    self.file.write_all(&buf[..n]).await?;
                    total_write += n as u64;
                }
                ByteCount {
                    read: total_read,
                    write: total_write,
                }
            }
            #[cfg(feature = "zstd")]
            Compression::Zstd => {
                let level = config
                    .get_i32("level")
                    .unwrap_or(zstd::DEFAULT_COMPRESSION_LEVEL);
                let mut compressor = match &config.dictionary {
                    Some(dict) => ZstdCompressor::with_dictionary(level, dict)?,
                    None => ZstdCompressor::new(level)?,
                };

                let mut in_buf = vec![0u8; DEFAULT_BLOCK_SIZE as usize];
                let mut out_buf = vec![0u8; zstd_safe::compress_bound(DEFAULT_BLOCK_SIZE as usize)];
                let mut total_read = 0u64;
                let mut total_write = 0u64;

                loop {
                    let n = reader.read(&mut in_buf).await?;
                    if n == 0 {
                        break;
                    }
                    total_read += n as u64;

                    let mut in_pos = 0;
                    while in_pos < n {
                        let status = compressor.compress(&in_buf[in_pos..n], &mut out_buf)?;
                        in_pos += status.bytes_consumed();
                        if status.bytes_produced() > 0 {
                            self.file
                                .write_all(&out_buf[..status.bytes_produced()])
                                .await?;
                            total_write += status.bytes_produced() as u64;
                        }
                    }
                }

                loop {
                    match compressor.finish(&mut out_buf)? {
                        StreamStatus::Done { bytes_produced, .. } => {
                            if bytes_produced > 0 {
                                self.file.write_all(&out_buf[..bytes_produced]).await?;
                                total_write += bytes_produced as u64;
                            }
                            break;
                        }
                        StreamStatus::Progress { bytes_produced, .. } => {
                            if bytes_produced > 0 {
                                self.file.write_all(&out_buf[..bytes_produced]).await?;
                                total_write += bytes_produced as u64;
                            }
                        }
                    }
                }

                ByteCount {
                    read: total_read,
                    write: total_write,
                }
            }
            #[cfg(feature = "xz")]
            Compression::Xz => {
                let level = config.get_i32("level").unwrap_or(6) as u32;
                let mut compressor = XzCompressor::new(level)?;

                let mut in_buf = vec![0u8; DEFAULT_BLOCK_SIZE as usize];
                let mut out_buf = vec![0u8; DEFAULT_BLOCK_SIZE as usize + 1024];
                let mut total_read = 0u64;
                let mut total_write = 0u64;

                loop {
                    let n = reader.read(&mut in_buf).await?;
                    if n == 0 {
                        break;
                    }
                    total_read += n as u64;

                    let mut in_pos = 0;
                    while in_pos < n {
                        let status = compressor.compress(&in_buf[in_pos..n], &mut out_buf)?;
                        in_pos += status.bytes_consumed();
                        if status.bytes_produced() > 0 {
                            self.file
                                .write_all(&out_buf[..status.bytes_produced()])
                                .await?;
                            total_write += status.bytes_produced() as u64;
                        }
                        if status.bytes_consumed() == 0 && status.bytes_produced() == 0 {
                            break;
                        }
                    }
                }

                loop {
                    match compressor.finish(&mut out_buf)? {
                        StreamStatus::Done { bytes_produced, .. } => {
                            if bytes_produced > 0 {
                                self.file.write_all(&out_buf[..bytes_produced]).await?;
                                total_write += bytes_produced as u64;
                            }
                            break;
                        }
                        StreamStatus::Progress { bytes_produced, .. } => {
                            if bytes_produced > 0 {
                                self.file.write_all(&out_buf[..bytes_produced]).await?;
                                total_write += bytes_produced as u64;
                            }
                        }
                    }
                }

                ByteCount {
                    read: total_read,
                    write: total_write,
                }
            }
            Compression::Unknown(id) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Unknown compression id: {}", id),
                ));
            }
        };

        self.file_pos += byte_count.write;
        // Extract the HashingReader from BufReader and finalize the hash
        let hashing_reader = reader.into_inner();
        let hash_bytes = hashing_reader.finalize_bytes();
        Ok((byte_count, hash_bytes))
    }

    /// Write a file as independently-compressed blocks for random access.
    ///
    /// Each block is compressed separately, allowing random access to any block
    /// without decompressing the entire file. Block offsets are stored in the
    /// block FST for seeking.
    ///
    /// # Arguments
    /// * `path` - The path within the archive
    /// * `reader` - Source data to read
    /// * `block_size` - Size of each uncompressed block (last block may be smaller)
    /// * `compression` - Compression algorithm for each block
    /// * `attrs` - File attributes
    // [spec:box:sem:chunked-io.root]
    // [spec:box:sem:chunked-io.root.explicit-insert]
    // [spec:box:syn:chunked-io.root.block-index-entry]
    pub async fn insert_chunked<R: tokio::io::AsyncRead + Unpin>(
        &mut self,
        path: BoxPath<'_>,
        mut reader: R,
        block_size: u32,
        compression: Compression,
        attrs: HashMap<String, Vec<u8>>,
    ) -> std::io::Result<&ChunkedFileRecord<'static>> {
        if block_size == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "chunked file block size must be positive",
            ));
        }
        let block_capacity = usize::try_from(block_size).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "chunked file block size does not fit in memory",
            )
        })?;
        let mut block_buf = Vec::new();
        block_buf
            .try_reserve_exact(block_capacity)
            .map_err(|error| {
                std::io::Error::new(
                    std::io::ErrorKind::OutOfMemory,
                    format!("cannot reserve {block_capacity} bytes for a chunked block: {error}"),
                )
            })?;
        block_buf.resize(block_capacity, 0);

        // Reject an invalid hierarchy before attribute interning, block-index
        // mutation, cursor advancement, or any archive-data writes.
        let parent_index = self.validate_parent(&path, None)?;
        let data_start = self.next_write_addr();

        // We'll determine the record index after inserting
        // For now, use a placeholder that we'll fix up
        let record_index_placeholder = u64::try_from(self.core.meta.records.len())
            .ok()
            .and_then(|count| count.checked_add(1))
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "chunked record index overflows u64",
                )
            })?;

        let mut total_compressed: u64 = 0;
        let mut total_decompressed: u64 = 0;
        let mut pending_block_entries = Vec::new();
        loop {
            // Read up to block_size bytes
            let mut bytes_read = 0;
            while bytes_read < block_size as usize {
                let n = reader.read(&mut block_buf[bytes_read..]).await?;
                if n == 0 {
                    break; // EOF
                }
                bytes_read += n;
            }

            if bytes_read == 0 {
                break; // No more data
            }

            let block_data = &block_buf[..bytes_read];
            let logical_offset = total_decompressed;

            // Record physical offset for this block
            let physical_offset =
                data_start
                    .get()
                    .checked_add(total_compressed)
                    .ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "chunked physical block offset overflows u64",
                        )
                    })?;

            // Build the 16-byte FST key: record_index (BE) || logical_offset (BE)
            let mut key = [0u8; 16];
            key[..8].copy_from_slice(&record_index_placeholder.to_be_bytes());
            key[8..].copy_from_slice(&logical_offset.to_be_bytes());
            pending_block_entries.push((key, physical_offset));

            // Seek to write position if needed
            let write_pos = physical_offset;
            if self.file_pos != write_pos {
                self.file.seek(SeekFrom::Start(write_pos)).await?;
                self.file_pos = write_pos;
            }

            // Compress the block (data is already in memory, use sync compression)
            let config = CompressionConfig::new(compression);
            let compressed = crate::compression::compress_bytes_sync(block_data, &config)?;
            self.file.write_all(&compressed).await?;
            let bytes_written = u64::try_from(compressed.len()).map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "compressed chunk length does not fit u64",
                )
            })?;
            self.file_pos = self.file_pos.checked_add(bytes_written).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "chunked writer file position overflows u64",
                )
            })?;

            total_compressed = total_compressed.checked_add(bytes_written).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "chunked compressed length overflows u64",
                )
            })?;
            total_decompressed = total_decompressed
                .checked_add(bytes_read as u64)
                .ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "chunked logical length overflows u64",
                    )
                })?;
        }

        if total_decompressed == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "chunked files must contain at least one non-empty block",
            ));
        }

        let next_write_position = self.core.position_after_advance(total_compressed)?;

        // Attribute interning is committed only for a record that can be
        // inserted. Conversion itself can mutate the key schema before a later
        // conflict, so retain a rollback snapshot until insertion succeeds.
        let previous_attr_keys = self.core.meta.attr_keys.clone();
        let previous_archive_attrs = self.core.meta.attrs.clone();
        let attrs = match self.convert_attrs(attrs) {
            Ok(attrs) => attrs,
            Err(error) => {
                self.core.meta.attr_keys = previous_attr_keys;
                self.core.meta.attrs = previous_archive_attrs;
                return Err(error);
            }
        };

        let record = ChunkedFileRecord {
            compression,
            block_size,
            length: total_compressed,
            decompressed_length: total_decompressed,
            name: Cow::Owned(path.filename().to_string()),
            data: data_start,
            attrs,
        };

        let index = match self.insert_inner_with_parent(path, record.into(), parent_index) {
            Ok(index) => index,
            Err(error) => {
                self.core.meta.attr_keys = previous_attr_keys;
                self.core.meta.attrs = previous_archive_attrs;
                return Err(error);
            }
        };

        // Fix up the local block entries with the committed record index, then
        // publish them atomically with the cursor advancement.
        let actual_index = index.get();
        for (key, _) in &mut pending_block_entries {
            key[..8].copy_from_slice(&actual_index.to_be_bytes());
        }
        self.core.set_position(next_write_position)?;
        self.core.block_entries.extend(pending_block_entries);

        Ok(self
            .core
            .meta
            .record(index)
            .unwrap()
            .as_chunked_file()
            .unwrap())
    }

    /// Write a pre-compressed file to the archive.
    ///
    /// This method must be called sequentially (not in parallel) because:
    /// - `next_write_addr()` depends on the previous file's position + length
    /// - `FileRecord.data` offset is calculated here, not during compression
    /// - Metadata index must be consistent with archive layout
    ///
    /// Use `compress_file` to prepare files for this method.
    pub async fn write_precompressed(
        &mut self,
        file: CompressedFile,
    ) -> std::io::Result<&FileRecord<'static>> {
        self.write_precompressed_with_parent(file, None).await
    }

    /// Write a pre-compressed file with optional cached parent index for O(1) insertion.
    pub async fn write_precompressed_with_parent(
        &mut self,
        file: CompressedFile,
        parent_index: Option<RecordIndex>,
    ) -> std::io::Result<&FileRecord<'static>> {
        self.write_precompressed_with_parent_and_dict(file, parent_index, None)
            .await
    }

    async fn write_precompressed_with_parent_and_dict(
        &mut self,
        mut file: CompressedFile,
        parent_index: Option<RecordIndex>,
        dictionary: Option<Vec<u8>>,
    ) -> std::io::Result<&FileRecord<'static>> {
        let parent_index = self.validate_parent(&file.box_path, parent_index)?;
        let next_addr = self.next_write_addr();
        let next_write_position = self.core.position_after_advance(file.compressed_length)?;
        if let CompressedData::Memory(bytes) = &file.data
            && u64::try_from(bytes.len()).ok() != Some(file.compressed_length)
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "prepared file length does not match its in-memory payload",
            ));
        }
        if let Some((attr_name, hash)) = file.checksum.take() {
            file.attrs.insert(attr_name.to_string(), hash);
        }

        let previous_attr_keys = self.core.meta.attr_keys.clone();
        let previous_archive_attrs = self.core.meta.attrs.clone();
        let previous_dictionary = self.core.meta.dictionary.clone();
        if let Some(dictionary) = dictionary.as_deref() {
            match self.core.meta.dictionary.as_deref() {
                Some(existing) if existing != dictionary => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "pre-compressed file uses a different Zstd dictionary",
                    ));
                }
                None => self.core.meta.dictionary = Some(dictionary.to_vec().into_boxed_slice()),
                Some(_) => {}
            }
        }
        let attrs = match self.convert_attrs(file.attrs) {
            Ok(attrs) => attrs,
            Err(error) => {
                self.core.meta.attr_keys = previous_attr_keys;
                self.core.meta.attrs = previous_archive_attrs;
                self.core.meta.dictionary = previous_dictionary;
                return Err(error);
            }
        };

        let write_result = async {
            if self.file_pos < next_addr.get() {
                let mut remaining = next_addr.get() - self.file_pos;
                const ZEROS: [u8; 4096] = [0u8; 4096];
                while remaining > 0 {
                    let chunk = usize::try_from(remaining.min(ZEROS.len() as u64))
                        .expect("padding chunk fits usize");
                    self.file.write_all(&ZEROS[..chunk]).await?;
                    remaining -= chunk as u64;
                }
                self.file_pos = next_addr.get();
            } else if self.file_pos != next_addr.get() {
                self.file.seek(SeekFrom::Start(next_addr.get())).await?;
                self.file_pos = next_addr.get();
            }

            match &file.data {
                CompressedData::Memory(bytes) => self.file.write_all(bytes).await?,
                CompressedData::TempFile(temp) => {
                    let mmap = mmap_io::MemoryMappedFile::open_ro(temp.path())
                        .map_err(std::io::Error::other)?;
                    let data = mmap
                        .as_slice(0, file.compressed_length)
                        .map_err(std::io::Error::other)?;
                    self.file.write_all(data).await?;
                }
            }
            Ok::<(), std::io::Error>(())
        }
        .await;
        if let Err(error) = write_result {
            self.core.meta.attr_keys = previous_attr_keys;
            self.core.meta.attrs = previous_archive_attrs;
            self.core.meta.dictionary = previous_dictionary;
            self.file_pos = u64::MAX;
            return Err(error);
        }

        let record = FileRecord {
            compression: file.compression,
            length: file.compressed_length,
            decompressed_length: file.decompressed_length,
            name: Cow::Owned(file.box_path.filename().to_string()),
            data: next_addr,
            attrs,
        };
        let index = match self.insert_inner_with_parent(file.box_path, record.into(), parent_index)
        {
            Ok(index) => index,
            Err(error) => {
                self.core.meta.attr_keys = previous_attr_keys;
                self.core.meta.attrs = previous_archive_attrs;
                self.core.meta.dictionary = previous_dictionary;
                self.file_pos = u64::MAX;
                return Err(error);
            }
        };
        self.core.set_position(next_write_position)?;
        self.file_pos = next_addr
            .get()
            .checked_add(file.compressed_length)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "pre-compressed writer file position overflows u64",
                )
            })?;

        Ok(self.core.meta.record(index).unwrap().as_file().unwrap())
    }

    /// Publish prepared chunk data as one record after all block work succeeds.
    // [spec:box:req:chunked-io.root.explicit-creation]
    async fn write_precompressed_chunked_with_parent(
        &mut self,
        mut file: CompressedChunkedFile,
        parent_index: Option<RecordIndex>,
    ) -> std::io::Result<(u64, u64)> {
        if file.decompressed_length == 0
            || file.compressed_length == 0
            || file.block_offsets.first() != Some(&0)
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "prepared chunked file has no non-empty first block",
            ));
        }
        if file.block_offsets.windows(2).any(|pair| pair[0] >= pair[1])
            || file
                .block_offsets
                .last()
                .is_none_or(|offset| *offset >= file.compressed_length)
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "prepared chunk offsets are outside the compressed payload",
            ));
        }

        let parent_index = self.validate_parent(&file.box_path, parent_index)?;
        let next_addr = self.next_write_addr();
        let next_write_position = self.core.position_after_advance(file.compressed_length)?;
        let record_index = u64::try_from(self.core.meta.records.len())
            .ok()
            .and_then(|count| count.checked_add(1))
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "chunked record index overflows u64",
                )
            })?;

        let mut block_entries = Vec::with_capacity(file.block_offsets.len());
        for (block_number, relative_offset) in file.block_offsets.iter().copied().enumerate() {
            let block_number = u64::try_from(block_number).map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "chunked block number does not fit u64",
                )
            })?;
            let logical_offset = block_number
                .checked_mul(u64::from(file.block_size))
                .ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "chunked logical block offset overflows u64",
                    )
                })?;
            let physical_offset =
                next_addr
                    .get()
                    .checked_add(relative_offset)
                    .ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "chunked physical block offset overflows u64",
                        )
                    })?;
            let mut key = [0u8; 16];
            key[..8].copy_from_slice(&record_index.to_be_bytes());
            key[8..].copy_from_slice(&logical_offset.to_be_bytes());
            block_entries.push((key, physical_offset));
        }

        if let Some((attr_name, hash)) = file.checksum.take() {
            file.attrs.insert(attr_name.to_string(), hash);
        }
        let previous_attr_keys = self.core.meta.attr_keys.clone();
        let previous_archive_attrs = self.core.meta.attrs.clone();
        let previous_dictionary = self.core.meta.dictionary.clone();
        if let Some(dictionary) = file.dictionary.as_deref() {
            match self.core.meta.dictionary.as_deref() {
                Some(existing) if existing != dictionary => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "prepared chunk uses a different Zstd dictionary",
                    ));
                }
                None => self.core.meta.dictionary = Some(dictionary.to_vec().into_boxed_slice()),
                Some(_) => {}
            }
        }
        let attrs = match self.convert_attrs(file.attrs) {
            Ok(attrs) => attrs,
            Err(error) => {
                self.core.meta.attr_keys = previous_attr_keys;
                self.core.meta.attrs = previous_archive_attrs;
                self.core.meta.dictionary = previous_dictionary;
                return Err(error);
            }
        };

        let write_result = async {
            if self.file_pos != next_addr.get() {
                self.file.seek(SeekFrom::Start(next_addr.get())).await?;
                self.file_pos = next_addr.get();
            }
            let mmap = mmap_io::MemoryMappedFile::open_ro(file.data.path())
                .map_err(std::io::Error::other)?;
            let data = mmap
                .as_slice(0, file.compressed_length)
                .map_err(std::io::Error::other)?;
            self.file.write_all(data).await?;
            Ok::<(), std::io::Error>(())
        }
        .await;
        if let Err(error) = write_result {
            self.core.meta.attr_keys = previous_attr_keys;
            self.core.meta.attrs = previous_archive_attrs;
            self.core.meta.dictionary = previous_dictionary;
            self.file_pos = u64::MAX;
            return Err(error);
        }

        let record = ChunkedFileRecord {
            compression: file.compression,
            block_size: file.block_size,
            length: file.compressed_length,
            decompressed_length: file.decompressed_length,
            name: Cow::Owned(file.box_path.filename().to_string()),
            data: next_addr,
            attrs,
        };
        let index = match self.insert_inner_with_parent(file.box_path, record.into(), parent_index)
        {
            Ok(index) => index,
            Err(error) => {
                self.core.meta.attr_keys = previous_attr_keys;
                self.core.meta.attrs = previous_archive_attrs;
                self.core.meta.dictionary = previous_dictionary;
                self.file_pos = u64::MAX;
                return Err(error);
            }
        };
        debug_assert_eq!(index.get(), record_index);

        self.core.set_position(next_write_position)?;
        self.core.block_entries.extend(block_entries);
        self.file_pos = next_addr
            .get()
            .checked_add(file.compressed_length)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "chunked writer file position overflows u64",
                )
            })?;

        Ok((file.decompressed_length, file.compressed_length))
    }

    pub fn set_attr<S: AsRef<str>>(
        &mut self,
        path: &BoxPath<'_>,
        key: S,
        value: crate::core::AttrValue<'_>,
    ) -> std::io::Result<()> {
        let index = match self.iter().find(|r| &r.path == path) {
            Some(v) => v.index,
            None => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Path not found: {}", path),
                ));
            }
        };

        let attr_type = value.attr_type();
        let key_idx = self.core.meta.attr_key_or_create(key.as_ref(), attr_type)?;
        let bytes = value.as_raw_bytes().into_owned().into_boxed_slice();
        let record = self.core.meta.record_mut(index).unwrap();
        record.attrs_mut().insert(key_idx, bytes);

        Ok(())
    }

    pub fn set_file_attr<S: AsRef<str>>(
        &mut self,
        key: S,
        value: crate::core::AttrValue<'_>,
    ) -> std::io::Result<()> {
        let attr_type = value.attr_type();
        let key_idx = self.core.meta.attr_key_or_create(key.as_ref(), attr_type)?;
        let bytes = value.as_raw_bytes().into_owned().into_boxed_slice();

        self.core.meta.attrs.insert(key_idx, bytes);

        Ok(())
    }

    /// Explicitly insert one ordinary (non-chunked) file from the filesystem.
    ///
    /// Parent directories are automatically created if they don't exist.
    /// Files smaller than 72 bytes are stored uncompressed regardless of the
    /// compression setting. High-level path creation also uses ordinary records
    /// unless its compression configuration explicitly selects chunked records.
    pub async fn insert_file<P: AsRef<Path>>(
        &mut self,
        fs_path: P,
        box_path: BoxPath<'static>,
        config: &CompressionConfig,
        with_checksum: bool,
        timestamps: bool,
        ownership: bool,
    ) -> std::io::Result<&FileRecord<'static>> {
        let fs_path = fs_path.as_ref();
        let meta = tokio::fs::metadata(fs_path).await?;
        let attrs = crate::fs::metadata_to_attrs(&meta, timestamps, ownership);

        // Don't compress small files
        let config = config.for_size(meta.len());

        // Ensure parent directories exist
        if let Some(parent) = box_path.parent() {
            self.mkdir_all(parent, HashMap::new())?;
        }

        let file = tokio::fs::File::open(fs_path).await?;
        let reader = tokio::io::BufReader::new(file);

        if with_checksum {
            self.insert_streaming::<_, blake3::Hasher>(&config, box_path, reader, attrs)
                .await
        } else {
            self.insert(&config, box_path, reader, attrs).await
        }
    }

    /// Add a file or directory from the filesystem.
    ///
    /// If the path is a directory and `options.recursive` is true, all contents
    /// are added recursively.
    // [spec:box:req:chunked-io.root.explicit-creation]
    pub async fn add_path<P: AsRef<Path>>(
        &mut self,
        path: P,
        options: AddOptions,
    ) -> std::io::Result<AddStats> {
        let path = path.as_ref();
        let mut stats = AddStats::default();

        let path_meta = tokio::fs::metadata(path).await?;

        if path_meta.is_file() {
            // Single file
            let box_path = self.make_box_path(path)?;
            stats += self
                .add_paths_parallel(
                    [FileJob {
                        fs_path: path.to_path_buf(),
                        box_path,
                        config: options.config,
                        attrs: HashMap::new(),
                    }],
                    options.checksum,
                    options.timestamps,
                    options.ownership,
                    1,
                )
                .await?;
            return Ok(stats);
        }

        // Directory - walk it
        let mut walker = WalkDir::new(path);

        while let Some(entry) = walker.next().await {
            let entry = entry?;
            let file_path = entry.path();

            // Skip hidden files if not allowed
            if !options.include_hidden && crate::fs::is_hidden(&file_path) {
                continue;
            }

            let file_type = entry.file_type().await?;
            let meta = entry.metadata().await?;

            // Skip non-recursive if not at top level
            if !options.recursive && file_path != path && file_type.is_dir() {
                continue;
            }

            let canonical_path = tokio::fs::canonicalize(&file_path).await?;

            // Skip the archive itself
            if self.path() == canonical_path {
                continue;
            }

            let box_path = self.make_box_path(&file_path)?;

            // Ensure parent directories exist
            if let Some(parent) = box_path.parent()
                && self.core.meta.index(&parent).is_none()
            {
                self.mkdir_all(parent, HashMap::new())?;
            }

            if file_type.is_symlink() {
                // Symlinks require their target to be added first (we need RecordIndex).
                // Skip symlinks here - they should be handled externally after all
                // files are added (e.g., by bundle.rs which does two-pass processing).
                if !options.follow_symlinks {
                    continue;
                }
            } else if file_type.is_dir() {
                if self.core.meta.index(&box_path).is_none() {
                    let dir_meta =
                        crate::fs::metadata_to_attrs(&meta, options.timestamps, options.ownership);
                    self.mkdir(box_path, dir_meta)?;
                    stats.dirs_added += 1;
                }
            } else if self.core.meta.index(&box_path).is_none() {
                // Regular file
                let attrs =
                    crate::fs::metadata_to_attrs(&meta, options.timestamps, options.ownership);

                // Ensure parent exists
                if let Some(parent) = box_path.parent()
                    && self.core.meta.index(&parent).is_none()
                {
                    self.mkdir_all(parent, HashMap::new())?;
                }

                stats += self
                    .add_paths_parallel(
                        [FileJob {
                            fs_path: file_path,
                            box_path,
                            config: options.config.clone(),
                            attrs,
                        }],
                        options.checksum,
                        options.timestamps,
                        options.ownership,
                        1,
                    )
                    .await?;
            }
        }

        Ok(stats)
    }

    /// Add multiple files in parallel, writing sequentially to the archive.
    ///
    /// This method compresses ordinary files and explicitly selected chunk
    /// blocks with one shared concurrency bound, then writes prepared files
    /// sequentially in input order to maintain archive consistency.
    ///
    /// # Arguments
    /// * `files` - Iterator of `FileJob` items, each specifying a file and its compression
    /// * `checksum` - Whether to compute Blake3 checksums
    /// * `concurrency` - Maximum compression work units across files and blocks
    ///
    /// # Memory Management
    /// Files smaller than the memory threshold are compressed to RAM.
    /// Larger files are compressed to temp files to prevent memory exhaustion.
    /// The threshold is calculated from available RAM and retained batch size.
    pub async fn add_paths_parallel<I>(
        &mut self,
        files: I,
        checksum: bool,
        timestamps: bool,
        ownership: bool,
        concurrency: usize,
    ) -> std::io::Result<AddStats>
    where
        I: IntoIterator<Item = FileJob>,
    {
        self.add_paths_parallel_with_progress(
            files,
            checksum,
            timestamps,
            ownership,
            concurrency,
            None,
        )
        .await
    }

    /// Add multiple files in parallel with progress reporting.
    ///
    /// Same as `add_paths_parallel` but accepts an optional progress sender
    /// that receives `ParallelProgress` updates.
    // [spec:box:sem:async-io.root.parallel-compression+1]
    // [spec:box:req:chunked-io.root.explicit-creation]
    pub async fn add_paths_parallel_with_progress<I>(
        &mut self,
        files: I,
        checksum: bool,
        timestamps: bool,
        ownership: bool,
        concurrency: usize,
        progress: Option<tokio::sync::mpsc::UnboundedSender<ParallelProgress>>,
    ) -> std::io::Result<AddStats>
    where
        I: IntoIterator<Item = FileJob>,
    {
        use futures::stream::FuturesUnordered;
        use tokio::sync::Semaphore;

        let files: Vec<_> = files.into_iter().collect();
        let file_count = files.len();
        let total_files = u64::try_from(file_count).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "file job count does not fit u64",
            )
        })?;
        let concurrency = concurrency.clamp(1, Semaphore::MAX_PERMITS);
        // Prepared results are retained until every task succeeds, so size the
        // in-memory tier across the whole batch rather than only active workers.
        let memory_threshold = calculate_memory_threshold(file_count.max(concurrency));
        let semaphore = Arc::new(Semaphore::new(concurrency));
        #[cfg(feature = "zstd")]
        let zstd_pool = Arc::new(ZstdCompressorPool::default());
        #[cfg(test)]
        let compression_test_probe = self.compression_test_probe.clone();

        // Build parent index cache for O(1) lookups (avoids O(depth) path traversal per file)
        let mut parent_cache: HashMap<BoxPath<'static>, RecordIndex> = HashMap::new();
        for job in &files {
            if let Some(parent) = job.box_path.parent()
                && !parent_cache.contains_key(&parent)
                && let Some(idx) = self.core.meta.index(&parent)
            {
                parent_cache.insert(parent.into_owned(), idx);
            }
        }

        if let Some(ref p) = progress {
            let _ = p.send(ParallelProgress::Started { total_files });
        }

        // File preparation is detached from archive state. Ordinary files consume
        // one permit; chunked files acquire the same permits per block, allowing
        // one large file to use the complete jobs budget without oversubscription.
        let mut tasks = FuturesUnordered::new();
        for (position, job) in files.into_iter().enumerate() {
            let progress = progress.clone();
            let semaphore = semaphore.clone();
            #[cfg(feature = "zstd")]
            let zstd_pool = zstd_pool.clone();
            #[cfg(test)]
            let compression_test_probe = compression_test_probe.clone();
            tasks.push(tokio::spawn(async move {
                let path = job.box_path.clone();
                if let Some(ref p) = progress {
                    let _ = p.send(ParallelProgress::Compressing { path: path.clone() });
                }
                let result = if checksum {
                    prepare_file_inner::<blake3::Hasher>(
                        job,
                        memory_threshold,
                        timestamps,
                        ownership,
                        semaphore,
                        concurrency,
                        #[cfg(feature = "zstd")]
                        zstd_pool,
                        #[cfg(test)]
                        compression_test_probe.clone(),
                    )
                    .await
                } else {
                    prepare_file_inner::<crate::checksum::NullChecksum>(
                        job,
                        memory_threshold,
                        timestamps,
                        ownership,
                        semaphore,
                        concurrency,
                        #[cfg(feature = "zstd")]
                        zstd_pool,
                        #[cfg(test)]
                        compression_test_probe.clone(),
                    )
                    .await
                };
                #[cfg(test)]
                if result.is_ok()
                    && let Some(probe) = compression_test_probe.as_ref()
                {
                    probe.record_prepared(path.clone());
                }
                if let Some(ref p) = progress
                    && result.is_ok()
                {
                    let _ = p.send(ParallelProgress::Compressed { path: path.clone() });
                }
                (position, path, result)
            }));
        }

        // Do not publish any data or metadata until every compression task has
        // succeeded. This makes task/config/read failures transactional.
        let mut prepared: Vec<Option<PreparedFile>> =
            std::iter::repeat_with(|| None).take(file_count).collect();
        let mut first_error = None;
        while let Some(joined) = tasks.next().await {
            match joined {
                Ok((position, _path, Ok(file))) => prepared[position] = Some(file),
                Ok((_position, _path, Err(error))) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
                Err(error) => {
                    if first_error.is_none() {
                        first_error = Some(std::io::Error::other(format!(
                            "file compression task failed: {error}"
                        )));
                    }
                }
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        let prepared: Vec<PreparedFile> = prepared
            .into_iter()
            .map(|file| {
                file.ok_or_else(|| std::io::Error::other("compression task produced no result"))
            })
            .collect::<std::io::Result<_>>()?;

        // A Box trailer carries one global Zstd dictionary. Validate all file
        // configs as a set before mutation; the first sequential write then
        // publishes that dictionary transactionally with its record.
        let mut selected_dictionary = self.core.meta.dictionary.as_deref().map(<[u8]>::to_vec);
        for file in &prepared {
            let Some(dictionary) = file.dictionary() else {
                continue;
            };
            if let Some(selected) = selected_dictionary.as_deref() {
                if selected != dictionary {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "parallel creation cannot mix different Zstd dictionaries",
                    ));
                }
            } else {
                selected_dictionary = Some(dictionary.to_vec());
            }
        }
        self.core
            .meta
            .records
            .try_reserve(file_count)
            .map_err(|error| {
                std::io::Error::new(
                    std::io::ErrorKind::OutOfMemory,
                    format!("cannot reserve archive records for parallel creation: {error}"),
                )
            })?;

        // Write compressed files sequentially
        let mut stats = AddStats::default();
        for compressed in prepared {
            let path = compressed.box_path().clone();
            let (bytes_original, bytes_compressed) = compressed.lengths();
            let files_added = stats
                .files_added
                .checked_add(1)
                .ok_or_else(|| std::io::Error::other("file statistics overflow u64"))?;
            let total_original = stats
                .bytes_original
                .checked_add(bytes_original)
                .ok_or_else(|| std::io::Error::other("original byte statistics overflow u64"))?;
            let total_compressed = stats
                .bytes_compressed
                .checked_add(bytes_compressed)
                .ok_or_else(|| std::io::Error::other("compressed byte statistics overflow u64"))?;

            // Get cached parent index, or ensure parent exists if not cached
            let parent_index = if let Some(parent) = path.parent() {
                match parent_cache.get(&parent).copied() {
                    Some(idx) => Some(idx),
                    None => {
                        // Parent not in cache - ensure it exists and cache it
                        let parent_owned = parent.into_owned();
                        if self.core.meta.index(&parent_owned).is_none() {
                            self.mkdir_all(parent_owned.clone(), HashMap::new())?;
                        }
                        let idx = self.core.meta.index(&parent_owned);
                        if let Some(idx) = idx {
                            parent_cache.insert(parent_owned, idx);
                        }
                        idx
                    }
                }
            } else {
                None // Root level file
            };

            match compressed {
                PreparedFile::Regular { file, dictionary } => {
                    self.write_precompressed_with_parent_and_dict(file, parent_index, dictionary)
                        .await?;
                }
                PreparedFile::Chunked(file) => {
                    self.write_precompressed_chunked_with_parent(file, parent_index)
                        .await?;
                }
            }
            stats.files_added = files_added;
            stats.bytes_original = total_original;
            stats.bytes_compressed = total_compressed;

            if let Some(ref p) = progress {
                let _ = p.send(ParallelProgress::Written {
                    path,
                    files_written: stats.files_added,
                    total_files,
                });
            }
        }

        if let Some(ref p) = progress {
            let _ = p.send(ParallelProgress::Finished);
        }

        Ok(stats)
    }
}

#[cfg(test)]
#[path = "writer_tests.rs"]
mod writer_tests;

/// Progress updates from parallel file compression.
#[derive(Debug, Clone)]
pub enum ParallelProgress {
    /// Compression started.
    Started { total_files: u64 },
    /// A file is being compressed.
    Compressing { path: BoxPath<'static> },
    /// A file finished compressing (waiting to be written).
    Compressed { path: BoxPath<'static> },
    /// A file was written to the archive.
    Written {
        path: BoxPath<'static>,
        files_written: u64,
        total_files: u64,
    },
    /// All files have been processed.
    Finished,
}

/// Options for adding files to an archive.
#[derive(Debug, Clone)]
pub struct AddOptions {
    /// Compression configuration to use.
    pub config: CompressionConfig,
    /// Whether to compute Blake3 checksums.
    pub checksum: bool,
    /// Whether to store file timestamps (created, modified, accessed).
    pub timestamps: bool,
    /// Whether to store file ownership (uid, gid).
    pub ownership: bool,
    /// Whether to recurse into directories.
    pub recursive: bool,
    /// Whether to include hidden files.
    pub include_hidden: bool,
    /// Whether to follow symlinks (if false, symlinks are stored as links).
    pub follow_symlinks: bool,
}

impl Default for AddOptions {
    fn default() -> Self {
        Self {
            config: CompressionConfig::new(Compression::default()),
            checksum: true,
            timestamps: false,
            ownership: false,
            recursive: true,
            include_hidden: false,
            follow_symlinks: false,
        }
    }
}

/// Statistics from adding files to an archive.
#[derive(Debug, Clone, Default)]
pub struct AddStats {
    /// Number of files added.
    pub files_added: u64,
    /// Number of directories added.
    pub dirs_added: u64,
    /// Number of symlinks added.
    pub links_added: u64,
    /// Total uncompressed size in bytes.
    pub bytes_original: u64,
    /// Total compressed size in bytes.
    pub bytes_compressed: u64,
}

impl AddAssign for AddStats {
    fn add_assign(&mut self, other: Self) {
        self.files_added += other.files_added;
        self.dirs_added += other.dirs_added;
        self.links_added += other.links_added;
        self.bytes_original += other.bytes_original;
        self.bytes_compressed += other.bytes_compressed;
    }
}

/// Calculate size threshold for temp file fallback based on available RAM.
///
/// Uses at most 50% of available RAM, divided by the number of result slots the
/// caller may retain. Each slot can have compressed and uncompressed data in flight.
pub fn calculate_memory_threshold(slots: usize) -> u64 {
    use sysinfo::System;
    // Reading process, CPU, disk, and network state is surprisingly costly and
    // none of it contributes to this calculation.
    let mut sys = System::new();
    sys.refresh_memory();
    let available = sys.available_memory();
    // Factor 4 accounts for compressed and uncompressed buffers per slot.
    available / (slots as u64 * 4).max(1)
}

/// Size the per-file streaming buffers to the file being processed.
///
/// Package trees commonly contain thousands of files that are much smaller than
/// [`DEFAULT_BLOCK_SIZE`]. Allocating two full-size blocks for every one of those
/// files causes far more allocator and page-fault work than the compression
/// itself. Keep a modest floor so files that grow while being read still make
/// useful progress, while retaining the full block size for large files.
fn file_buffer_size(file_size: u64) -> usize {
    const MIN_FILE_BUFFER_SIZE: u64 = 8 * 1024;

    file_size
        .clamp(MIN_FILE_BUFFER_SIZE, DEFAULT_BLOCK_SIZE as u64)
        .try_into()
        .expect("DEFAULT_BLOCK_SIZE fits in usize")
}

type CompressedBlockTask = tokio::task::JoinHandle<std::io::Result<Vec<u8>>>;

async fn write_next_compressed_block(
    pending: &mut futures::stream::FuturesOrdered<CompressedBlockTask>,
    output: &mut File,
    block_offsets: &mut Vec<u64>,
    compressed_length: &mut u64,
) -> std::io::Result<()> {
    let compressed = pending
        .next()
        .await
        .ok_or_else(|| std::io::Error::other("missing pending chunk compression task"))?
        .map_err(|error| {
            std::io::Error::other(format!("chunk compression task failed: {error}"))
        })??;
    if compressed.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "compression produced an empty chunk",
        ));
    }
    block_offsets.push(*compressed_length);
    output.write_all(&compressed).await?;
    *compressed_length = compressed_length
        .checked_add(u64::try_from(compressed.len()).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "compressed chunk length does not fit u64",
            )
        })?)
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "chunked compressed length overflows u64",
            )
        })?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
// [spec:box:req:chunked-io.root.explicit-creation]
async fn compress_chunked_file_inner<C: Checksum>(
    fs_path: &Path,
    box_path: BoxPath<'static>,
    config: &CompressionConfig,
    initial_size: u64,
    timestamps: bool,
    ownership: bool,
    extra_attrs: HashMap<String, Vec<u8>>,
    semaphore: Arc<tokio::sync::Semaphore>,
    max_in_flight: usize,
    #[cfg(test)] compression_test_probe: Option<Arc<CompressionTestProbe>>,
) -> std::io::Result<CompressedChunkedFile> {
    let mut source = File::open(fs_path).await?;
    let meta = source.metadata().await?;

    let mut attrs = crate::fs::metadata_to_attrs(&meta, timestamps, ownership);
    attrs.extend(extra_attrs);
    let config = config.for_size(initial_size);
    let output_file = tempfile::NamedTempFile::new()?;
    let mut output = File::create(output_file.path()).await?;
    let mut pending = futures::stream::FuturesOrdered::new();
    let mut hasher = C::default();
    let mut decompressed_length = 0u64;
    let mut compressed_length = 0u64;
    let mut block_offsets = Vec::new();
    let block_capacity =
        usize::try_from(DEFAULT_BLOCK_SIZE).expect("default block size fits usize");
    let max_in_flight = max_in_flight.max(1);

    loop {
        let mut block = vec![0u8; block_capacity];
        let mut bytes_read = 0usize;
        while bytes_read < block_capacity {
            let count = source.read(&mut block[bytes_read..]).await?;
            if count == 0 {
                break;
            }
            bytes_read += count;
        }
        if bytes_read == 0 {
            break;
        }
        block.truncate(bytes_read);
        digest::Digest::update(&mut hasher, &block);
        decompressed_length = decompressed_length
            .checked_add(u64::try_from(bytes_read).map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "chunked input length does not fit u64",
                )
            })?)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "chunked logical length overflows u64",
                )
            })?;

        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| std::io::Error::other("compression semaphore closed"))?;
        let block_config = config.clone();
        #[cfg(test)]
        let probe_path = box_path.clone();
        #[cfg(test)]
        let block_test_probe = compression_test_probe.clone();
        pending.push_back(tokio::task::spawn_blocking(move || {
            let _permit = permit;
            #[cfg(test)]
            let _test_activity = block_test_probe
                .as_ref()
                .map(|probe| probe.enter(&probe_path));
            crate::compression::compress_bytes_sync(&block, &block_config)
        }));

        if pending.len() >= max_in_flight {
            write_next_compressed_block(
                &mut pending,
                &mut output,
                &mut block_offsets,
                &mut compressed_length,
            )
            .await?;
        }
    }

    while !pending.is_empty() {
        write_next_compressed_block(
            &mut pending,
            &mut output,
            &mut block_offsets,
            &mut compressed_length,
        )
        .await?;
    }
    if decompressed_length == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "an explicitly chunked source must be non-empty",
        ));
    }
    output.flush().await?;

    let hash_bytes = digest::Digest::finalize(hasher).to_vec();
    let checksum = (!C::NAME.is_empty()).then_some((C::NAME, hash_bytes));
    let dictionary = match config.compression {
        #[cfg(feature = "zstd")]
        Compression::Zstd => config.dictionary.clone(),
        _ => None,
    };
    Ok(CompressedChunkedFile {
        box_path,
        data: output_file,
        compression: config.compression,
        block_size: DEFAULT_BLOCK_SIZE,
        block_offsets,
        compressed_length,
        decompressed_length,
        attrs,
        checksum,
        dictionary,
    })
}

#[allow(clippy::too_many_arguments)]
async fn prepare_file_inner<C: Checksum>(
    job: FileJob,
    memory_threshold: u64,
    timestamps: bool,
    ownership: bool,
    semaphore: Arc<tokio::sync::Semaphore>,
    max_in_flight: usize,
    #[cfg(feature = "zstd")] zstd_pool: Arc<ZstdCompressorPool>,
    #[cfg(test)] compression_test_probe: Option<Arc<CompressionTestProbe>>,
) -> std::io::Result<PreparedFile> {
    let initial_size = tokio::fs::metadata(&job.fs_path).await?.len();
    if job.config.is_chunked() {
        let compressed = compress_chunked_file_inner::<C>(
            &job.fs_path,
            job.box_path,
            &job.config,
            initial_size,
            timestamps,
            ownership,
            job.attrs,
            semaphore,
            max_in_flight,
            #[cfg(test)]
            compression_test_probe,
        )
        .await?;
        Ok(PreparedFile::Chunked(compressed))
    } else {
        let configured_dictionary = job.config.dictionary.clone();
        let _permit = semaphore
            .acquire_owned()
            .await
            .map_err(|_| std::io::Error::other("compression semaphore closed"))?;
        #[cfg(test)]
        let _test_activity = compression_test_probe
            .as_ref()
            .map(|probe| probe.enter(&job.box_path));
        #[cfg(test)]
        if let Some(probe) = compression_test_probe.as_ref() {
            probe.wait_if_deferred(&job.box_path).await;
        }
        let compressed = compress_file_inner::<C>(
            &job.fs_path,
            job.box_path,
            &job.config,
            memory_threshold,
            timestamps,
            ownership,
            job.attrs,
            #[cfg(feature = "zstd")]
            Some(&zstd_pool),
        )
        .await?;
        let dictionary = match compressed.compression {
            #[cfg(feature = "zstd")]
            Compression::Zstd => configured_dictionary,
            _ => None,
        };
        Ok(PreparedFile::Regular {
            file: compressed,
            dictionary,
        })
    }
}

/// Compress a file to memory or temp file based on size threshold.
///
/// This function is safe to run in parallel - it has no shared mutable state.
/// The resulting `CompressedFile` can be passed to `BoxFileWriter::write_precompressed`.
///
/// The `extra_attrs` parameter allows passing additional attributes that will be
/// merged with the metadata-derived attributes. Extra attrs take precedence.
pub async fn compress_file<C: Checksum>(
    fs_path: &Path,
    box_path: BoxPath<'static>,
    config: &CompressionConfig,
    memory_threshold: u64,
    timestamps: bool,
    ownership: bool,
    extra_attrs: HashMap<String, Vec<u8>>,
) -> std::io::Result<CompressedFile> {
    compress_file_inner::<C>(
        fs_path,
        box_path,
        config,
        memory_threshold,
        timestamps,
        ownership,
        extra_attrs,
        #[cfg(feature = "zstd")]
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
// [spec:box:sem:async-io.root.parallel-compression+1]
// [spec:box:req:checksums.root]
// [spec:box:req:checksums.root.disabled]
async fn compress_file_inner<C: Checksum>(
    fs_path: &Path,
    box_path: BoxPath<'static>,
    config: &CompressionConfig,
    memory_threshold: u64,
    timestamps: bool,
    ownership: bool,
    extra_attrs: HashMap<String, Vec<u8>>,
    #[cfg(feature = "zstd")] zstd_pool: Option<&ZstdCompressorPool>,
) -> std::io::Result<CompressedFile> {
    let file = tokio::fs::File::open(fs_path).await?;
    let meta = file.metadata().await?;
    let file_size = meta.len();
    let mut attrs = crate::fs::metadata_to_attrs(&meta, timestamps, ownership);
    // Merge extra attrs (they take precedence)
    attrs.extend(extra_attrs);

    // Don't compress small files
    let config = config.for_size(file_size);

    // Wrap in HashingReader to compute checksum while reading
    let mut hashing_reader = HashingReader::<_, C>::new(file);
    let buffer_size = file_buffer_size(file_size);

    let (data, compressed_length, decompressed_length) = if file_size <= memory_threshold {
        // Small file: compress to memory
        let mut buffer = Vec::new();
        let byte_count = match config.compression {
            Compression::Stored => {
                let mut read_buf = vec![0u8; buffer_size];
                let mut total = 0u64;
                loop {
                    let n = hashing_reader.read(&mut read_buf).await?;
                    if n == 0 {
                        break;
                    }
                    total += n as u64;
                    buffer.extend_from_slice(&read_buf[..n]);
                }
                ByteCount {
                    read: total,
                    write: total,
                }
            }
            #[cfg(feature = "zstd")]
            Compression::Zstd => {
                use crate::compression::zstd::ZstdCompressor;
                let level = config
                    .get_i32("level")
                    .unwrap_or(zstd::DEFAULT_COMPRESSION_LEVEL);
                let pooled = config.dictionary.is_none() && zstd_pool.is_some();
                let mut compressor = match &config.dictionary {
                    Some(dict) => ZstdCompressor::with_dictionary(level, dict)?,
                    None => match zstd_pool {
                        Some(pool) => pool.take(level)?,
                        None => ZstdCompressor::new(level)?,
                    },
                };
                let mut read_buf = vec![0u8; buffer_size];
                let mut out_buf = vec![0u8; buffer_size];
                let mut total_read = 0u64;

                // Compress loop
                loop {
                    let n = hashing_reader.read(&mut read_buf).await?;
                    if n == 0 {
                        break;
                    }
                    total_read += n as u64;

                    let mut in_pos = 0;
                    while in_pos < n {
                        let status = compressor.compress(&read_buf[in_pos..n], &mut out_buf)?;
                        let consumed = status.bytes_consumed();
                        let produced = status.bytes_produced();
                        if produced > 0 {
                            buffer.extend_from_slice(&out_buf[..produced]);
                        }
                        in_pos += consumed;
                    }
                }

                // Finish loop
                loop {
                    let status = compressor.finish(&mut out_buf)?;
                    let produced = status.bytes_produced();
                    if produced > 0 {
                        buffer.extend_from_slice(&out_buf[..produced]);
                    }
                    if status.is_done() {
                        break;
                    }
                }

                if pooled {
                    zstd_pool
                        .expect("pooled compressor has a pool")
                        .put(level, compressor);
                }

                ByteCount {
                    read: total_read,
                    write: buffer.len() as u64,
                }
            }
            #[cfg(feature = "xz")]
            Compression::Xz => {
                use crate::compression::xz::XzCompressor;
                let level = config.get_i32("level").unwrap_or(6) as u32;
                let mut compressor = XzCompressor::new(level)?;
                let mut read_buf = vec![0u8; buffer_size];
                let mut out_buf = vec![0u8; buffer_size];
                let mut total_read = 0u64;

                // Compress loop
                loop {
                    let n = hashing_reader.read(&mut read_buf).await?;
                    if n == 0 {
                        break;
                    }
                    total_read += n as u64;

                    let mut in_pos = 0;
                    while in_pos < n {
                        let status = compressor.compress(&read_buf[in_pos..n], &mut out_buf)?;
                        let consumed = status.bytes_consumed();
                        let produced = status.bytes_produced();
                        if produced > 0 {
                            buffer.extend_from_slice(&out_buf[..produced]);
                        }
                        in_pos += consumed;
                    }
                }

                // Finish loop
                loop {
                    let status = compressor.finish(&mut out_buf)?;
                    let produced = status.bytes_produced();
                    if produced > 0 {
                        buffer.extend_from_slice(&out_buf[..produced]);
                    }
                    if status.is_done() {
                        break;
                    }
                }

                ByteCount {
                    read: total_read,
                    write: buffer.len() as u64,
                }
            }
            Compression::Unknown(id) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Unknown compression ID: {}", id),
                ));
            }
        };
        (
            CompressedData::Memory(buffer),
            byte_count.write,
            byte_count.read,
        )
    } else {
        // Large file: compress to temp file
        let temp = tempfile::NamedTempFile::new()?;
        let mut temp_file = tokio::fs::File::create(temp.path()).await?;
        let byte_count = match config.compression {
            Compression::Stored => {
                let mut read_buf = vec![0u8; buffer_size];
                let mut total = 0u64;
                loop {
                    let n = hashing_reader.read(&mut read_buf).await?;
                    if n == 0 {
                        break;
                    }
                    total += n as u64;
                    temp_file.write_all(&read_buf[..n]).await?;
                }
                ByteCount {
                    read: total,
                    write: total,
                }
            }
            #[cfg(feature = "zstd")]
            Compression::Zstd => {
                use crate::compression::zstd::ZstdCompressor;
                let level = config
                    .get_i32("level")
                    .unwrap_or(zstd::DEFAULT_COMPRESSION_LEVEL);
                let pooled = config.dictionary.is_none() && zstd_pool.is_some();
                let mut compressor = match &config.dictionary {
                    Some(dict) => ZstdCompressor::with_dictionary(level, dict)?,
                    None => match zstd_pool {
                        Some(pool) => pool.take(level)?,
                        None => ZstdCompressor::new(level)?,
                    },
                };
                let mut read_buf = vec![0u8; buffer_size];
                let mut out_buf = vec![0u8; buffer_size];
                let mut total_read = 0u64;
                let mut total_write = 0u64;

                // Compress loop
                loop {
                    let n = hashing_reader.read(&mut read_buf).await?;
                    if n == 0 {
                        break;
                    }
                    total_read += n as u64;

                    let mut in_pos = 0;
                    while in_pos < n {
                        let status = compressor.compress(&read_buf[in_pos..n], &mut out_buf)?;
                        let consumed = status.bytes_consumed();
                        let produced = status.bytes_produced();
                        if produced > 0 {
                            temp_file.write_all(&out_buf[..produced]).await?;
                            total_write += produced as u64;
                        }
                        in_pos += consumed;
                    }
                }

                // Finish loop
                loop {
                    let status = compressor.finish(&mut out_buf)?;
                    let produced = status.bytes_produced();
                    if produced > 0 {
                        temp_file.write_all(&out_buf[..produced]).await?;
                        total_write += produced as u64;
                    }
                    if status.is_done() {
                        break;
                    }
                }

                if pooled {
                    zstd_pool
                        .expect("pooled compressor has a pool")
                        .put(level, compressor);
                }

                ByteCount {
                    read: total_read,
                    write: total_write,
                }
            }
            #[cfg(feature = "xz")]
            Compression::Xz => {
                use crate::compression::xz::XzCompressor;
                let level = config.get_i32("level").unwrap_or(6) as u32;
                let mut compressor = XzCompressor::new(level)?;
                let mut read_buf = vec![0u8; buffer_size];
                let mut out_buf = vec![0u8; buffer_size];
                let mut total_read = 0u64;
                let mut total_write = 0u64;

                // Compress loop
                loop {
                    let n = hashing_reader.read(&mut read_buf).await?;
                    if n == 0 {
                        break;
                    }
                    total_read += n as u64;

                    let mut in_pos = 0;
                    while in_pos < n {
                        let status = compressor.compress(&read_buf[in_pos..n], &mut out_buf)?;
                        let consumed = status.bytes_consumed();
                        let produced = status.bytes_produced();
                        if produced > 0 {
                            temp_file.write_all(&out_buf[..produced]).await?;
                            total_write += produced as u64;
                        }
                        in_pos += consumed;
                    }
                }

                // Finish loop
                loop {
                    let status = compressor.finish(&mut out_buf)?;
                    let produced = status.bytes_produced();
                    if produced > 0 {
                        temp_file.write_all(&out_buf[..produced]).await?;
                        total_write += produced as u64;
                    }
                    if status.is_done() {
                        break;
                    }
                }

                ByteCount {
                    read: total_read,
                    write: total_write,
                }
            }
            Compression::Unknown(id) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Unknown compression ID: {}", id),
                ));
            }
        };
        temp_file.flush().await?;
        (
            CompressedData::TempFile(temp),
            byte_count.write,
            byte_count.read,
        )
    };

    // Finalize the hash
    let hash_bytes = hashing_reader.finalize_bytes();

    let checksum = if C::NAME.is_empty() {
        None
    } else {
        Some((C::NAME, hash_bytes))
    };

    Ok(CompressedFile {
        box_path,
        data,
        compression: config.compression,
        compressed_length,
        decompressed_length,
        attrs,
        checksum,
    })
}
