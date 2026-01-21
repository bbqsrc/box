use crate::compat::HashMap;
use std::borrow::Cow;
use std::default::Default;
use std::io::SeekFrom;
use std::num::NonZeroU64;
use std::ops::AddAssign;
use std::path::{Path, PathBuf};

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

/// 8MB buffer for efficient sequential writes
const WRITE_BUFFER_SIZE: usize = 8 * 1024 * 1024;

/// Async writer for Box archives.
///
/// This is a frontend that wraps the sans-IO [`ArchiveWriter`] core,
/// providing async I/O operations for writing archives.
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
}

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
    pub async fn open<P: AsRef<Path>>(path: P) -> std::io::Result<BoxFileWriter> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.as_ref())
            .await?;

        // Try to load the header so we can easily rewrite it when saving.
        // If header is invalid, we're not even loading a .box file.
        let mut reader = tokio::io::BufReader::new(file);
        let header = read_header(&mut reader, 0).await?;
        let ptr = header
            .trailer
            .ok_or_else(|| std::io::Error::other("no trailer found"))?;
        let meta = read_trailer(&mut reader, ptr, 0, header.version).await?;

        // Get the file back from the BufReader
        let file = reader.into_inner();

        // Compute next write position from existing records
        let next_write_pos = meta
            .records
            .iter()
            .rev()
            .find_map(|r| r.as_file())
            .map(|r| r.data.get() + r.length)
            .unwrap_or(BoxHeader::SIZE as u64);

        // Create the core writer from existing header and metadata
        let core = ArchiveWriter::from_existing(header, meta, next_write_pos);

        let f = BoxFileWriter {
            core,
            file: BufWriter::with_capacity(WRITE_BUFFER_SIZE, file),
            path: tokio::fs::canonicalize(path.as_ref()).await?,
            file_pos: 0, // Unknown after reading, will seek on first write
            finished: false,
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
            ArchiveWriter::from_existing(header, BoxMetadata::default(), BoxHeader::SIZE as u64);

        let mut boxfile = BoxFileWriter {
            core,
            file: BufWriter::with_capacity(WRITE_BUFFER_SIZE, file),
            path: tokio::fs::canonicalize(path.as_ref()).await?,
            file_pos: 0, // Will be set by write_header
            finished: false,
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

    /// Insert a record with optional pre-computed parent index for O(1) lookup.
    fn insert_inner_with_parent(
        &mut self,
        path: BoxPath<'_>,
        record: Record<'static>,
        cached_parent: Option<RecordIndex>,
    ) -> std::io::Result<RecordIndex> {
        tracing::trace!("insert_inner path: {:?}", path);
        match path.parent() {
            Some(parent_path) => {
                tracing::trace!("insert_inner parent: {:?}", parent_path);

                // Use cached parent index if provided, otherwise do lookup
                let parent_index = match cached_parent {
                    Some(idx) => idx,
                    None => self.core.meta.index(&parent_path).ok_or_else(|| {
                        std::io::Error::other(format!(
                            "No record found for path: {:?}",
                            parent_path
                        ))
                    })?,
                };

                tracing::trace!(
                    "Inserting record into parent {:?}: {:?}",
                    &parent_index,
                    &record
                );
                let new_index = self.core.meta.insert_record(record);
                tracing::trace!("Inserted with index: {:?}", &new_index);
                let parent = self
                    .core
                    .meta
                    .record_mut(parent_index)
                    .unwrap()
                    .as_directory_mut()
                    .unwrap();
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

        // Now create this directory if it doesn't exist
        if self.core.meta.index(&path).is_none() {
            self.mkdir(path, attrs)?;
        }

        Ok(())
    }

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
        self.core.advance_position(byte_count.write);

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
        self.core.advance_position(byte_count.write);

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
    pub async fn insert_chunked<R: tokio::io::AsyncRead + Unpin>(
        &mut self,
        path: BoxPath<'_>,
        mut reader: R,
        block_size: u32,
        compression: Compression,
        attrs: HashMap<String, Vec<u8>>,
    ) -> std::io::Result<&ChunkedFileRecord<'static>> {
        let attrs = self.convert_attrs(attrs)?;
        let data_start = self.next_write_addr();

        // We'll determine the record index after inserting
        // For now, use a placeholder that we'll fix up
        let record_index_placeholder = (self.core.meta.records.len() + 1) as u64;

        let mut total_compressed: u64 = 0;
        let mut total_decompressed: u64 = 0;
        let mut block_buf = vec![0u8; block_size as usize];

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
            let physical_offset = data_start.get() + total_compressed;

            // Build the 16-byte FST key: record_index (BE) || logical_offset (BE)
            let mut key = [0u8; 16];
            key[..8].copy_from_slice(&record_index_placeholder.to_be_bytes());
            key[8..].copy_from_slice(&logical_offset.to_be_bytes());
            self.core.block_entries.push((key, physical_offset));

            // Seek to write position if needed
            let write_pos = data_start.get() + total_compressed;
            if self.file_pos != write_pos {
                self.file.seek(SeekFrom::Start(write_pos)).await?;
                self.file_pos = write_pos;
            }

            // Compress the block (data is already in memory, use sync compression)
            let config = CompressionConfig::new(compression);
            let compressed = crate::compression::compress_bytes_sync(block_data, &config)?;
            self.file.write_all(&compressed).await?;
            let bytes_written = compressed.len() as u64;
            self.file_pos += bytes_written;

            total_compressed += bytes_written;
            total_decompressed += bytes_read as u64;
        }

        // Update cached write position
        self.core.advance_position(total_compressed);

        let record = ChunkedFileRecord {
            compression,
            block_size,
            length: total_compressed,
            decompressed_length: total_decompressed,
            name: Cow::Owned(path.filename().to_string()),
            data: data_start,
            attrs,
        };

        let index = self.insert_inner(path, record.into())?;

        // Fix up the block_entries with the correct record index
        let actual_index = index.get();
        for (key, _) in self.core.block_entries.iter_mut().rev() {
            // Check if this entry has our placeholder
            let stored_index = u64::from_be_bytes(key[..8].try_into().unwrap());
            if stored_index == record_index_placeholder {
                key[..8].copy_from_slice(&actual_index.to_be_bytes());
            } else {
                // We've passed all entries for this record
                break;
            }
        }

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
        let next_addr = self.next_write_addr();

        // Write padding bytes for alignment instead of seeking
        // (seeking triggers BufWriter flush, padding just extends the buffer)
        if self.file_pos < next_addr.get() {
            let mut remaining = (next_addr.get() - self.file_pos) as usize;
            const ZEROS: [u8; 4096] = [0u8; 4096];
            while remaining > 0 {
                let chunk = remaining.min(ZEROS.len());
                self.file.write_all(&ZEROS[..chunk]).await?;
                remaining -= chunk;
            }
            self.file_pos = next_addr.get();
        }

        // Write compressed data from memory or temp file
        match &file.data {
            CompressedData::Memory(bytes) => {
                self.file.write_all(bytes).await?;
            }
            CompressedData::TempFile(temp) => {
                // mmap the temp file for efficient copying
                let mmap = mmap_io::MemoryMappedFile::open_ro(temp.path())
                    .map_err(std::io::Error::other)?;
                let data = mmap
                    .as_slice(0, file.compressed_length)
                    .map_err(std::io::Error::other)?;
                self.file.write_all(data).await?;
                // temp file auto-deleted when CompressedFile is dropped
            }
        }

        // Update cached write position and file position
        self.core.advance_position(file.compressed_length);
        self.file_pos = next_addr.get() + file.compressed_length;

        // Convert string attrs to internal keys
        let attrs = self.convert_attrs(file.attrs)?;

        // Create record with the correct offset
        let record = FileRecord {
            compression: file.compression,
            length: file.compressed_length,
            decompressed_length: file.decompressed_length,
            name: Cow::Owned(file.box_path.filename().to_string()),
            data: next_addr,
            attrs,
        };

        let index = self.insert_inner_with_parent(file.box_path, record.into(), parent_index)?;

        // Set checksum attribute if present
        if let Some((attr_name, hash)) = file.checksum {
            let key = self
                .core
                .meta
                .attr_key_or_create(attr_name, AttrType::U256)?;
            self.core
                .meta
                .record_mut(index)
                .unwrap()
                .attrs_mut()
                .insert(key, hash.into_boxed_slice());
        }

        Ok(self.core.meta.record(index).unwrap().as_file().unwrap())
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

    /// Insert a file from the filesystem with optional Blake3 checksum.
    ///
    /// Parent directories are automatically created if they don't exist.
    /// Files smaller than 72 bytes are stored uncompressed regardless of the
    /// compression setting.
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
            let record = self
                .insert_file(
                    path,
                    box_path,
                    &options.config,
                    options.checksum,
                    options.timestamps,
                    options.ownership,
                )
                .await?;
            stats.files_added += 1;
            stats.bytes_original += record.decompressed_length;
            stats.bytes_compressed += record.length;
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

                // Don't compress small files
                let config = options.config.for_size(meta.len());

                // Ensure parent exists
                if let Some(parent) = box_path.parent()
                    && self.core.meta.index(&parent).is_none()
                {
                    self.mkdir_all(parent, HashMap::new())?;
                }

                let file = tokio::fs::File::open(&file_path).await?;
                let reader = tokio::io::BufReader::new(file);

                let record = if options.checksum {
                    self.insert_streaming::<_, blake3::Hasher>(&config, box_path, reader, attrs)
                        .await?
                } else {
                    self.insert(&config, box_path, reader, attrs).await?
                };

                stats.files_added += 1;
                stats.bytes_original += record.decompressed_length;
                stats.bytes_compressed += record.length;
            }
        }

        Ok(stats)
    }

    /// Add multiple files in parallel, writing sequentially to the archive.
    ///
    /// This method compresses files in parallel using bounded concurrency,
    /// then writes them sequentially to maintain archive consistency.
    ///
    /// # Arguments
    /// * `files` - Iterator of `FileJob` items, each specifying a file and its compression
    /// * `checksum` - Whether to compute Blake3 checksums
    /// * `concurrency` - Maximum number of files to compress in parallel
    ///
    /// # Memory Management
    /// Files smaller than the memory threshold are compressed to RAM.
    /// Larger files are compressed to temp files to prevent memory exhaustion.
    /// The threshold is calculated based on available RAM and concurrency.
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
        use std::sync::Arc;
        use tokio::sync::{Semaphore, mpsc};

        let memory_threshold = calculate_memory_threshold(concurrency);

        // Unbounded channel to avoid deadlock - tasks can always send without blocking
        let (tx, mut rx) = mpsc::unbounded_channel::<std::io::Result<CompressedFile>>();

        // Semaphore to limit concurrent compression tasks
        let semaphore = Arc::new(Semaphore::new(concurrency));

        // Spawn compression tasks
        let files: Vec<_> = files.into_iter().collect();
        let total_files = files.len() as u64;

        // Pre-allocate records vector to avoid reallocations during writes
        self.core.meta.records.reserve(total_files as usize);

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

        // Spawn all compression tasks - they will be limited by the semaphore
        for job in files {
            let tx = tx.clone();
            let progress = progress.clone();
            let semaphore = semaphore.clone();

            tokio::spawn(async move {
                // Acquire semaphore inside the task to avoid blocking the spawn loop
                let permit = match semaphore.acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => return, // Semaphore closed, task cancelled
                };

                let _permit = permit; // Hold permit until compression completes
                let path = job.box_path.clone();

                if let Some(ref p) = progress {
                    let _ = p.send(ParallelProgress::Compressing { path: path.clone() });
                }

                let result = if checksum {
                    compress_file::<blake3::Hasher>(
                        &job.fs_path,
                        job.box_path,
                        &job.config,
                        memory_threshold,
                        timestamps,
                        ownership,
                        job.attrs,
                    )
                    .await
                } else {
                    compress_file::<crate::checksum::NullChecksum>(
                        &job.fs_path,
                        job.box_path,
                        &job.config,
                        memory_threshold,
                        timestamps,
                        ownership,
                        job.attrs,
                    )
                    .await
                };

                if let Some(ref p) = progress {
                    let _ = p.send(ParallelProgress::Compressed { path });
                }

                // Send result (unbounded channel never blocks)
                let _ = tx.send(result);
            });
        }

        // Drop sender so receiver knows when all tasks are done
        drop(tx);

        // Write compressed files sequentially
        let mut stats = AddStats::default();
        while let Some(result) = rx.recv().await {
            let compressed = result?;
            let path = compressed.box_path.clone();

            // Get cached parent index, or ensure parent exists if not cached
            let parent_index = if let Some(parent) = compressed.box_path.parent() {
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

            let record = self
                .write_precompressed_with_parent(compressed, parent_index)
                .await?;
            stats.files_added += 1;
            stats.bytes_original += record.decompressed_length;
            stats.bytes_compressed += record.length;

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
            config: CompressionConfig::new(Compression::Zstd),
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
/// Uses at most 50% of available RAM, divided by the number of concurrent tasks.
/// Each task could have both compressed and uncompressed data in flight.
pub fn calculate_memory_threshold(concurrency: usize) -> u64 {
    use sysinfo::System;
    let sys = System::new_all();
    let available = sys.available_memory();
    // Use at most 50% of available RAM, divided by concurrent tasks
    // Factor of 4 accounts for: compressed + uncompressed buffers per task
    available / (concurrency as u64 * 4).max(1)
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
    let file = tokio::fs::File::open(fs_path).await?;
    let meta = file.metadata().await?;
    let file_size = meta.len();
    let mut attrs = crate::fs::metadata_to_attrs(&meta, timestamps, ownership);
    // Merge extra attrs (they take precedence)
    attrs.extend(extra_attrs);

    // Don't compress small files
    let config = config.for_size(file_size);

    // Wrap in HashingReader to compute checksum while reading
    let hashing_reader = HashingReader::<_, C>::new(file);
    let mut buf_reader = BufReader::new(hashing_reader);

    let (data, compressed_length, decompressed_length) = if file_size <= memory_threshold {
        // Small file: compress to memory
        let mut buffer = Vec::new();
        let byte_count = match config.compression {
            Compression::Stored => {
                let mut read_buf = vec![0u8; DEFAULT_BLOCK_SIZE as usize];
                let mut total = 0u64;
                loop {
                    let n = buf_reader.read(&mut read_buf).await?;
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
                let mut compressor = match &config.dictionary {
                    Some(dict) => ZstdCompressor::with_dictionary(level, dict)?,
                    None => ZstdCompressor::new(level)?,
                };
                let mut read_buf = vec![0u8; DEFAULT_BLOCK_SIZE as usize];
                let mut out_buf = vec![0u8; DEFAULT_BLOCK_SIZE as usize];
                let mut total_read = 0u64;

                // Compress loop
                loop {
                    let n = buf_reader.read(&mut read_buf).await?;
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
            #[cfg(feature = "xz")]
            Compression::Xz => {
                use crate::compression::xz::XzCompressor;
                let level = config.get_i32("level").unwrap_or(6) as u32;
                let mut compressor = XzCompressor::new(level)?;
                let mut read_buf = vec![0u8; DEFAULT_BLOCK_SIZE as usize];
                let mut out_buf = vec![0u8; DEFAULT_BLOCK_SIZE as usize];
                let mut total_read = 0u64;

                // Compress loop
                loop {
                    let n = buf_reader.read(&mut read_buf).await?;
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
                let mut read_buf = vec![0u8; DEFAULT_BLOCK_SIZE as usize];
                let mut total = 0u64;
                loop {
                    let n = buf_reader.read(&mut read_buf).await?;
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
                let mut compressor = match &config.dictionary {
                    Some(dict) => ZstdCompressor::with_dictionary(level, dict)?,
                    None => ZstdCompressor::new(level)?,
                };
                let mut read_buf = vec![0u8; DEFAULT_BLOCK_SIZE as usize];
                let mut out_buf = vec![0u8; DEFAULT_BLOCK_SIZE as usize];
                let mut total_read = 0u64;
                let mut total_write = 0u64;

                // Compress loop
                loop {
                    let n = buf_reader.read(&mut read_buf).await?;
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
            #[cfg(feature = "xz")]
            Compression::Xz => {
                use crate::compression::xz::XzCompressor;
                let level = config.get_i32("level").unwrap_or(6) as u32;
                let mut compressor = XzCompressor::new(level)?;
                let mut read_buf = vec![0u8; DEFAULT_BLOCK_SIZE as usize];
                let mut out_buf = vec![0u8; DEFAULT_BLOCK_SIZE as usize];
                let mut total_read = 0u64;
                let mut total_write = 0u64;

                // Compress loop
                loop {
                    let n = buf_reader.read(&mut read_buf).await?;
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
    let hash_bytes = buf_reader.into_inner().finalize_bytes();

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
