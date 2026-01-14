//! Sans-IO archive writer state machine.
//!
//! `ArchiveWriter` manages archive metadata state without performing any I/O.
//! Frontends (async, sync, kernel) use this core for metadata management,
//! while handling their own I/O and compression operations.

// This module requires std for encoding and complex operations
#![cfg(feature = "std")]

use crate::compat::HashMap;
use std::borrow::Cow;
use std::num::NonZeroU64;

use crate::compression::Compression;
use crate::encode;
use crate::header::BoxHeader;
use crate::path::BoxPath;
use crate::record::{
    ChunkedFileRecord, DirectoryRecord, ExternalLinkRecord, FileRecord, LinkRecord, Record,
};

use super::meta::{AttrType, AttrValue, BoxMetadata, MetadataIter, RecordIndex};

/// Options for creating a new archive.
#[derive(Debug, Clone, Default)]
pub struct WriterOptions {
    /// Alignment for file data (0 = no alignment).
    pub alignment: u32,
    /// Allow `\xNN` escape sequences in paths.
    pub allow_escapes: bool,
    /// Allow external symlinks pointing outside the archive.
    pub allow_external_symlinks: bool,
}

impl WriterOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_alignment(mut self, alignment: u32) -> Self {
        self.alignment = alignment;
        self
    }

    pub fn with_escapes(mut self, allow: bool) -> Self {
        self.allow_escapes = allow;
        self
    }

    pub fn with_external_symlinks(mut self, allow: bool) -> Self {
        self.allow_external_symlinks = allow;
        self
    }
}

/// Sans-IO archive writer state machine.
///
/// This holds the archive header and metadata, providing methods
/// for building the archive structure without I/O.
///
/// # Example
///
/// ```ignore
/// // Create writer
/// let mut writer = ArchiveWriter::new(WriterOptions::default());
///
/// // Get header bytes to write
/// let header = writer.encode_header();
/// // Frontend writes header...
///
/// // Create directory
/// writer.mkdir("path/to/dir".into(), HashMap::new())?;
///
/// // Get position for file data
/// let pos = writer.next_write_addr();
/// // Frontend compresses and writes file data to pos...
///
/// // Register file with writer
/// writer.insert_file(
///     "path/to/file.txt".into(),
///     Compression::Zstd,
///     pos,
///     compressed_len,
///     decompressed_len,
///     HashMap::new(),
/// )?;
///
/// // Finalize
/// let (trailer_offset, metadata_bytes) = writer.finish()?;
/// // Frontend writes metadata_bytes at trailer_offset...
/// // Frontend updates header with trailer_offset...
/// ```
pub struct ArchiveWriter {
    /// Archive header configuration
    pub header: BoxHeader,
    /// Metadata being built
    pub meta: BoxMetadata<'static>,
    /// Next write position (tracks where file data ends)
    next_write_pos: u64,
    /// Block entries for chunked files: ([16-byte key], physical_offset)
    /// Key format: record_index (u64 BE) || logical_offset (u64 BE)
    pub block_entries: Vec<([u8; 16], u64)>,
}

impl ArchiveWriter {
    // ========================================================================
    // Construction
    // ========================================================================

    /// Create a new archive writer with default options.
    pub fn new(options: WriterOptions) -> Self {
        let header = BoxHeader {
            version: 1,
            allow_external_symlinks: options.allow_external_symlinks,
            allow_escapes: options.allow_escapes,
            alignment: options.alignment,
            trailer: None,
        };

        Self {
            header,
            meta: BoxMetadata::default(),
            next_write_pos: BoxHeader::SIZE as u64,
            block_entries: Vec::new(),
        }
    }

    /// Create a writer with specific alignment.
    pub fn with_alignment(alignment: u32) -> Self {
        Self::new(WriterOptions::default().with_alignment(alignment))
    }

    /// Create a writer that allows escape sequences in paths.
    pub fn with_escapes(allow: bool) -> Self {
        Self::new(WriterOptions::default().with_escapes(allow))
    }

    /// Create a writer with custom options.
    pub fn with_options(
        alignment: u32,
        allow_escapes: bool,
        allow_external_symlinks: bool,
    ) -> Self {
        Self::new(
            WriterOptions::default()
                .with_alignment(alignment)
                .with_escapes(allow_escapes)
                .with_external_symlinks(allow_external_symlinks),
        )
    }

    /// Create a writer from an existing archive (for appending).
    ///
    /// `next_write_pos` should be the end of the last file's data.
    pub fn from_existing(
        header: BoxHeader,
        meta: BoxMetadata<'static>,
        next_write_pos: u64,
    ) -> Self {
        Self {
            header,
            meta,
            next_write_pos,
            block_entries: Vec::new(),
        }
    }

    // ========================================================================
    // Header accessors
    // ========================================================================

    #[inline]
    pub fn version(&self) -> u8 {
        self.header.version
    }

    #[inline]
    pub fn alignment(&self) -> u32 {
        self.header.alignment
    }

    #[inline]
    pub fn allow_escapes(&self) -> bool {
        self.header.allow_escapes
    }

    #[inline]
    pub fn allow_external_symlinks(&self) -> bool {
        self.header.allow_external_symlinks
    }

    #[inline]
    pub fn metadata(&self) -> &BoxMetadata<'static> {
        &self.meta
    }

    // ========================================================================
    // Header encoding
    // ========================================================================

    /// Encode the header to a 32-byte array.
    ///
    /// Note: This encodes the current header state. Call this again after
    /// `finish()` to get the header with the trailer offset set.
    pub fn encode_header(&self) -> [u8; 32] {
        encode::encode_header_array(&encode::HeaderConfig {
            version: self.header.version,
            allow_external_symlinks: self.header.allow_external_symlinks,
            allow_escapes: self.header.allow_escapes,
            alignment: self.header.alignment,
            trailer_offset: self.header.trailer.map(|x| x.get()).unwrap_or(0),
        })
    }

    // ========================================================================
    // Write position management
    // ========================================================================

    /// Get the next aligned write address.
    ///
    /// This is where the frontend should write the next file's data.
    pub fn next_write_addr(&self) -> NonZeroU64 {
        let offset = self.next_write_pos;

        let v = match self.header.alignment as u64 {
            0 => offset,
            alignment => {
                let diff = offset % alignment;
                if diff == 0 {
                    offset
                } else {
                    offset + (alignment - diff)
                }
            }
        };

        NonZeroU64::new(v).unwrap()
    }

    /// Advance the write position after data has been written.
    ///
    /// Called by the frontend after writing file data.
    pub fn advance_position(&mut self, bytes_written: u64) {
        self.next_write_pos = self.next_write_addr().get() + bytes_written;
    }

    /// Set the write position explicitly.
    ///
    /// Use this when resuming from an existing archive.
    pub fn set_position(&mut self, pos: u64) {
        self.next_write_pos = pos;
    }

    // ========================================================================
    // Directory operations
    // ========================================================================

    /// Create a directory.
    pub fn mkdir(
        &mut self,
        path: BoxPath<'_>,
        attrs: HashMap<String, Vec<u8>>,
    ) -> std::io::Result<RecordIndex> {
        tracing::trace!("mkdir: {}", path);

        let record = DirectoryRecord {
            name: Cow::Owned(path.filename().to_string()),
            entries: vec![],
            attrs: self.convert_attrs(attrs)?,
        };

        self.insert_inner(path, record.into())
    }

    /// Create a directory and all its parent directories if they don't exist.
    pub fn mkdir_all(
        &mut self,
        path: BoxPath<'_>,
        attrs: HashMap<String, Vec<u8>>,
    ) -> std::io::Result<RecordIndex> {
        // First ensure all parent directories exist
        if let Some(parent) = path.parent() {
            if self.meta.index(&parent).is_none() {
                self.mkdir_all(parent.into_owned(), HashMap::new())?;
            }
        }

        // Now create this directory if it doesn't exist, otherwise return existing
        if let Some(idx) = self.meta.index(&path) {
            Ok(idx)
        } else {
            self.mkdir(path, attrs)
        }
    }

    // ========================================================================
    // File insertion (metadata only, data written by frontend)
    // ========================================================================

    /// Insert a file record.
    ///
    /// Called after the frontend has written the compressed data.
    /// The `data_offset` should be the value from `next_write_addr()` before writing.
    pub fn insert_file(
        &mut self,
        path: BoxPath<'static>,
        compression: Compression,
        data_offset: NonZeroU64,
        compressed_len: u64,
        decompressed_len: u64,
        attrs: HashMap<String, Vec<u8>>,
    ) -> std::io::Result<RecordIndex> {
        let attrs = self.convert_attrs(attrs)?;

        let record = FileRecord {
            compression,
            length: compressed_len,
            decompressed_length: decompressed_len,
            name: Cow::Owned(path.filename().to_string()),
            data: data_offset,
            attrs,
        };

        self.insert_inner(path, record.into())
    }

    /// Insert a file record with a pre-computed parent index for O(1) lookup.
    pub fn insert_file_with_parent(
        &mut self,
        path: BoxPath<'static>,
        compression: Compression,
        data_offset: NonZeroU64,
        compressed_len: u64,
        decompressed_len: u64,
        attrs: HashMap<String, Vec<u8>>,
        parent: Option<RecordIndex>,
    ) -> std::io::Result<RecordIndex> {
        let attrs = self.convert_attrs(attrs)?;

        let record = FileRecord {
            compression,
            length: compressed_len,
            decompressed_length: decompressed_len,
            name: Cow::Owned(path.filename().to_string()),
            data: data_offset,
            attrs,
        };

        self.insert_inner_with_parent(path, record.into(), parent)
    }

    /// Insert a chunked file record.
    ///
    /// Called after the frontend has written all blocks.
    /// `block_entries` should contain (16-byte key, physical_offset) pairs.
    pub fn insert_chunked_file(
        &mut self,
        path: BoxPath<'static>,
        compression: Compression,
        block_size: u32,
        data_offset: NonZeroU64,
        compressed_len: u64,
        decompressed_len: u64,
        mut block_entries: Vec<([u8; 16], u64)>,
        attrs: HashMap<String, Vec<u8>>,
    ) -> std::io::Result<RecordIndex> {
        let attrs = self.convert_attrs(attrs)?;

        let record = ChunkedFileRecord {
            compression,
            block_size,
            length: compressed_len,
            decompressed_length: decompressed_len,
            name: Cow::Owned(path.filename().to_string()),
            data: data_offset,
            attrs,
        };

        let index = self.insert_inner(path, record.into())?;

        // Store block entries for FST building
        self.block_entries.append(&mut block_entries);

        Ok(index)
    }

    /// Add block entries for a chunked file.
    ///
    /// Called incrementally as blocks are written by the frontend.
    /// `record_index` is the record index (call after insert_chunked_file_record).
    /// `logical_offset` is the decompressed byte offset.
    /// `physical_offset` is the compressed byte offset in the file.
    pub fn add_block_entry(
        &mut self,
        record_index: RecordIndex,
        logical_offset: u64,
        physical_offset: u64,
    ) {
        let mut key = [0u8; 16];
        key[..8].copy_from_slice(&record_index.get().to_be_bytes());
        key[8..].copy_from_slice(&logical_offset.to_be_bytes());
        self.block_entries.push((key, physical_offset));
    }

    // ========================================================================
    // Symlink insertion
    // ========================================================================

    /// Add a symlink pointing to another record in the archive.
    pub fn link(
        &mut self,
        path: BoxPath<'_>,
        target: RecordIndex,
        attrs: HashMap<String, Vec<u8>>,
    ) -> std::io::Result<RecordIndex> {
        // Validate that the target index exists
        if self.meta.record(target).is_none() {
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
        self.header.allow_external_symlinks = true;

        let record = ExternalLinkRecord {
            name: Cow::Owned(path.filename().to_string()),
            target: Cow::Owned(target.to_string()),
            attrs: self.convert_attrs(attrs)?,
        };

        self.insert_inner(path, record.into())
    }

    // ========================================================================
    // Attribute operations
    // ========================================================================

    /// Set an attribute on a record.
    pub fn set_attr(
        &mut self,
        path: &BoxPath<'_>,
        key: &str,
        value: AttrValue<'_>,
    ) -> std::io::Result<()> {
        let index = self.meta.index(path).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("No record found for path: {}", path),
            )
        })?;

        let attr_key = self.meta.attr_key_or_create(key, value.attr_type())?;
        let bytes = value.as_raw_bytes().into_owned().into_boxed_slice();

        self.meta
            .record_mut(index)
            .unwrap()
            .attrs_mut()
            .insert(attr_key, bytes);

        Ok(())
    }

    /// Set a file-level attribute (applies to the whole archive).
    pub fn set_file_attr(&mut self, key: &str, value: AttrValue<'_>) -> std::io::Result<()> {
        let attr_key = self.meta.attr_key_or_create(key, value.attr_type())?;
        let bytes = value.as_raw_bytes().into_owned().into_boxed_slice();
        self.meta.attrs.insert(attr_key, bytes);
        Ok(())
    }

    /// Convert external attribute map to internal keyed format.
    ///
    /// Also sets archive-level uid/gid defaults from the first file.
    pub fn convert_attrs(
        &mut self,
        attrs: HashMap<String, Vec<u8>>,
    ) -> std::io::Result<HashMap<usize, Box<[u8]>>> {
        // Set archive-level uid/gid defaults from first file if not already set
        if let Some(uid) = attrs.get("unix.uid") {
            let uid_key = self.meta.attr_key_or_create("unix.uid", AttrType::Vu32)?;
            self.meta
                .attrs
                .entry(uid_key)
                .or_insert_with(|| uid.clone().into_boxed_slice());
        }
        if let Some(gid) = attrs.get("unix.gid") {
            let gid_key = self.meta.attr_key_or_create("unix.gid", AttrType::Vu32)?;
            self.meta
                .attrs
                .entry(gid_key)
                .or_insert_with(|| gid.clone().into_boxed_slice());
        }

        // Filter out uid/gid that match archive defaults
        let attrs: Vec<_> = {
            let default_uid = self
                .meta
                .attr_key("unix.uid")
                .and_then(|k| self.meta.attrs.get(&k).map(|v| &**v));
            let default_gid = self
                .meta
                .attr_key("unix.gid")
                .and_then(|k| self.meta.attrs.get(&k).map(|v| &**v));

            attrs
                .into_iter()
                .filter(|(k, v)| {
                    if k == "unix.uid" && default_uid.is_some_and(|d| v.as_slice() == d) {
                        return false;
                    }
                    if k == "unix.gid" && default_gid.is_some_and(|d| v.as_slice() == d) {
                        return false;
                    }
                    true
                })
                .collect()
        };

        // Convert keys
        let mut result = HashMap::new();
        for (k, v) in attrs {
            let attr_type = match k.as_str() {
                "unix.mode" | "unix.uid" | "unix.gid" => AttrType::Vu32,
                "created" | "modified" | "accessed" => AttrType::DateTime,
                "created.seconds" | "modified.seconds" | "accessed.seconds" => AttrType::U8,
                "created.nanoseconds" | "modified.nanoseconds" | "accessed.nanoseconds" => {
                    AttrType::Vu64
                }
                "blake3" => AttrType::U256,
                _ => AttrType::Bytes,
            };
            let key = self.meta.attr_key_or_create(&k, attr_type)?;
            result.insert(key, v.into_boxed_slice());
        }
        Ok(result)
    }

    // ========================================================================
    // Finalization
    // ========================================================================

    /// Finalize the archive and return the encoded metadata.
    ///
    /// Returns (trailer_offset, metadata_bytes) for the frontend to write.
    /// After calling this, the frontend should:
    /// 1. Write metadata_bytes at trailer_offset
    /// 2. Re-encode and write the header (which now has the trailer offset)
    pub fn finish(&mut self) -> std::io::Result<(u64, Vec<u8>)> {
        // Build FST from existing metadata
        let fst_bytes = self.build_fst()?;
        self.meta.fst = fst_bytes
            .as_ref()
            .and_then(|bytes| box_fst::Fst::new(Cow::Owned(bytes.clone())).ok());

        // Build block FST for chunked files
        let block_fst_bytes = self.build_block_fst()?;
        self.meta.block_fst = block_fst_bytes
            .as_ref()
            .and_then(|bytes| box_fst::Fst::new(Cow::Owned(bytes.clone())).ok());

        // Set trailer offset
        let trailer_offset = self.next_write_addr().get();
        self.header.trailer = NonZeroU64::new(trailer_offset);

        // Encode metadata
        let mut meta_buf = Vec::new();
        encode::encode_metadata_v1(&mut meta_buf, &self.meta);

        Ok((trailer_offset, meta_buf))
    }

    /// Get the current record count.
    pub fn record_count(&self) -> usize {
        self.meta.records.len()
    }

    /// Iterate over records.
    pub fn iter(&self) -> MetadataIter<'_, 'static> {
        self.meta.iter()
    }

    // ========================================================================
    // Internal methods
    // ========================================================================

    fn insert_inner(
        &mut self,
        path: BoxPath<'_>,
        record: Record<'static>,
    ) -> std::io::Result<RecordIndex> {
        self.insert_inner_with_parent(path, record, None)
    }

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
                    None => self.meta.index(&parent_path).ok_or_else(|| {
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
                let new_index = self.meta.insert_record(record);
                tracing::trace!("Inserted with index: {:?}", &new_index);
                let parent = self
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
                let new_index = self.meta.insert_record(record);
                self.meta.root.push(new_index);
                Ok(new_index)
            }
        }
    }

    fn build_fst(&self) -> std::io::Result<Option<Vec<u8>>> {
        // Collect all paths by traversing metadata
        let mut paths: Vec<(Vec<u8>, u64)> = Vec::new();
        self.collect_paths(&self.meta.root, &mut Vec::new(), &mut paths);

        // Empty archives have no FST
        if paths.is_empty() {
            return Ok(None);
        }

        // FST requires sorted input
        paths.sort_unstable_by(|a, b| a.0.cmp(&b.0));

        // Build FST
        let mut builder = box_fst::FstBuilder::new();
        for (path, index) in paths {
            builder
                .insert(&path, index)
                .map_err(|e| std::io::Error::other(e.to_string()))?;
        }
        builder
            .finish()
            .map(Some)
            .map_err(|e| std::io::Error::other(e.to_string()))
    }

    fn build_block_fst(&self) -> std::io::Result<Option<Vec<u8>>> {
        if self.block_entries.is_empty() {
            return Ok(None);
        }

        // Clone and sort (entries may not be in order if multiple chunked files)
        let mut blocks = self.block_entries.clone();
        blocks.sort_unstable_by(|a, b| a.0.cmp(&b.0));

        let mut builder = box_fst::FstBuilder::new();
        for (key, offset) in blocks {
            builder
                .insert(&key, offset)
                .map_err(|e| std::io::Error::other(e.to_string()))?;
        }
        builder
            .finish()
            .map(Some)
            .map_err(|e| std::io::Error::other(e.to_string()))
    }

    fn collect_paths(
        &self,
        entries: &[RecordIndex],
        prefix: &mut Vec<u8>,
        paths: &mut Vec<(Vec<u8>, u64)>,
    ) {
        for &index in entries {
            let record = self.meta.record(index).unwrap();
            let name = record.name().as_bytes();

            // Build full path with \x1f separator (BoxPath separator)
            let path_start = prefix.len();
            if !prefix.is_empty() {
                prefix.push(0x1f);
            }
            prefix.extend_from_slice(name);

            paths.push((prefix.clone(), index.get()));

            // Recurse into directories
            if let Record::Directory(dir) = record {
                self.collect_paths(&dir.entries, prefix, paths);
            }

            prefix.truncate(path_start);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path(s: &str) -> BoxPath<'static> {
        BoxPath::new(s).unwrap()
    }

    #[test]
    fn test_new_writer() {
        let writer = ArchiveWriter::new(WriterOptions::default());
        assert_eq!(writer.version(), 1);
        assert_eq!(writer.alignment(), 0);
        assert!(!writer.allow_escapes());
        assert!(!writer.allow_external_symlinks());
    }

    #[test]
    fn test_with_alignment() {
        let writer = ArchiveWriter::with_alignment(4096);
        assert_eq!(writer.alignment(), 4096);
    }

    #[test]
    fn test_next_write_addr_no_alignment() {
        let mut writer = ArchiveWriter::new(WriterOptions::default());
        // Initial position is after header (32 bytes)
        assert_eq!(writer.next_write_addr().get(), 32);

        writer.advance_position(100);
        assert_eq!(writer.next_write_addr().get(), 132);
    }

    #[test]
    fn test_next_write_addr_with_alignment() {
        let mut writer = ArchiveWriter::with_alignment(64);
        // Header is 32 bytes, next aligned position is 64
        assert_eq!(writer.next_write_addr().get(), 64);

        writer.advance_position(10);
        // 64 + 10 = 74, next aligned position is 128
        assert_eq!(writer.next_write_addr().get(), 128);
    }

    #[test]
    fn test_mkdir() {
        let mut writer = ArchiveWriter::new(WriterOptions::default());
        let idx = writer.mkdir(path("test"), HashMap::new()).unwrap();
        assert_eq!(idx.get(), 1);

        let record = writer.meta.record(idx).unwrap();
        assert!(record.as_directory().is_some());
        assert_eq!(record.name(), "test");
    }

    #[test]
    fn test_mkdir_all() {
        let mut writer = ArchiveWriter::new(WriterOptions::default());
        writer.mkdir_all(path("a/b/c"), HashMap::new()).unwrap();

        // Should have created a, a/b, and a/b/c
        assert!(writer.meta.index(&path("a")).is_some());
        assert!(writer.meta.index(&path("a/b")).is_some());
        assert!(writer.meta.index(&path("a/b/c")).is_some());
    }

    #[test]
    fn test_encode_header() {
        let writer = ArchiveWriter::with_options(4096, true, true);
        let header = writer.encode_header();

        assert_eq!(&header[0..4], b"\xffBOX");
        assert_eq!(header[4], 1); // version
        assert_eq!(header[5], 0x03); // flags: both bits set
    }
}
