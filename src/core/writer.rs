//! Sans-IO archive writer state machine.
//!
//! `ArchiveWriter` manages archive metadata state without performing any I/O.
//! Frontends (async, sync, kernel) use this core for metadata management,
//! while handling their own I/O and compression operations.

// This module requires std for encoding and complex operations
#![cfg(feature = "std")]

use crate::compat::{BTreeMap, HashMap};
use std::borrow::Cow;
use std::collections::HashSet;
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
// [spec:box:def:archive-state.root]
// [spec:box:def:archive-state.root.writer]
// [spec:box:sem:sans-io.root]
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
    ) -> std::io::Result<Self> {
        let writer = Self {
            header,
            meta,
            next_write_pos,
            block_entries: Vec::new(),
        };
        writer.checked_next_write_addr()?;
        writer.build_fst()?;
        writer.build_block_fst()?;
        Ok(writer)
    }

    /// Find the first byte after all regular and chunked payloads in existing
    /// metadata. Malformed record ranges are rejected instead of wrapping.
    pub fn existing_data_end(meta: &BoxMetadata<'_>) -> std::io::Result<u64> {
        let mut data_end = BoxHeader::SIZE as u64;
        for record in &meta.records {
            let range = match record {
                Record::File(file) => Some((file.data.get(), file.length)),
                Record::ChunkedFile(file) => Some((file.data.get(), file.length)),
                _ => None,
            };
            let Some((data_offset, length)) = range else {
                continue;
            };
            let record_end = data_offset.checked_add(length).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "record data range overflows u64",
                )
            })?;
            data_end = data_end.max(record_end);
        }
        Ok(data_end)
    }

    /// Validate existing payloads against the trailer that precedes an append.
    pub fn existing_data_end_for_append(
        header: &BoxHeader,
        meta: &BoxMetadata<'_>,
        file_len: u64,
    ) -> std::io::Result<u64> {
        let trailer = header
            .trailer
            .ok_or_else(|| invalid_metadata("archive has no trailer"))?
            .get();
        if trailer > file_len {
            return Err(invalid_metadata(format!(
                "trailer offset {trailer} exceeds archive length {file_len}"
            )));
        }

        for record in &meta.records {
            let range = match record {
                Record::File(file) => Some((file.data.get(), file.length)),
                Record::ChunkedFile(file) => Some((file.data.get(), file.length)),
                _ => None,
            };
            let Some((data_offset, length)) = range else {
                continue;
            };
            if data_offset < BoxHeader::SIZE as u64 || data_offset > trailer {
                return Err(invalid_metadata(format!(
                    "record data offset {data_offset} is outside the pre-trailer payload envelope [{}, {trailer}]",
                    BoxHeader::SIZE
                )));
            }
            let record_end = data_offset
                .checked_add(length)
                .ok_or_else(|| invalid_metadata("record data range overflows u64"))?;
            if record_end > trailer {
                return Err(invalid_metadata(format!(
                    "record data end {record_end} exceeds trailer offset {trailer}"
                )));
            }
        }

        let data_end = Self::existing_data_end(meta)?;
        let aligned_end = checked_aligned_write_address(data_end, header.alignment)?.get();
        if aligned_end > trailer {
            return Err(invalid_metadata(format!(
                "aligned append position {aligned_end} exceeds trailer offset {trailer}"
            )));
        }
        Ok(data_end)
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
    // [spec:box:sem:sans-io.root.alignment]
    pub fn next_write_addr(&self) -> NonZeroU64 {
        self.checked_next_write_addr()
            .expect("ArchiveWriter position invariant violated")
    }

    /// Advance the write position after data has been written.
    ///
    /// Called by the frontend after writing file data.
    // [spec:box:sem:sans-io.root.alignment]
    pub fn advance_position(&mut self, bytes_written: u64) -> std::io::Result<()> {
        let position = self.position_after_advance(bytes_written)?;
        self.next_write_pos = position;
        Ok(())
    }

    pub(crate) fn position_after_advance(&self, bytes_written: u64) -> std::io::Result<u64> {
        let position = self
            .checked_next_write_addr()?
            .get()
            .checked_add(bytes_written)
            .ok_or_else(|| invalid_metadata("writer position overflows u64"))?;
        checked_aligned_write_address(position, self.header.alignment)?;
        Ok(position)
    }

    /// Set the write position explicitly.
    ///
    /// Use this when resuming from an existing archive.
    pub fn set_position(&mut self, pos: u64) -> std::io::Result<()> {
        checked_aligned_write_address(pos, self.header.alignment)?;
        self.next_write_pos = pos;
        Ok(())
    }

    fn checked_next_write_addr(&self) -> std::io::Result<NonZeroU64> {
        checked_aligned_write_address(self.next_write_pos, self.header.alignment)
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
    // [spec:box:req:sans-io.root.hierarchy]
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

        // Now create this directory if it doesn't exist, otherwise return it only
        // when the existing record is itself a directory.
        if let Some(idx) = self.meta.index(&path) {
            if self
                .meta
                .record(idx)
                .is_some_and(|record| record.as_directory().is_some())
            {
                Ok(idx)
            } else {
                Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Path {:?} exists and is not a directory", path),
                ))
            }
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
        if block_size == 0 || decompressed_len == 0 || block_entries.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "chunked files require a positive block size and at least one non-empty block",
            ));
        }
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
    // [spec:box:syn:chunked-io.root.block-index-entry]
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
    // [spec:box:req:sans-io.root.hierarchy]
    // [spec:box:req:records.root.references.insertion-target]
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
    // [spec:box:req:records.root.references.external-header-flag]
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
        attrs_map: HashMap<String, Vec<u8>>,
    ) -> std::io::Result<HashMap<usize, Box<[u8]>>> {
        use crate::attrs;

        // Set archive-level uid/gid defaults from first file if not already set
        if let Some(uid) = attrs_map.get(attrs::UNIX_UID) {
            let uid_key = self
                .meta
                .attr_key_or_create(attrs::UNIX_UID, AttrType::Vu32)?;
            self.meta
                .attrs
                .entry(uid_key)
                .or_insert_with(|| uid.clone().into_boxed_slice());
        }
        if let Some(gid) = attrs_map.get(attrs::UNIX_GID) {
            let gid_key = self
                .meta
                .attr_key_or_create(attrs::UNIX_GID, AttrType::Vu32)?;
            self.meta
                .attrs
                .entry(gid_key)
                .or_insert_with(|| gid.clone().into_boxed_slice());
        }

        // Filter out uid/gid that match archive defaults
        let filtered: Vec<_> = {
            let default_uid = self
                .meta
                .attr_key(attrs::UNIX_UID)
                .and_then(|k| self.meta.attrs.get(&k).map(|v| &**v));
            let default_gid = self
                .meta
                .attr_key(attrs::UNIX_GID)
                .and_then(|k| self.meta.attrs.get(&k).map(|v| &**v));

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

        // Convert keys
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
    // [spec:box:sem:sans-io.root.finalization]
    pub fn finish(&mut self) -> std::io::Result<(u64, Vec<u8>)> {
        // Build both indexes before mutating metadata so a malformed existing
        // index leaves the writer retryable after the caller fixes its input.
        let fst_bytes = self.build_fst()?;
        let block_fst_bytes = self.build_block_fst()?;

        self.meta.fst = fst_bytes
            .as_ref()
            .and_then(|bytes| box_fst::Fst::new(Cow::Owned(bytes.clone())).ok());
        self.meta.block_fst = block_fst_bytes
            .as_ref()
            .and_then(|bytes| box_fst::Fst::new(Cow::Owned(bytes.clone())).ok());

        // Writers always encode v1 metadata. Opening a legacy archive for
        // append therefore upgrades its header together with its trailer.
        self.header.version = crate::header::VERSION;

        // Set trailer offset
        let trailer_offset = self.checked_next_write_addr()?.get();
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

    // [spec:box:req:sans-io.root.hierarchy]
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

                let parent_record = self.meta.record(parent_index).ok_or_else(|| {
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

                let new_index = self.meta.insert_record(record);
                tracing::trace!("Inserted with index: {:?}", &new_index);
                let Some(parent) = self
                    .meta
                    .record_mut(parent_index)
                    .and_then(Record::as_directory_mut)
                else {
                    self.meta.records.pop();
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
                let new_index = self.meta.insert_record(record);
                self.meta.root.push(new_index);
                Ok(new_index)
            }
        }
    }

    fn build_fst(&self) -> std::io::Result<Option<Vec<u8>>> {
        let mut paths = BTreeMap::<Vec<u8>, u64>::new();
        let mut record_paths = HashMap::<RecordIndex, Vec<u8>>::new();
        let mut existing_directories = Vec::new();

        // A decoded v1 archive has no root vector: its FST is the authoritative
        // source for every pre-existing path. Preserve all of those entries and
        // remember directory paths so children appended beneath an existing
        // directory can be collected without reconstructing a v1 root tree.
        if let Some(fst) = &self.meta.fst {
            for (path, value) in fst.prefix_iter(&[]) {
                let index = RecordIndex::try_new(value)
                    .ok_or_else(|| invalid_metadata("path FST contains the zero record index"))?;
                self.merge_path_entry(&mut paths, &mut record_paths, path.clone(), index)?;

                let record = self.meta.record(index).ok_or_else(|| {
                    invalid_metadata(format!(
                        "path FST references missing record index {}",
                        index.get()
                    ))
                })?;
                if let Record::Directory(directory) = record
                    && !directory.entries.is_empty()
                {
                    existing_directories.push((index, path, directory.entries.clone()));
                }
            }
        }

        let mut hierarchy_paths = HashMap::<RecordIndex, Vec<u8>>::new();
        let mut expanded_directories = HashSet::<RecordIndex>::new();

        // Existing v1 directories may acquire children while the writer is
        // open. Seed traversal from their FST paths rather than relying on the
        // intentionally empty v1 root vector.
        for (index, _, _) in &existing_directories {
            expanded_directories.insert(*index);
        }
        for (_, prefix, entries) in existing_directories {
            self.collect_paths(
                &entries,
                &prefix,
                &mut paths,
                &mut record_paths,
                &mut hierarchy_paths,
                &mut expanded_directories,
            )?;
        }

        // New root records and legacy v0 trees are represented by root/child
        // indices. This traversal is iterative and rejects invalid graphs.
        self.collect_paths(
            &self.meta.root,
            &[],
            &mut paths,
            &mut record_paths,
            &mut hierarchy_paths,
            &mut expanded_directories,
        )?;

        // Empty archives have no FST
        if paths.is_empty() {
            return Ok(None);
        }

        let mut builder = box_fst::FstBuilder::new();
        for (path, index) in paths {
            builder
                .insert(&path, index)
                .map_err(|error| invalid_metadata(format!("cannot build path FST: {error}")))?;
        }
        builder
            .finish()
            .map(Some)
            .map_err(|error| invalid_metadata(format!("cannot finish path FST: {error}")))
    }

    // [spec:box:syn:chunked-io.root.block-index-entry]
    fn build_block_fst(&self) -> std::io::Result<Option<Vec<u8>>> {
        let mut blocks = BTreeMap::<[u8; 16], u64>::new();

        // Reopened writers must retain block entries for chunked records that
        // were already present before this append session.
        if let Some(fst) = &self.meta.block_fst {
            for (key, offset) in fst.prefix_iter(&[]) {
                let key: [u8; 16] = key.try_into().map_err(|key: Vec<u8>| {
                    invalid_metadata(format!(
                        "block FST key has length {}, expected 16",
                        key.len()
                    ))
                })?;
                self.merge_block_entry(&mut blocks, key, offset)?;
            }
        }

        for &(key, offset) in &self.block_entries {
            self.merge_block_entry(&mut blocks, key, offset)?;
        }

        self.validate_block_sequences(&blocks)?;

        if blocks.is_empty() {
            return Ok(None);
        }

        let mut builder = box_fst::FstBuilder::new();
        for (key, offset) in blocks {
            builder
                .insert(&key, offset)
                .map_err(|error| invalid_metadata(format!("cannot build block FST: {error}")))?;
        }
        builder
            .finish()
            .map(Some)
            .map_err(|error| invalid_metadata(format!("cannot finish block FST: {error}")))
    }

    fn merge_path_entry(
        &self,
        paths: &mut BTreeMap<Vec<u8>, u64>,
        record_paths: &mut HashMap<RecordIndex, Vec<u8>>,
        path: Vec<u8>,
        index: RecordIndex,
    ) -> std::io::Result<()> {
        let record = self.meta.record(index).ok_or_else(|| {
            invalid_metadata(format!(
                "path index references missing record index {}",
                index.get()
            ))
        })?;
        validate_indexed_path_structure(&path, record.name())?;

        if let Some(existing_index) = paths.get(&path) {
            if *existing_index != index.get() {
                return Err(invalid_metadata(format!(
                    "path FST conflict for {:?}: record indices {} and {}",
                    String::from_utf8_lossy(&path),
                    existing_index,
                    index.get()
                )));
            }
        } else {
            paths.insert(path.clone(), index.get());
        }

        if let Some(existing_path) = record_paths.get(&index) {
            if existing_path != &path {
                return Err(invalid_metadata(format!(
                    "record index {} is mapped to both {:?} and {:?}",
                    index.get(),
                    String::from_utf8_lossy(existing_path),
                    String::from_utf8_lossy(&path)
                )));
            }
        } else {
            record_paths.insert(index, path);
        }

        Ok(())
    }

    fn merge_block_entry(
        &self,
        blocks: &mut BTreeMap<[u8; 16], u64>,
        key: [u8; 16],
        physical_offset: u64,
    ) -> std::io::Result<()> {
        let record_index_value = u64::from_be_bytes(key[..8].try_into().unwrap());
        let logical_offset = u64::from_be_bytes(key[8..].try_into().unwrap());
        let record_index = RecordIndex::try_new(record_index_value)
            .ok_or_else(|| invalid_metadata("block FST contains the zero record index"))?;
        let record = self
            .meta
            .record(record_index)
            .and_then(Record::as_chunked_file)
            .ok_or_else(|| {
                invalid_metadata(format!(
                    "block FST references non-chunked or missing record index {}",
                    record_index.get()
                ))
            })?;

        if logical_offset >= record.decompressed_length {
            return Err(invalid_metadata(format!(
                "block FST logical offset {logical_offset} is outside record {} with logical length {}",
                record_index.get(),
                record.decompressed_length
            )));
        }
        let data_end = record
            .data
            .get()
            .checked_add(record.length)
            .ok_or_else(|| {
                invalid_metadata(format!(
                    "chunked record {} data range overflows u64",
                    record_index.get()
                ))
            })?;
        if physical_offset < record.data.get() || physical_offset >= data_end {
            return Err(invalid_metadata(format!(
                "block FST physical offset {physical_offset} is outside record {} data range [{}, {data_end})",
                record_index.get(),
                record.data.get()
            )));
        }

        if let Some(existing_offset) = blocks.get(&key) {
            if *existing_offset != physical_offset {
                return Err(invalid_metadata(format!(
                    "block FST conflict for record {} logical offset {logical_offset}: physical offsets {existing_offset} and {physical_offset}",
                    record_index.get()
                )));
            }
        } else {
            blocks.insert(key, physical_offset);
        }

        Ok(())
    }

    fn validate_block_sequences(&self, blocks: &BTreeMap<[u8; 16], u64>) -> std::io::Result<()> {
        for (position, record) in self.meta.records.iter().enumerate() {
            let Record::ChunkedFile(record) = record else {
                continue;
            };
            let record_index_value = u64::try_from(position)
                .ok()
                .and_then(|position| position.checked_add(1))
                .ok_or_else(|| invalid_metadata("record index overflows u64"))?;
            let record_index = RecordIndex::new(record_index_value)?;
            if record.block_size == 0 || record.length == 0 || record.decompressed_length == 0 {
                return Err(invalid_metadata(format!(
                    "chunked record {} has an empty data or logical block envelope",
                    record_index.get()
                )));
            }

            let mut first_key = [0u8; 16];
            first_key[..8].copy_from_slice(&record_index.get().to_be_bytes());
            let mut last_key = first_key;
            last_key[8..].fill(u8::MAX);
            let entries: Vec<_> = blocks
                .range(first_key..=last_key)
                .map(|(key, physical_offset)| {
                    (
                        u64::from_be_bytes(key[8..].try_into().unwrap()),
                        *physical_offset,
                    )
                })
                .collect();
            if entries.is_empty() {
                return Err(invalid_metadata(format!(
                    "chunked record {} has no block FST entries",
                    record_index.get()
                )));
            }

            let block_size = u64::from(record.block_size);
            let expected_last = (record.decompressed_length - 1) / block_size * block_size;
            let mut expected_logical = 0u64;
            let mut previous_physical = None;
            for (logical_offset, physical_offset) in entries.iter().copied() {
                if logical_offset != expected_logical {
                    return Err(invalid_metadata(format!(
                        "chunked record {} block starts at logical offset {logical_offset}, expected {expected_logical}",
                        record_index.get()
                    )));
                }
                if let Some(previous) = previous_physical
                    && physical_offset <= previous
                {
                    return Err(invalid_metadata(format!(
                        "chunked record {} block offsets are not strictly increasing: {previous} then {physical_offset}",
                        record_index.get()
                    )));
                }
                previous_physical = Some(physical_offset);
                expected_logical = expected_logical.checked_add(block_size).ok_or_else(|| {
                    invalid_metadata("chunked logical block offset overflows u64")
                })?;
            }

            if entries[0].1 != record.data.get() {
                return Err(invalid_metadata(format!(
                    "chunked record {} first physical block starts at {}, expected {}",
                    record_index.get(),
                    entries[0].1,
                    record.data.get()
                )));
            }
            if entries.last().unwrap().0 != expected_last {
                return Err(invalid_metadata(format!(
                    "chunked record {} last block starts at logical offset {}, expected {expected_last}",
                    record_index.get(),
                    entries.last().unwrap().0
                )));
            }
        }

        Ok(())
    }

    fn collect_paths(
        &self,
        entries: &[RecordIndex],
        prefix: &[u8],
        paths: &mut BTreeMap<Vec<u8>, u64>,
        record_paths: &mut HashMap<RecordIndex, Vec<u8>>,
        hierarchy_paths: &mut HashMap<RecordIndex, Vec<u8>>,
        expanded_directories: &mut HashSet<RecordIndex>,
    ) -> std::io::Result<()> {
        let mut pending = Vec::new();
        for &index in entries.iter().rev() {
            pending.push((index, prefix.to_vec()));
        }

        while let Some((index, prefix)) = pending.pop() {
            let record = self.meta.record(index).ok_or_else(|| {
                invalid_metadata(format!(
                    "directory hierarchy references missing record index {}",
                    index.get()
                ))
            })?;
            let name = record.name().as_bytes();

            let separator_len = usize::from(!prefix.is_empty());
            let path_len = prefix
                .len()
                .checked_add(separator_len)
                .and_then(|length| length.checked_add(name.len()))
                .ok_or_else(|| invalid_metadata("archive path length overflows usize"))?;
            let mut path = Vec::new();
            path.try_reserve_exact(path_len).map_err(|error| {
                std::io::Error::new(
                    std::io::ErrorKind::OutOfMemory,
                    format!("cannot allocate archive path: {error}"),
                )
            })?;
            path.extend_from_slice(&prefix);
            if !prefix.is_empty() {
                path.push(0x1f);
            }
            path.extend_from_slice(name);

            if let Some(previous_path) = hierarchy_paths.insert(index, path.clone()) {
                return Err(invalid_metadata(format!(
                    "record index {} appears more than once in the directory hierarchy at {:?} and {:?}",
                    index.get(),
                    String::from_utf8_lossy(&previous_path),
                    String::from_utf8_lossy(&path)
                )));
            }
            self.merge_path_entry(paths, record_paths, path.clone(), index)?;

            if let Record::Directory(directory) = record
                && expanded_directories.insert(index)
            {
                for &child in directory.entries.iter().rev() {
                    pending.push((child, path.clone()));
                }
            }
        }

        Ok(())
    }
}

fn invalid_metadata(message: impl Into<String>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message.into())
}

fn checked_aligned_write_address(offset: u64, alignment: u32) -> std::io::Result<NonZeroU64> {
    if offset < BoxHeader::SIZE as u64 {
        return Err(invalid_metadata(format!(
            "writer position {offset} overlaps the {}-byte header",
            BoxHeader::SIZE
        )));
    }

    let aligned = match u64::from(alignment) {
        0 => offset,
        alignment => {
            let remainder = offset % alignment;
            if remainder == 0 {
                offset
            } else {
                offset
                    .checked_add(alignment - remainder)
                    .ok_or_else(|| invalid_metadata("aligned writer position overflows u64"))?
            }
        }
    };
    if aligned == u64::MAX {
        return Err(invalid_metadata(
            "writer position leaves no address space for archive metadata",
        ));
    }
    NonZeroU64::new(aligned).ok_or_else(|| invalid_metadata("writer position must be non-zero"))
}

fn validate_indexed_path_structure(path: &[u8], record_name: &str) -> std::io::Result<()> {
    let path_str = std::str::from_utf8(path)
        .map_err(|_| invalid_metadata("path FST contains a non-UTF-8 path"))?;
    let mut components = path_str.split('\x1f');
    let mut final_component = None;
    for component in components.by_ref() {
        if component.is_empty() {
            return Err(invalid_metadata(format!(
                "path FST contains an empty path component in {:?}",
                String::from_utf8_lossy(path)
            )));
        }
        final_component = Some(component);
    }

    if final_component != Some(record_name) {
        return Err(invalid_metadata(format!(
            "path FST entry {:?} does not match record name {:?}",
            String::from_utf8_lossy(path),
            record_name
        )));
    }

    Ok(())
}

#[cfg(test)]
#[path = "writer_tests.rs"]
mod tests;
