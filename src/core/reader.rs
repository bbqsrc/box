//! Sans-IO archive reader state machine.
//!
//! `ArchiveReader` manages archive state without performing any I/O.
//! Frontends (async, sync, kernel) use this core for parsing and decompression,
//! while handling their own I/O operations.

// This module requires std for I/O operations and decompression
#![cfg(feature = "std")]

use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::compression::decompress_bytes_sync;
use crate::header::BoxHeader;
use crate::parse::{self, ParseError};
use crate::path::BoxPath;
use crate::record::{ChunkedFileRecord, FileRecord, LinkRecord, Record};

use super::meta::{AttrKey, AttrValue, BoxMetadata, MetadataIter, RecordIndex};

/// Sans-IO archive reader state machine.
///
/// This holds the parsed archive header and metadata, providing methods
/// for record lookup, attribute access, and decompression without I/O.
///
/// # Example
///
/// ```ignore
/// // Frontend reads 32 header bytes
/// let header = ArchiveReader::parse_header(&header_bytes)?;
///
/// // Frontend reads trailer bytes (mmap or read)
/// let meta = ArchiveReader::parse_metadata(&trailer_bytes, header.version)?;
///
/// // Create reader
/// let reader = ArchiveReader::new(header, meta, 0);
///
/// // Use reader for lookups and decompression
/// if let Some(record) = reader.find(&path) {
///     let (offset, len) = reader.file_location(record.as_file().unwrap())?;
///     // Frontend reads bytes at offset
///     let data = reader.decompress(record.as_file().unwrap(), &compressed_bytes)?;
/// }
/// ```
// [spec:box:def:archive-state.root]
// [spec:box:def:archive-state.root.reader]
// [spec:box:sem:sans-io.root]
pub struct ArchiveReader<'a> {
    /// Parsed 32-byte header
    pub header: BoxHeader,
    /// Parsed metadata (records, FST, attributes, dictionary)
    pub meta: BoxMetadata<'a>,
    /// Offset of this archive within the file (for embedded archives)
    pub offset: u64,
}

impl<'a> ArchiveReader<'a> {
    // ========================================================================
    // Construction (parsing from bytes)
    // ========================================================================

    /// Parse the 32-byte header from a byte buffer.
    ///
    /// Returns the parsed header data. The caller should use this to determine
    /// the trailer offset for reading metadata.
    // [spec:box:sem:sans-io.root.header-metadata]
    pub fn parse_header(buf: &[u8]) -> Result<BoxHeader, ParseError> {
        if buf.len() < 32 {
            return Err(ParseError::NeedMoreBytes(32 - buf.len()));
        }

        let (header_data, _) = parse::parse_header(buf)?;

        Ok(BoxHeader {
            version: header_data.version,
            allow_external_symlinks: header_data.allow_external_symlinks,
            allow_escapes: header_data.allow_escapes,
            alignment: header_data.alignment,
            trailer: std::num::NonZeroU64::new(header_data.trailer_offset),
        })
    }

    /// Parse metadata from a byte buffer (version-aware).
    ///
    /// For v0 archives, use `crate::de::v0::deserialize_metadata_borrowed`.
    /// For v1+ archives, use `crate::parse::parse_metadata_v1`.
    // [spec:box:sem:sans-io.root.header-metadata]
    pub fn parse_metadata(buf: &'a [u8], version: u8) -> Result<BoxMetadata<'a>, std::io::Error> {
        crate::de::deserialize_metadata_borrowed(buf, &mut 0, version)
    }

    /// Create a new reader from parsed parts.
    pub fn new(header: BoxHeader, meta: BoxMetadata<'a>, offset: u64) -> Self {
        Self {
            header,
            meta,
            offset,
        }
    }

    /// Convert to owned metadata (useful for storing beyond the borrow lifetime).
    pub fn into_owned(self) -> ArchiveReader<'static> {
        ArchiveReader {
            header: self.header,
            meta: self.meta.into_owned(),
            offset: self.offset,
        }
    }

    // ========================================================================
    // Header accessors
    // ========================================================================

    /// Get the archive format version.
    #[inline]
    pub fn version(&self) -> u8 {
        self.header.version
    }

    /// Get the alignment value.
    #[inline]
    pub fn alignment(&self) -> u32 {
        self.header.alignment
    }

    /// Check if path escapes (../) are allowed.
    #[inline]
    pub fn allow_escapes(&self) -> bool {
        self.header.allow_escapes
    }

    /// Check if external symlinks are allowed.
    #[inline]
    pub fn allow_external_symlinks(&self) -> bool {
        self.header.allow_external_symlinks
    }

    /// Get a reference to the metadata.
    #[inline]
    pub fn metadata(&self) -> &BoxMetadata<'a> {
        &self.meta
    }

    /// Get the dictionary for Zstd decompression (if any).
    #[inline]
    pub fn dictionary(&self) -> Option<&[u8]> {
        self.meta.dictionary()
    }

    // ========================================================================
    // Record lookup
    // ========================================================================

    /// Find a record by path.
    #[inline]
    pub fn find(&self, path: &BoxPath<'_>) -> Option<&Record<'a>> {
        self.meta.index(path).and_then(|idx| self.meta.record(idx))
    }

    /// Find a record by path, returning both index and record.
    #[inline]
    pub fn find_with_index(&self, path: &BoxPath<'_>) -> Option<(RecordIndex, &Record<'a>)> {
        self.meta
            .index(path)
            .and_then(|idx| self.meta.record(idx).map(|r| (idx, r)))
    }

    /// Get a record by index.
    #[inline]
    pub fn record(&self, index: RecordIndex) -> Option<&Record<'a>> {
        self.meta.record(index)
    }

    /// Iterate over all records in the archive.
    #[inline]
    pub fn iter(&self) -> MetadataIter<'_, 'a> {
        self.meta.iter()
    }

    // ========================================================================
    // File data location (for frontend to read)
    // ========================================================================

    /// Get the absolute file location for a file record.
    ///
    /// Returns (absolute_offset, length) for the frontend to read, or invalid
    /// data when the embedded archive base and record offset overflow.
    #[inline]
    // [spec:box:sem:sans-io.root.data-location]
    pub fn file_location(&self, record: &FileRecord<'_>) -> std::io::Result<(u64, u64)> {
        let offset = self.offset.checked_add(record.data.get()).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "archive and record data offsets overflow u64",
            )
        })?;
        Ok((offset, record.length))
    }

    /// Get the absolute file location for a chunked file record.
    ///
    /// Returns (absolute_offset, length) for the frontend to read, or invalid
    /// data when the embedded archive base and record offset overflow.
    #[inline]
    // [spec:box:sem:sans-io.root.data-location]
    pub fn chunked_location(&self, record: &ChunkedFileRecord<'_>) -> std::io::Result<(u64, u64)> {
        let offset = self.offset.checked_add(record.data.get()).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "archive and record data offsets overflow u64",
            )
        })?;
        Ok((offset, record.length))
    }

    // ========================================================================
    // Decompression (sync, buffer-to-buffer)
    // ========================================================================

    /// Decompress file data that was read by the frontend.
    ///
    /// Takes the compressed data and returns decompressed bytes.
    // [spec:box:sem:sans-io.root.data-location]
    pub fn decompress(&self, record: &FileRecord<'_>, data: &[u8]) -> std::io::Result<Vec<u8>> {
        decompress_bytes_sync(data, record.compression, self.meta.dictionary())
    }

    /// Decompress a single block from a chunked file.
    ///
    /// Takes the compressed block data and returns decompressed bytes.
    // [spec:box:sem:sans-io.root.data-location]
    pub fn decompress_chunked_block(
        &self,
        record: &ChunkedFileRecord<'_>,
        block_data: &[u8],
    ) -> std::io::Result<Vec<u8>> {
        decompress_bytes_sync(block_data, record.compression, self.meta.dictionary())
    }

    // ========================================================================
    // Attribute access
    // ========================================================================

    /// Get an attribute value for a record.
    #[inline]
    pub fn get_attr<'r>(&self, record: &'r Record<'_>, key: &str) -> Option<&'r [u8]> {
        let key_idx = self.meta.attr_key(key)?;
        record.attrs().get(&key_idx).map(|v| &**v)
    }

    /// Get a typed attribute value for a record.
    pub fn get_attr_value<'r>(&self, record: &'r Record<'_>, key: &str) -> Option<AttrValue<'r>> {
        let key_idx = self.meta.attr_key(key)?;
        let attr_type = self.meta.attr_key_type(key_idx)?;
        let bytes = record.attrs().get(&key_idx)?;
        Some(self.meta.parse_attr_value(bytes, attr_type))
    }

    /// Get the file mode (permissions) for a record, with fallback to default.
    // [spec:box:def:attributes.root.standard-keys]
    pub fn get_mode(&self, record: &Record<'_>) -> u32 {
        // Try to get from record attrs first
        if let Some(value) = self.get_attr_value(record, crate::attrs::UNIX_MODE) {
            if let AttrValue::Vu32(mode) = value {
                return mode;
            }
        }

        // Fall back to file-level default
        if let Some(bytes) = self.meta.file_attr(crate::attrs::UNIX_MODE) {
            if let Some(attr_type) = self
                .meta
                .attr_key(crate::attrs::UNIX_MODE)
                .and_then(|k| self.meta.attr_key_type(k))
            {
                if let AttrValue::Vu32(mode) = self.meta.parse_attr_value(bytes, attr_type) {
                    return mode;
                }
            }
        }

        // Default permissions based on record type
        match record {
            Record::Directory(_) => crate::fs::DEFAULT_DIR_MODE,
            Record::File(_) | Record::ChunkedFile(_) => crate::fs::DEFAULT_FILE_MODE,
            Record::Link(_) | Record::ExternalLink(_) => 0o777,
        }
    }

    /// Get all file-level attributes.
    #[inline]
    pub fn file_attrs(&self) -> BTreeMap<&str, AttrValue<'_>> {
        self.meta.file_attrs()
    }

    /// Get all attribute keys defined in the archive.
    #[inline]
    pub fn attr_keys(&self) -> &[AttrKey] {
        self.meta.attr_keys_with_types()
    }

    // ========================================================================
    // Symlink helpers
    // ========================================================================

    /// Resolve a link record to its target.
    ///
    /// Returns the target record index and record, or None if not found.
    pub fn resolve_link(&self, link: &LinkRecord<'_>) -> Option<(RecordIndex, &Record<'a>)> {
        let target_record = self.meta.record(link.target)?;
        Some((link.target, target_record))
    }

    /// Compute the relative path from a link's location to its target.
    ///
    /// Given the link's path and target's path, computes the relative symlink target
    /// (e.g., "../x86_64-unknown-linux-musl/libclang_rt.builtins.a").
    pub fn compute_relative_symlink_target(
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

    /// Get the path for a record index.
    ///
    /// This is O(n) for tree traversal, O(n) for FST iteration.
    /// Used primarily for resolving symlink targets during extraction.
    #[inline]
    pub fn path_for_index(&self, index: RecordIndex) -> Option<BoxPath<'static>> {
        self.meta.path_for_index(index)
    }

    /// Validate an indexed archive path before a frontend converts it to a
    /// platform path for extraction.
    pub(crate) fn validate_extraction_path(
        &self,
        path: &BoxPath<'_>,
        index: RecordIndex,
    ) -> std::io::Result<()> {
        self.meta
            .validate_extraction_path(path, index, self.header.allow_escapes)
    }

    /// Resolve a link target to an indexed path that is safe to materialize.
    pub(crate) fn extraction_path_for_index(
        &self,
        index: RecordIndex,
    ) -> std::io::Result<BoxPath<'static>> {
        let path = self.meta.path_for_index(index).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("could not find path for record index {}", index.get()),
            )
        })?;
        self.validate_extraction_path(&path, index)?;
        Ok(path)
    }

    /// Return validated direct children with their authoritative logical paths.
    pub(crate) fn extraction_children_by_index(
        &self,
        dir_index: RecordIndex,
        dir_path: &BoxPath<'_>,
    ) -> std::io::Result<Vec<(BoxPath<'static>, RecordIndex)>> {
        self.meta
            .extraction_children_by_index(dir_index, dir_path, self.header.allow_escapes)
    }

    // ========================================================================
    // Chunked file block lookup
    // ========================================================================

    /// Get all blocks for a chunked file record.
    ///
    /// Returns a vector of (logical_offset, physical_offset) pairs.
    #[inline]
    // [spec:box:sem:chunked-io.root.block-queries]
    pub fn blocks_for_record(&self, index: RecordIndex) -> Vec<(u64, u64)> {
        self.meta.blocks_for_record(index)
    }

    /// Find the block containing a logical offset within a chunked file.
    ///
    /// Returns (physical_offset, block_logical_offset) for the block containing
    /// the given logical offset, or None if not found.
    #[inline]
    // [spec:box:sem:chunked-io.root.block-queries]
    pub fn find_block(&self, index: RecordIndex, logical_offset: u64) -> Option<(u64, u64)> {
        self.meta.find_block(index, logical_offset)
    }

    /// Find the next block after a given logical offset.
    ///
    /// Returns (next_logical_offset, physical_offset), or None if no more blocks.
    #[inline]
    // [spec:box:sem:chunked-io.root.block-queries]
    pub fn next_block(&self, index: RecordIndex, current_logical: u64) -> Option<(u64, u64)> {
        self.meta.next_block(index, current_logical)
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::num::NonZeroU64;

    use super::*;
    use crate::compression::Compression;
    use crate::core::meta::{AttrMap, AttrType};

    fn file_record_with_attrs(attrs: AttrMap) -> Record<'static> {
        Record::File(FileRecord {
            compression: Compression::Stored,
            length: 0,
            decompressed_length: 0,
            data: NonZeroU64::new(32).unwrap(),
            name: Cow::Borrowed("file"),
            attrs,
        })
    }

    #[test]
    // [spec:box:sem:sans-io.root.header-metadata/test]
    fn test_parse_header() {
        let mut buf = [0u8; 32];
        buf[0..4].copy_from_slice(b"\xffBOX");
        buf[4] = 1; // version
        buf[5] = 0x03; // flags: both bits set
        buf[0x08..0x0C].copy_from_slice(&4096u32.to_le_bytes()); // alignment
        buf[0x10..0x18].copy_from_slice(&1024u64.to_le_bytes()); // trailer

        let header = ArchiveReader::parse_header(&buf).unwrap();
        assert_eq!(header.version, 1);
        assert!(header.allow_external_symlinks);
        assert!(header.allow_escapes);
        assert_eq!(header.alignment, 4096);
        assert_eq!(header.trailer.unwrap().get(), 1024);
    }

    #[test]
    // [spec:box:sem:sans-io.root.header-metadata/test]
    fn test_parse_header_invalid_magic() {
        let buf = [0u8; 32];
        assert!(ArchiveReader::parse_header(&buf).is_err());
    }

    #[test]
    // [spec:box:sem:sans-io.root.header-metadata/test]
    fn test_parse_header_too_short() {
        let buf = b"\xffBOX";
        assert!(matches!(
            ArchiveReader::parse_header(buf),
            Err(ParseError::NeedMoreBytes(28))
        ));
    }

    // [spec:box:def:versioning.root.v0/test/unit]
    // [spec:box:req:wire.root.bounds.attrmap-envelope/test/unit]
    #[test]
    fn v0_metadata_accepts_legacy_inclusive_attrmap_byte_counts() {
        // v0-era writers measured attribute maps by seeking back over them,
        // so every declared byte count also covers its own u64 field.
        let mut buf = Vec::new();
        buf.extend_from_slice(&[0x81, 0x81]); // root: record index 1
        buf.push(0x81); // one record
        buf.push(0x00); // v0 whole-byte record type: File
        buf.push(0x00); // compression: Stored
        buf.extend_from_slice(&2u64.to_le_bytes()); // compressed length
        buf.extend_from_slice(&2u64.to_le_bytes()); // decompressed length
        buf.extend_from_slice(&32u64.to_le_bytes()); // data offset
        buf.push(0x84);
        buf.extend_from_slice(b"file");
        // file attrs: one entry (key 0, two value bytes) is 5 content bytes,
        // declared as 13
        buf.extend_from_slice(&13u64.to_le_bytes());
        buf.extend_from_slice(&[0x81, 0x80, 0x82, 0xAA, 0xBB]);
        buf.push(0x81); // one v0 attribute key (no type byte)
        buf.push(0x84);
        buf.extend_from_slice(b"user");
        // archive attrs close the trailer: empty map declared as 9
        buf.extend_from_slice(&9u64.to_le_bytes());
        buf.push(0x80);

        let meta = ArchiveReader::parse_metadata(&buf, 0).unwrap();
        assert_eq!(meta.root.len(), 1);
        assert_eq!(meta.root[0].get(), 1);
        assert_eq!(meta.attr_keys.len(), 1);
        assert_eq!(meta.attr_keys[0].name, "user");
        assert!(meta.attrs.is_empty());
        let Record::File(file) = &meta.records[0] else {
            panic!("expected a file record");
        };
        assert_eq!(file.name, "file");
        assert_eq!(
            file.attrs.get(&0).map(|v| v.as_ref()),
            Some(&[0xAA, 0xBB][..])
        );
    }

    // [spec:box:req:wire.root.bounds.attrmap-envelope/test/unit]
    #[test]
    fn v0_metadata_rejects_mismatched_attrmap_byte_counts() {
        let empty_v0 = |declared: u64| {
            let mut buf = Vec::new();
            buf.push(0x80); // no root entries
            buf.push(0x80); // no records
            buf.push(0x80); // no attribute keys
            buf.extend_from_slice(&declared.to_le_bytes());
            buf.push(0x80); // zero archive attributes
            buf
        };

        // Both the entries-only count and the legacy inclusive count parse.
        assert!(ArchiveReader::parse_metadata(&empty_v0(1), 0).is_ok());
        assert!(ArchiveReader::parse_metadata(&empty_v0(9), 0).is_ok());

        let err = ArchiveReader::parse_metadata(&empty_v0(5), 0).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    // [spec:box:sem:sans-io.root.data-location/test/unit]
    #[test]
    fn data_locations_reject_embedded_archive_offset_overflow() {
        let reader = ArchiveReader::new(BoxHeader::default(), BoxMetadata::default(), 1);
        let file = FileRecord {
            compression: Compression::Stored,
            length: 1,
            decompressed_length: 1,
            data: NonZeroU64::new(u64::MAX).unwrap(),
            name: Cow::Borrowed("file"),
            attrs: Default::default(),
        };
        let chunked = ChunkedFileRecord {
            compression: Compression::Stored,
            block_size: 1,
            length: 1,
            decompressed_length: 1,
            data: NonZeroU64::new(u64::MAX).unwrap(),
            name: Cow::Borrowed("chunked"),
            attrs: Default::default(),
        };

        for error in [
            reader.file_location(&file).unwrap_err(),
            reader.chunked_location(&chunked).unwrap_err(),
        ] {
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        }
    }

    #[test]
    // [spec:box:def:attributes.root.standard-keys/test/unit]
    fn record_u8_mode_uses_type_fallback() {
        let mut meta = BoxMetadata::default();
        let mode_key = meta
            .attr_key_or_create(crate::attrs::UNIX_MODE, AttrType::U8)
            .unwrap();
        let mut attrs = AttrMap::default();
        attrs.insert(mode_key, vec![0o7].into_boxed_slice());
        let record = file_record_with_attrs(attrs);
        let reader = ArchiveReader::new(BoxHeader::default(), meta, 0);

        assert_eq!(reader.get_mode(&record), crate::fs::DEFAULT_FILE_MODE);
    }

    #[test]
    // [spec:box:def:attributes.root.standard-keys/test/unit]
    fn default_u8_mode_uses_type_fallback() {
        let mut meta = BoxMetadata::default();
        let mode_key = meta
            .attr_key_or_create(crate::attrs::UNIX_MODE, AttrType::U8)
            .unwrap();
        meta.attrs.insert(mode_key, vec![0o7].into_boxed_slice());
        let record = file_record_with_attrs(AttrMap::default());
        let reader = ArchiveReader::new(BoxHeader::default(), meta, 0);

        assert_eq!(reader.get_mode(&record), crate::fs::DEFAULT_FILE_MODE);
    }
}
