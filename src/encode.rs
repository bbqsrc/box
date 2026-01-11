//! Sans-IO encoding primitives for the Box format.
//!
//! These functions write to byte buffers without any I/O traits, making them
//! suitable for use in async runtimes, sync contexts, or even kernel space.
//!
//! All functions either write to a provided buffer or return encoded bytes.

use crate::compression::constants::*;
use crate::core::{AttrKey, RecordIndex};
use crate::{AttrMap, Compression};

// ============================================================================
// PRIMITIVE ENCODERS
// ============================================================================

/// Encode a u8 to a buffer.
#[inline]
pub fn encode_u8(buf: &mut Vec<u8>, value: u8) {
    buf.push(value);
}

/// Encode a little-endian u32 to a buffer.
#[inline]
pub fn encode_u32_le(buf: &mut Vec<u8>, value: u32) {
    buf.extend_from_slice(&value.to_le_bytes());
}

/// Encode a little-endian u64 to a buffer.
#[inline]
pub fn encode_u64_le(buf: &mut Vec<u8>, value: u64) {
    buf.extend_from_slice(&value.to_le_bytes());
}

/// Encode a VLQ u64 (FastVint format) to a buffer.
///
/// Returns the number of bytes written.
pub fn encode_vu64(buf: &mut Vec<u8>, value: u64) -> usize {
    let start_len = buf.len();
    // Reserve maximum possible size
    buf.reserve(9);

    // Use fastvint to encode
    let mut cursor = std::io::Cursor::new(Vec::with_capacity(9));
    fastvint::WriteVintExt::write_vu64(&mut cursor, value).unwrap();
    buf.extend_from_slice(cursor.get_ref());

    buf.len() - start_len
}

/// Encode a VLQ u64 and return the bytes directly.
///
/// Returns a fixed-size array with the encoded bytes and the actual length.
pub fn encode_vu64_array(value: u64) -> ([u8; 9], usize) {
    let mut buf = [0u8; 9];
    let mut cursor = std::io::Cursor::new(&mut buf[..]);
    fastvint::WriteVintExt::write_vu64(&mut cursor, value).unwrap();
    let len = cursor.position() as usize;
    (buf, len)
}

/// Encode a zigzag-encoded i64 (Vi64 format) to a buffer.
///
/// Returns the number of bytes written.
pub fn encode_vi64(buf: &mut Vec<u8>, value: i64) -> usize {
    let start_len = buf.len();
    let mut cursor = std::io::Cursor::new(Vec::with_capacity(9));
    fastvint::WriteVintExt::write_vi64(&mut cursor, value).unwrap();
    buf.extend_from_slice(cursor.get_ref());
    buf.len() - start_len
}

// ============================================================================
// STRING AND BYTES ENCODERS
// ============================================================================

/// Encode a length-prefixed byte slice.
pub fn encode_bytes(buf: &mut Vec<u8>, data: &[u8]) {
    encode_vu64(buf, data.len() as u64);
    buf.extend_from_slice(data);
}

/// Encode a length-prefixed UTF-8 string.
pub fn encode_str(buf: &mut Vec<u8>, s: &str) {
    encode_bytes(buf, s.as_bytes());
}

// ============================================================================
// RECORD ENCODERS
// ============================================================================

/// Encode the record type/compression byte.
#[inline]
pub fn encode_record_header(buf: &mut Vec<u8>, record_type: u8, compression: Compression) {
    buf.push(compression.id() | record_type);
}

/// Encode a RecordIndex (1-based, non-zero Vu64).
pub fn encode_record_index(buf: &mut Vec<u8>, index: RecordIndex) {
    encode_vu64(buf, index.get());
}

// ============================================================================
// ATTRMAP ENCODERS
// ============================================================================

/// Encode an AttrMap to a buffer.
///
/// The byte count field is calculated and written automatically.
pub fn encode_attrmap(buf: &mut Vec<u8>, attrs: &AttrMap) {
    // Reserve space for the u64 byte count
    let byte_count_pos = buf.len();
    buf.extend_from_slice(&[0u8; 8]); // placeholder

    let content_start = buf.len();

    // Entry count
    encode_vu64(buf, attrs.len() as u64);

    // Entries
    for (key, value) in attrs.iter() {
        encode_vu64(buf, *key as u64);
        encode_bytes(buf, value);
    }

    // Calculate and write byte count (excluding the u64 itself)
    let byte_count = (buf.len() - content_start) as u64;
    buf[byte_count_pos..byte_count_pos + 8].copy_from_slice(&byte_count.to_le_bytes());
}

/// Encode attribute keys (Vec<AttrKey>) to a buffer.
pub fn encode_attr_keys(buf: &mut Vec<u8>, keys: &[AttrKey]) {
    encode_vu64(buf, keys.len() as u64);
    for key in keys {
        buf.push(key.attr_type as u8);
        encode_str(buf, &key.name);
    }
}

// ============================================================================
// HEADER ENCODER
// ============================================================================

/// Magic bytes for Box format.
pub const MAGIC_BYTES: &[u8; 4] = b"\xffBOX";

/// Header configuration for encoding.
#[derive(Debug, Clone, Default)]
pub struct HeaderConfig {
    pub version: u8,
    pub allow_external_symlinks: bool,
    pub allow_escapes: bool,
    pub alignment: u32,
    pub trailer_offset: u64,
}

impl HeaderConfig {
    /// Create a new header config with default version.
    pub fn new() -> Self {
        Self {
            version: 1,
            ..Default::default()
        }
    }

    /// Set the trailer offset.
    pub fn with_trailer(mut self, offset: u64) -> Self {
        self.trailer_offset = offset;
        self
    }

    /// Set the alignment.
    pub fn with_alignment(mut self, alignment: u32) -> Self {
        self.alignment = alignment;
        self
    }
}

/// Encode the 32-byte Box header.
///
/// Always writes exactly 32 bytes.
pub fn encode_header(buf: &mut Vec<u8>, config: &HeaderConfig) {
    let start = buf.len();
    buf.reserve(32);

    // Magic bytes (0x00-0x03)
    buf.extend_from_slice(MAGIC_BYTES);

    // Version (0x04)
    buf.push(config.version);

    // Flags (0x05)
    let flags = ((config.allow_escapes as u8) << 1) | config.allow_external_symlinks as u8;
    buf.push(flags);

    // Reserved1 (0x06-0x07)
    buf.extend_from_slice(&[0u8; 2]);

    // Alignment (0x08-0x0B)
    buf.extend_from_slice(&config.alignment.to_le_bytes());

    // Reserved2 (0x0C-0x0F)
    buf.extend_from_slice(&[0u8; 4]);

    // Trailer offset (0x10-0x17)
    buf.extend_from_slice(&config.trailer_offset.to_le_bytes());

    // Reserved3 (0x18-0x1F)
    buf.extend_from_slice(&[0u8; 8]);

    debug_assert_eq!(buf.len() - start, 32);
}

/// Encode the 32-byte Box header to a fixed array.
pub fn encode_header_array(config: &HeaderConfig) -> [u8; 32] {
    let mut buf = [0u8; 32];

    // Magic bytes (0x00-0x03)
    buf[0..4].copy_from_slice(MAGIC_BYTES);

    // Version (0x04)
    buf[4] = config.version;

    // Flags (0x05)
    buf[5] = ((config.allow_escapes as u8) << 1) | config.allow_external_symlinks as u8;

    // Reserved1 (0x06-0x07) - already zero

    // Alignment (0x08-0x0B)
    buf[0x08..0x0C].copy_from_slice(&config.alignment.to_le_bytes());

    // Reserved2 (0x0C-0x0F) - already zero

    // Trailer offset (0x10-0x17)
    buf[0x10..0x18].copy_from_slice(&config.trailer_offset.to_le_bytes());

    // Reserved3 (0x18-0x1F) - already zero

    buf
}

// ============================================================================
// FST ENCODER
// ============================================================================

/// Encode an FST with u64 length prefix.
pub fn encode_fst(buf: &mut Vec<u8>, fst_bytes: Option<&[u8]>) {
    match fst_bytes {
        Some(bytes) => {
            encode_u64_le(buf, bytes.len() as u64);
            buf.extend_from_slice(bytes);
        }
        None => {
            encode_u64_le(buf, 0);
        }
    }
}

// ============================================================================
// RECORD-SPECIFIC ENCODERS
// ============================================================================

/// Encode a directory record (without the AttrMap, which needs separate handling).
pub fn encode_directory_record_header(buf: &mut Vec<u8>, name: &str) {
    buf.push(RECORD_TYPE_DIRECTORY);
    encode_str(buf, name);
}

/// Encode a file record header (type/compression + fixed fields, before name/attrs).
pub fn encode_file_record_header(
    buf: &mut Vec<u8>,
    compression: Compression,
    length: u64,
    decompressed_length: u64,
    data_offset: u64,
    name: &str,
) {
    buf.push(compression.id() | RECORD_TYPE_FILE);
    encode_u64_le(buf, length);
    encode_u64_le(buf, decompressed_length);
    encode_u64_le(buf, data_offset);
    encode_str(buf, name);
}

/// Encode a chunked file record header.
pub fn encode_chunked_file_record_header(
    buf: &mut Vec<u8>,
    compression: Compression,
    block_size: u32,
    length: u64,
    decompressed_length: u64,
    data_offset: u64,
    name: &str,
) {
    buf.push(compression.id() | RECORD_TYPE_CHUNKED_FILE);
    encode_u32_le(buf, block_size);
    encode_u64_le(buf, length);
    encode_u64_le(buf, decompressed_length);
    encode_u64_le(buf, data_offset);
    encode_str(buf, name);
}

/// Encode a symlink record header.
pub fn encode_symlink_record_header(buf: &mut Vec<u8>, name: &str, target: RecordIndex) {
    buf.push(RECORD_TYPE_SYMLINK);
    encode_str(buf, name);
    encode_record_index(buf, target);
}

/// Encode an external symlink record header.
pub fn encode_external_symlink_record_header(buf: &mut Vec<u8>, name: &str, target: &str) {
    buf.push(RECORD_TYPE_EXTERNAL_SYMLINK);
    encode_str(buf, name);
    encode_str(buf, target);
}

// ============================================================================
// DICTIONARY ENCODER
// ============================================================================

/// Encode a dictionary with Vu64 length prefix.
/// If None, writes length 0.
pub fn encode_dictionary(buf: &mut Vec<u8>, dict: Option<&[u8]>) {
    match dict {
        Some(bytes) => {
            encode_vu64(buf, bytes.len() as u64);
            buf.extend_from_slice(bytes);
        }
        None => {
            encode_vu64(buf, 0);
        }
    }
}

// ============================================================================
// FULL RECORD ENCODERS
// ============================================================================

use crate::{
    ChunkedFileRecord, DirectoryRecord, ExternalLinkRecord, FileRecord, LinkRecord, Record,
};

/// Encode a complete FileRecord (v1 format).
pub fn encode_file_record(buf: &mut Vec<u8>, record: &FileRecord<'_>) {
    buf.push(record.compression.id() | RECORD_TYPE_FILE);
    encode_u64_le(buf, record.length);
    encode_u64_le(buf, record.decompressed_length);
    encode_u64_le(buf, record.data.get());
    encode_str(buf, &record.name);
    encode_attrmap(buf, &record.attrs);
}

/// Encode a complete ChunkedFileRecord (v1 format).
pub fn encode_chunked_file_record(buf: &mut Vec<u8>, record: &ChunkedFileRecord<'_>) {
    buf.push(record.compression.id() | RECORD_TYPE_CHUNKED_FILE);
    encode_u32_le(buf, record.block_size);
    encode_u64_le(buf, record.length);
    encode_u64_le(buf, record.decompressed_length);
    encode_u64_le(buf, record.data.get());
    encode_str(buf, &record.name);
    encode_attrmap(buf, &record.attrs);
}

/// Encode a complete DirectoryRecord (v1 format - no entries).
pub fn encode_directory_record_v1(buf: &mut Vec<u8>, record: &DirectoryRecord<'_>) {
    buf.push(RECORD_TYPE_DIRECTORY);
    encode_str(buf, &record.name);
    // v1: entries not serialized (looked up via FST)
    encode_attrmap(buf, &record.attrs);
}

/// Encode a complete LinkRecord.
pub fn encode_link_record(buf: &mut Vec<u8>, record: &LinkRecord<'_>) {
    buf.push(RECORD_TYPE_SYMLINK);
    encode_str(buf, &record.name);
    encode_record_index(buf, record.target);
    encode_attrmap(buf, &record.attrs);
}

/// Encode a complete ExternalLinkRecord.
pub fn encode_external_link_record(buf: &mut Vec<u8>, record: &ExternalLinkRecord<'_>) {
    buf.push(RECORD_TYPE_EXTERNAL_SYMLINK);
    encode_str(buf, &record.name);
    encode_str(buf, &record.target);
    encode_attrmap(buf, &record.attrs);
}

/// Encode a Record (v1 format).
pub fn encode_record_v1(buf: &mut Vec<u8>, record: &Record<'_>) {
    match record {
        Record::File(r) => encode_file_record(buf, r),
        Record::ChunkedFile(r) => encode_chunked_file_record(buf, r),
        Record::Directory(r) => encode_directory_record_v1(buf, r),
        Record::Link(r) => encode_link_record(buf, r),
        Record::ExternalLink(r) => encode_external_link_record(buf, r),
    }
}

// ============================================================================
// METADATA ENCODER (V1)
// ============================================================================

use crate::BoxMetadata;

/// Encode BoxMetadata in v1 format.
///
/// v1 layout: attr_keys → attrs → dictionary → records → fst → block_fst
pub fn encode_metadata_v1(buf: &mut Vec<u8>, meta: &BoxMetadata<'_>) {
    // v1: root not serialized (paths indexed by FST)

    encode_attr_keys(buf, &meta.attr_keys);
    encode_attrmap(buf, &meta.attrs);
    encode_dictionary(buf, meta.dictionary.as_deref());

    // Records
    encode_vu64(buf, meta.records.len() as u64);
    for record in &meta.records {
        encode_record_v1(buf, record);
    }

    // FST
    encode_fst(buf, meta.fst.as_ref().map(|f| f.as_bytes()));

    // Block FST
    encode_fst(buf, meta.block_fst.as_ref().map(|f| f.as_bytes()));
}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse;

    #[test]
    fn test_encode_u32_le() {
        let mut buf = Vec::new();
        encode_u32_le(&mut buf, 0x04030201);
        assert_eq!(buf, [0x01, 0x02, 0x03, 0x04]);
    }

    #[test]
    fn test_encode_u64_le() {
        let mut buf = Vec::new();
        encode_u64_le(&mut buf, 0x0807060504030201);
        assert_eq!(buf, [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
    }

    #[test]
    fn test_encode_vu64_roundtrip() {
        for value in [0u64, 1, 127, 128, 16511, 16512, u64::MAX] {
            let mut buf = Vec::new();
            encode_vu64(&mut buf, value);
            let (parsed, consumed) = parse::parse_vu64(&buf).unwrap();
            assert_eq!(parsed, value);
            assert_eq!(consumed, buf.len());
        }
    }

    #[test]
    fn test_encode_str_roundtrip() {
        let mut buf = Vec::new();
        encode_str(&mut buf, "hello");
        let (parsed, consumed) = parse::parse_str(&buf).unwrap();
        assert_eq!(parsed, "hello");
        assert_eq!(consumed, buf.len());
    }

    #[test]
    fn test_encode_header_roundtrip() {
        let config = HeaderConfig {
            version: 1,
            allow_external_symlinks: true,
            allow_escapes: true,
            alignment: 4096,
            trailer_offset: 1024,
        };

        let mut buf = Vec::new();
        encode_header(&mut buf, &config);
        assert_eq!(buf.len(), 32);

        let (parsed, consumed) = parse::parse_header(&buf).unwrap();
        assert_eq!(consumed, 32);
        assert_eq!(parsed.version, 1);
        assert!(parsed.allow_external_symlinks);
        assert!(parsed.allow_escapes);
        assert_eq!(parsed.alignment, 4096);
        assert_eq!(parsed.trailer_offset, 1024);
    }

    #[test]
    fn test_encode_header_array() {
        let config = HeaderConfig::new().with_alignment(512).with_trailer(2048);

        let arr = encode_header_array(&config);
        let (parsed, _) = parse::parse_header(&arr).unwrap();
        assert_eq!(parsed.alignment, 512);
        assert_eq!(parsed.trailer_offset, 2048);
    }

    #[test]
    fn test_encode_record_header() {
        let mut buf = Vec::new();
        encode_record_header(&mut buf, RECORD_TYPE_FILE, Compression::Zstd);
        assert_eq!(buf, [0x12]); // 0x10 | 0x02

        let (parsed, _) = parse::parse_record_header(&buf).unwrap();
        assert_eq!(parsed.record_type, parse::RecordType::File);
        assert_eq!(parsed.compression, Compression::Zstd);
    }

    #[test]
    fn test_encode_attrmap() {
        let mut attrs = AttrMap::new();
        attrs.insert(0, vec![1, 2, 3].into_boxed_slice());
        attrs.insert(1, vec![4, 5].into_boxed_slice());

        let mut buf = Vec::new();
        encode_attrmap(&mut buf, &attrs);

        // Verify byte count is correct
        let byte_count = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        assert_eq!(byte_count as usize, buf.len() - 8);
    }

    #[test]
    fn test_encode_file_record_roundtrip() {
        use std::borrow::Cow;
        use std::num::NonZeroU64;

        let record = FileRecord {
            compression: Compression::Zstd,
            length: 1024,
            decompressed_length: 4096,
            data: NonZeroU64::new(0x1000).unwrap(),
            name: Cow::Borrowed("test.txt"),
            attrs: AttrMap::new(),
        };

        let mut buf = Vec::new();
        encode_file_record(&mut buf, &record);

        let (parsed, consumed) = parse::parse_record_v1(&buf).unwrap();
        assert_eq!(consumed, buf.len());

        let parsed_file = parsed.as_file().unwrap();
        assert_eq!(parsed_file.compression, Compression::Zstd);
        assert_eq!(parsed_file.length, 1024);
        assert_eq!(parsed_file.decompressed_length, 4096);
        assert_eq!(parsed_file.data.get(), 0x1000);
        assert_eq!(parsed_file.name.as_ref(), "test.txt");
    }

    #[test]
    fn test_encode_directory_record_roundtrip() {
        use std::borrow::Cow;

        let record = DirectoryRecord {
            name: Cow::Borrowed("mydir"),
            entries: Vec::new(),
            attrs: AttrMap::new(),
        };

        let mut buf = Vec::new();
        encode_directory_record_v1(&mut buf, &record);

        let (parsed, consumed) = parse::parse_record_v1(&buf).unwrap();
        assert_eq!(consumed, buf.len());

        let parsed_dir = parsed.as_directory().unwrap();
        assert_eq!(parsed_dir.name.as_ref(), "mydir");
    }

    #[test]
    fn test_encode_dictionary_roundtrip() {
        // With dictionary
        let dict = b"test dictionary data";
        let mut buf = Vec::new();
        encode_dictionary(&mut buf, Some(dict));

        let (parsed, consumed) = parse::parse_dictionary(&buf).unwrap();
        assert_eq!(consumed, buf.len());
        assert_eq!(parsed.as_deref(), Some(dict.as_slice()));

        // Without dictionary
        let mut buf = Vec::new();
        encode_dictionary(&mut buf, None);

        let (parsed, consumed) = parse::parse_dictionary(&buf).unwrap();
        assert_eq!(consumed, buf.len());
        assert!(parsed.is_none());
    }
}
