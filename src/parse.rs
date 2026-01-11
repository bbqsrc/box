//! Sans-IO parsing primitives for the Box format.
//!
//! These functions work on byte slices without any I/O traits, making them
//! suitable for use in async runtimes, sync contexts, or even kernel space.
//!
//! All functions return `(value, bytes_consumed)` on success, allowing the
//! caller to manage buffer positions.

use crate::compat::{Box, Cow, String, Vec};
use crate::compression::constants::*;
use crate::core::RecordIndex;
use crate::{BoxPath, Compression};

// For to_string() method on &str in no_std
#[cfg(feature = "alloc")]
use alloc::string::ToString;

/// Error type for parsing operations.
#[derive(Debug)]
pub enum ParseError {
    /// Need more bytes to complete parsing. Contains minimum additional bytes needed.
    NeedMoreBytes(usize),
    /// Invalid data encountered.
    InvalidData(&'static str),
    /// Invalid UTF-8 in string.
    InvalidUtf8,
    /// Unknown record type encountered.
    UnknownRecordType(u8),
}

impl core::fmt::Display for ParseError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ParseError::NeedMoreBytes(n) => write!(f, "need {} more bytes", n),
            ParseError::InvalidData(msg) => write!(f, "invalid data: {}", msg),
            ParseError::InvalidUtf8 => write!(f, "invalid UTF-8"),
            ParseError::UnknownRecordType(id) => write!(f, "unknown record type: 0x{:02x}", id),
        }
    }
}

impl core::error::Error for ParseError {}

#[cfg(feature = "std")]
impl From<ParseError> for std::io::Error {
    fn from(e: ParseError) -> Self {
        match e {
            ParseError::NeedMoreBytes(_) => {
                std::io::Error::new(std::io::ErrorKind::UnexpectedEof, e)
            }
            ParseError::InvalidData(_)
            | ParseError::InvalidUtf8
            | ParseError::UnknownRecordType(_) => {
                std::io::Error::new(std::io::ErrorKind::InvalidData, e)
            }
        }
    }
}

pub type ParseResult<T> = Result<(T, usize), ParseError>;

// ============================================================================
// PRIMITIVE PARSERS
// ============================================================================

/// Parse a single byte.
#[inline]
pub fn parse_u8(data: &[u8]) -> ParseResult<u8> {
    if data.is_empty() {
        return Err(ParseError::NeedMoreBytes(1));
    }
    Ok((data[0], 1))
}

/// Parse a little-endian u32.
#[inline]
pub fn parse_u32_le(data: &[u8]) -> ParseResult<u32> {
    if data.len() < 4 {
        return Err(ParseError::NeedMoreBytes(4 - data.len()));
    }
    let bytes: [u8; 4] = data[..4].try_into().unwrap();
    Ok((u32::from_le_bytes(bytes), 4))
}

/// Parse a little-endian u64.
#[inline]
pub fn parse_u64_le(data: &[u8]) -> ParseResult<u64> {
    if data.len() < 8 {
        return Err(ParseError::NeedMoreBytes(8 - data.len()));
    }
    let bytes: [u8; 8] = data[..8].try_into().unwrap();
    Ok((u64::from_le_bytes(bytes), 8))
}

/// Parse a VLQ-encoded u64 (FastVint format).
///
/// FastVint uses prefix-based length encoding where the number of leading
/// zeros in the first byte determines the total byte count.
pub fn parse_vu64(data: &[u8]) -> ParseResult<u64> {
    if data.is_empty() {
        return Err(ParseError::NeedMoreBytes(1));
    }

    // Use fastvint's slice-based decoding (no_std compatible)
    let (value, len) = fastvint::decode_vu64_slice(data);

    if data.len() < len {
        return Err(ParseError::NeedMoreBytes(len - data.len()));
    }

    Ok((value, len))
}

/// Parse a zigzag-encoded i64 (Vi64 format).
pub fn parse_vi64(data: &[u8]) -> ParseResult<i64> {
    if data.is_empty() {
        return Err(ParseError::NeedMoreBytes(1));
    }

    // Use fastvint's slice-based decoding (no_std compatible)
    let (value, len) = fastvint::decode_vi64_slice(data);

    if data.len() < len {
        return Err(ParseError::NeedMoreBytes(len - data.len()));
    }

    Ok((value, len))
}

// ============================================================================
// STRING AND BYTES PARSERS
// ============================================================================

/// Parse a length-prefixed byte slice.
///
/// Returns the byte slice and total bytes consumed (including length prefix).
pub fn parse_bytes(data: &[u8]) -> ParseResult<&[u8]> {
    let (len, prefix_size) = parse_vu64(data)?;
    let len = len as usize;
    let total = prefix_size + len;

    if data.len() < total {
        return Err(ParseError::NeedMoreBytes(total - data.len()));
    }

    Ok((&data[prefix_size..total], total))
}

/// Parse a length-prefixed UTF-8 string.
///
/// Returns the string slice and total bytes consumed (including length prefix).
pub fn parse_str(data: &[u8]) -> ParseResult<&str> {
    let (bytes, consumed) = parse_bytes(data)?;
    let s = core::str::from_utf8(bytes).map_err(|_| ParseError::InvalidUtf8)?;
    Ok((s, consumed))
}

/// Parse a BoxPath (validated path string).
pub fn parse_boxpath(data: &[u8]) -> ParseResult<BoxPath<'_>> {
    let (s, consumed) = parse_str(data)?;
    let path = BoxPath(Cow::Borrowed(s));
    path.validate_basic()
        .map_err(|_| ParseError::InvalidData("invalid path"))?;
    Ok((path, consumed))
}

// ============================================================================
// RECORD PARSERS
// ============================================================================

/// Parsed record type and compression from the type/compression byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordHeader {
    pub record_type: RecordType,
    pub compression: Compression,
}

/// Record type enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordType {
    Directory,
    File,
    Symlink,
    ChunkedFile,
    ExternalSymlink,
    Unknown(u8),
}

/// Parse the record type/compression byte.
pub fn parse_record_header(data: &[u8]) -> ParseResult<RecordHeader> {
    let (byte, consumed) = parse_u8(data)?;

    let type_id = byte & 0x0F;
    let compression_id = byte & 0xF0;

    let record_type = match type_id {
        RECORD_TYPE_DIRECTORY => RecordType::Directory,
        RECORD_TYPE_FILE => RecordType::File,
        RECORD_TYPE_SYMLINK => RecordType::Symlink,
        RECORD_TYPE_CHUNKED_FILE => RecordType::ChunkedFile,
        RECORD_TYPE_EXTERNAL_SYMLINK => RecordType::ExternalSymlink,
        other => RecordType::Unknown(other),
    };

    let compression = match compression_id {
        COMPRESSION_STORED => Compression::Stored,
        COMPRESSION_ZSTD => Compression::Zstd,
        COMPRESSION_XZ => Compression::Xz,
        other => Compression::Unknown(other),
    };

    Ok((
        RecordHeader {
            record_type,
            compression,
        },
        consumed,
    ))
}

/// Parse a RecordIndex (1-based, non-zero Vu64).
pub fn parse_record_index(data: &[u8]) -> ParseResult<RecordIndex> {
    let (value, consumed) = parse_vu64(data)?;
    let index = RecordIndex::try_new(value)
        .ok_or(ParseError::InvalidData("record index must be non-zero"))?;
    Ok((index, consumed))
}

// ============================================================================
// ATTRMAP PARSERS
// ============================================================================

/// Parse the AttrMap header (byte count and entry count).
///
/// Returns (byte_count, entry_count, header_bytes_consumed).
pub fn parse_attrmap_header(data: &[u8]) -> ParseResult<(u64, u64)> {
    let (byte_count, n1) = parse_u64_le(data)?;
    let (entry_count, n2) = parse_vu64(&data[n1..])?;
    Ok(((byte_count, entry_count), n1 + n2))
}

/// Parse a single AttrMap entry (key index + value bytes).
///
/// Returns (key_index, value_bytes, total_consumed).
pub fn parse_attrmap_entry(data: &[u8]) -> ParseResult<(usize, &[u8])> {
    let (key, n1) = parse_vu64(data)?;
    let (value, n2) = parse_bytes(&data[n1..])?;
    Ok(((key as usize, value), n1 + n2))
}

// ============================================================================
// HEADER PARSERS
// ============================================================================

/// Magic bytes for Box format.
pub const MAGIC_BYTES: &[u8; 4] = b"\xffBOX";

/// Parsed Box header.
#[derive(Debug, Clone)]
pub struct HeaderData {
    pub version: u8,
    pub allow_external_symlinks: bool,
    pub allow_escapes: bool,
    pub alignment: u32,
    pub trailer_offset: u64,
}

/// Parse the 32-byte Box header.
pub fn parse_header(data: &[u8]) -> ParseResult<HeaderData> {
    if data.len() < 32 {
        return Err(ParseError::NeedMoreBytes(32 - data.len()));
    }

    // Magic bytes at 0x00
    if &data[0..4] != MAGIC_BYTES {
        return Err(ParseError::InvalidData("invalid magic bytes"));
    }

    // Version at 0x04
    let version = data[4];

    // Flags at 0x05
    let flags = data[5];
    let allow_external_symlinks = (flags & 0x01) != 0;
    let allow_escapes = (flags & 0x02) != 0;

    // Reserved1 at 0x06-0x07 (ignored)

    // Alignment at 0x08
    let alignment = u32::from_le_bytes(data[0x08..0x0C].try_into().unwrap());

    // Reserved2 at 0x0C-0x0F (ignored)

    // Trailer offset at 0x10
    let trailer_offset = u64::from_le_bytes(data[0x10..0x18].try_into().unwrap());

    // Reserved3 at 0x18-0x1F (ignored)

    Ok((
        HeaderData {
            version,
            allow_external_symlinks,
            allow_escapes,
            alignment,
            trailer_offset,
        },
        32,
    ))
}

// ============================================================================
// ATTRMAP PARSERS
// ============================================================================

/// Parse a complete AttrMap (header + all entries).
pub fn parse_attrmap(data: &[u8]) -> ParseResult<crate::AttrMap> {
    use crate::compat::HashMap;

    let mut pos = 0;
    let ((byte_count, entry_count), header_consumed) = parse_attrmap_header(data)?;
    pos += header_consumed;

    // byte_count includes the entry_count VLQ but not the byte_count u64 itself
    let _ = byte_count; // Used for validation/skipping in streaming contexts

    let mut map = HashMap::with_capacity(entry_count as usize);
    for _ in 0..entry_count {
        let ((key, value), consumed) = parse_attrmap_entry(&data[pos..])?;
        map.insert(key, value.to_vec().into_boxed_slice());
        pos += consumed;
    }

    Ok((map, pos))
}

// ============================================================================
// DICTIONARY PARSER
// ============================================================================

/// Parse a dictionary (Vu64 length prefix + bytes).
/// Returns None if length is 0.
pub fn parse_dictionary(data: &[u8]) -> ParseResult<Option<Box<[u8]>>> {
    let (len, prefix_consumed) = parse_vu64(data)?;
    if len == 0 {
        return Ok((None, prefix_consumed));
    }

    let len = len as usize;
    let total = prefix_consumed + len;
    if data.len() < total {
        return Err(ParseError::NeedMoreBytes(total - data.len()));
    }

    let dict = data[prefix_consumed..total].to_vec().into_boxed_slice();
    Ok((Some(dict), total))
}

// ============================================================================
// ATTR KEY PARSERS
// ============================================================================

use crate::core::{AttrKey, AttrType};

/// Parse attribute keys in v0 format (no type tag, defaults to Json).
pub fn parse_attr_keys_v0(data: &[u8]) -> ParseResult<Vec<AttrKey>> {
    let mut pos = 0;
    let (count, consumed) = parse_vu64(data)?;
    pos += consumed;

    let mut keys = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let (name, consumed) = parse_str(&data[pos..])?;
        pos += consumed;
        keys.push(AttrKey {
            name: name.to_string(),
            attr_type: AttrType::Json, // v0 default
        });
    }

    Ok((keys, pos))
}

/// Parse attribute keys in v1 format (with type tag byte).
pub fn parse_attr_keys_v1(data: &[u8]) -> ParseResult<Vec<AttrKey>> {
    let mut pos = 0;
    let (count, consumed) = parse_vu64(data)?;
    pos += consumed;

    let mut keys = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let (type_tag, consumed) = parse_u8(&data[pos..])?;
        pos += consumed;

        let attr_type = AttrType::from_u8(type_tag)
            .ok_or(ParseError::InvalidData("unknown attribute type tag"))?;

        let (name, consumed) = parse_str(&data[pos..])?;
        pos += consumed;

        keys.push(AttrKey {
            name: name.to_string(),
            attr_type,
        });
    }

    Ok((keys, pos))
}

// ============================================================================
// RECORD PARSERS
// ============================================================================

use crate::compat::NonZeroU64;
use crate::{
    ChunkedFileRecord, DirectoryRecord, ExternalLinkRecord, FileRecord, LinkRecord, Record,
};

/// Parse a FileRecord (v1 format - compression already known from header).
pub fn parse_file_record<'a>(
    data: &'a [u8],
    compression: Compression,
) -> ParseResult<FileRecord<'a>> {
    let mut pos = 0;

    let (length, consumed) = parse_u64_le(data)?;
    pos += consumed;

    let (decompressed_length, consumed) = parse_u64_le(&data[pos..])?;
    pos += consumed;

    let (data_offset, consumed) = parse_u64_le(&data[pos..])?;
    pos += consumed;

    let (name, consumed) = parse_str(&data[pos..])?;
    pos += consumed;

    let (attrs, consumed) = parse_attrmap(&data[pos..])?;
    pos += consumed;

    let data_ptr = NonZeroU64::new(data_offset)
        .ok_or(ParseError::InvalidData("file data offset must not be zero"))?;

    Ok((
        FileRecord {
            compression,
            length,
            decompressed_length,
            data: data_ptr,
            name: Cow::Borrowed(name),
            attrs,
        },
        pos,
    ))
}

/// Parse a ChunkedFileRecord (v1 format - compression already known from header).
pub fn parse_chunked_file_record<'a>(
    data: &'a [u8],
    compression: Compression,
) -> ParseResult<ChunkedFileRecord<'a>> {
    let mut pos = 0;

    let (block_size, consumed) = parse_u32_le(data)?;
    pos += consumed;

    let (length, consumed) = parse_u64_le(&data[pos..])?;
    pos += consumed;

    let (decompressed_length, consumed) = parse_u64_le(&data[pos..])?;
    pos += consumed;

    let (data_offset, consumed) = parse_u64_le(&data[pos..])?;
    pos += consumed;

    let (name, consumed) = parse_str(&data[pos..])?;
    pos += consumed;

    let (attrs, consumed) = parse_attrmap(&data[pos..])?;
    pos += consumed;

    let data_ptr = NonZeroU64::new(data_offset).ok_or(ParseError::InvalidData(
        "chunked file data offset must not be zero",
    ))?;

    Ok((
        ChunkedFileRecord {
            compression,
            block_size,
            length,
            decompressed_length,
            data: data_ptr,
            name: Cow::Borrowed(name),
            attrs,
        },
        pos,
    ))
}

/// Parse a DirectoryRecord (v1 format - no entries, looked up via FST).
pub fn parse_directory_record_v1(data: &[u8]) -> ParseResult<DirectoryRecord<'_>> {
    let mut pos = 0;

    let (name, consumed) = parse_str(data)?;
    pos += consumed;

    let (attrs, consumed) = parse_attrmap(&data[pos..])?;
    pos += consumed;

    Ok((
        DirectoryRecord {
            name: Cow::Borrowed(name),
            entries: Vec::new(), // v1: entries not serialized
            attrs,
        },
        pos,
    ))
}

/// Parse a LinkRecord.
pub fn parse_link_record(data: &[u8]) -> ParseResult<LinkRecord<'_>> {
    let mut pos = 0;

    let (name, consumed) = parse_str(data)?;
    pos += consumed;

    let (target, consumed) = parse_record_index(&data[pos..])?;
    pos += consumed;

    let (attrs, consumed) = parse_attrmap(&data[pos..])?;
    pos += consumed;

    Ok((
        LinkRecord {
            name: Cow::Borrowed(name),
            target,
            attrs,
        },
        pos,
    ))
}

/// Parse an ExternalLinkRecord.
pub fn parse_external_link_record(data: &[u8]) -> ParseResult<ExternalLinkRecord<'_>> {
    let mut pos = 0;

    let (name, consumed) = parse_str(data)?;
    pos += consumed;

    let (target, consumed) = parse_str(&data[pos..])?;
    pos += consumed;

    let (attrs, consumed) = parse_attrmap(&data[pos..])?;
    pos += consumed;

    Ok((
        ExternalLinkRecord {
            name: Cow::Borrowed(name),
            target: Cow::Borrowed(target),
            attrs,
        },
        pos,
    ))
}

/// Parse a Record in v1 format.
pub fn parse_record_v1(data: &[u8]) -> ParseResult<Record<'_>> {
    let (header, mut pos) = parse_record_header(data)?;

    let record = match header.record_type {
        RecordType::Directory => {
            let (dir, consumed) = parse_directory_record_v1(&data[pos..])?;
            pos += consumed;
            Record::Directory(dir)
        }
        RecordType::File => {
            let (file, consumed) = parse_file_record(&data[pos..], header.compression)?;
            pos += consumed;
            Record::File(file)
        }
        RecordType::ChunkedFile => {
            let (file, consumed) = parse_chunked_file_record(&data[pos..], header.compression)?;
            pos += consumed;
            Record::ChunkedFile(file)
        }
        RecordType::Symlink => {
            let (link, consumed) = parse_link_record(&data[pos..])?;
            pos += consumed;
            Record::Link(link)
        }
        RecordType::ExternalSymlink => {
            let (link, consumed) = parse_external_link_record(&data[pos..])?;
            pos += consumed;
            Record::ExternalLink(link)
        }
        RecordType::Unknown(id) => {
            return Err(ParseError::UnknownRecordType(id));
        }
    };

    Ok((record, pos))
}

// ============================================================================
// FST PARSER
// ============================================================================

/// Parse an FST (u64 length prefix + FST bytes).
/// Returns None if length is 0.
pub fn parse_fst(data: &[u8]) -> ParseResult<Option<box_fst::Fst<Cow<'_, [u8]>>>> {
    if data.len() < 8 {
        return Err(ParseError::NeedMoreBytes(8 - data.len()));
    }

    let (fst_length, _) = parse_u64_le(data)?;
    let fst_length = fst_length as usize;

    if fst_length == 0 {
        return Ok((None, 8));
    }

    let total = 8 + fst_length;
    if data.len() < total {
        return Err(ParseError::NeedMoreBytes(total - data.len()));
    }

    let fst_data = &data[8..total];
    let fst = box_fst::Fst::new(Cow::Borrowed(fst_data))
        .map_err(|_| ParseError::InvalidData("invalid FST data"))?;

    Ok((Some(fst), total))
}

// ============================================================================
// METADATA PARSER (V1)
// ============================================================================

use crate::BoxMetadata;

/// Parse BoxMetadata in v1 format.
///
/// v1 layout: attr_keys → attrs → dictionary → records → fst → block_fst
pub fn parse_metadata_v1(data: &[u8]) -> ParseResult<BoxMetadata<'_>> {
    let mut pos = 0;

    // v1: root not serialized (paths indexed by FST)
    let root = Vec::new();

    let (attr_keys, consumed) = parse_attr_keys_v1(data)?;
    pos += consumed;

    let (attrs, consumed) = parse_attrmap(&data[pos..])?;
    pos += consumed;

    let (dictionary, consumed) = parse_dictionary(&data[pos..])?;
    pos += consumed;

    let (record_count, consumed) = parse_vu64(&data[pos..])?;
    pos += consumed;

    let mut records = Vec::with_capacity(record_count as usize);
    for _ in 0..record_count {
        let (record, consumed) = parse_record_v1(&data[pos..])?;
        pos += consumed;
        records.push(record);
    }

    let (fst, consumed) = parse_fst(&data[pos..])?;
    pos += consumed;

    let (block_fst, consumed) = parse_fst(&data[pos..])?;
    pos += consumed;

    Ok((
        BoxMetadata {
            root,
            records,
            attr_keys,
            attrs,
            dictionary,
            fst,
            block_fst,
        },
        pos,
    ))
}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_u8() {
        assert_eq!(parse_u8(&[0x42]).unwrap(), (0x42, 1));
        assert!(matches!(parse_u8(&[]), Err(ParseError::NeedMoreBytes(1))));
    }

    #[test]
    fn test_parse_u32_le() {
        assert_eq!(
            parse_u32_le(&[0x01, 0x02, 0x03, 0x04]).unwrap(),
            (0x04030201, 4)
        );
        assert!(matches!(
            parse_u32_le(&[0x01, 0x02]),
            Err(ParseError::NeedMoreBytes(2))
        ));
    }

    #[test]
    fn test_parse_u64_le() {
        let bytes = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
        assert_eq!(parse_u64_le(&bytes).unwrap(), (0x0807060504030201, 8));
    }

    #[test]
    fn test_parse_vu64_single_byte() {
        // Values 0-127 fit in single byte with high bit set
        assert_eq!(parse_vu64(&[0x80]).unwrap(), (0, 1));
        assert_eq!(parse_vu64(&[0x81]).unwrap(), (1, 1));
        assert_eq!(parse_vu64(&[0xFF]).unwrap(), (127, 1));
    }

    #[test]
    fn test_parse_str() {
        // Length 5, then "hello"
        let data = [0x85, b'h', b'e', b'l', b'l', b'o'];
        let (s, consumed) = parse_str(&data).unwrap();
        assert_eq!(s, "hello");
        assert_eq!(consumed, 6);
    }

    #[test]
    fn test_parse_record_header() {
        // File with Zstd compression
        let (header, consumed) = parse_record_header(&[0x12]).unwrap();
        assert_eq!(header.record_type, RecordType::File);
        assert_eq!(header.compression, Compression::Zstd);
        assert_eq!(consumed, 1);

        // Directory (no compression)
        let (header, _) = parse_record_header(&[0x01]).unwrap();
        assert_eq!(header.record_type, RecordType::Directory);
        assert_eq!(header.compression, Compression::Stored);
    }

    #[test]
    fn test_parse_header() {
        let mut data = [0u8; 32];
        data[0..4].copy_from_slice(MAGIC_BYTES);
        data[4] = 1; // version
        data[5] = 0x03; // flags: both bits set
        data[0x08..0x0C].copy_from_slice(&4096u32.to_le_bytes()); // alignment
        data[0x10..0x18].copy_from_slice(&1024u64.to_le_bytes()); // trailer

        let (header, consumed) = parse_header(&data).unwrap();
        assert_eq!(header.version, 1);
        assert!(header.allow_external_symlinks);
        assert!(header.allow_escapes);
        assert_eq!(header.alignment, 4096);
        assert_eq!(header.trailer_offset, 1024);
        assert_eq!(consumed, 32);
    }

    #[test]
    fn test_parse_header_invalid_magic() {
        let data = [0u8; 32];
        assert!(matches!(
            parse_header(&data),
            Err(ParseError::InvalidData(_))
        ));
    }

    #[test]
    fn test_parse_header_too_short() {
        let data = [0xFFu8, b'B', b'O', b'X'];
        assert!(matches!(
            parse_header(&data),
            Err(ParseError::NeedMoreBytes(28))
        ));
    }
}
