//! Sans-IO parsing primitives for the Box format.
//!
//! These functions work on byte slices without any I/O traits, making them
//! suitable for use in async runtimes, sync contexts, or even kernel space.
//!
//! All functions return `(value, bytes_consumed)` on success, allowing the
//! caller to manage buffer positions.

#[cfg(not(feature = "std"))]
extern crate alloc;

use crate::compat::{Box, Cow, Vec};
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

#[inline]
fn checked_usize(value: u64, message: &'static str) -> Result<usize, ParseError> {
    usize::try_from(value).map_err(|_| ParseError::InvalidData(message))
}

#[inline]
fn checked_end(start: usize, len: usize, message: &'static str) -> Result<usize, ParseError> {
    start
        .checked_add(len)
        .ok_or(ParseError::InvalidData(message))
}

#[inline]
fn checked_count(
    data: &[u8],
    pos: usize,
    count: u64,
    conversion_message: &'static str,
    remaining_message: &'static str,
) -> Result<usize, ParseError> {
    let count = checked_usize(count, conversion_message)?;
    let remaining = data
        .len()
        .checked_sub(pos)
        .ok_or(ParseError::InvalidData("parser position exceeds input"))?;
    if count > remaining {
        return Err(ParseError::InvalidData(remaining_message));
    }
    Ok(count)
}

macro_rules! checked_advance {
    ($pos:expr, $consumed:expr, $message:expr $(,)?) => {
        checked_end(*$pos, $consumed, $message).map(|end| *$pos = end)
    };
}

macro_rules! remaining {
    ($data:expr, $pos:expr) => {
        $data
            .get($pos..)
            .ok_or(ParseError::InvalidData("parser position exceeds input"))
    };
}

/// Copy a complete FastVint into a padded buffer before calling the optimized
/// decoder. Some decoder implementations use wide loads after inspecting the
/// prefix, so the caller must establish the encoded length first.
#[inline]
fn preflight_fastvint(data: &[u8]) -> Result<([u8; 9], usize), ParseError> {
    let first = *data.first().ok_or(ParseError::NeedMoreBytes(1))?;
    let encoded_len = first.leading_zeros() as usize + 1;
    if data.len() < encoded_len {
        return Err(ParseError::NeedMoreBytes(encoded_len - data.len()));
    }

    let mut padded = [0u8; 9];
    padded[..encoded_len].copy_from_slice(&data[..encoded_len]);
    Ok((padded, encoded_len))
}

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
// [spec:box:def:wire.root.primitives]
// [spec:box:req:wire.root.bounds.lengths-and-counts]
pub fn parse_vu64(data: &[u8]) -> ParseResult<u64> {
    let (padded, encoded_len) = preflight_fastvint(data)?;
    let (value, decoded_len) = fastvint::decode_vu64_slice(&padded);
    if decoded_len != encoded_len {
        return Err(ParseError::InvalidData("invalid FastVint u64 encoding"));
    }
    Ok((value, encoded_len))
}

/// Parse a zigzag-encoded i64 (Vi64 format).
// [spec:box:def:wire.root.primitives]
// [spec:box:req:wire.root.bounds.lengths-and-counts]
pub fn parse_vi64(data: &[u8]) -> ParseResult<i64> {
    let (padded, encoded_len) = preflight_fastvint(data)?;
    let (value, decoded_len) = fastvint::decode_vi64_slice(&padded);
    if decoded_len != encoded_len {
        return Err(ParseError::InvalidData("invalid FastVint i64 encoding"));
    }
    Ok((value, encoded_len))
}

// ============================================================================
// STRING AND BYTES PARSERS
// ============================================================================

/// Parse a length-prefixed byte slice.
///
/// Returns the byte slice and total bytes consumed (including length prefix).
// [spec:box:req:wire.root.bounds.lengths-and-counts]
pub fn parse_bytes(data: &[u8]) -> ParseResult<&[u8]> {
    let (len, prefix_size) = parse_vu64(data)?;
    let len = checked_usize(len, "byte-string length does not fit usize")?;
    let total = checked_end(prefix_size, len, "byte-string endpoint overflows usize")?;

    if data.len() < total {
        return Err(ParseError::NeedMoreBytes(total - data.len()));
    }

    Ok((&data[prefix_size..total], total))
}

/// Parse a length-prefixed UTF-8 string.
///
/// Returns the string slice and total bytes consumed (including length prefix).
// [spec:box:def:wire.root.primitives]
pub fn parse_str(data: &[u8]) -> ParseResult<&str> {
    let (bytes, consumed) = parse_bytes(data)?;
    let s = core::str::from_utf8(bytes).map_err(|_| ParseError::InvalidUtf8)?;
    Ok((s, consumed))
}

/// Parse a BoxPath (validated path string).
// [spec:box:req:paths.root]
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
// [spec:box:req:records.root.type-byte]
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
        #[cfg(feature = "zstd")]
        COMPRESSION_ZSTD => Compression::Zstd,
        #[cfg(feature = "xz")]
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
    let (entry_count, n2) = parse_vu64(remaining!(data, n1)?)?;
    let consumed = checked_end(n1, n2, "attribute-map header length overflows usize")?;
    Ok(((byte_count, entry_count), consumed))
}

/// Parse a single AttrMap entry (key index + value bytes).
///
/// Returns (key_index, value_bytes, total_consumed).
pub fn parse_attrmap_entry(data: &[u8]) -> ParseResult<(usize, &[u8])> {
    let (key, n1) = parse_vu64(data)?;
    let key = checked_usize(key, "attribute key index does not fit usize")?;
    let (value, n2) = parse_bytes(remaining!(data, n1)?)?;
    let consumed = checked_end(n1, n2, "attribute-map entry length overflows usize")?;
    Ok(((key, value), consumed))
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
// [spec:box:req:wire.root.header]
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
// [spec:box:def:attributes.root]
// [spec:box:req:wire.root.bounds.attrmap-envelope]
// [spec:box:req:wire.root.bounds.lengths-and-counts]
pub fn parse_attrmap(data: &[u8]) -> ParseResult<crate::AttrMap> {
    use crate::compat::HashMap;

    // byte_count includes the entry_count VLQ but not the byte_count u64 itself
    let (byte_count, byte_count_consumed) = parse_u64_le(data)?;
    let byte_count = checked_usize(byte_count, "attribute-map byte count does not fit usize")?;
    let envelope_end = checked_end(
        byte_count_consumed,
        byte_count,
        "attribute-map endpoint overflows usize",
    )?;
    if data.len() < envelope_end {
        return Err(ParseError::NeedMoreBytes(envelope_end - data.len()));
    }

    // Restrict all entry parsing to the declared envelope so malformed entries
    // cannot consume the next trailer field.
    let envelope = &data[byte_count_consumed..envelope_end];
    let (entry_count, mut pos) = parse_vu64(envelope)?;
    let entry_count = checked_count(
        envelope,
        pos,
        entry_count,
        "attribute-map entry count does not fit usize",
        "attribute-map entry count exceeds its declared byte envelope",
    )?;

    let mut map = HashMap::new();
    map.try_reserve(entry_count).map_err(|_| {
        ParseError::InvalidData("attribute-map entry count exceeds allocation limits")
    })?;

    for _ in 0..entry_count {
        let ((key, value), consumed) = parse_attrmap_entry(remaining!(envelope, pos)?)?;
        map.insert(key, value.to_vec().into_boxed_slice());
        checked_advance!(
            &mut pos,
            consumed,
            "attribute-map entry endpoint overflows usize",
        )?;
    }

    // Legacy writers measured the map by seeking back over it, so their
    // declared count also covers the byte-count field's own eight bytes and
    // the envelope extends eight bytes past the encoded entries.
    if pos != envelope.len() && pos.saturating_add(8) != envelope.len() {
        return Err(ParseError::InvalidData(
            "attribute-map byte count does not match encoded entries",
        ));
    }

    let consumed = checked_end(
        byte_count_consumed,
        pos,
        "attribute-map endpoint overflows usize",
    )?;
    Ok((map, consumed))
}

// ============================================================================
// DICTIONARY PARSER
// ============================================================================

/// Parse a dictionary (Vu64 length prefix + bytes).
/// Returns None if length is 0.
// [spec:box:req:wire.root.bounds.lengths-and-counts]
pub fn parse_dictionary(data: &[u8]) -> ParseResult<Option<Box<[u8]>>> {
    let (len, prefix_consumed) = parse_vu64(data)?;
    if len == 0 {
        return Ok((None, prefix_consumed));
    }

    let len = checked_usize(len, "dictionary length does not fit usize")?;
    let total = checked_end(prefix_consumed, len, "dictionary endpoint overflows usize")?;
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
// [spec:box:req:wire.root.bounds.lengths-and-counts]
pub fn parse_attr_keys_v0(data: &[u8]) -> ParseResult<Vec<AttrKey>> {
    let mut pos = 0;
    let (count, consumed) = parse_vu64(data)?;
    checked_advance!(
        &mut pos,
        consumed,
        "attribute-key count endpoint overflows usize",
    )?;
    let count = checked_count(
        data,
        pos,
        count,
        "attribute-key count does not fit usize",
        "attribute-key count exceeds remaining trailer bytes",
    )?;

    let mut keys = Vec::new();
    keys.try_reserve_exact(count)
        .map_err(|_| ParseError::InvalidData("attribute-key count exceeds allocation limits"))?;
    for _ in 0..count {
        let (name, consumed) = parse_str(remaining!(data, pos)?)?;
        checked_advance!(&mut pos, consumed, "attribute-key endpoint overflows usize")?;
        keys.push(AttrKey {
            name: name.to_string(),
            attr_type: AttrType::Json, // v0 default
        });
    }

    Ok((keys, pos))
}

/// Parse attribute keys in v1 format (with type tag byte).
// [spec:box:req:wire.root.bounds.lengths-and-counts]
pub fn parse_attr_keys_v1(data: &[u8]) -> ParseResult<Vec<AttrKey>> {
    let mut pos = 0;
    let (count, consumed) = parse_vu64(data)?;
    checked_advance!(
        &mut pos,
        consumed,
        "attribute-key count endpoint overflows usize",
    )?;
    let count = checked_count(
        data,
        pos,
        count,
        "attribute-key count does not fit usize",
        "attribute-key count exceeds remaining trailer bytes",
    )?;

    let mut keys = Vec::new();
    keys.try_reserve_exact(count)
        .map_err(|_| ParseError::InvalidData("attribute-key count exceeds allocation limits"))?;
    for _ in 0..count {
        let (type_tag, consumed) = parse_u8(remaining!(data, pos)?)?;
        checked_advance!(
            &mut pos,
            consumed,
            "attribute-key type endpoint overflows usize",
        )?;

        let attr_type = AttrType::from_u8(type_tag)
            .ok_or(ParseError::InvalidData("unknown attribute type tag"))?;

        let (name, consumed) = parse_str(remaining!(data, pos)?)?;
        checked_advance!(&mut pos, consumed, "attribute-key endpoint overflows usize")?;

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
// [spec:box:req:records.root.type-byte]
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
// [spec:box:req:fst-format.root.validation]
// [spec:box:req:wire.root.bounds.fst-envelope]
pub fn parse_fst(data: &[u8]) -> ParseResult<Option<box_fst::Fst<Cow<'_, [u8]>>>> {
    if data.len() < 8 {
        return Err(ParseError::NeedMoreBytes(8 - data.len()));
    }

    let (fst_length, _) = parse_u64_le(data)?;
    let fst_length = checked_usize(fst_length, "FST length does not fit usize")?;

    if fst_length == 0 {
        return Ok((None, 8));
    }

    let total = checked_end(8, fst_length, "FST endpoint overflows usize")?;
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
// [spec:box:req:versioning.root.v1]
// [spec:box:req:wire.root.bounds.lengths-and-counts]
pub fn parse_metadata_v1(data: &[u8]) -> ParseResult<BoxMetadata<'_>> {
    let mut pos = 0;

    // v1: root not serialized (paths indexed by FST)
    let root = Vec::new();

    let (attr_keys, consumed) = parse_attr_keys_v1(data)?;
    checked_advance!(
        &mut pos,
        consumed,
        "attribute-key section endpoint overflows usize",
    )?;

    let (attrs, consumed) = parse_attrmap(remaining!(data, pos)?)?;
    checked_advance!(
        &mut pos,
        consumed,
        "archive attribute-map endpoint overflows usize",
    )?;

    let (dictionary, consumed) = parse_dictionary(remaining!(data, pos)?)?;
    checked_advance!(
        &mut pos,
        consumed,
        "dictionary section endpoint overflows usize",
    )?;

    let (record_count, consumed) = parse_vu64(remaining!(data, pos)?)?;
    checked_advance!(&mut pos, consumed, "record-count endpoint overflows usize")?;
    let record_count = checked_count(
        data,
        pos,
        record_count,
        "record count does not fit usize",
        "record count exceeds remaining trailer bytes",
    )?;

    let mut records = Vec::new();
    records
        .try_reserve_exact(record_count)
        .map_err(|_| ParseError::InvalidData("record count exceeds allocation limits"))?;
    for _ in 0..record_count {
        let (record, consumed) = parse_record_v1(remaining!(data, pos)?)?;
        checked_advance!(
            &mut pos,
            consumed,
            "record section endpoint overflows usize",
        )?;
        records.push(record);
    }

    let (fst, consumed) = parse_fst(remaining!(data, pos)?)?;
    checked_advance!(&mut pos, consumed, "path-FST endpoint overflows usize")?;

    let block_fst = if pos == data.len() {
        None
    } else {
        let (block_fst, consumed) = parse_fst(remaining!(data, pos)?)?;
        checked_advance!(&mut pos, consumed, "block-FST endpoint overflows usize")?;
        block_fst
    };

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

    fn encoded_vu64(value: u64) -> Vec<u8> {
        fastvint::Vu64::new(value).bytes().to_vec()
    }

    fn empty_metadata(include_block_fst: bool) -> Vec<u8> {
        let mut data = Vec::new();
        data.push(0x80); // zero attribute keys
        data.extend_from_slice(&1u64.to_le_bytes());
        data.push(0x80); // zero archive attributes
        data.push(0x80); // no dictionary
        data.push(0x80); // zero records
        data.extend_from_slice(&0u64.to_le_bytes()); // no path FST
        if include_block_fst {
            data.extend_from_slice(&0u64.to_le_bytes());
        }
        data
    }

    #[test]
    fn test_parse_u8() {
        assert_eq!(parse_u8(&[0x42]).unwrap(), (0x42, 1));
        assert!(matches!(parse_u8(&[]), Err(ParseError::NeedMoreBytes(1))));
    }

    // [spec:box:def:wire.root.primitives/test]
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

    // [spec:box:def:wire.root.primitives/test]
    #[test]
    fn test_parse_u64_le() {
        let bytes = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
        assert_eq!(parse_u64_le(&bytes).unwrap(), (0x0807060504030201, 8));
    }

    // [spec:box:def:wire.root.primitives/test/unit]
    // [spec:box:req:wire.root.bounds.lengths-and-counts/test/unit]
    #[test]
    fn fastvint_parsers_preflight_truncated_wide_values() {
        let truncated = [0u8; 8];
        assert!(matches!(
            parse_vu64(&truncated),
            Err(ParseError::NeedMoreBytes(1))
        ));
        assert!(matches!(
            parse_vi64(&truncated),
            Err(ParseError::NeedMoreBytes(1))
        ));

        assert!(matches!(
            parse_vu64(&[0x01]),
            Err(ParseError::NeedMoreBytes(7))
        ));
        assert!(matches!(
            parse_vi64(&[0x01]),
            Err(ParseError::NeedMoreBytes(7))
        ));
    }

    // [spec:box:req:wire.root.bounds.lengths-and-counts/test/unit]
    // [spec:box:req:wire.root.bounds.fst-envelope/test/unit]
    #[test]
    fn length_envelopes_reject_u64_max_without_panicking() {
        let encoded_max = encoded_vu64(u64::MAX);
        assert!(matches!(
            parse_bytes(&encoded_max),
            Err(ParseError::InvalidData(_))
        ));
        assert!(matches!(
            parse_dictionary(&encoded_max),
            Err(ParseError::InvalidData(_))
        ));

        let fst = u64::MAX.to_le_bytes();
        assert!(matches!(parse_fst(&fst), Err(ParseError::InvalidData(_))));
    }

    // [spec:box:req:wire.root.bounds.lengths-and-counts/test/unit]
    // [spec:box:req:wire.root.bounds.fst-envelope/test/unit]
    #[test]
    fn length_envelopes_report_truncation() {
        assert!(matches!(
            parse_bytes(&[0x82, b'a']),
            Err(ParseError::NeedMoreBytes(1))
        ));
        assert!(matches!(
            parse_dictionary(&[0x82, b'a']),
            Err(ParseError::NeedMoreBytes(1))
        ));

        let mut fst = Vec::from(4u64.to_le_bytes());
        fst.extend_from_slice(&[0, 0]);
        assert!(matches!(parse_fst(&fst), Err(ParseError::NeedMoreBytes(2))));
    }

    // [spec:box:req:wire.root.bounds.attrmap-envelope/test/unit]
    #[test]
    fn attrmap_enforces_an_exact_declared_envelope() {
        let mut empty = Vec::from(1u64.to_le_bytes());
        empty.push(0x80);
        let (attrs, consumed) = parse_attrmap(&empty).unwrap();
        assert!(attrs.is_empty());
        assert_eq!(consumed, empty.len());

        // v0-era writers measured the map by seeking back over it, so an empty
        // map declares 9 bytes: its own u64 plus the one-byte entry count.
        let mut legacy_empty = Vec::from(9u64.to_le_bytes());
        legacy_empty.push(0x80);
        legacy_empty.extend_from_slice(&[0u8; 8]); // next trailer field
        let (attrs, consumed) = parse_attrmap(&legacy_empty).unwrap();
        assert!(attrs.is_empty());
        assert_eq!(consumed, 9);

        // One entry (key 1, two value bytes) is 5 content bytes, declared 13.
        let mut legacy_entry = Vec::from(13u64.to_le_bytes());
        legacy_entry.extend_from_slice(&[0x81, 0x81, 0x82, 0xAA, 0xBB]);
        legacy_entry.extend_from_slice(&[0u8; 8]);
        let (attrs, consumed) = parse_attrmap(&legacy_entry).unwrap();
        assert_eq!(consumed, 13);
        assert_eq!(attrs.get(&1).map(|v| v.as_ref()), Some(&[0xAA, 0xBB][..]));

        let mut trailing = Vec::from(2u64.to_le_bytes());
        trailing.extend_from_slice(&[0x80, 0xAA]);
        assert!(matches!(
            parse_attrmap(&trailing),
            Err(ParseError::InvalidData(_))
        ));
    }

    // [spec:box:req:wire.root.bounds.attrmap-envelope/test/unit]
    // [spec:box:req:wire.root.bounds.lengths-and-counts/test/unit]
    #[test]
    fn attrmap_cannot_bleed_into_the_following_field() {
        let mut data = Vec::from(2u64.to_le_bytes());
        data.extend_from_slice(&[0x81, 0x80]); // one entry and its key
        data.push(0x80); // a value length outside the declared envelope
        assert!(matches!(
            parse_attrmap(&data),
            Err(ParseError::NeedMoreBytes(1))
        ));

        let max_count = encoded_vu64(u64::MAX);
        let mut count_data = Vec::from((max_count.len() as u64).to_le_bytes());
        count_data.extend_from_slice(&max_count);
        assert!(matches!(
            parse_attrmap(&count_data),
            Err(ParseError::InvalidData(_))
        ));

        assert!(matches!(
            parse_attrmap(&u64::MAX.to_le_bytes()),
            Err(ParseError::InvalidData(_))
        ));
    }

    // [spec:box:req:wire.root.bounds.lengths-and-counts/test/unit]
    #[test]
    fn attribute_key_counts_are_bounded_before_allocation() {
        let encoded_max = encoded_vu64(u64::MAX);
        assert!(matches!(
            parse_attr_keys_v0(&encoded_max),
            Err(ParseError::InvalidData(_))
        ));
        assert!(matches!(
            parse_attr_keys_v1(&encoded_max),
            Err(ParseError::InvalidData(_))
        ));
        assert!(matches!(
            parse_attr_keys_v1(&[0x81]),
            Err(ParseError::InvalidData(_))
        ));
    }

    // [spec:box:req:versioning.root.v1/test/unit]
    // [spec:box:req:wire.root.bounds.lengths-and-counts/test/unit]
    #[test]
    fn metadata_rejects_an_unbounded_record_count() {
        let mut data = empty_metadata(false);
        data.truncate(11); // retain fields through the dictionary
        data.extend_from_slice(&encoded_vu64(u64::MAX));
        assert!(matches!(
            parse_metadata_v1(&data),
            Err(ParseError::InvalidData(_))
        ));
    }

    // [spec:box:req:versioning.root.v1/test/unit]
    #[test]
    fn metadata_accepts_empty_indexes_and_optional_block_fst() {
        for include_block_fst in [false, true] {
            let data = empty_metadata(include_block_fst);
            let (metadata, consumed) = parse_metadata_v1(&data).unwrap();
            assert_eq!(consumed, data.len());
            assert!(metadata.root.is_empty());
            assert!(metadata.records.is_empty());
            assert!(metadata.attr_keys.is_empty());
            assert!(metadata.attrs.is_empty());
            assert!(metadata.dictionary.is_none());
            assert!(metadata.fst.is_none());
            assert!(metadata.block_fst.is_none());
        }
    }

    // [spec:box:def:wire.root.primitives/test]
    #[test]
    fn test_parse_vu64_single_byte() {
        // Values 0-127 fit in single byte with high bit set
        assert_eq!(parse_vu64(&[0x80]).unwrap(), (0, 1));
        assert_eq!(parse_vu64(&[0x81]).unwrap(), (1, 1));
        assert_eq!(parse_vu64(&[0xFF]).unwrap(), (127, 1));
    }

    // [spec:box:def:wire.root.primitives/test]
    #[test]
    fn test_parse_str() {
        // Length 5, then "hello"
        let data = [0x85, b'h', b'e', b'l', b'l', b'o'];
        let (s, consumed) = parse_str(&data).unwrap();
        assert_eq!(s, "hello");
        assert_eq!(consumed, 6);
    }

    // [spec:box:req:records.root.type-byte/test]
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

    // [spec:box:req:wire.root.header/test]
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

    // [spec:box:req:wire.root.header/test]
    #[test]
    fn test_parse_header_invalid_magic() {
        let data = [0u8; 32];
        assert!(matches!(
            parse_header(&data),
            Err(ParseError::InvalidData(_))
        ));
    }

    // [spec:box:req:wire.root.header/test]
    #[test]
    fn test_parse_header_too_short() {
        let data = [0xFFu8, b'B', b'O', b'X'];
        assert!(matches!(
            parse_header(&data),
            Err(ParseError::NeedMoreBytes(28))
        ));
    }
}
