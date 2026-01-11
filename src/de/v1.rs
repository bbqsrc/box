//! v1 format deserialization (borrowed only).
//!
//! v1 differences from v0:
//! - Directory entries are NOT serialized (looked up via FST prefix queries)
//! - Metadata root is NOT serialized (paths indexed by FST)
//! - Attribute keys have a type tag byte before the name
//! - Combined type/compression byte (bits 0-3 = type, bits 4-7 = compression)

use std::borrow::Cow;
use std::num::NonZeroU64;

use crate::{
    BoxMetadata, ChunkedFileRecord, Compression, DirectoryRecord, ExternalLinkRecord, FileRecord,
    LinkRecord, Record,
    compression::constants::*,
    core::{AttrKey, AttrType},
};

use super::common::{AttrMapBorrowed, parse_fst_borrowed};
use super::{
    DeserializeBorrowed, read_u8_slice, read_u32_le_slice, read_u64_le_slice, read_vlq_u64,
};

// ============================================================================
// HELPERS
// ============================================================================

/// Parse combined type/compression byte.
/// Returns (record_type, compression).
fn parse_type_compression(byte: u8) -> (u8, Compression) {
    let record_type = byte & 0x0F;
    let compression_id = byte & 0xF0; // High nibble already in position
    let compression = match compression_id {
        COMPRESSION_STORED => Compression::Stored,
        COMPRESSION_ZSTD => Compression::Zstd,
        COMPRESSION_XZ => Compression::Xz,
        id => Compression::Unknown(id),
    };
    (record_type, compression)
}

// ============================================================================
// BORROWED DESERIALIZATION (v1)
// ============================================================================

/// Deserialize FileRecord in v1 format (compression already parsed from combined byte).
fn deserialize_file_borrowed<'a>(
    data: &'a [u8],
    pos: &mut usize,
    compression: Compression,
) -> std::io::Result<FileRecord<'a>> {
    let length = read_u64_le_slice(data, pos)?;
    let decompressed_length = read_u64_le_slice(data, pos)?;
    let data_offset = read_u64_le_slice(data, pos)?;
    let name = <Cow<'a, str>>::deserialize_borrowed(data, pos)?;
    let attrs = AttrMapBorrowed::deserialize_borrowed(data, pos)?;

    Ok(FileRecord {
        compression,
        length,
        decompressed_length,
        name,
        attrs,
        data: NonZeroU64::new(data_offset).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "file data offset must not be zero",
            )
        })?,
    })
}

/// Deserialize ChunkedFileRecord in v1 format (compression already parsed from combined byte).
fn deserialize_chunked_file_borrowed<'a>(
    data: &'a [u8],
    pos: &mut usize,
    compression: Compression,
) -> std::io::Result<ChunkedFileRecord<'a>> {
    let block_size = read_u32_le_slice(data, pos)?;
    let length = read_u64_le_slice(data, pos)?;
    let decompressed_length = read_u64_le_slice(data, pos)?;
    let data_offset = read_u64_le_slice(data, pos)?;
    let name = <Cow<'a, str>>::deserialize_borrowed(data, pos)?;
    let attrs = AttrMapBorrowed::deserialize_borrowed(data, pos)?;

    Ok(ChunkedFileRecord {
        compression,
        block_size,
        length,
        decompressed_length,
        name,
        attrs,
        data: NonZeroU64::new(data_offset).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "chunked file data offset must not be zero",
            )
        })?,
    })
}

/// Deserialize Vec<AttrKey> in v1 format (with type tag).
pub(crate) fn deserialize_attr_keys_borrowed<'a>(
    data: &'a [u8],
    pos: &mut usize,
) -> std::io::Result<Vec<AttrKey>> {
    let len = read_vlq_u64(data, pos)? as usize;
    let mut keys = Vec::with_capacity(len);
    for _ in 0..len {
        // v1: read type tag first
        let type_tag = read_u8_slice(data, pos)?;
        let attr_type = AttrType::from_u8(type_tag).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unknown attribute type tag: {}", type_tag),
            )
        })?;
        let name = <&'a str>::deserialize_borrowed(data, pos)?.to_string();
        keys.push(AttrKey { name, attr_type });
    }
    Ok(keys)
}

/// Deserialize DirectoryRecord in v1 format (no entries).
pub(crate) fn deserialize_directory_borrowed<'a>(
    data: &'a [u8],
    pos: &mut usize,
) -> std::io::Result<DirectoryRecord<'a>> {
    let name = <Cow<'a, str>>::deserialize_borrowed(data, pos)?;
    // v1: entries not in binary format (looked up via FST)
    let entries = Vec::new();
    let attrs = AttrMapBorrowed::deserialize_borrowed(data, pos)?;

    Ok(DirectoryRecord {
        name,
        entries,
        attrs,
    })
}

/// Deserialize Record in v1 format.
pub(crate) fn deserialize_record_borrowed<'a>(
    data: &'a [u8],
    pos: &mut usize,
) -> std::io::Result<Record<'a>> {
    let type_compression = read_u8_slice(data, pos)?;
    let (record_type, compression) = parse_type_compression(type_compression);

    let record = match record_type {
        RECORD_TYPE_DIRECTORY => Record::Directory(deserialize_directory_borrowed(data, pos)?),
        RECORD_TYPE_FILE => Record::File(deserialize_file_borrowed(data, pos, compression)?),
        RECORD_TYPE_CHUNKED_FILE => {
            Record::ChunkedFile(deserialize_chunked_file_borrowed(data, pos, compression)?)
        }
        RECORD_TYPE_SYMLINK => Record::Link(LinkRecord::deserialize_borrowed(data, pos)?),
        RECORD_TYPE_EXTERNAL_SYMLINK => {
            Record::ExternalLink(ExternalLinkRecord::deserialize_borrowed(data, pos)?)
        }
        _ => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid or unsupported record type: 0x{:02x}", record_type),
            ));
        }
    };
    Ok(record)
}

/// Deserialize BoxMetadata in v1 format (no root).
pub(crate) fn deserialize_metadata_borrowed<'a>(
    data: &'a [u8],
    pos: &mut usize,
) -> std::io::Result<BoxMetadata<'a>> {
    // v1: schema before data (attr_keys → attrs → dictionary → records)
    // root not serialized (paths indexed by FST)
    let root = Vec::new();

    let attr_keys = deserialize_attr_keys_borrowed(data, pos)?;
    let attrs = AttrMapBorrowed::deserialize_borrowed(data, pos)?;

    // Dictionary: [Vu64 length][bytes] - length=0 means no dictionary
    let dict_len = read_vlq_u64(data, pos)? as usize;
    let dictionary = if dict_len > 0 {
        if *pos + dict_len > data.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "unexpected end of data reading dictionary",
            ));
        }
        let dict_bytes = data[*pos..*pos + dict_len].to_vec().into_boxed_slice();
        *pos += dict_len;
        Some(dict_bytes)
    } else {
        None
    };

    let record_count = read_vlq_u64(data, pos)? as usize;
    let mut records = Vec::with_capacity(record_count);
    for _ in 0..record_count {
        records.push(deserialize_record_borrowed(data, pos)?);
    }

    let fst = parse_fst_borrowed(data, pos);
    let block_fst = parse_fst_borrowed(data, pos);

    Ok(BoxMetadata {
        root,
        records,
        attr_keys,
        attrs,
        dictionary,
        fst,
        block_fst,
    })
}
