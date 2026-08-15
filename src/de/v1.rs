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
    DeserializeBorrowed, checked_count, read_context, read_u8_slice, read_u32_le_slice,
    read_u64_le_slice, read_vlq_u64,
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
        #[cfg(feature = "zstd")]
        COMPRESSION_ZSTD => Compression::Zstd,
        #[cfg(feature = "xz")]
        COMPRESSION_XZ => Compression::Xz,
        id => Compression::Unknown(id),
    };
    (record_type, compression)
}

// ============================================================================
// BORROWED DESERIALIZATION (v1)
// ============================================================================

/// Deserialize FileRecord in v1 format (compression already parsed from combined byte).
// [spec:box:req:records.root.references]
// [spec:box:req:wire.root.bounds.record-scalars]
fn deserialize_file_borrowed<'a>(
    data: &'a [u8],
    pos: &mut usize,
    compression: Compression,
) -> std::io::Result<FileRecord<'a>> {
    let offset = *pos;
    let length = read_context(
        read_u64_le_slice(data, pos),
        "file compressed length",
        offset,
    )?;
    let offset = *pos;
    let decompressed_length = read_context(
        read_u64_le_slice(data, pos),
        "file decompressed length",
        offset,
    )?;
    let data_offset_field = *pos;
    let data_offset = read_context(
        read_u64_le_slice(data, pos),
        "file data offset",
        data_offset_field,
    )?;
    let offset = *pos;
    let name = read_context(
        <Cow<'a, str>>::deserialize_borrowed(data, pos),
        "file name",
        offset,
    )?;
    let offset = *pos;
    let attrs = read_context(
        AttrMapBorrowed::deserialize_borrowed(data, pos),
        "file attributes",
        offset,
    )?;
    let data = read_context(
        NonZeroU64::new(data_offset).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "file data offset must not be zero",
            )
        }),
        "file data offset",
        data_offset_field,
    )?;

    Ok(FileRecord {
        compression,
        length,
        decompressed_length,
        name,
        attrs,
        data,
    })
}

/// Deserialize ChunkedFileRecord in v1 format (compression already parsed from combined byte).
// [spec:box:req:records.root.references]
// [spec:box:req:wire.root.bounds.record-scalars]
fn deserialize_chunked_file_borrowed<'a>(
    data: &'a [u8],
    pos: &mut usize,
    compression: Compression,
) -> std::io::Result<ChunkedFileRecord<'a>> {
    let offset = *pos;
    let block_size = read_context(
        read_u32_le_slice(data, pos),
        "chunked-file block size",
        offset,
    )?;
    let offset = *pos;
    let length = read_context(
        read_u64_le_slice(data, pos),
        "chunked-file compressed length",
        offset,
    )?;
    let offset = *pos;
    let decompressed_length = read_context(
        read_u64_le_slice(data, pos),
        "chunked-file decompressed length",
        offset,
    )?;
    let data_offset_field = *pos;
    let data_offset = read_context(
        read_u64_le_slice(data, pos),
        "chunked-file data offset",
        data_offset_field,
    )?;
    let offset = *pos;
    let name = read_context(
        <Cow<'a, str>>::deserialize_borrowed(data, pos),
        "chunked-file name",
        offset,
    )?;
    let offset = *pos;
    let attrs = read_context(
        AttrMapBorrowed::deserialize_borrowed(data, pos),
        "chunked-file attributes",
        offset,
    )?;
    let data = read_context(
        NonZeroU64::new(data_offset).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "chunked-file data offset must not be zero",
            )
        }),
        "chunked-file data offset",
        data_offset_field,
    )?;

    Ok(ChunkedFileRecord {
        compression,
        block_size,
        length,
        decompressed_length,
        name,
        attrs,
        data,
    })
}

/// Deserialize Vec<AttrKey> in v1 format (with type tag).
// [spec:box:req:attributes.root.integrity]
pub(crate) fn deserialize_attr_keys_borrowed<'a>(
    data: &'a [u8],
    pos: &mut usize,
) -> std::io::Result<Vec<AttrKey>> {
    let count_offset = *pos;
    let len = read_context(read_vlq_u64(data, pos), "attribute-key count", count_offset)?;
    let len = checked_count(data, *pos, len, "attribute-key count", count_offset)?;
    let mut keys = Vec::with_capacity(len);
    for index in 0..len {
        // v1: read type tag first
        let type_offset = *pos;
        let type_tag = read_context(
            read_u8_slice(data, pos),
            format_args!("type tag for attribute key {} of {len}", index + 1),
            type_offset,
        )?;
        let attr_type = AttrType::from_u8(type_tag).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "unknown attribute type tag 0x{type_tag:02x} for attribute key {} of {len} \
                     at trailer byte {type_offset}",
                    index + 1
                ),
            )
        })?;
        let name_offset = *pos;
        let name = read_context(
            <&'a str>::deserialize_borrowed(data, pos),
            format_args!("name of attribute key {} of {len}", index + 1),
            name_offset,
        )?
        .to_string();
        keys.push(AttrKey { name, attr_type });
    }
    Ok(keys)
}

/// Deserialize DirectoryRecord in v1 format (no entries).
pub(crate) fn deserialize_directory_borrowed<'a>(
    data: &'a [u8],
    pos: &mut usize,
) -> std::io::Result<DirectoryRecord<'a>> {
    let offset = *pos;
    let name = read_context(
        <Cow<'a, str>>::deserialize_borrowed(data, pos),
        "directory name",
        offset,
    )?;
    // v1: entries not in binary format (looked up via FST)
    let entries = Vec::new();
    let offset = *pos;
    let attrs = read_context(
        AttrMapBorrowed::deserialize_borrowed(data, pos),
        "directory attributes",
        offset,
    )?;

    Ok(DirectoryRecord {
        name,
        entries,
        attrs,
    })
}

/// Deserialize Record in v1 format.
// [spec:box:req:records.root.type-byte]
// [spec:box:req:wire.root.bounds.record-scalars]
pub(crate) fn deserialize_record_borrowed<'a>(
    data: &'a [u8],
    pos: &mut usize,
) -> std::io::Result<Record<'a>> {
    let record_offset = *pos;
    let type_compression = read_context(
        read_u8_slice(data, pos),
        "record type/compression byte",
        record_offset,
    )?;
    let (record_type, compression) = parse_type_compression(type_compression);

    let record = match record_type {
        RECORD_TYPE_DIRECTORY => {
            let offset = *pos;
            Record::Directory(read_context(
                deserialize_directory_borrowed(data, pos),
                "directory record body",
                offset,
            )?)
        }
        RECORD_TYPE_FILE => {
            let offset = *pos;
            Record::File(read_context(
                deserialize_file_borrowed(data, pos, compression),
                "file record body",
                offset,
            )?)
        }
        RECORD_TYPE_CHUNKED_FILE => {
            let offset = *pos;
            Record::ChunkedFile(read_context(
                deserialize_chunked_file_borrowed(data, pos, compression),
                "chunked-file record body",
                offset,
            )?)
        }
        RECORD_TYPE_SYMLINK => {
            let offset = *pos;
            Record::Link(read_context(
                LinkRecord::deserialize_borrowed(data, pos),
                "symlink record body",
                offset,
            )?)
        }
        RECORD_TYPE_EXTERNAL_SYMLINK => {
            let offset = *pos;
            Record::ExternalLink(read_context(
                ExternalLinkRecord::deserialize_borrowed(data, pos),
                "external-symlink record body",
                offset,
            )?)
        }
        _ => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "invalid or unsupported record type 0x{record_type:02x} in combined \
                     type/compression byte 0x{type_compression:02x} at trailer byte {record_offset}"
                ),
            ));
        }
    };
    Ok(record)
}

/// Deserialize BoxMetadata in v1 format (no root).
// [spec:box:sem:dictionaries.root]
// [spec:box:req:wire.root.bounds.lengths-and-counts]
// [spec:box:sem:records.root.references.deferred-relationships]
// [spec:box:req:versioning.root.v1]
pub(crate) fn deserialize_metadata_borrowed<'a>(
    data: &'a [u8],
    pos: &mut usize,
) -> std::io::Result<BoxMetadata<'a>> {
    // v1: schema before data (attr_keys → attrs → dictionary → records)
    // root not serialized (paths indexed by FST)
    let root = Vec::new();

    let offset = *pos;
    let attr_keys = read_context(
        deserialize_attr_keys_borrowed(data, pos),
        "archive attribute-key schema",
        offset,
    )?;
    let offset = *pos;
    let attrs = read_context(
        AttrMapBorrowed::deserialize_borrowed(data, pos),
        "archive attributes",
        offset,
    )?;

    // Dictionary: [Vu64 length][bytes] - length=0 means no dictionary
    let dict_length_offset = *pos;
    let dict_len = read_context(
        read_vlq_u64(data, pos),
        "compression-dictionary length",
        dict_length_offset,
    )?;
    let dict_len = usize::try_from(dict_len).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "compression-dictionary length at trailer byte {dict_length_offset} does not \
                 fit in memory: {dict_len} bytes"
            ),
        )
    })?;
    let dictionary = if dict_len > 0 {
        let dict_offset = *pos;
        let Some(end) = dict_offset.checked_add(dict_len) else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "compression dictionary at trailer byte {dict_offset} has an overflowing \
                     length: {dict_len} bytes"
                ),
            ));
        };
        if end > data.len() {
            let available = data.len().saturating_sub(dict_offset);
            let missing = dict_len - available;
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!(
                    "cannot read compression dictionary at trailer byte {dict_offset} \
                     (0x{dict_offset:x}): its length prefix declares {dict_len} bytes, \
                     but only {available} remain ({missing} bytes missing)"
                ),
            ));
        }
        let dict_bytes = data[dict_offset..end].to_vec().into_boxed_slice();
        *pos = end;
        Some(dict_bytes)
    } else {
        None
    };

    let count_offset = *pos;
    let record_count = read_context(read_vlq_u64(data, pos), "record count", count_offset)?;
    let record_count = checked_count(data, *pos, record_count, "record count", count_offset)?;
    let mut records = Vec::with_capacity(record_count);
    for index in 0..record_count {
        let offset = *pos;
        records.push(read_context(
            deserialize_record_borrowed(data, pos),
            format_args!("record {} of {record_count}", index + 1),
            offset,
        )?);
    }

    let offset = *pos;
    let fst = read_context(parse_fst_borrowed(data, pos), "path-index FST", offset)?;
    let block_fst = if *pos == data.len() {
        // The block index is optional; reaching EOF exactly after the path FST
        // is its presence marker.
        None
    } else {
        let offset = *pos;
        read_context(parse_fst_borrowed(data, pos), "block-index FST", offset)?
    };

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
