//! v0 format deserialization (borrowed only).
//!
//! v0 differences from v1:
//! - Directory entries are serialized in the directory record
//! - Metadata root (list of top-level record indices) is serialized
//! - Attribute keys have no type tag (default to Json)

use std::borrow::Cow;

use crate::{
    BoxMetadata, DirectoryRecord, ExternalLinkRecord, FileRecord, LinkRecord, Record,
    core::{AttrKey, AttrType, RecordIndex},
};

use super::common::AttrMapBorrowed;
use super::{DeserializeBorrowed, checked_count, read_context, read_u8_slice, read_vlq_u64};

// ============================================================================
// BORROWED DESERIALIZATION (v0)
// ============================================================================

/// Deserialize Vec<AttrKey> in v0 format (no type tag, default to Json).
pub(crate) fn deserialize_attr_keys_borrowed<'a>(
    data: &'a [u8],
    pos: &mut usize,
) -> std::io::Result<Vec<AttrKey>> {
    let count_offset = *pos;
    let len = read_context(read_vlq_u64(data, pos), "attribute-key count", count_offset)?;
    let len = checked_count(data, *pos, len, "attribute-key count", count_offset)?;
    let mut keys = Vec::with_capacity(len);
    for index in 0..len {
        // v0: no type tag, default to Json
        let attr_type = AttrType::Json;
        let offset = *pos;
        let name = read_context(
            <&'a str>::deserialize_borrowed(data, pos),
            format_args!("name of attribute key {} of {len}", index + 1),
            offset,
        )?
        .to_string();
        keys.push(AttrKey { name, attr_type });
    }
    Ok(keys)
}

/// Deserialize DirectoryRecord in v0 format (with entries).
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
    // v0: entries are serialized
    let offset = *pos;
    let entries = read_context(
        <Vec<RecordIndex>>::deserialize_borrowed(data, pos),
        "directory child-record indices",
        offset,
    )?;
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

/// Deserialize Record in v0 format.
pub(crate) fn deserialize_record_borrowed<'a>(
    data: &'a [u8],
    pos: &mut usize,
) -> std::io::Result<Record<'a>> {
    let record_offset = *pos;
    let ty = read_context(read_u8_slice(data, pos), "record type byte", record_offset)?;
    let record = match ty {
        0 => {
            let offset = *pos;
            Record::File(read_context(
                FileRecord::deserialize_borrowed(data, pos),
                "file record body",
                offset,
            )?)
        }
        1 => {
            let offset = *pos;
            Record::Directory(read_context(
                deserialize_directory_borrowed(data, pos),
                "directory record body",
                offset,
            )?)
        }
        2 => {
            let offset = *pos;
            Record::Link(read_context(
                LinkRecord::deserialize_borrowed(data, pos),
                "symlink record body",
                offset,
            )?)
        }
        3 => {
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
                    "invalid or unsupported record type 0x{ty:02x} at trailer byte {record_offset}"
                ),
            ));
        }
    };
    Ok(record)
}

/// Deserialize BoxMetadata in v0 format (with root).
pub(crate) fn deserialize_metadata_borrowed<'a>(
    data: &'a [u8],
    pos: &mut usize,
) -> std::io::Result<BoxMetadata<'a>> {
    // v0: root is serialized
    let offset = *pos;
    let root = read_context(
        <Vec<RecordIndex>>::deserialize_borrowed(data, pos),
        "root record-index list",
        offset,
    )?;

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

    // v0: no FST, no dictionary, no block FST (root and entries are serialized explicitly)
    Ok(BoxMetadata {
        root,
        records,
        attr_keys,
        attrs,
        dictionary: None,
        fst: None,
        block_fst: None,
    })
}
