//! Shared deserialization implementations (borrowed only).

use crate::compat::HashMap;
use std::borrow::Cow;
use std::num::NonZeroU64;

use crate::compression::constants::*;
use crate::{AttrMap, Compression, ExternalLinkRecord, FileRecord, LinkRecord, core::RecordIndex};

use super::{
    DeserializeBorrowed, checked_count, read_context, read_u8_slice, read_u64_le_slice,
    read_vlq_u64, wrap_io_error,
};

// ============================================================================
// TYPE ALIAS FOR CLARITY
// ============================================================================

/// Alias for AttrMap used in borrowed deserialization.
pub(super) type AttrMapBorrowed = AttrMap;

// ============================================================================
// BORROWED IMPLEMENTATIONS
// ============================================================================

// [spec:box:req:wire.root.bounds.lengths-and-counts]
impl<'a> DeserializeBorrowed<'a> for Box<[u8]> {
    fn deserialize_borrowed(data: &'a [u8], pos: &mut usize) -> std::io::Result<Self> {
        let length_offset = *pos;
        let len = read_context(
            read_vlq_u64(data, pos),
            "byte-string length prefix",
            length_offset,
        )?;
        let len = usize::try_from(len).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "byte-string length at trailer byte {length_offset} does not fit in memory: {len}"
                ),
            )
        })?;
        let bytes_offset = *pos;
        let Some(end) = bytes_offset.checked_add(len) else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "byte string at trailer byte {bytes_offset} has an overflowing length: {len} bytes"
                ),
            ));
        };
        if end > data.len() {
            let available = data.len().saturating_sub(bytes_offset);
            let missing = len - available;
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!(
                    "cannot read byte-string contents at trailer byte {bytes_offset} \
                     (0x{bytes_offset:x}): its length prefix declares {len} bytes, \
                     but only {available} remain ({missing} bytes missing)"
                ),
            ));
        }
        let bytes = data[bytes_offset..end].to_vec().into_boxed_slice();
        *pos = end;
        Ok(bytes)
    }
}

// [spec:box:def:attributes.root]
// [spec:box:req:attributes.root.integrity]
// [spec:box:req:wire.root.bounds.attrmap-envelope]
impl<'a> DeserializeBorrowed<'a> for AttrMap {
    fn deserialize_borrowed(data: &'a [u8], pos: &mut usize) -> std::io::Result<Self> {
        let byte_count_offset = *pos;
        let byte_count = read_context(
            read_u64_le_slice(data, pos),
            "attribute-map byte count",
            byte_count_offset,
        )?;
        let count_offset = *pos;
        let len = read_context(
            read_vlq_u64(data, pos),
            "attribute-map entry count",
            count_offset,
        )?;
        let len = checked_count(data, *pos, len, "attribute-map entry count", count_offset)?;
        let mut map: HashMap<usize, Box<[u8]>> = HashMap::with_capacity(len);
        for index in 0..len {
            let key_offset = *pos;
            let key = read_context(
                read_vlq_u64(data, pos),
                format_args!("key index for attribute {} of {len}", index + 1),
                key_offset,
            )?;
            let key = usize::try_from(key).map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "key index for attribute {} of {len} at trailer byte {key_offset} \
                         does not fit in memory: {key}",
                        index + 1
                    ),
                )
            })?;
            let value_offset = *pos;
            let value = read_context(
                <Box<[u8]>>::deserialize_borrowed(data, pos),
                format_args!(
                    "value for attribute {} of {len} (key index {key})",
                    index + 1
                ),
                value_offset,
            )?;
            map.insert(key, value);
        }

        let consumed = (*pos).saturating_sub(count_offset) as u64;
        // Legacy writers measured the map by seeking back over it, so their
        // declared count also covers the byte-count field's own eight bytes.
        let legacy_consumed = consumed.saturating_add(8);
        if consumed != byte_count && legacy_consumed != byte_count {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "attribute map at trailer byte {byte_count_offset} declares {byte_count} \
                     bytes after its byte-count field, but its {len} entries consume {consumed} \
                     bytes ({legacy_consumed} counting the byte-count field itself)"
                ),
            ));
        }
        Ok(map)
    }
}

impl<'a> DeserializeBorrowed<'a> for Compression {
    fn deserialize_borrowed(data: &'a [u8], pos: &mut usize) -> std::io::Result<Self> {
        let offset = *pos;
        let id = read_context(read_u8_slice(data, pos), "compression identifier", offset)?;

        use Compression::*;

        let compression = match id {
            COMPRESSION_STORED => Stored,
            #[cfg(feature = "zstd")]
            COMPRESSION_ZSTD => Zstd,
            #[cfg(feature = "xz")]
            COMPRESSION_XZ => Xz,
            id => Unknown(id),
        };

        Ok(compression)
    }
}

// [spec:box:req:wire.root.bounds.record-scalars]
impl<'a> DeserializeBorrowed<'a> for FileRecord<'a> {
    fn deserialize_borrowed(data: &'a [u8], pos: &mut usize) -> std::io::Result<Self> {
        let offset = *pos;
        let compression = read_context(
            Compression::deserialize_borrowed(data, pos),
            "file compression",
            offset,
        )?;
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
            AttrMap::deserialize_borrowed(data, pos),
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
}

impl<'a> DeserializeBorrowed<'a> for LinkRecord<'a> {
    fn deserialize_borrowed(data: &'a [u8], pos: &mut usize) -> std::io::Result<Self> {
        let offset = *pos;
        let name = read_context(
            <Cow<'a, str>>::deserialize_borrowed(data, pos),
            "symlink name",
            offset,
        )?;
        let offset = *pos;
        let target = read_context(
            RecordIndex::deserialize_borrowed(data, pos),
            "symlink target record index",
            offset,
        )?;
        let offset = *pos;
        let attrs = read_context(
            AttrMap::deserialize_borrowed(data, pos),
            "symlink attributes",
            offset,
        )?;

        Ok(LinkRecord {
            name,
            target,
            attrs,
        })
    }
}

impl<'a> DeserializeBorrowed<'a> for ExternalLinkRecord<'a> {
    fn deserialize_borrowed(data: &'a [u8], pos: &mut usize) -> std::io::Result<Self> {
        let offset = *pos;
        let name = read_context(
            <Cow<'a, str>>::deserialize_borrowed(data, pos),
            "external symlink name",
            offset,
        )?;
        let offset = *pos;
        let target = read_context(
            <Cow<'a, str>>::deserialize_borrowed(data, pos),
            "external symlink target",
            offset,
        )?;
        let offset = *pos;
        let attrs = read_context(
            AttrMap::deserialize_borrowed(data, pos),
            "external symlink attributes",
            offset,
        )?;

        Ok(ExternalLinkRecord {
            name,
            target,
            attrs,
        })
    }
}

// ============================================================================
// FST PARSING HELPER
// ============================================================================

/// Parse FST from remaining borrowed data.
/// v1 format: [u64 length][FST bytes]
// [spec:box:req:fst-format.root.validation]
// [spec:box:req:wire.root.bounds.fst-envelope]
pub(super) fn parse_fst_borrowed<'a>(
    data: &'a [u8],
    pos: &mut usize,
) -> std::io::Result<Option<box_fst::Fst<Cow<'a, [u8]>>>> {
    let length_offset = *pos;
    let fst_length = read_context(read_u64_le_slice(data, pos), "FST length", length_offset)?;
    let fst_length = usize::try_from(fst_length).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("FST length at trailer byte {length_offset} does not fit in memory"),
        )
    })?;

    if fst_length == 0 {
        return Ok(None);
    }

    let fst_offset = *pos;
    let Some(end) = fst_offset.checked_add(fst_length) else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("FST at trailer byte {fst_offset} has an overflowing length: {fst_length}"),
        ));
    };
    if end > data.len() {
        let available = data.len().saturating_sub(fst_offset);
        let missing = fst_length - available;
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            format!(
                "cannot read FST data at trailer byte {fst_offset} (0x{fst_offset:x}): \
                 its length prefix declares {fst_length} bytes, but only {available} remain \
                 ({missing} bytes missing)"
            ),
        ));
    }

    let fst_data = &data[fst_offset..end];
    let fst = box_fst::Fst::new(Cow::Borrowed(fst_data)).map_err(|source| {
        wrap_io_error(
            std::io::Error::new(std::io::ErrorKind::InvalidData, source),
            format!("FST data at trailer bytes {fst_offset}..{end} is structurally invalid"),
        )
    })?;
    *pos = end;
    Ok(Some(fst))
}
