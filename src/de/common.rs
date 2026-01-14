//! Shared deserialization implementations (borrowed only).

use crate::compat::HashMap;
use std::borrow::Cow;
use std::num::NonZeroU64;

use crate::compression::constants::*;
use crate::{AttrMap, Compression, ExternalLinkRecord, FileRecord, LinkRecord, core::RecordIndex};

use super::{DeserializeBorrowed, read_u8_slice, read_u64_le_slice, read_vlq_u64};

// ============================================================================
// TYPE ALIAS FOR CLARITY
// ============================================================================

/// Alias for AttrMap used in borrowed deserialization.
pub(super) type AttrMapBorrowed = AttrMap;

// ============================================================================
// BORROWED IMPLEMENTATIONS
// ============================================================================

impl<'a> DeserializeBorrowed<'a> for Box<[u8]> {
    fn deserialize_borrowed(data: &'a [u8], pos: &mut usize) -> std::io::Result<Self> {
        let len = read_vlq_u64(data, pos)? as usize;
        if *pos + len > data.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "unexpected end of data reading bytes",
            ));
        }
        let bytes = data[*pos..*pos + len].to_vec().into_boxed_slice();
        *pos += len;
        Ok(bytes)
    }
}

impl<'a> DeserializeBorrowed<'a> for AttrMap {
    fn deserialize_borrowed(data: &'a [u8], pos: &mut usize) -> std::io::Result<Self> {
        let _byte_count = read_u64_le_slice(data, pos)?;
        let len = read_vlq_u64(data, pos)? as usize;
        let mut map: HashMap<usize, Box<[u8]>> = HashMap::with_capacity(len);
        for _ in 0..len {
            let key = read_vlq_u64(data, pos)? as usize;
            let value = <Box<[u8]>>::deserialize_borrowed(data, pos)?;
            map.insert(key, value);
        }
        Ok(map)
    }
}

impl<'a> DeserializeBorrowed<'a> for Compression {
    fn deserialize_borrowed(data: &'a [u8], pos: &mut usize) -> std::io::Result<Self> {
        let id = read_u8_slice(data, pos)?;

        use Compression::*;

        let compression = match id {
            COMPRESSION_STORED => Stored,
            COMPRESSION_ZSTD => Zstd,
            COMPRESSION_XZ => Xz,
            id => Unknown(id),
        };

        Ok(compression)
    }
}

impl<'a> DeserializeBorrowed<'a> for FileRecord<'a> {
    fn deserialize_borrowed(data: &'a [u8], pos: &mut usize) -> std::io::Result<Self> {
        let compression = Compression::deserialize_borrowed(data, pos)?;
        let length = read_u64_le_slice(data, pos)?;
        let decompressed_length = read_u64_le_slice(data, pos)?;
        let data_offset = read_u64_le_slice(data, pos)?;
        let name = <Cow<'a, str>>::deserialize_borrowed(data, pos)?;
        let attrs = AttrMap::deserialize_borrowed(data, pos)?;

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
}

impl<'a> DeserializeBorrowed<'a> for LinkRecord<'a> {
    fn deserialize_borrowed(data: &'a [u8], pos: &mut usize) -> std::io::Result<Self> {
        let name = <Cow<'a, str>>::deserialize_borrowed(data, pos)?;
        let target = RecordIndex::deserialize_borrowed(data, pos)?;
        let attrs = AttrMap::deserialize_borrowed(data, pos)?;

        Ok(LinkRecord {
            name,
            target,
            attrs,
        })
    }
}

impl<'a> DeserializeBorrowed<'a> for ExternalLinkRecord<'a> {
    fn deserialize_borrowed(data: &'a [u8], pos: &mut usize) -> std::io::Result<Self> {
        let name = <Cow<'a, str>>::deserialize_borrowed(data, pos)?;
        let target = <Cow<'a, str>>::deserialize_borrowed(data, pos)?;
        let attrs = AttrMap::deserialize_borrowed(data, pos)?;

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
pub(super) fn parse_fst_borrowed<'a>(
    data: &'a [u8],
    pos: &mut usize,
) -> Option<box_fst::Fst<Cow<'a, [u8]>>> {
    // Check if we have enough bytes for the length prefix
    if *pos + 8 > data.len() {
        return None;
    }

    // Read u64 length prefix
    let length_bytes: [u8; 8] = data[*pos..*pos + 8].try_into().ok()?;
    let fst_length = u64::from_le_bytes(length_bytes) as usize;
    *pos += 8;

    // Check if we have enough bytes for the FST data
    if fst_length == 0 || *pos + fst_length > data.len() {
        return None;
    }

    // Parse FST from exactly fst_length bytes
    let fst_data = &data[*pos..*pos + fst_length];
    *pos += fst_length;
    box_fst::Fst::new(Cow::Borrowed(fst_data)).ok()
}
