//! v1 format deserialization.
//!
//! v1 differences from v0:
//! - Directory entries are NOT serialized (looked up via FST prefix queries)
//! - Metadata root is NOT serialized (paths indexed by FST)
//! - Attribute keys have a type tag byte before the name
//! - Combined type/compression byte (bits 0-3 = type, bits 4-7 = compression)

use std::borrow::Cow;
use std::collections::HashMap;
use std::num::NonZeroU64;

use fastvint::AsyncReadVintExt;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};

use crate::{
    AttrMap, BoxMetadata, ChunkedFileRecord, Compression, DirectoryRecord, ExternalLinkRecord,
    FileRecord, LinkRecord, Record,
    compression::constants::*,
    file::meta::{AttrKey, AttrType},
};

use super::{
    DeserializeBorrowed, DeserializeOwned,
    common::{parse_fst_borrowed, parse_fst_owned},
    read_u8_slice, read_u32_le, read_u32_le_slice, read_u64_le, read_u64_le_slice, read_vlq_u64,
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
    let attrs = AttrMap::deserialize_borrowed(data, pos)?;

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
    let attrs = AttrMap::deserialize_borrowed(data, pos)?;

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
    let attrs = AttrMap::deserialize_borrowed(data, pos)?;

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

// ============================================================================
// OWNED DESERIALIZATION (v1)
// ============================================================================

/// Deserialize Vec<AttrKey> in v1 format (with type tag).
pub(crate) async fn deserialize_attr_keys_owned<R: AsyncRead + AsyncSeek + Unpin + Send>(
    reader: &mut R,
) -> std::io::Result<Vec<AttrKey>> {
    let start = reader.stream_position().await?;
    let len = reader.read_vu64().await? as usize;
    let mut keys = Vec::with_capacity(len);
    for _ in 0..len {
        // v1: read type tag first
        let type_tag = reader.read_u8().await?;
        let attr_type = AttrType::from_u8(type_tag).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unknown attribute type tag: {}", type_tag),
            )
        })?;
        let name = String::deserialize_owned(reader).await?;
        keys.push(AttrKey { name, attr_type });
    }
    let end = reader.stream_position().await?;
    tracing::debug!(
        start = format_args!("{:#x}", start),
        end = format_args!("{:#x}", end),
        bytes = end - start,
        count = len,
        "deserialized AttrKeys (v1)"
    );
    Ok(keys)
}

/// Deserialize DirectoryRecord in v1 format (no entries).
pub(crate) async fn deserialize_directory_owned<R: AsyncRead + AsyncSeek + Unpin + Send>(
    reader: &mut R,
) -> std::io::Result<DirectoryRecord<'static>> {
    let start = reader.stream_position().await?;
    let name = String::deserialize_owned(reader).await?;

    // v1: entries not serialized
    let entries = Vec::new();

    let attrs = <HashMap<usize, Box<[u8]>>>::deserialize_owned(reader).await?;

    let end = reader.stream_position().await?;
    tracing::debug!(start = format_args!("{:#x}", start), end = format_args!("{:#x}", end), bytes = end - start, %name, "deserialized DirectoryRecord (v1)");

    Ok(DirectoryRecord {
        name: Cow::Owned(name),
        entries,
        attrs,
    })
}

/// Deserialize FileRecord in v1 format (compression already parsed from combined byte).
async fn deserialize_file_owned<R: AsyncRead + AsyncSeek + Unpin + Send>(
    reader: &mut R,
    compression: Compression,
) -> std::io::Result<FileRecord<'static>> {
    let start = reader.stream_position().await?;
    let length = read_u64_le(reader).await?;
    let decompressed_length = read_u64_le(reader).await?;
    let data = read_u64_le(reader).await?;
    let name = String::deserialize_owned(reader).await?;
    let attrs = <HashMap<usize, Box<[u8]>>>::deserialize_owned(reader).await?;

    let end = reader.stream_position().await?;
    tracing::debug!(start = format_args!("{:#x}", start), end = format_args!("{:#x}", end), bytes = end - start, %name, "deserialized FileRecord (v1)");

    Ok(FileRecord {
        compression,
        length,
        decompressed_length,
        name: Cow::Owned(name),
        attrs,
        data: NonZeroU64::new(data).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "file data offset must not be zero",
            )
        })?,
    })
}

/// Deserialize ChunkedFileRecord in v1 format (compression already parsed from combined byte).
async fn deserialize_chunked_file_owned<R: AsyncRead + AsyncSeek + Unpin + Send>(
    reader: &mut R,
    compression: Compression,
) -> std::io::Result<ChunkedFileRecord<'static>> {
    let start = reader.stream_position().await?;
    let block_size = read_u32_le(reader).await?;
    let length = read_u64_le(reader).await?;
    let decompressed_length = read_u64_le(reader).await?;
    let data = read_u64_le(reader).await?;
    let name = String::deserialize_owned(reader).await?;
    let attrs = <HashMap<usize, Box<[u8]>>>::deserialize_owned(reader).await?;

    let end = reader.stream_position().await?;
    tracing::debug!(start = format_args!("{:#x}", start), end = format_args!("{:#x}", end), bytes = end - start, %name, "deserialized ChunkedFileRecord (v1)");

    Ok(ChunkedFileRecord {
        compression,
        block_size,
        length,
        decompressed_length,
        name: Cow::Owned(name),
        attrs,
        data: NonZeroU64::new(data).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "chunked file data offset must not be zero",
            )
        })?,
    })
}

/// Deserialize Record in v1 format.
pub(crate) async fn deserialize_record_owned<R: AsyncRead + AsyncSeek + Unpin + Send>(
    reader: &mut R,
) -> std::io::Result<Record<'static>> {
    let start = reader.stream_position().await?;
    let type_compression = reader.read_u8().await?;
    let (record_type, compression) = parse_type_compression(type_compression);

    let record = match record_type {
        RECORD_TYPE_DIRECTORY => Record::Directory(deserialize_directory_owned(reader).await?),
        RECORD_TYPE_FILE => Record::File(deserialize_file_owned(reader, compression).await?),
        RECORD_TYPE_CHUNKED_FILE => {
            Record::ChunkedFile(deserialize_chunked_file_owned(reader, compression).await?)
        }
        RECORD_TYPE_SYMLINK => Record::Link(LinkRecord::deserialize_owned(reader).await?),
        RECORD_TYPE_EXTERNAL_SYMLINK => {
            Record::ExternalLink(ExternalLinkRecord::deserialize_owned(reader).await?)
        }
        _ => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid or unsupported record type: 0x{:02x}", record_type),
            ));
        }
    };
    let end = reader.stream_position().await?;
    tracing::debug!(
        start = format_args!("{:#x}", start),
        end = format_args!("{:#x}", end),
        bytes = end - start,
        type_compression,
        "deserialized Record (v1)"
    );
    Ok(record)
}

/// Deserialize BoxMetadata in v1 format (no root).
pub(crate) async fn deserialize_metadata_owned<R: AsyncRead + AsyncSeek + Unpin + Send>(
    reader: &mut R,
) -> std::io::Result<BoxMetadata<'static>> {
    let start = reader.stream_position().await?;

    // v1: schema before data (attr_keys → attrs → dictionary → records)
    // root not serialized (paths indexed by FST)
    let root = Vec::new();

    let attr_keys = deserialize_attr_keys_owned(reader).await?;
    let attrs = <HashMap<usize, Box<[u8]>>>::deserialize_owned(reader).await?;

    // Dictionary: [Vu64 length][bytes] - length=0 means no dictionary
    let dict_len = reader.read_vu64().await? as usize;
    let dictionary = if dict_len > 0 {
        let mut dict_bytes = vec![0u8; dict_len];
        reader.read_exact(&mut dict_bytes).await?;
        Some(dict_bytes.into_boxed_slice())
    } else {
        None
    };

    let record_count = reader.read_vu64().await?;
    let mut records = Vec::with_capacity(record_count as usize);
    for _ in 0..record_count {
        records.push(deserialize_record_owned(reader).await?);
    }

    let fst = parse_fst_owned(reader).await?;
    let block_fst = parse_fst_owned(reader).await?;

    let end = reader.stream_position().await?;
    tracing::debug!(
        start = format_args!("{:#x}", start),
        end = format_args!("{:#x}", end),
        bytes = end - start,
        records = records.len(),
        dictionary = dictionary.as_ref().map(|d| d.len()).unwrap_or(0),
        fst_size = fst.as_ref().map(|f| f.len()).unwrap_or(0),
        block_fst_size = block_fst.as_ref().map(|f| f.len()).unwrap_or(0),
        "deserialized BoxMetadata (v1)"
    );

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
