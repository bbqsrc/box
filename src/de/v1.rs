//! v1 format deserialization.
//!
//! v1 differences from v0:
//! - Directory entries are NOT serialized (looked up via FST prefix queries)
//! - Metadata root is NOT serialized (paths indexed by FST)
//! - Attribute keys have a type tag byte before the name

use std::borrow::Cow;
use std::collections::HashMap;

use fastvint::AsyncReadVintExt;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};

use crate::{
    AttrMap, BoxMetadata, DirectoryRecord, ExternalLinkRecord, FileRecord, LinkRecord, Record,
    file::meta::{AttrKey, AttrType},
};

use super::{
    DeserializeBorrowed, DeserializeOwned, read_u8_slice, read_vlq_u64,
    common::{parse_fst_borrowed, parse_fst_owned},
};

// ============================================================================
// BORROWED DESERIALIZATION (v1)
// ============================================================================

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
    let ty = read_u8_slice(data, pos)?;
    let record = match ty {
        0 => Record::File(FileRecord::deserialize_borrowed(data, pos)?),
        1 => Record::Directory(deserialize_directory_borrowed(data, pos)?),
        2 => Record::Link(LinkRecord::deserialize_borrowed(data, pos)?),
        3 => Record::ExternalLink(ExternalLinkRecord::deserialize_borrowed(data, pos)?),
        _ => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid or unsupported record type: {}", ty),
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
    // v1: root not serialized (paths indexed by FST)
    let root = Vec::new();

    let record_count = read_vlq_u64(data, pos)? as usize;
    let mut records = Vec::with_capacity(record_count);
    for _ in 0..record_count {
        records.push(deserialize_record_borrowed(data, pos)?);
    }

    let attr_keys = deserialize_attr_keys_borrowed(data, pos)?;
    let attrs = AttrMap::deserialize_borrowed(data, pos)?;
    let fst = parse_fst_borrowed(data, pos);

    Ok(BoxMetadata {
        root,
        records,
        attr_keys,
        attrs,
        fst,
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

/// Deserialize Record in v1 format.
pub(crate) async fn deserialize_record_owned<R: AsyncRead + AsyncSeek + Unpin + Send>(
    reader: &mut R,
) -> std::io::Result<Record<'static>> {
    let start = reader.stream_position().await?;
    let ty = reader.read_u8().await?;
    let record = match ty {
        0 => Record::File(FileRecord::deserialize_owned(reader).await?),
        1 => Record::Directory(deserialize_directory_owned(reader).await?),
        2 => Record::Link(LinkRecord::deserialize_owned(reader).await?),
        3 => Record::ExternalLink(ExternalLinkRecord::deserialize_owned(reader).await?),
        _ => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid or unsupported record type: {}", ty),
            ));
        }
    };
    let end = reader.stream_position().await?;
    tracing::debug!(
        start = format_args!("{:#x}", start),
        end = format_args!("{:#x}", end),
        bytes = end - start,
        ty,
        "deserialized Record (v1)"
    );
    Ok(record)
}

/// Deserialize BoxMetadata in v1 format (no root).
pub(crate) async fn deserialize_metadata_owned<R: AsyncRead + AsyncSeek + Unpin + Send>(
    reader: &mut R,
) -> std::io::Result<BoxMetadata<'static>> {
    let start = reader.stream_position().await?;

    // v1: root not serialized
    let root = Vec::new();

    let record_count = reader.read_vu64().await?;
    let mut records = Vec::with_capacity(record_count as usize);
    for _ in 0..record_count {
        records.push(deserialize_record_owned(reader).await?);
    }

    let attr_keys = deserialize_attr_keys_owned(reader).await?;
    let attrs = <HashMap<usize, Box<[u8]>>>::deserialize_owned(reader).await?;
    let fst = parse_fst_owned(reader).await?;

    let end = reader.stream_position().await?;
    tracing::debug!(
        start = format_args!("{:#x}", start),
        end = format_args!("{:#x}", end),
        bytes = end - start,
        records = records.len(),
        fst_size = fst.as_ref().map(|f| f.len()).unwrap_or(0),
        "deserialized BoxMetadata (v1)"
    );

    Ok(BoxMetadata {
        root,
        records,
        attr_keys,
        attrs,
        fst,
    })
}
