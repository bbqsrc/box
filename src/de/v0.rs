//! v0 format deserialization.
//!
//! v0 differences from v1:
//! - Directory entries are serialized in the directory record
//! - Metadata root (list of top-level record indices) is serialized
//! - Attribute keys have no type tag (default to Json)

use std::borrow::Cow;
use std::collections::HashMap;

use fastvint::AsyncReadVintExt;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};

use crate::{
    AttrMap, BoxMetadata, DirectoryRecord, ExternalLinkRecord, FileRecord, LinkRecord, Record,
    file::RecordIndex,
    file::meta::{AttrKey, AttrType},
};

use super::{DeserializeBorrowed, DeserializeOwned, read_u8_slice, read_vlq_u64};

// ============================================================================
// BORROWED DESERIALIZATION (v0)
// ============================================================================

/// Deserialize Vec<AttrKey> in v0 format (no type tag, default to Json).
pub(crate) fn deserialize_attr_keys_borrowed<'a>(
    data: &'a [u8],
    pos: &mut usize,
) -> std::io::Result<Vec<AttrKey>> {
    let len = read_vlq_u64(data, pos)? as usize;
    let mut keys = Vec::with_capacity(len);
    for _ in 0..len {
        // v0: no type tag, default to Json
        let attr_type = AttrType::Json;
        let name = <&'a str>::deserialize_borrowed(data, pos)?.to_string();
        keys.push(AttrKey { name, attr_type });
    }
    Ok(keys)
}

/// Deserialize DirectoryRecord in v0 format (with entries).
pub(crate) fn deserialize_directory_borrowed<'a>(
    data: &'a [u8],
    pos: &mut usize,
) -> std::io::Result<DirectoryRecord<'a>> {
    let name = <Cow<'a, str>>::deserialize_borrowed(data, pos)?;
    // v0: entries are serialized
    let entries = <Vec<RecordIndex>>::deserialize_borrowed(data, pos)?;
    let attrs = AttrMap::deserialize_borrowed(data, pos)?;

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

/// Deserialize BoxMetadata in v0 format (with root).
pub(crate) fn deserialize_metadata_borrowed<'a>(
    data: &'a [u8],
    pos: &mut usize,
) -> std::io::Result<BoxMetadata<'a>> {
    // v0: root is serialized
    let root = <Vec<RecordIndex>>::deserialize_borrowed(data, pos)?;

    let record_count = read_vlq_u64(data, pos)? as usize;
    let mut records = Vec::with_capacity(record_count);
    for _ in 0..record_count {
        records.push(deserialize_record_borrowed(data, pos)?);
    }

    let attr_keys = deserialize_attr_keys_borrowed(data, pos)?;
    let attrs = AttrMap::deserialize_borrowed(data, pos)?;

    // v0: no FST (root and entries are serialized explicitly)
    Ok(BoxMetadata {
        root,
        records,
        attr_keys,
        attrs,
        fst: None,
    })
}

// ============================================================================
// OWNED DESERIALIZATION (v0)
// ============================================================================

/// Deserialize Vec<AttrKey> in v0 format (no type tag, default to Json).
pub(crate) async fn deserialize_attr_keys_owned<R: AsyncRead + AsyncSeek + Unpin + Send>(
    reader: &mut R,
) -> std::io::Result<Vec<AttrKey>> {
    let start = reader.stream_position().await?;
    let len = reader.read_vu64().await? as usize;
    let mut keys = Vec::with_capacity(len);
    for _ in 0..len {
        // v0: no type tag, default to Json
        let attr_type = AttrType::Json;
        let name = String::deserialize_owned(reader).await?;
        keys.push(AttrKey { name, attr_type });
    }
    let end = reader.stream_position().await?;
    tracing::debug!(
        start = format_args!("{:#x}", start),
        end = format_args!("{:#x}", end),
        bytes = end - start,
        count = len,
        "deserialized AttrKeys (v0)"
    );
    Ok(keys)
}

/// Deserialize DirectoryRecord in v0 format (with entries).
pub(crate) async fn deserialize_directory_owned<R: AsyncRead + AsyncSeek + Unpin + Send>(
    reader: &mut R,
) -> std::io::Result<DirectoryRecord<'static>> {
    let start = reader.stream_position().await?;
    let name = String::deserialize_owned(reader).await?;

    // v0: entries are serialized
    let len = reader.read_vu64().await? as usize;
    let mut entries = Vec::with_capacity(len);
    for _ in 0..len {
        entries.push(RecordIndex::deserialize_owned(reader).await?);
    }

    let attrs = <HashMap<usize, Box<[u8]>>>::deserialize_owned(reader).await?;

    let end = reader.stream_position().await?;
    tracing::debug!(start = format_args!("{:#x}", start), end = format_args!("{:#x}", end), bytes = end - start, %name, "deserialized DirectoryRecord (v0)");

    Ok(DirectoryRecord {
        name: Cow::Owned(name),
        entries,
        attrs,
    })
}

/// Deserialize Record in v0 format.
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
        "deserialized Record (v0)"
    );
    Ok(record)
}

/// Deserialize BoxMetadata in v0 format (with root).
pub(crate) async fn deserialize_metadata_owned<R: AsyncRead + AsyncSeek + Unpin + Send>(
    reader: &mut R,
) -> std::io::Result<BoxMetadata<'static>> {
    let start = reader.stream_position().await?;

    // v0: root is serialized
    let root = <Vec<RecordIndex>>::deserialize_owned(reader).await?;

    let record_count = reader.read_vu64().await?;
    let mut records = Vec::with_capacity(record_count as usize);
    for _ in 0..record_count {
        records.push(deserialize_record_owned(reader).await?);
    }

    let attr_keys = deserialize_attr_keys_owned(reader).await?;
    let attrs = <HashMap<usize, Box<[u8]>>>::deserialize_owned(reader).await?;

    let end = reader.stream_position().await?;
    tracing::debug!(
        start = format_args!("{:#x}", start),
        end = format_args!("{:#x}", end),
        bytes = end - start,
        records = records.len(),
        "deserialized BoxMetadata (v0)"
    );

    // v0: no FST (root and entries are serialized explicitly)
    Ok(BoxMetadata {
        root,
        records,
        attr_keys,
        attrs,
        fst: None,
    })
}
