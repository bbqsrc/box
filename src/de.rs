use std::borrow::Cow;
use std::collections::HashMap;
use std::num::NonZeroU64;

use fastvint::{AsyncReadVintExt, ReadVintExt};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};

use crate::{
    AttrMap, BoxHeader, BoxMetadata, BoxPath, Compression, DirectoryRecord, ExternalLinkRecord,
    FileRecord, LinkRecord, Record,
    file::RecordIndex,
    file::meta::{AttrKey, AttrType},
};

use crate::compression::constants::*;

// ============================================================================
// BORROWED DESERIALIZATION (zero-copy from byte slices)
// ============================================================================

/// Read a VLQ-encoded u64 from a byte slice, advancing the position.
fn read_vlq_u64(data: &[u8], pos: &mut usize) -> std::io::Result<u64> {
    let mut cursor = std::io::Cursor::new(&data[*pos..]);
    let value = ReadVintExt::read_vu64(&mut cursor)?;
    *pos += cursor.position() as usize;
    Ok(value)
}

/// Read a little-endian u64 from a byte slice, advancing the position.
fn read_u64_le_slice(data: &[u8], pos: &mut usize) -> std::io::Result<u64> {
    if *pos + 8 > data.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "unexpected end of data reading u64",
        ));
    }
    let bytes: [u8; 8] = data[*pos..*pos + 8].try_into().unwrap();
    *pos += 8;
    Ok(u64::from_le_bytes(bytes))
}

/// Read a u8 from a byte slice, advancing the position.
fn read_u8_slice(data: &[u8], pos: &mut usize) -> std::io::Result<u8> {
    if *pos >= data.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "unexpected end of data reading u8",
        ));
    }
    let byte = data[*pos];
    *pos += 1;
    Ok(byte)
}

/// Trait for deserializing from a borrowed byte slice (zero-copy).
pub(crate) trait DeserializeBorrowed<'a>: Send {
    fn deserialize_borrowed(data: &'a [u8], pos: &mut usize) -> std::io::Result<Self>
    where
        Self: Sized;
}

impl<'a> DeserializeBorrowed<'a> for &'a str {
    fn deserialize_borrowed(data: &'a [u8], pos: &mut usize) -> std::io::Result<Self> {
        let len = read_vlq_u64(data, pos)? as usize;
        if *pos + len > data.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "unexpected end of data reading string",
            ));
        }
        let bytes = &data[*pos..*pos + len];
        *pos += len;
        std::str::from_utf8(bytes)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }
}

impl<'a> DeserializeBorrowed<'a> for Cow<'a, str> {
    fn deserialize_borrowed(data: &'a [u8], pos: &mut usize) -> std::io::Result<Self> {
        let s = <&'a str>::deserialize_borrowed(data, pos)?;
        Ok(Cow::Borrowed(s))
    }
}

impl<'a> DeserializeBorrowed<'a> for BoxPath<'a> {
    fn deserialize_borrowed(data: &'a [u8], pos: &mut usize) -> std::io::Result<Self> {
        let s = <Cow<'a, str>>::deserialize_borrowed(data, pos)?;
        Ok(BoxPath(s))
    }
}

impl<'a> DeserializeBorrowed<'a> for RecordIndex {
    fn deserialize_borrowed(data: &'a [u8], pos: &mut usize) -> std::io::Result<Self> {
        let value = read_vlq_u64(data, pos)?;
        RecordIndex::new(value)
    }
}

impl<'a> DeserializeBorrowed<'a> for Vec<RecordIndex> {
    fn deserialize_borrowed(data: &'a [u8], pos: &mut usize) -> std::io::Result<Self> {
        let len = read_vlq_u64(data, pos)? as usize;
        let mut vec = Vec::with_capacity(len);
        for _ in 0..len {
            vec.push(RecordIndex::deserialize_borrowed(data, pos)?);
        }
        Ok(vec)
    }
}

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
            COMPRESSION_BROTLI => Brotli,
            COMPRESSION_DEFLATE => Deflate,
            COMPRESSION_ZSTD => Zstd,
            COMPRESSION_XZ => Xz,
            COMPRESSION_SNAPPY => Snappy,
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

/// Deserialize DirectoryRecord with version awareness.
pub(crate) fn deserialize_directory_borrowed<'a>(
    data: &'a [u8],
    pos: &mut usize,
    version: u8,
) -> std::io::Result<DirectoryRecord<'a>> {
    let name = <Cow<'a, str>>::deserialize_borrowed(data, pos)?;
    let entries = if version == 0 {
        <Vec<RecordIndex>>::deserialize_borrowed(data, pos)?
    } else {
        Vec::new() // v1+: entries not in binary format
    };
    let attrs = AttrMap::deserialize_borrowed(data, pos)?;

    Ok(DirectoryRecord {
        name,
        entries,
        attrs,
    })
}

impl<'a> DeserializeBorrowed<'a> for DirectoryRecord<'a> {
    fn deserialize_borrowed(data: &'a [u8], pos: &mut usize) -> std::io::Result<Self> {
        deserialize_directory_borrowed(data, pos, 0) // Default to v0 for trait
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

/// Deserialize Record with version awareness.
pub(crate) fn deserialize_record_borrowed<'a>(
    data: &'a [u8],
    pos: &mut usize,
    version: u8,
) -> std::io::Result<Record<'a>> {
    let ty = read_u8_slice(data, pos)?;
    let record = match ty {
        0 => Record::File(FileRecord::deserialize_borrowed(data, pos)?),
        1 => Record::Directory(deserialize_directory_borrowed(data, pos, version)?),
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

impl<'a> DeserializeBorrowed<'a> for Record<'a> {
    fn deserialize_borrowed(data: &'a [u8], pos: &mut usize) -> std::io::Result<Self> {
        deserialize_record_borrowed(data, pos, 0) // Default to v0 for trait
    }
}

/// Deserialize Vec<AttrKey> with version awareness.
pub(crate) fn deserialize_attr_keys_borrowed<'a>(
    data: &'a [u8],
    pos: &mut usize,
    version: u8,
) -> std::io::Result<Vec<AttrKey>> {
    let len = read_vlq_u64(data, pos)? as usize;
    let mut keys = Vec::with_capacity(len);
    for _ in 0..len {
        let attr_type = if version == 0 {
            // v0: no type tag, default to Json
            AttrType::Json
        } else {
            // v1+: read type tag first
            let type_tag = read_u8_slice(data, pos)?;
            AttrType::from_u8(type_tag).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("unknown attribute type tag: {}", type_tag),
                )
            })?
        };
        let name = <&'a str>::deserialize_borrowed(data, pos)?.to_string();
        keys.push(AttrKey { name, attr_type });
    }
    Ok(keys)
}

/// Deserialize BoxMetadata with version awareness.
pub(crate) fn deserialize_metadata_borrowed<'a>(
    data: &'a [u8],
    pos: &mut usize,
    version: u8,
) -> std::io::Result<BoxMetadata<'a>> {
    let root = if version == 0 {
        <Vec<RecordIndex>>::deserialize_borrowed(data, pos)?
    } else {
        Vec::new()
    };

    let record_count = read_vlq_u64(data, pos)? as usize;
    let mut records = Vec::with_capacity(record_count);
    for _ in 0..record_count {
        records.push(deserialize_record_borrowed(data, pos, version)?);
    }

    let attr_keys = deserialize_attr_keys_borrowed(data, pos, version)?;
    let attrs = AttrMap::deserialize_borrowed(data, pos)?;

    // Skip 0-padding to 8-byte boundary
    while *pos < data.len() && data[*pos] == 0 {
        *pos += 1;
    }

    // Parse FST from remaining bytes (no length prefix)
    let fst = if *pos >= data.len() {
        None
    } else {
        box_fst::Fst::new(Cow::Borrowed(&data[*pos..])).ok()
    };

    Ok(BoxMetadata {
        root,
        records,
        attr_keys,
        attrs,
        fst,
    })
}

// ============================================================================
// OWNED DESERIALIZATION (async from readers)
// ============================================================================

/// Read a u64 in little-endian format
async fn read_u64_le<R: AsyncRead + Unpin>(reader: &mut R) -> std::io::Result<u64> {
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf).await?;
    Ok(u64::from_le_bytes(buf))
}

/// Read a u32 in little-endian format
async fn read_u32_le<R: AsyncRead + Unpin>(reader: &mut R) -> std::io::Result<u32> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf).await?;
    Ok(u32::from_le_bytes(buf))
}

pub(crate) trait DeserializeOwned: Send {
    fn deserialize_owned<R: AsyncRead + AsyncSeek + Unpin + Send>(
        reader: &mut R,
    ) -> impl std::future::Future<Output = std::io::Result<Self>> + Send
    where
        Self: Sized;
}

impl<T: DeserializeOwned> DeserializeOwned for Vec<T> {
    async fn deserialize_owned<R: AsyncRead + AsyncSeek + Unpin + Send>(
        reader: &mut R,
    ) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let start = reader.stream_position().await?;
        let len = reader.read_vu64().await?;
        let mut buf = Vec::with_capacity(len as usize);
        for _ in 0..len {
            buf.push(T::deserialize_owned(reader).await?);
        }
        let end = reader.stream_position().await?;
        tracing::debug!(
            start = format_args!("{:#x}", start),
            end = format_args!("{:#x}", end),
            bytes = end - start,
            count = len,
            "deserialized Vec"
        );
        Ok(buf)
    }
}

impl DeserializeOwned for BoxPath<'static> {
    async fn deserialize_owned<R: AsyncRead + AsyncSeek + Unpin + Send>(
        reader: &mut R,
    ) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let start = reader.stream_position().await?;
        let path = BoxPath(Cow::Owned(String::deserialize_owned(reader).await?));
        let end = reader.stream_position().await?;
        tracing::debug!(
            start = format_args!("{:#x}", start),
            end = format_args!("{:#x}", end),
            bytes = end - start,
            "deserialized BoxPath"
        );
        Ok(path)
    }
}

impl DeserializeOwned for AttrMap {
    async fn deserialize_owned<R: AsyncRead + AsyncSeek + Unpin + Send>(
        reader: &mut R,
    ) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let start = reader.stream_position().await?;
        let _byte_count = read_u64_le(reader).await?;
        let len = reader.read_vu64().await?;
        let mut buf: HashMap<usize, Box<[u8]>> = HashMap::with_capacity(len as usize);
        for _ in 0..len {
            let key = reader.read_vu64().await?;
            let value = <Vec<u8>>::deserialize_owned(reader).await?;
            buf.insert(key as usize, value.into_boxed_slice());
        }
        let end = reader.stream_position().await?;
        tracing::debug!(
            start = format_args!("{:#x}", start),
            end = format_args!("{:#x}", end),
            bytes = end - start,
            count = len,
            "deserialized AttrMap"
        );
        Ok(buf)
    }
}

impl DeserializeOwned for FileRecord<'static> {
    async fn deserialize_owned<R: AsyncRead + AsyncSeek + Unpin + Send>(
        reader: &mut R,
    ) -> std::io::Result<Self> {
        let start = reader.stream_position().await?;
        let compression = Compression::deserialize_owned(reader).await?;
        let length = read_u64_le(reader).await?;
        let decompressed_length = read_u64_le(reader).await?;
        let data = read_u64_le(reader).await?;
        let name = String::deserialize_owned(reader).await?;
        let attrs = <HashMap<usize, Box<[u8]>>>::deserialize_owned(reader).await?;

        let end = reader.stream_position().await?;
        tracing::debug!(start = format_args!("{:#x}", start), end = format_args!("{:#x}", end), bytes = end - start, %name, "deserialized FileRecord");

        Ok(FileRecord {
            compression,
            length,
            decompressed_length,
            name: Cow::Owned(name),
            attrs,
            data: NonZeroU64::new(data).expect("non zero"),
        })
    }
}

impl DeserializeOwned for RecordIndex {
    async fn deserialize_owned<R: AsyncRead + AsyncSeek + Unpin + Send>(
        reader: &mut R,
    ) -> std::io::Result<Self> {
        let start = reader.stream_position().await?;
        let value = reader.read_vu64().await?;
        let end = reader.stream_position().await?;
        tracing::debug!(
            start = format_args!("{:#x}", start),
            end = format_args!("{:#x}", end),
            bytes = end - start,
            value,
            "deserialized RecordIndex"
        );
        RecordIndex::new(value)
    }
}

/// Deserialize DirectoryRecord with version awareness (owned).
pub(crate) async fn deserialize_directory_owned<R: AsyncRead + AsyncSeek + Unpin + Send>(
    reader: &mut R,
    version: u8,
) -> std::io::Result<DirectoryRecord<'static>> {
    let start = reader.stream_position().await?;
    let name = String::deserialize_owned(reader).await?;

    let entries = if version == 0 {
        let len = reader.read_vu64().await? as usize;
        let mut entries = Vec::with_capacity(len);
        for _ in 0..len {
            entries.push(RecordIndex::deserialize_owned(reader).await?);
        }
        entries
    } else {
        Vec::new()
    };

    let attrs = <HashMap<usize, Box<[u8]>>>::deserialize_owned(reader).await?;

    let end = reader.stream_position().await?;
    tracing::debug!(start = format_args!("{:#x}", start), end = format_args!("{:#x}", end), bytes = end - start, %name, "deserialized DirectoryRecord");

    Ok(DirectoryRecord {
        name: Cow::Owned(name),
        entries,
        attrs,
    })
}

/// Deserialize Record with version awareness (owned).
pub(crate) async fn deserialize_record_owned<R: AsyncRead + AsyncSeek + Unpin + Send>(
    reader: &mut R,
    version: u8,
) -> std::io::Result<Record<'static>> {
    let start = reader.stream_position().await?;
    let ty = reader.read_u8().await?;
    let record = match ty {
        0 => Record::File(FileRecord::deserialize_owned(reader).await?),
        1 => Record::Directory(deserialize_directory_owned(reader, version).await?),
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
        "deserialized Record"
    );
    Ok(record)
}

impl DeserializeOwned for LinkRecord<'static> {
    async fn deserialize_owned<R: AsyncRead + AsyncSeek + Unpin + Send>(
        reader: &mut R,
    ) -> std::io::Result<Self> {
        let start = reader.stream_position().await?;
        let name = String::deserialize_owned(reader).await?;
        let target = RecordIndex::deserialize_owned(reader).await?;
        let attrs = <HashMap<usize, Box<[u8]>>>::deserialize_owned(reader).await?;

        let end = reader.stream_position().await?;
        tracing::debug!(start = format_args!("{:#x}", start), end = format_args!("{:#x}", end), bytes = end - start, %name, "deserialized LinkRecord");

        Ok(LinkRecord {
            name: Cow::Owned(name),
            target,
            attrs,
        })
    }
}

impl DeserializeOwned for ExternalLinkRecord<'static> {
    async fn deserialize_owned<R: AsyncRead + AsyncSeek + Unpin + Send>(
        reader: &mut R,
    ) -> std::io::Result<Self> {
        let start = reader.stream_position().await?;
        let name = String::deserialize_owned(reader).await?;
        let target = String::deserialize_owned(reader).await?;
        let attrs = <HashMap<usize, Box<[u8]>>>::deserialize_owned(reader).await?;

        let end = reader.stream_position().await?;
        tracing::debug!(start = format_args!("{:#x}", start), end = format_args!("{:#x}", end), bytes = end - start, %name, "deserialized ExternalLinkRecord");

        Ok(ExternalLinkRecord {
            name: Cow::Owned(name),
            target: Cow::Owned(target),
            attrs,
        })
    }
}

impl DeserializeOwned for BoxHeader {
    async fn deserialize_owned<R: AsyncRead + AsyncSeek + Unpin + Send>(
        reader: &mut R,
    ) -> std::io::Result<Self> {
        let start = reader.stream_position().await?;
        let magic_bytes = read_u32_le(reader).await?.to_le_bytes();

        if &magic_bytes != crate::header::MAGIC_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Magic bytes invalid",
            ));
        }

        let version = reader.read_u8().await?;
        let flags = reader.read_u8().await?;
        let allow_external_symlinks = (flags & 0x01) != 0;
        let allow_escapes = (flags & 0x02) != 0;
        reader.read_exact(&mut [0u8; 2]).await?; // skip reserved1 remaining
        let alignment = read_u32_le(reader).await?;
        reader.read_exact(&mut [0u8; 4]).await?; // skip reserved2
        let trailer = read_u64_le(reader).await?;

        let end = reader.stream_position().await?;
        tracing::debug!(
            start = format_args!("{:#x}", start),
            end = format_args!("{:#x}", end),
            bytes = end - start,
            version,
            alignment,
            "deserialized BoxHeader"
        );

        Ok(BoxHeader {
            magic_bytes,
            version,
            allow_escapes,
            allow_external_symlinks,
            alignment,
            trailer: NonZeroU64::new(trailer),
        })
    }
}

/// Deserialize BoxMetadata with version awareness (owned).
pub(crate) async fn deserialize_metadata_owned<R: AsyncRead + AsyncSeek + Unpin + Send>(
    reader: &mut R,
    version: u8,
) -> std::io::Result<BoxMetadata<'static>> {
    let start = reader.stream_position().await?;

    let root = if version == 0 {
        <Vec<RecordIndex>>::deserialize_owned(reader).await?
    } else {
        Vec::new()
    };

    let record_count = reader.read_vu64().await?;
    let mut records = Vec::with_capacity(record_count as usize);
    for _ in 0..record_count {
        records.push(deserialize_record_owned(reader, version).await?);
    }

    let attr_keys = deserialize_attr_keys_owned(reader, version).await?;
    let attrs = <HashMap<usize, Box<[u8]>>>::deserialize_owned(reader).await?;

    // Skip 0-padding to 8-byte boundary
    loop {
        let mut byte = [0u8; 1];
        match reader.read_exact(&mut byte).await {
            Ok(_) if byte[0] == 0 => continue, // Skip padding
            Ok(_) => {
                // Non-zero byte found, seek back one byte
                reader.seek(std::io::SeekFrom::Current(-1)).await?;
                break;
            }
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break, // EOF
            Err(e) => return Err(e),
        }
    }

    // Read remaining bytes and parse as FST
    let mut fst_bytes = Vec::new();
    reader.read_to_end(&mut fst_bytes).await?;
    let fst = if fst_bytes.is_empty() {
        None
    } else {
        box_fst::Fst::new(Cow::Owned(fst_bytes)).ok()
    };

    let end = reader.stream_position().await?;
    tracing::debug!(
        start = format_args!("{:#x}", start),
        end = format_args!("{:#x}", end),
        bytes = end - start,
        records = records.len(),
        fst_size = fst.as_ref().map(|f| f.len()).unwrap_or(0),
        "deserialized BoxMetadata"
    );

    Ok(BoxMetadata {
        root,
        records,
        attr_keys,
        attrs,
        fst,
    })
}

impl DeserializeOwned for Compression {
    async fn deserialize_owned<R: AsyncRead + AsyncSeek + Unpin + Send>(
        reader: &mut R,
    ) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let start = reader.stream_position().await?;
        let id = reader.read_u8().await?;

        use Compression::*;

        let compression = match id {
            COMPRESSION_STORED => Stored,
            COMPRESSION_BROTLI => Brotli,
            COMPRESSION_DEFLATE => Deflate,
            COMPRESSION_ZSTD => Zstd,
            COMPRESSION_XZ => Xz,
            COMPRESSION_SNAPPY => Snappy,
            id => Unknown(id),
        };

        let end = reader.stream_position().await?;
        tracing::debug!(
            start = format_args!("{:#x}", start),
            end = format_args!("{:#x}", end),
            bytes = end - start,
            id,
            "deserialized Compression"
        );

        Ok(compression)
    }
}

impl DeserializeOwned for String {
    async fn deserialize_owned<R: AsyncRead + AsyncSeek + Unpin + Send>(
        reader: &mut R,
    ) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let start = reader.stream_position().await?;
        let len = reader.read_vu64().await?;
        let mut buf = vec![0u8; len as usize];
        reader.read_exact(&mut buf).await?;
        let end = reader.stream_position().await?;
        tracing::debug!(
            start = format_args!("{:#x}", start),
            end = format_args!("{:#x}", end),
            bytes = end - start,
            len,
            "deserialized String"
        );
        String::from_utf8(buf).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }
}

impl DeserializeOwned for Vec<u8> {
    async fn deserialize_owned<R: AsyncRead + AsyncSeek + Unpin + Send>(
        reader: &mut R,
    ) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let start = reader.stream_position().await?;
        let len = reader.read_vu64().await?;
        let mut buf = vec![0u8; len as usize];
        reader.read_exact(&mut buf).await?;
        let end = reader.stream_position().await?;
        tracing::debug!(
            start = format_args!("{:#x}", start),
            end = format_args!("{:#x}", end),
            bytes = end - start,
            len,
            "deserialized Vec<u8>"
        );
        Ok(buf)
    }
}

/// Deserialize Vec<AttrKey> with version awareness (owned).
pub(crate) async fn deserialize_attr_keys_owned<R: AsyncRead + AsyncSeek + Unpin + Send>(
    reader: &mut R,
    version: u8,
) -> std::io::Result<Vec<AttrKey>> {
    let start = reader.stream_position().await?;
    let len = reader.read_vu64().await? as usize;
    let mut keys = Vec::with_capacity(len);
    for _ in 0..len {
        let attr_type = if version == 0 {
            // v0: no type tag, default to Json
            AttrType::Json
        } else {
            // v1+: read type tag first
            let type_tag = reader.read_u8().await?;
            AttrType::from_u8(type_tag).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("unknown attribute type tag: {}", type_tag),
                )
            })?
        };
        let name = String::deserialize_owned(reader).await?;
        keys.push(AttrKey { name, attr_type });
    }
    let end = reader.stream_position().await?;
    tracing::debug!(
        start = format_args!("{:#x}", start),
        end = format_args!("{:#x}", end),
        bytes = end - start,
        count = len,
        "deserialized AttrKeys"
    );
    Ok(keys)
}
