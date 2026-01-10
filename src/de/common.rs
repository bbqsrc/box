//! Shared deserialization implementations that don't vary by format version.

use std::borrow::Cow;
use std::collections::HashMap;
use std::num::NonZeroU64;

use fastvint::AsyncReadVintExt;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};

use crate::compression::constants::*;
use crate::header::{BoxHeader, MAGIC_BYTES};
use crate::{
    AttrMap, BoxPath, Compression, ExternalLinkRecord, FileRecord, LinkRecord, file::RecordIndex,
};

use super::{
    DeserializeBorrowed, DeserializeOwned, read_u8_slice, read_u32_le, read_u64_le,
    read_u64_le_slice, read_vlq_u64,
};

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
// OWNED IMPLEMENTATIONS
// ============================================================================

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
        path.validate()?;
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

        if &magic_bytes != MAGIC_BYTES {
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
            COMPRESSION_ZSTD => Zstd,
            COMPRESSION_XZ => Xz,
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

// ============================================================================
// FST PARSING HELPERS
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

/// Parse FST from reader.
/// v1 format: [u64 length][FST bytes]
pub(super) async fn parse_fst_owned<R: AsyncRead + AsyncSeek + Unpin + Send>(
    reader: &mut R,
) -> std::io::Result<Option<box_fst::Fst<Cow<'static, [u8]>>>> {
    // Read u64 length prefix
    let mut length_buf = [0u8; 8];
    match reader.read_exact(&mut length_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Ok(None); // EOF, no FST
        }
        Err(e) => return Err(e),
    }
    let fst_length = u64::from_le_bytes(length_buf) as usize;

    if fst_length == 0 {
        return Ok(None);
    }

    // Read exactly fst_length bytes
    let mut fst_bytes = vec![0u8; fst_length];
    reader.read_exact(&mut fst_bytes).await?;
    Ok(box_fst::Fst::new(Cow::Owned(fst_bytes)).ok())
}
