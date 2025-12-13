use std::borrow::Cow;
use std::collections::HashMap;
use std::num::NonZeroU64;

use fastvlq::AsyncReadVlqExt;
use string_interner::DefaultStringInterner;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};

use crate::{
    AttrMap, BoxHeader, BoxMetadata, BoxPath, Compression, DirectoryRecord, FileRecord, LinkRecord,
    Record, file::RecordIndex,
};

use crate::compression::constants::*;

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
        let mut buf: HashMap<usize, Vec<u8>> = HashMap::with_capacity(len as usize);
        for _ in 0..len {
            let key = reader.read_vu64().await?;
            let value = <Vec<u8>>::deserialize_owned(reader).await?;
            buf.insert(key as usize, value);
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
        let attrs = <HashMap<usize, Vec<u8>>>::deserialize_owned(reader).await?;

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

impl DeserializeOwned for DirectoryRecord<'static> {
    async fn deserialize_owned<R: AsyncRead + AsyncSeek + Unpin + Send>(
        reader: &mut R,
    ) -> std::io::Result<Self> {
        let start = reader.stream_position().await?;
        let name = String::deserialize_owned(reader).await?;

        // Entries vec
        let len = reader.read_vu64().await? as usize;
        let mut entries = Vec::with_capacity(len);
        for _ in 0..len {
            entries.push(RecordIndex::deserialize_owned(reader).await?);
        }

        let attrs = <HashMap<usize, Vec<u8>>>::deserialize_owned(reader).await?;

        let end = reader.stream_position().await?;
        tracing::debug!(start = format_args!("{:#x}", start), end = format_args!("{:#x}", end), bytes = end - start, %name, "deserialized DirectoryRecord");

        Ok(DirectoryRecord {
            name: Cow::Owned(name),
            entries,
            attrs,
        })
    }
}

impl DeserializeOwned for LinkRecord<'static> {
    async fn deserialize_owned<R: AsyncRead + AsyncSeek + Unpin + Send>(
        reader: &mut R,
    ) -> std::io::Result<Self> {
        let start = reader.stream_position().await?;
        let name = String::deserialize_owned(reader).await?;
        let target = BoxPath::deserialize_owned(reader).await?;
        let attrs = <HashMap<usize, Vec<u8>>>::deserialize_owned(reader).await?;

        let end = reader.stream_position().await?;
        tracing::debug!(start = format_args!("{:#x}", start), end = format_args!("{:#x}", end), bytes = end - start, %name, "deserialized LinkRecord");

        Ok(LinkRecord {
            name: Cow::Owned(name),
            target,
            attrs,
        })
    }
}

impl DeserializeOwned for Record<'static> {
    async fn deserialize_owned<R: AsyncRead + AsyncSeek + Unpin + Send>(
        reader: &mut R,
    ) -> std::io::Result<Self> {
        let start = reader.stream_position().await?;
        let ty = reader.read_u8().await?;
        let record = match ty {
            0 => Record::File(FileRecord::deserialize_owned(reader).await?),
            1 => Record::Directory(DirectoryRecord::deserialize_owned(reader).await?),
            2 => Record::Link(LinkRecord::deserialize_owned(reader).await?),
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("invalid or unsupported field type: {}", ty),
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
        reader.read_exact(&mut [0u8; 3]).await?; // skip reserved1
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
            alignment,
            trailer: NonZeroU64::new(trailer),
        })
    }
}

impl DeserializeOwned for BoxMetadata<'static> {
    async fn deserialize_owned<R: AsyncRead + AsyncSeek + Unpin + Send>(
        reader: &mut R,
    ) -> std::io::Result<Self> {
        let start = reader.stream_position().await?;
        let root = <Vec<RecordIndex>>::deserialize_owned(reader).await?;
        let records = <Vec<Record<'static>>>::deserialize_owned(reader).await?;
        let attr_keys = <DefaultStringInterner>::deserialize_owned(reader).await?;
        let attrs = <HashMap<usize, Vec<u8>>>::deserialize_owned(reader).await?;

        let end = reader.stream_position().await?;
        tracing::debug!(
            start = format_args!("{:#x}", start),
            end = format_args!("{:#x}", end),
            bytes = end - start,
            records = records.len(),
            "deserialized BoxMetadata"
        );

        Ok(BoxMetadata {
            root,
            records,
            attr_keys,
            attrs,
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

impl DeserializeOwned for DefaultStringInterner {
    async fn deserialize_owned<R: AsyncRead + AsyncSeek + Unpin + Send>(
        reader: &mut R,
    ) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let start = reader.stream_position().await?;
        let len = reader.read_vu64().await? as usize;
        let mut interner = DefaultStringInterner::new();
        for _ in 0..len {
            let s = String::deserialize_owned(reader).await?;
            // Symbols are assigned in order: 0, 1, 2... matching the indices
            interner.get_or_intern(s);
        }
        let end = reader.stream_position().await?;
        tracing::debug!(
            start = format_args!("{:#x}", start),
            end = format_args!("{:#x}", end),
            bytes = end - start,
            count = len,
            "deserialized StringInterner"
        );
        Ok(interner)
    }
}
