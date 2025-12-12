use std::collections::HashMap;
use std::num::NonZeroU64;

use fastvlq::AsyncReadVlqExt;
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::{
    AttrMap, BoxHeader, BoxMetadata, BoxPath, Compression, DirectoryRecord, FileRecord, LinkRecord,
    Record, file::Inode,
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
    fn deserialize_owned<R: AsyncRead + Unpin + Send>(
        reader: &mut R,
    ) -> impl std::future::Future<Output = std::io::Result<Self>> + Send
    where
        Self: Sized;
}

impl<T: DeserializeOwned> DeserializeOwned for Vec<T> {
    async fn deserialize_owned<R: AsyncRead + Unpin + Send>(reader: &mut R) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let len = reader.read_vu64().await?;
        let mut buf = Vec::with_capacity(len as usize);
        for _ in 0..len {
            buf.push(T::deserialize_owned(reader).await?);
        }
        Ok(buf)
    }
}

impl DeserializeOwned for BoxPath {
    async fn deserialize_owned<R: AsyncRead + Unpin + Send>(reader: &mut R) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        Ok(BoxPath(String::deserialize_owned(reader).await?))
    }
}

impl DeserializeOwned for AttrMap {
    async fn deserialize_owned<R: AsyncRead + Unpin + Send>(reader: &mut R) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let _byte_count = read_u64_le(reader).await?;
        let len = reader.read_vu64().await?;
        let mut buf: HashMap<usize, Vec<u8>> = HashMap::with_capacity(len as usize);
        for _ in 0..len {
            let key = reader.read_vu64().await?;
            let value = <Vec<u8>>::deserialize_owned(reader).await?;
            buf.insert(key as usize, value);
        }
        Ok(buf)
    }
}

impl DeserializeOwned for FileRecord {
    async fn deserialize_owned<R: AsyncRead + Unpin + Send>(
        reader: &mut R,
    ) -> std::io::Result<Self> {
        let compression = Compression::deserialize_owned(reader).await?;
        let length = read_u64_le(reader).await?;
        let decompressed_length = read_u64_le(reader).await?;
        let data = read_u64_le(reader).await?;
        let name = String::deserialize_owned(reader).await?;
        let attrs = <HashMap<usize, Vec<u8>>>::deserialize_owned(reader).await?;

        Ok(FileRecord {
            compression,
            length,
            decompressed_length,
            name,
            attrs,
            data: NonZeroU64::new(data).expect("non zero"),
        })
    }
}

impl DeserializeOwned for Inode {
    async fn deserialize_owned<R: AsyncRead + Unpin + Send>(
        reader: &mut R,
    ) -> std::io::Result<Self> {
        let value = reader.read_vu64().await?;
        Inode::new(value)
    }
}

impl DeserializeOwned for DirectoryRecord {
    async fn deserialize_owned<R: AsyncRead + Unpin + Send>(
        reader: &mut R,
    ) -> std::io::Result<Self> {
        let name = String::deserialize_owned(reader).await?;

        // Inodes vec
        let len = reader.read_vu64().await? as usize;
        let mut inodes = Vec::with_capacity(len);
        for _ in 0..len {
            inodes.push(Inode::deserialize_owned(reader).await?);
        }

        let attrs = <HashMap<usize, Vec<u8>>>::deserialize_owned(reader).await?;

        Ok(DirectoryRecord {
            name,
            inodes,
            attrs,
        })
    }
}

impl DeserializeOwned for LinkRecord {
    async fn deserialize_owned<R: AsyncRead + Unpin + Send>(
        reader: &mut R,
    ) -> std::io::Result<Self> {
        let name = String::deserialize_owned(reader).await?;
        let target = BoxPath::deserialize_owned(reader).await?;
        let attrs = <HashMap<usize, Vec<u8>>>::deserialize_owned(reader).await?;

        Ok(LinkRecord {
            name,
            target,
            attrs,
        })
    }
}

impl DeserializeOwned for Record {
    async fn deserialize_owned<R: AsyncRead + Unpin + Send>(
        reader: &mut R,
    ) -> std::io::Result<Self> {
        let ty = reader.read_u8().await?;
        match ty {
            0 => Ok(Record::File(FileRecord::deserialize_owned(reader).await?)),
            1 => Ok(Record::Directory(
                DirectoryRecord::deserialize_owned(reader).await?,
            )),
            2 => Ok(Record::Link(LinkRecord::deserialize_owned(reader).await?)),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid or unsupported field type: {}", ty),
            )),
        }
    }
}

impl DeserializeOwned for BoxHeader {
    async fn deserialize_owned<R: AsyncRead + Unpin + Send>(
        reader: &mut R,
    ) -> std::io::Result<Self> {
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

        Ok(BoxHeader {
            magic_bytes,
            version,
            alignment,
            trailer: NonZeroU64::new(trailer),
        })
    }
}

impl DeserializeOwned for BoxMetadata {
    async fn deserialize_owned<R: AsyncRead + Unpin + Send>(
        reader: &mut R,
    ) -> std::io::Result<Self> {
        let root = <Vec<Inode>>::deserialize_owned(reader).await?;
        let inodes = <Vec<Record>>::deserialize_owned(reader).await?;
        let attr_keys = <Vec<String>>::deserialize_owned(reader).await?;
        let attrs = <HashMap<usize, Vec<u8>>>::deserialize_owned(reader).await?;

        Ok(BoxMetadata {
            root,
            inodes,
            attr_keys,
            attrs,
        })
    }
}

impl DeserializeOwned for Compression {
    async fn deserialize_owned<R: AsyncRead + Unpin + Send>(reader: &mut R) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let id = reader.read_u8().await?;

        use Compression::*;

        Ok(match id {
            COMPRESSION_STORED => Stored,
            COMPRESSION_BROTLI => Brotli,
            COMPRESSION_DEFLATE => Deflate,
            COMPRESSION_ZSTD => Zstd,
            COMPRESSION_XZ => Xz,
            COMPRESSION_SNAPPY => Snappy,
            id => Unknown(id),
        })
    }
}

impl DeserializeOwned for String {
    async fn deserialize_owned<R: AsyncRead + Unpin + Send>(reader: &mut R) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let len = reader.read_vu64().await?;
        let mut buf = vec![0u8; len as usize];
        reader.read_exact(&mut buf).await?;
        String::from_utf8(buf).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }
}

impl DeserializeOwned for Vec<u8> {
    async fn deserialize_owned<R: AsyncRead + Unpin + Send>(reader: &mut R) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let len = reader.read_vu64().await?;
        let mut buf = vec![0u8; len as usize];
        reader.read_exact(&mut buf).await?;
        Ok(buf)
    }
}
