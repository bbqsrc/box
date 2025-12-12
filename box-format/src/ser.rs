use std::io::SeekFrom;

use fastvlq::AsyncWriteVlqExt;
use tokio::io::{AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt};

use crate::{
    AttrMap, BoxHeader, BoxMetadata, BoxPath, Compression, DirectoryRecord, FileRecord, LinkRecord,
    Record, file::Inode,
};

/// Write a u32 in little-endian format
async fn write_u32_le<W: AsyncWrite + Unpin>(writer: &mut W, value: u32) -> std::io::Result<()> {
    writer.write_all(&value.to_le_bytes()).await
}

/// Write a u64 in little-endian format
async fn write_u64_le<W: AsyncWrite + Unpin>(writer: &mut W, value: u64) -> std::io::Result<()> {
    writer.write_all(&value.to_le_bytes()).await
}

pub(crate) trait Serialize: Send + Sync {
    fn write<W: AsyncWrite + AsyncSeek + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> impl std::future::Future<Output = std::io::Result<()>> + Send;
}

impl<T: Serialize> Serialize for Vec<T> {
    async fn write<W: AsyncWrite + AsyncSeek + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> std::io::Result<()> {
        writer.write_vu64(self.len() as u64).await?;

        for item in self.iter() {
            item.write(writer).await?;
        }
        Ok(())
    }
}

impl Serialize for String {
    async fn write<W: AsyncWrite + AsyncSeek + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> std::io::Result<()> {
        writer.write_vu64(self.len() as u64).await?;
        writer.write_all(self.as_bytes()).await
    }
}

impl Serialize for Vec<u8> {
    async fn write<W: AsyncWrite + AsyncSeek + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> std::io::Result<()> {
        writer.write_vu64(self.len() as u64).await?;
        writer.write_all(self).await
    }
}

impl Serialize for AttrMap {
    async fn write<W: AsyncWrite + AsyncSeek + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> std::io::Result<()> {
        // Write the length in bytes so implementations can skip the entire map if they so choose.

        // Write it as u64::MAX, then seek back
        let size_index = writer.stream_position().await?;
        write_u64_le(writer, std::u64::MAX).await?;
        writer.write_vu64(self.len() as u64).await?;

        for (key, value) in self.iter() {
            writer.write_vu64(*key as u64).await?;
            value.write(writer).await?;
        }

        // Go back and write size
        let cur_index = writer.stream_position().await?;
        writer.seek(SeekFrom::Start(size_index)).await?;
        write_u64_le(writer, cur_index - size_index).await?;
        writer.seek(SeekFrom::Start(cur_index)).await?;

        Ok(())
    }
}

impl Serialize for BoxPath {
    async fn write<W: AsyncWrite + AsyncSeek + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> std::io::Result<()> {
        self.0.write(writer).await
    }
}

impl Serialize for Inode {
    async fn write<W: AsyncWrite + AsyncSeek + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> std::io::Result<()> {
        writer.write_vu64(self.get()).await
    }
}

impl Serialize for FileRecord {
    async fn write<W: AsyncWrite + AsyncSeek + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> std::io::Result<()> {
        // Record id - 0 for file
        writer.write_u8(0x0).await?;

        writer.write_u8(self.compression.id()).await?;
        write_u64_le(writer, self.length).await?;
        write_u64_le(writer, self.decompressed_length).await?;
        write_u64_le(writer, self.data.get()).await?;

        self.name.write(writer).await?;
        self.attrs.write(writer).await
    }
}

impl Serialize for DirectoryRecord {
    async fn write<W: AsyncWrite + AsyncSeek + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> std::io::Result<()> {
        // Record id - 1 for directory
        writer.write_u8(0x1).await?;

        self.name.write(writer).await?;
        self.inodes.write(writer).await?;
        self.attrs.write(writer).await
    }
}

impl Serialize for LinkRecord {
    async fn write<W: AsyncWrite + AsyncSeek + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> std::io::Result<()> {
        // Record id - 2 for symlink
        writer.write_u8(0x2).await?;

        self.name.write(writer).await?;
        self.target.write(writer).await?;
        self.attrs.write(writer).await
    }
}

impl Serialize for Record {
    async fn write<W: AsyncWrite + AsyncSeek + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> std::io::Result<()> {
        match self {
            Record::File(file) => file.write(writer).await,
            Record::Directory(directory) => directory.write(writer).await,
            Record::Link(link) => link.write(writer).await,
        }
    }
}

impl Serialize for BoxHeader {
    async fn write<W: AsyncWrite + AsyncSeek + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> std::io::Result<()> {
        writer.write_all(&self.magic_bytes).await?;
        writer.write_u8(self.version).await?;
        writer.write_all(&[0u8; 3]).await?; // reserved1
        write_u32_le(writer, self.alignment).await?;
        writer.write_all(&[0u8; 4]).await?; // reserved2
        write_u64_le(writer, self.trailer.map(|x| x.get()).unwrap_or(0)).await
    }
}

impl Serialize for BoxMetadata {
    async fn write<W: AsyncWrite + AsyncSeek + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> std::io::Result<()> {
        self.root.write(writer).await?;
        self.inodes.write(writer).await?;
        self.attr_keys.write(writer).await?;
        self.attrs.write(writer).await?;
        Ok(())
    }
}

impl Serialize for Compression {
    async fn write<W: AsyncWrite + AsyncSeek + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> std::io::Result<()> {
        writer.write_u8(self.id()).await
    }
}
