use std::io::SeekFrom;

use fastvint::AsyncWriteVintExt;
use tokio::io::{AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt};

use crate::{
    AttrMap, BoxHeader, BoxMetadata, BoxPath, ChunkedFileRecord, Compression, DirectoryRecord,
    ExternalLinkRecord, FileRecord, LinkRecord, Record,
    compression::constants::{
        RECORD_TYPE_CHUNKED_FILE, RECORD_TYPE_DIRECTORY, RECORD_TYPE_EXTERNAL_SYMLINK,
        RECORD_TYPE_FILE, RECORD_TYPE_SYMLINK,
    },
    file::RecordIndex,
    file::meta::AttrKey,
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

impl Serialize for str {
    async fn write<W: AsyncWrite + AsyncSeek + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> std::io::Result<()> {
        writer.write_vu64(self.len() as u64).await?;
        writer.write_all(self.as_bytes()).await?;
        Ok(())
    }
}

impl Serialize for String {
    async fn write<W: AsyncWrite + AsyncSeek + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> std::io::Result<()> {
        self.as_str().write(writer).await
    }
}

impl<'a> Serialize for std::borrow::Cow<'a, str> {
    async fn write<W: AsyncWrite + AsyncSeek + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> std::io::Result<()> {
        self.as_ref().write(writer).await
    }
}

impl Serialize for Box<[u8]> {
    async fn write<W: AsyncWrite + AsyncSeek + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> std::io::Result<()> {
        writer.write_vu64(self.len() as u64).await?;
        writer.write_all(self).await?;
        Ok(())
    }
}

impl Serialize for Vec<AttrKey> {
    async fn write<W: AsyncWrite + AsyncSeek + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> std::io::Result<()> {
        // v1 format: count, (type_tag, len, bytes)*
        writer.write_vu64(self.len() as u64).await?;
        for attr_key in self.iter() {
            writer.write_u8(attr_key.attr_type as u8).await?;
            writer.write_vu64(attr_key.name.len() as u64).await?;
            writer.write_all(attr_key.name.as_bytes()).await?;
        }
        Ok(())
    }
}

impl Serialize for AttrMap {
    async fn write<W: AsyncWrite + AsyncSeek + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> std::io::Result<()> {
        // Write the length in bytes so implementations can skip the entire map if they so choose.
        // Write it as u64::MAX, then seek back to fill in the actual size.
        let start = writer.stream_position().await?;
        write_u64_le(writer, u64::MAX).await?;
        writer.write_vu64(self.len() as u64).await?;

        for (key, value) in self.iter() {
            writer.write_vu64(*key as u64).await?;
            value.write(writer).await?;
        }

        // Go back and write size (excluding the u64 length field itself)
        let end = writer.stream_position().await?;
        writer.seek(SeekFrom::Start(start)).await?;
        write_u64_le(writer, end - start - 8).await?;
        writer.seek(SeekFrom::Start(end)).await?;

        Ok(())
    }
}

impl Serialize for BoxPath<'_> {
    async fn write<W: AsyncWrite + AsyncSeek + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> std::io::Result<()> {
        self.0.write(writer).await
    }
}

impl Serialize for RecordIndex {
    async fn write<W: AsyncWrite + AsyncSeek + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> std::io::Result<()> {
        writer.write_vu64(self.get()).await
    }
}

impl Serialize for FileRecord<'_> {
    async fn write<W: AsyncWrite + AsyncSeek + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> std::io::Result<()> {
        // Combined type/compression byte: bits 0-3 = type, bits 4-7 = compression
        // Compression IDs are already in high nibble position (0x00, 0x10, 0x20, etc.)
        writer
            .write_u8(self.compression.id() | RECORD_TYPE_FILE)
            .await?;
        write_u64_le(writer, self.length).await?;
        write_u64_le(writer, self.decompressed_length).await?;
        write_u64_le(writer, self.data.get()).await?;
        self.name.write(writer).await?;
        self.attrs.write(writer).await?;
        Ok(())
    }
}

impl Serialize for ChunkedFileRecord<'_> {
    async fn write<W: AsyncWrite + AsyncSeek + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> std::io::Result<()> {
        // Combined type/compression byte: bits 0-3 = type, bits 4-7 = compression
        writer
            .write_u8(self.compression.id() | RECORD_TYPE_CHUNKED_FILE)
            .await?;
        write_u32_le(writer, self.block_size).await?;
        write_u64_le(writer, self.length).await?;
        write_u64_le(writer, self.decompressed_length).await?;
        write_u64_le(writer, self.data.get()).await?;
        self.name.write(writer).await?;
        self.attrs.write(writer).await?;
        Ok(())
    }
}

impl Serialize for DirectoryRecord<'_> {
    async fn write<W: AsyncWrite + AsyncSeek + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> std::io::Result<()> {
        // Combined type/compression byte (compression is 0 for directories)
        writer.write_u8(RECORD_TYPE_DIRECTORY).await?;
        self.name.write(writer).await?;
        // v1: entries not serialized (found via FST prefix queries)
        self.attrs.write(writer).await?;
        Ok(())
    }
}

impl Serialize for LinkRecord<'_> {
    async fn write<W: AsyncWrite + AsyncSeek + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> std::io::Result<()> {
        // Combined type/compression byte (compression is 0 for symlinks)
        writer.write_u8(RECORD_TYPE_SYMLINK).await?;
        self.name.write(writer).await?;
        self.target.write(writer).await?;
        self.attrs.write(writer).await?;
        Ok(())
    }
}

impl Serialize for ExternalLinkRecord<'_> {
    async fn write<W: AsyncWrite + AsyncSeek + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> std::io::Result<()> {
        // Combined type/compression byte (compression is 0 for external symlinks)
        writer.write_u8(RECORD_TYPE_EXTERNAL_SYMLINK).await?;
        self.name.write(writer).await?;
        self.target.write(writer).await?;
        self.attrs.write(writer).await?;
        Ok(())
    }
}

impl Serialize for Record<'_> {
    async fn write<W: AsyncWrite + AsyncSeek + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> std::io::Result<()> {
        match self {
            Record::File(file) => file.write(writer).await,
            Record::ChunkedFile(file) => file.write(writer).await,
            Record::Directory(directory) => directory.write(writer).await,
            Record::Link(link) => link.write(writer).await,
            Record::ExternalLink(link) => link.write(writer).await,
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
        // flags byte: bit 1 = allow_escapes, bit 0 = allow_external_symlinks
        let flags = ((self.allow_escapes as u8) << 1) | self.allow_external_symlinks as u8;
        writer.write_u8(flags).await?;
        writer.write_all(&[0u8; 2]).await?; // reserved1 remaining
        write_u32_le(writer, self.alignment).await?;
        writer.write_all(&[0u8; 4]).await?; // reserved2
        write_u64_le(writer, self.trailer.map(|x| x.get()).unwrap_or(0)).await?;
        writer.write_all(&[0u8; 8]).await?; // reserved3
        Ok(())
    }
}

impl Serialize for BoxMetadata<'_> {
    async fn write<W: AsyncWrite + AsyncSeek + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> std::io::Result<()> {
        // v1: schema before data (attr_keys → attrs → dictionary → records → fst → block_fst)
        // root not serialized (paths indexed by FST)
        self.attr_keys.write(writer).await?;
        self.attrs.write(writer).await?;
        // Dictionary: [Vu64 length][bytes] - length=0 means no dictionary
        if let Some(dict) = &self.dictionary {
            writer.write_vu64(dict.len() as u64).await?;
            writer.write_all(dict).await?;
        } else {
            writer.write_vu64(0).await?;
        }
        self.records.write(writer).await?;

        // Write FST with padding and u64 length prefix
        write_fst(writer, self.fst.as_ref().map(|f| f.as_bytes())).await?;

        // Write block FST with padding and u64 length prefix
        write_fst(writer, self.block_fst.as_ref().map(|f| f.as_bytes())).await?;

        Ok(())
    }
}

/// Write an FST with u64 length prefix (no padding).
async fn write_fst<W: AsyncWrite + AsyncSeek + Unpin + Send>(
    writer: &mut W,
    fst_bytes: Option<&[u8]>,
) -> std::io::Result<()> {
    // Write u64 length prefix and data
    if let Some(bytes) = fst_bytes {
        write_u64_le(writer, bytes.len() as u64).await?;
        writer.write_all(bytes).await?;
    } else {
        write_u64_le(writer, 0).await?;
    }

    Ok(())
}

impl Serialize for Compression {
    async fn write<W: AsyncWrite + AsyncSeek + Unpin + Send>(
        &self,
        writer: &mut W,
    ) -> std::io::Result<()> {
        writer.write_u8(self.id()).await
    }
}
