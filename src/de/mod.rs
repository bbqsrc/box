use std::borrow::Cow;

use fastvint::ReadVintExt;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek};

use crate::{BoxMetadata, BoxPath, file::RecordIndex};

mod common;
pub(crate) mod v0;
pub(crate) mod v1;


// ============================================================================
// READ HELPERS (borrowed/sync)
// ============================================================================

/// Read a VLQ-encoded u64 from a byte slice, advancing the position.
pub(super) fn read_vlq_u64(data: &[u8], pos: &mut usize) -> std::io::Result<u64> {
    let mut cursor = std::io::Cursor::new(&data[*pos..]);
    let value = ReadVintExt::read_vu64(&mut cursor)?;
    *pos += cursor.position() as usize;
    Ok(value)
}

/// Read a little-endian u64 from a byte slice, advancing the position.
pub(super) fn read_u64_le_slice(data: &[u8], pos: &mut usize) -> std::io::Result<u64> {
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
pub(super) fn read_u8_slice(data: &[u8], pos: &mut usize) -> std::io::Result<u8> {
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

// ============================================================================
// READ HELPERS (owned/async)
// ============================================================================

/// Read a u64 in little-endian format
pub(super) async fn read_u64_le<R: AsyncRead + Unpin>(reader: &mut R) -> std::io::Result<u64> {
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf).await?;
    Ok(u64::from_le_bytes(buf))
}

/// Read a u32 in little-endian format
pub(super) async fn read_u32_le<R: AsyncRead + Unpin>(reader: &mut R) -> std::io::Result<u32> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf).await?;
    Ok(u32::from_le_bytes(buf))
}

// ============================================================================
// DESERIALIZATION TRAITS
// ============================================================================

/// Trait for deserializing from a borrowed byte slice (zero-copy).
pub(crate) trait DeserializeBorrowed<'a>: Send {
    fn deserialize_borrowed(data: &'a [u8], pos: &mut usize) -> std::io::Result<Self>
    where
        Self: Sized;
}

pub(crate) trait DeserializeOwned: Send {
    fn deserialize_owned<R: AsyncRead + AsyncSeek + Unpin + Send>(
        reader: &mut R,
    ) -> impl std::future::Future<Output = std::io::Result<Self>> + Send
    where
        Self: Sized;
}

// ============================================================================
// COMMON TRAIT IMPLEMENTATIONS (borrowed)
// ============================================================================

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

// ============================================================================
// VERSION DISPATCH FUNCTIONS
// ============================================================================

/// Deserialize BoxMetadata with version awareness (borrowed).
pub(crate) fn deserialize_metadata_borrowed<'a>(
    data: &'a [u8],
    pos: &mut usize,
    version: u8,
) -> std::io::Result<BoxMetadata<'a>> {
    match version {
        0 => v0::deserialize_metadata_borrowed(data, pos),
        _ => v1::deserialize_metadata_borrowed(data, pos),
    }
}

/// Deserialize BoxMetadata with version awareness (owned).
pub(crate) async fn deserialize_metadata_owned<R: AsyncRead + AsyncSeek + Unpin + Send>(
    reader: &mut R,
    version: u8,
) -> std::io::Result<BoxMetadata<'static>> {
    match version {
        0 => v0::deserialize_metadata_owned(reader).await,
        _ => v1::deserialize_metadata_owned(reader).await,
    }
}
