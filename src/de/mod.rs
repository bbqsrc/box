//! Deserialization helpers using sans-IO parsing.
//!
//! This module provides:
//! - `DeserializeBorrowed` trait for zero-copy parsing from byte slices
//! - Sync helper functions for parsing from byte slices

use std::borrow::Cow;
use std::fmt;

use fastvint::ReadVintExt;

use crate::{BoxMetadata, BoxPath, core::RecordIndex};

mod common;
pub(crate) mod v0;
pub(crate) mod v1;

// ============================================================================
// READ HELPERS (borrowed/sync)
// ============================================================================

#[derive(Debug)]
struct ErrorContext {
    message: String,
    source: std::io::Error,
}

impl fmt::Display for ErrorContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ErrorContext {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

/// Add a descriptive layer to an I/O error without discarding its source chain.
pub(crate) fn wrap_io_error(source: std::io::Error, message: impl Into<String>) -> std::io::Error {
    let kind = source.kind();
    std::io::Error::new(
        kind,
        ErrorContext {
            message: message.into(),
            source,
        },
    )
}

pub(super) fn read_context<T>(
    result: std::io::Result<T>,
    what: impl fmt::Display,
    offset: usize,
) -> std::io::Result<T> {
    result.map_err(|source| {
        wrap_io_error(
            source,
            format!("failed to read {what} at trailer byte {offset} (0x{offset:x})"),
        )
    })
}

pub(super) fn checked_count(
    data: &[u8],
    pos: usize,
    count: u64,
    what: &str,
    count_offset: usize,
) -> std::io::Result<usize> {
    let count = usize::try_from(count).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "{what} at trailer byte {count_offset} does not fit in memory: {count} entries"
            ),
        )
    })?;
    let remaining = data.len().saturating_sub(pos);
    if count > remaining {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "{what} at trailer byte {count_offset} declares {count} entries, but only \
                 {remaining} trailer bytes remain (each entry needs at least one byte)"
            ),
        ));
    }
    Ok(count)
}

fn unexpected_eof(data: &[u8], offset: usize, needed: usize, what: &str) -> std::io::Error {
    let available = data.len().saturating_sub(offset);
    let missing = needed.saturating_sub(available);
    std::io::Error::new(
        std::io::ErrorKind::UnexpectedEof,
        format!(
            "cannot read {what} at trailer byte {offset} (0x{offset:x}): \
             need {needed} bytes, but only {available} remain ({missing} bytes missing)"
        ),
    )
}

/// Read a VLQ-encoded u64 from a byte slice, advancing the position.
pub(super) fn read_vlq_u64(data: &[u8], pos: &mut usize) -> std::io::Result<u64> {
    let start = *pos;
    if start > data.len() {
        return Err(unexpected_eof(data, start, 1, "a variable-length integer"));
    }

    let mut cursor = std::io::Cursor::new(&data[*pos..]);
    let value = ReadVintExt::read_vu64(&mut cursor).map_err(|source| {
        wrap_io_error(
            source,
            format!(
                "cannot read a variable-length integer at trailer byte {start} \
                 (0x{start:x}); {} bytes remain",
                data.len() - start
            ),
        )
    })?;
    *pos += cursor.position() as usize;
    Ok(value)
}

/// Read a little-endian u64 from a byte slice, advancing the position.
pub(super) fn read_u64_le_slice(data: &[u8], pos: &mut usize) -> std::io::Result<u64> {
    if data.len().saturating_sub(*pos) < 8 {
        return Err(unexpected_eof(data, *pos, 8, "a little-endian u64"));
    }
    let bytes: [u8; 8] = data[*pos..*pos + 8].try_into().unwrap();
    *pos += 8;
    Ok(u64::from_le_bytes(bytes))
}

/// Read a little-endian u32 from a byte slice, advancing the position.
pub(super) fn read_u32_le_slice(data: &[u8], pos: &mut usize) -> std::io::Result<u32> {
    if data.len().saturating_sub(*pos) < 4 {
        return Err(unexpected_eof(data, *pos, 4, "a little-endian u32"));
    }
    let bytes: [u8; 4] = data[*pos..*pos + 4].try_into().unwrap();
    *pos += 4;
    Ok(u32::from_le_bytes(bytes))
}

/// Read a u8 from a byte slice, advancing the position.
pub(super) fn read_u8_slice(data: &[u8], pos: &mut usize) -> std::io::Result<u8> {
    if *pos >= data.len() {
        return Err(unexpected_eof(data, *pos, 1, "a byte"));
    }
    let byte = data[*pos];
    *pos += 1;
    Ok(byte)
}

// ============================================================================
// DESERIALIZATION TRAIT (borrowed)
// ============================================================================

/// Trait for deserializing from a borrowed byte slice (zero-copy).
pub(crate) trait DeserializeBorrowed<'a>: Send {
    fn deserialize_borrowed(data: &'a [u8], pos: &mut usize) -> std::io::Result<Self>
    where
        Self: Sized;
}

// ============================================================================
// COMMON TRAIT IMPLEMENTATIONS (borrowed)
// ============================================================================

impl<'a> DeserializeBorrowed<'a> for &'a str {
    fn deserialize_borrowed(data: &'a [u8], pos: &mut usize) -> std::io::Result<Self> {
        let length_offset = *pos;
        let len = read_context(
            read_vlq_u64(data, pos),
            "string length prefix",
            length_offset,
        )?;
        let len = usize::try_from(len).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "string length at trailer byte {length_offset} does not fit in memory: {len}"
                ),
            )
        })?;
        let string_offset = *pos;
        let Some(end) = string_offset.checked_add(len) else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "string at trailer byte {string_offset} has an overflowing length: {len} bytes"
                ),
            ));
        };
        if end > data.len() {
            let available = data.len().saturating_sub(string_offset);
            let missing = len - available;
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!(
                    "cannot read string contents at trailer byte {string_offset} \
                     (0x{string_offset:x}): its length prefix declares {len} bytes, \
                     but only {available} remain ({missing} bytes missing)"
                ),
            ));
        }
        let bytes = &data[string_offset..end];
        *pos = end;
        std::str::from_utf8(bytes).map_err(|error| {
            wrap_io_error(
                std::io::Error::new(std::io::ErrorKind::InvalidData, error),
                format!("string bytes {string_offset}..{end} in the trailer are not valid UTF-8"),
            )
        })
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
        let offset = *pos;
        let s = <Cow<'a, str>>::deserialize_borrowed(data, pos)?;
        let path = BoxPath(s);
        read_context(path.validate(), "Box path", offset)?;
        Ok(path)
    }
}

impl<'a> DeserializeBorrowed<'a> for RecordIndex {
    fn deserialize_borrowed(data: &'a [u8], pos: &mut usize) -> std::io::Result<Self> {
        let offset = *pos;
        let value = read_context(read_vlq_u64(data, pos), "record index", offset)?;
        read_context(RecordIndex::new(value), "record index", offset)
    }
}

impl<'a> DeserializeBorrowed<'a> for Vec<RecordIndex> {
    fn deserialize_borrowed(data: &'a [u8], pos: &mut usize) -> std::io::Result<Self> {
        let count_offset = *pos;
        let len = read_context(
            read_vlq_u64(data, pos),
            "record-index list length",
            count_offset,
        )?;
        let len = checked_count(data, *pos, len, "record-index list length", count_offset)?;
        let mut vec = Vec::with_capacity(len);
        for index in 0..len {
            let offset = *pos;
            vec.push(read_context(
                RecordIndex::deserialize_borrowed(data, pos),
                format_args!("record index {} of {len}", index + 1),
                offset,
            )?);
        }
        Ok(vec)
    }
}

// ============================================================================
// VERSION DISPATCH FUNCTIONS (borrowed)
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
