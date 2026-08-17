// SPDX-License-Identifier: GPL-2.0-only
//! Box archive parsing for kernel module
//!
//! This module provides parsing functions for the box archive format,
//! adapted for kernel use (no_std + alloc).

use alloc::borrow::Cow;
use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;

use box_fst::Fst;

use crate::error::KernelError;
use crate::metadata::{ArchiveData, AttrKey, AttrMap, AttrType, Compression, Record, RecordData};
use hashbrown::HashMap;

/// Magic bytes for Box format.
pub const MAGIC_BYTES: &[u8; 4] = b"\xffBOX";

/// Box header size in bytes.
pub const HEADER_SIZE: usize = 32;

/// Parsed Box header.
#[derive(Debug, Clone)]
pub struct BoxHeader {
    pub version: u8,
    pub allow_external_symlinks: bool,
    pub allow_escapes: bool,
    pub alignment: u32,
    pub trailer_offset: u64,
}

/// Parse the 32-byte Box header.
// [spec:box:syn:kernel-parser.root]
// [spec:box:syn:kernel-parser.root.header]
pub fn parse_header(data: &[u8]) -> Result<BoxHeader, KernelError> {
    if data.len() < HEADER_SIZE {
        return Err(KernelError::BadData);
    }

    // Magic bytes at 0x00
    if &data[0..4] != MAGIC_BYTES {
        return Err(KernelError::BadData);
    }

    // Version at 0x04
    let version = data[4];
    if version != 1 {
        return Err(KernelError::BadData);
    }

    // Flags at 0x05
    let flags = data[5];
    let allow_external_symlinks = (flags & 0x01) != 0;
    let allow_escapes = (flags & 0x02) != 0;

    // Alignment at 0x08
    let alignment = u32::from_le_bytes(data[0x08..0x0C].try_into().unwrap());

    // Trailer offset at 0x10
    let trailer_offset = u64::from_le_bytes(data[0x10..0x18].try_into().unwrap());

    if trailer_offset == 0 {
        return Err(KernelError::BadData);
    }

    Ok(BoxHeader {
        version,
        allow_external_symlinks,
        allow_escapes,
        alignment,
        trailer_offset,
    })
}

// ============================================================================
// VLQ / FASTVINT PARSING
// ============================================================================

/// Decode a VLQ-encoded u64 (FastVint format).
/// Returns (value, bytes_consumed).
#[inline]
fn decode_vu64(data: &[u8]) -> Result<(u64, usize), KernelError> {
    if data.is_empty() {
        return Err(KernelError::BadData);
    }

    let first = data[0];
    let leading_zeros = first.leading_zeros() as usize;
    let len = leading_zeros + 1;

    if data.len() < len {
        return Err(KernelError::BadData);
    }

    // FastVint stores the bytes following the prefix least-significant first
    // and offsets each length's value range so every representation is unique.
    const OFFSETS: [u64; 9] = [
        0,
        0x80,
        0x4080,
        0x20_4080,
        0x1020_4080,
        0x0008_1020_4080,
        0x0408_1020_4080,
        0x0002_0408_1020_4080,
        0x0102_0408_1020_4080,
    ];

    let prefix_mask = (0xffu16 >> len) as u8;
    let mut raw = if len < 9 {
        u64::from(first & prefix_mask) << ((len - 1) * 8)
    } else {
        0
    };
    for (index, byte) in data[1..len].iter().enumerate() {
        raw |= u64::from(*byte) << (index * 8);
    }

    let value = raw
        .checked_add(OFFSETS[len - 1])
        .ok_or(KernelError::BadData)?;

    Ok((value, len))
}

/// Decode a zigzag-encoded i64.
#[inline]
fn decode_vi64(data: &[u8]) -> Result<(i64, usize), KernelError> {
    let (unsigned, len) = decode_vu64(data)?;
    // Zigzag decode: (n >> 1) ^ -(n & 1)
    let signed = ((unsigned >> 1) as i64) ^ (-((unsigned & 1) as i64));
    Ok((signed, len))
}

/// Parse a length-prefixed string.
fn parse_str(data: &[u8]) -> Result<(&str, usize), KernelError> {
    let (len, prefix_size) = decode_vu64(data)?;
    let len = usize::try_from(len).map_err(|_| KernelError::BadData)?;
    let total = prefix_size.checked_add(len).ok_or(KernelError::BadData)?;

    if data.len() < total {
        return Err(KernelError::BadData);
    }

    let s = core::str::from_utf8(&data[prefix_size..total]).map_err(|_| KernelError::BadData)?;
    Ok((s, total))
}

/// Parse a length-prefixed byte slice.
fn parse_bytes(data: &[u8]) -> Result<(&[u8], usize), KernelError> {
    let (len, prefix_size) = decode_vu64(data)?;
    let len = usize::try_from(len).map_err(|_| KernelError::BadData)?;
    let total = prefix_size.checked_add(len).ok_or(KernelError::BadData)?;

    if data.len() < total {
        return Err(KernelError::BadData);
    }

    Ok((&data[prefix_size..total], total))
}

// ============================================================================
// RECORD TYPE CONSTANTS
// ============================================================================

const RECORD_TYPE_DIRECTORY: u8 = 0x01;
const RECORD_TYPE_FILE: u8 = 0x02;
const RECORD_TYPE_SYMLINK: u8 = 0x03;
const RECORD_TYPE_CHUNKED_FILE: u8 = 0x0A;
const RECORD_TYPE_EXTERNAL_SYMLINK: u8 = 0x0B;

const COMPRESSION_STORED: u8 = 0x00;
const COMPRESSION_ZSTD: u8 = 0x10;
const COMPRESSION_XZ: u8 = 0x20;

// ============================================================================
// ATTRMAP PARSING
// ============================================================================

/// Parse an AttrMap (attribute key index -> raw value bytes).
/// Returns (attr_map, bytes_consumed).
fn parse_attrmap(data: &[u8]) -> Result<(AttrMap, usize), KernelError> {
    if data.len() < 8 {
        return Err(KernelError::BadData);
    }

    // Read byte count (includes entry count VLQ but not the u64 itself)
    let byte_count = usize::try_from(u64::from_le_bytes(data[0..8].try_into().unwrap()))
        .map_err(|_| KernelError::BadData)?;

    // Total size is 8 (for byte_count) + byte_count
    let total = 8usize.checked_add(byte_count).ok_or(KernelError::BadData)?;
    if data.len() < total {
        return Err(KernelError::BadData);
    }

    // Confine every nested decoder to the declared byte-count envelope. Bytes
    // following the map belong to the next trailer field and must never be
    // consumed to complete a malformed entry.
    let contents = &data[8..total];
    let mut pos = 0usize;

    // Read entry count
    let (count, consumed) = decode_vu64(contents)?;
    pos += consumed;
    let count = usize::try_from(count).map_err(|_| KernelError::BadData)?;
    // Every entry needs at least one byte for its key and one for its value
    // length, so this also prevents attacker-controlled oversized allocation.
    if count > contents.len().saturating_sub(pos) / 2 {
        return Err(KernelError::BadData);
    }

    let mut attrs = HashMap::with_capacity(count);

    // Parse each entry: key_index(vu64) + value(bytes)
    for _ in 0..count {
        if pos >= contents.len() {
            return Err(KernelError::BadData);
        }

        let (key_index, consumed) = decode_vu64(&contents[pos..])?;
        pos += consumed;
        let key_index = usize::try_from(key_index).map_err(|_| KernelError::BadData)?;

        let (value, consumed) = parse_bytes(&contents[pos..])?;
        pos += consumed;

        attrs.insert(key_index, value.to_vec().into_boxed_slice());
    }

    if pos != contents.len() {
        return Err(KernelError::BadData);
    }

    Ok((attrs, total))
}

/// Validate an AttrMap and return its encoded length.
fn skip_attrmap(data: &[u8]) -> Result<usize, KernelError> {
    parse_attrmap(data).map(|(_, consumed)| consumed)
}

// ============================================================================
// RECORD PARSING
// ============================================================================

/// Standard attribute key names used for inode metadata.
const ATTR_KEY_UNIX_MODE: &str = "unix.mode";
const ATTR_KEY_MODIFIED: &str = "modified";

fn decode_exact_vu32(value: &[u8]) -> Option<u32> {
    let (decoded, consumed) = decode_vu64(value).ok()?;
    if consumed != value.len() {
        return None;
    }
    u32::try_from(decoded).ok()
}

fn decode_exact_vi64(value: &[u8]) -> Option<i64> {
    let (decoded, consumed) = decode_vi64(value).ok()?;
    (consumed == value.len()).then_some(decoded)
}

/// Extract mode from attrs if present, otherwise use default.
// [spec:box:def:attributes.root.standard-keys]
fn extract_mode(attrs: &AttrMap, attr_keys: &[AttrKey], default_mode: u16) -> u16 {
    for (idx, key) in attr_keys.iter().enumerate() {
        if key.name == ATTR_KEY_UNIX_MODE {
            if key.attr_type != AttrType::Vu32 {
                return default_mode;
            }
            if let Some(mode) = attrs
                .get(&idx)
                .and_then(|value| decode_exact_vu32(value))
                .and_then(|mode| u16::try_from(mode).ok())
            {
                return mode;
            }
            return default_mode;
        }
    }
    default_mode
}

/// Extract the DateTime-encoded modification time, in minutes from the Box epoch.
// [spec:box:def:attributes.root.standard-keys]
fn extract_mtime(attrs: &AttrMap, attr_keys: &[AttrKey]) -> i64 {
    for (idx, key) in attr_keys.iter().enumerate() {
        if key.name == ATTR_KEY_MODIFIED {
            if key.attr_type != AttrType::DateTime {
                return 0;
            }
            return attrs
                .get(&idx)
                .and_then(|value| decode_exact_vi64(value))
                .unwrap_or(0);
        }
    }
    0
}

/// Parse a single record from the trailer.
// [spec:box:syn:kernel-parser.root.records]
fn parse_record(data: &[u8], attr_keys: &[AttrKey]) -> Result<(Record, usize), KernelError> {
    if data.is_empty() {
        return Err(KernelError::BadData);
    }

    let header_byte = data[0];
    let type_id = header_byte & 0x0F;
    let compression_id = header_byte & 0xF0;

    let compression = match compression_id {
        COMPRESSION_STORED => Compression::Stored,
        COMPRESSION_ZSTD => Compression::Zstd,
        COMPRESSION_XZ => Compression::Xz,
        _ => Compression::Unknown(compression_id),
    };

    let mut pos = 1;

    match type_id {
        RECORD_TYPE_DIRECTORY => {
            // Directory: name + attrs
            let (name, consumed) = parse_str(&data[pos..])?;
            pos += consumed;

            let (attrs, consumed) = parse_attrmap(&data[pos..])?;
            pos += consumed;

            let mode = extract_mode(&attrs, attr_keys, 0o40755);
            let mtime = extract_mtime(&attrs, attr_keys);

            Ok((
                Record {
                    name: String::from(name),
                    data: RecordData::Directory {
                        children: Vec::new(),
                    },
                    mode,
                    mtime,
                    attrs,
                },
                pos,
            ))
        }
        RECORD_TYPE_FILE => {
            // File: length(u64) + decompressed_length(u64) + data_offset(u64) + name + attrs
            if data.len() < pos + 24 {
                return Err(KernelError::BadData);
            }

            let compressed_size = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let decompressed_size = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let data_offset = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;

            let (name, consumed) = parse_str(&data[pos..])?;
            pos += consumed;

            let (attrs, consumed) = parse_attrmap(&data[pos..])?;
            pos += consumed;

            let mode = extract_mode(&attrs, attr_keys, 0o100644);
            let mtime = extract_mtime(&attrs, attr_keys);

            Ok((
                Record {
                    name: String::from(name),
                    data: RecordData::File {
                        compression,
                        data_offset,
                        compressed_size,
                        decompressed_size,
                    },
                    mode,
                    mtime,
                    attrs,
                },
                pos,
            ))
        }
        RECORD_TYPE_CHUNKED_FILE => {
            // ChunkedFile: block_size(u32) + length(u64) + decompressed_length(u64) + data_offset(u64) + name + attrs
            if data.len() < pos + 28 {
                return Err(KernelError::BadData);
            }

            let block_size = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
            pos += 4;
            let compressed_size = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let decompressed_size = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let data_offset = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;

            let (name, consumed) = parse_str(&data[pos..])?;
            pos += consumed;

            let (attrs, consumed) = parse_attrmap(&data[pos..])?;
            pos += consumed;

            let mode = extract_mode(&attrs, attr_keys, 0o100644);
            let mtime = extract_mtime(&attrs, attr_keys);

            Ok((
                Record {
                    name: String::from(name),
                    data: RecordData::ChunkedFile {
                        compression,
                        block_size,
                        data_offset,
                        compressed_size,
                        decompressed_size,
                    },
                    mode,
                    mtime,
                    attrs,
                },
                pos,
            ))
        }
        RECORD_TYPE_SYMLINK => {
            // Symlink: name + target_index(vu64) + attrs
            let (name, consumed) = parse_str(&data[pos..])?;
            pos += consumed;

            let (target_index, consumed) = decode_vu64(&data[pos..])?;
            pos += consumed;

            let (attrs, consumed) = parse_attrmap(&data[pos..])?;
            pos += consumed;

            let mode = extract_mode(&attrs, attr_keys, 0o120777);
            let mtime = extract_mtime(&attrs, attr_keys);

            Ok((
                Record {
                    name: String::from(name),
                    data: RecordData::InternalLink { target_index },
                    mode,
                    mtime,
                    attrs,
                },
                pos,
            ))
        }
        RECORD_TYPE_EXTERNAL_SYMLINK => {
            // External symlink: name + target(string) + attrs
            let (name, consumed) = parse_str(&data[pos..])?;
            pos += consumed;

            let (target, consumed) = parse_str(&data[pos..])?;
            pos += consumed;

            let (attrs, consumed) = parse_attrmap(&data[pos..])?;
            pos += consumed;

            let mode = extract_mode(&attrs, attr_keys, 0o120777);
            let mtime = extract_mtime(&attrs, attr_keys);

            Ok((
                Record {
                    name: String::from(name),
                    data: RecordData::ExternalLink {
                        target: String::from(target),
                    },
                    mode,
                    mtime,
                    attrs,
                },
                pos,
            ))
        }
        _ => Err(KernelError::BadData),
    }
}

/// Parse attribute keys section into Vec<AttrKey>.
/// Returns (attr_keys, bytes_consumed).
fn parse_attr_keys(data: &[u8]) -> Result<(Vec<AttrKey>, usize), KernelError> {
    let mut pos = 0;

    // Read count
    let (count, consumed) = decode_vu64(data)?;
    pos += consumed;
    let count = usize::try_from(count).map_err(|_| KernelError::BadData)?;

    // Each key needs at least its type byte and an encoded string length.
    // Check that lower bound before reserving from an untrusted count.
    let remaining = data.len().checked_sub(pos).ok_or(KernelError::BadData)?;
    if count > remaining / 2 {
        return Err(KernelError::BadData);
    }

    let mut keys = Vec::new();
    keys.try_reserve_exact(count)
        .map_err(|_| KernelError::BadData)?;

    // Parse each key: type(1 byte) + name(string)
    for _ in 0..count {
        if data.len() <= pos {
            return Err(KernelError::BadData);
        }
        let type_byte = data[pos];
        pos += 1;
        let attr_type = AttrType::from(type_byte);
        if matches!(attr_type, AttrType::Unknown(_)) {
            return Err(KernelError::BadData);
        }

        let (name, consumed) = parse_str(&data[pos..])?;
        pos += consumed;

        keys.push(AttrKey {
            name: String::from(name),
            attr_type,
        });
    }

    Ok((keys, pos))
}

/// Skip over dictionary.
fn skip_dictionary(data: &[u8]) -> Result<usize, KernelError> {
    let (len, prefix_consumed) = decode_vu64(data)?;
    let len = usize::try_from(len).map_err(|_| KernelError::BadData)?;
    let total = prefix_consumed
        .checked_add(len)
        .ok_or(KernelError::BadData)?;
    if data.len() < total {
        return Err(KernelError::BadData);
    }
    Ok(total)
}

/// Skip over FST.
fn skip_fst(data: &[u8]) -> Result<usize, KernelError> {
    if data.len() < 8 {
        return Err(KernelError::BadData);
    }

    let fst_len = usize::try_from(u64::from_le_bytes(data[0..8].try_into().unwrap()))
        .map_err(|_| KernelError::BadData)?;
    let total = 8usize.checked_add(fst_len).ok_or(KernelError::BadData)?;
    if data.len() < total {
        return Err(KernelError::BadData);
    }

    Ok(total)
}

// ============================================================================
// FST PARSING (for directory lookups)
// ============================================================================

/// Parse the FST data for later use.
/// Returns the raw FST bytes that can be used for lookups.
fn parse_fst_data(data: &[u8]) -> Result<Option<Box<[u8]>>, KernelError> {
    if data.len() < 8 {
        return Err(KernelError::BadData);
    }

    let fst_len = usize::try_from(u64::from_le_bytes(data[0..8].try_into().unwrap()))
        .map_err(|_| KernelError::BadData)?;
    if fst_len == 0 {
        return Ok(None);
    }

    let total = 8usize.checked_add(fst_len).ok_or(KernelError::BadData)?;
    if data.len() < total {
        return Err(KernelError::BadData);
    }

    let fst_bytes = data[8..total].to_vec().into_boxed_slice();
    Fst::<_, u64>::new(Cow::Borrowed(fst_bytes.as_ref())).map_err(|_| KernelError::BadData)?;
    Ok(Some(fst_bytes))
}

// ============================================================================
// FULL METADATA PARSING
// ============================================================================

/// Parse the complete trailer into archive data.
/// Returns ArchiveData that can be added to BoxfsMetadata via add_archive().
// [spec:box:syn:kernel-parser.root]
// [spec:box:syn:kernel-parser.root.trailer]
pub fn parse_trailer(data: &[u8]) -> Result<ArchiveData, KernelError> {
    let mut pos = 0;

    // Parse attr_keys
    let (attr_keys, consumed) = parse_attr_keys(data)?;
    pos += consumed;

    // Skip global attrs (not used for individual file xattrs)
    let consumed = skip_attrmap(&data[pos..])?;
    pos += consumed;

    // Skip dictionary
    let consumed = skip_dictionary(&data[pos..])?;
    pos += consumed;

    // Parse record count
    let (record_count, consumed) = decode_vu64(&data[pos..])?;
    pos += consumed;
    let record_count = usize::try_from(record_count).map_err(|_| KernelError::BadData)?;

    // The smallest record is a directory with an empty name and AttrMap
    // (1-byte header, 1-byte name length, and 9-byte empty AttrMap). Two
    // 8-byte FST length fields must follow the records.
    const MIN_RECORD_SIZE: usize = 11;
    const MIN_FST_TRAILERS_SIZE: usize = 16;
    let remaining = data.len().checked_sub(pos).ok_or(KernelError::BadData)?;
    let available_for_records = remaining
        .checked_sub(MIN_FST_TRAILERS_SIZE)
        .ok_or(KernelError::BadData)?;
    if record_count > available_for_records / MIN_RECORD_SIZE {
        return Err(KernelError::BadData);
    }

    // Parse all records
    let mut records = Vec::new();
    records
        .try_reserve_exact(record_count)
        .map_err(|_| KernelError::BadData)?;
    for _ in 0..record_count {
        let (record, consumed) = parse_record(&data[pos..], &attr_keys)?;
        pos += consumed;
        records.push(record);
    }

    // Parse FST for path lookups
    let fst_data = parse_fst_data(&data[pos..])?;
    let consumed = skip_fst(&data[pos..])?;
    pos += consumed;

    // Parse block FST for chunked file block lookups
    let block_fst_data = parse_fst_data(&data[pos..])?;

    // Build parent-child relationships from FST
    build_directory_tree(&mut records, fst_data.as_deref());

    Ok(ArchiveData {
        id: 0, // Will be assigned by add_archive()
        records,
        archive_size: 0,      // Set later by caller
        data_offset_base: 32, // Header size, data starts after
        fst_data,
        block_fst_data,
        attr_keys,
    })
}

/// Build directory tree relationships from FST data.
fn build_directory_tree(records: &mut [Record], fst_data: Option<&[u8]>) {
    if records.is_empty() {
        return;
    }

    // First pass: for each record, find its parent directory by path
    // The FST maps paths to record indices

    // For now, we do a simple approach:
    // Records are stored in order, and we use the FST for lookups
    // Directory children are populated when we iterate the FST

    let Some(fst_bytes) = fst_data else {
        return;
    };

    // Parse FST to build path -> index mapping
    // Then use that to populate directory children

    // For efficiency, we iterate FST entries and populate parent directories
    let fst = match Fst::<_, u64>::new(Cow::Borrowed(fst_bytes)) {
        Ok(f) => f,
        Err(_) => return,
    };

    // Build a map of parent paths to their children
    use alloc::collections::BTreeMap;
    let mut parent_children: BTreeMap<Vec<u8>, Vec<u64>> = BTreeMap::new();

    for (path, index) in fst.prefix_iter(&[]) {
        if index == 0 {
            continue;
        }

        // Find parent path (everything before last 0x1f separator)
        let parent_path = if let Some(sep_pos) = path.iter().rposition(|&b| b == 0x1f) {
            path[..sep_pos].to_vec()
        } else {
            Vec::new() // Root level
        };

        parent_children
            .entry(parent_path)
            .or_insert_with(Vec::new)
            .push(index);
    }

    // Now populate directory children
    for (parent_path, children) in parent_children {
        // Find the parent directory's index
        let parent_index = if parent_path.is_empty() {
            // Root children - find root directory
            records
                .iter()
                .position(|r| r.is_dir() && r.name.is_empty())
                .map(|i| i as u64 + 1)
        } else {
            fst.get(&parent_path)
        };

        if let Some(idx) = parent_index {
            if idx > 0 && (idx as usize) <= records.len() {
                if let RecordData::Directory {
                    children: ref mut dir_children,
                } = records[idx as usize - 1].data
                {
                    *dir_children = children;
                }
            }
        }
    }
}

// ============================================================================
// FST LOOKUP
// ============================================================================

/// Look up a path in the FST and return the record index.
pub fn fst_lookup(fst_data: &[u8], path: &str) -> Option<u64> {
    let fst = Fst::<_, u64>::new(Cow::Borrowed(fst_data)).ok()?;

    // Convert path to FST key format (using 0x1f as separator)
    let key: Vec<u8> = path
        .as_bytes()
        .iter()
        .map(|&b| if b == b'/' { 0x1f } else { b })
        .collect();

    fst.get(&key)
}

/// Get direct children of a directory from FST.
pub fn fst_children(fst_data: &[u8], parent_path: &str) -> Vec<(String, u64)> {
    let fst = match Fst::<_, u64>::new(Cow::Borrowed(fst_data)) {
        Ok(f) => f,
        Err(_) => return Vec::new(),
    };

    // Build prefix for this directory
    let prefix: Vec<u8> = if parent_path.is_empty() {
        Vec::new()
    } else {
        let mut p: Vec<u8> = parent_path
            .as_bytes()
            .iter()
            .map(|&b| if b == b'/' { 0x1f } else { b })
            .collect();
        p.push(0x1f); // Add separator for children
        p
    };

    let mut children = Vec::new();

    for (key, index) in fst.prefix_iter(&prefix) {
        if index == 0 {
            continue;
        }

        // Check if this is a direct child (no more separators after prefix)
        let suffix = &key[prefix.len()..];
        if suffix.contains(&0x1f) {
            continue; // Not a direct child
        }

        // Extract the name
        if let Ok(name) = core::str::from_utf8(suffix) {
            children.push((String::from(name), index));
        }
    }

    children
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{BoxfsMetadata, SYNTHETIC_ROOT_INDEX};
    use box_format::{
        BoxPath, Compression as WriterCompression, CompressionConfig, HashMap as WriterHashMap,
        sync::BoxWriter,
    };
    use std::io::Cursor;

    fn encoded_attrmap(declared_len: u64, contents_and_following: &[u8]) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(&declared_len.to_le_bytes());
        data.extend_from_slice(contents_and_following);
        data
    }

    fn encoded_vu64_max() -> [u8; 9] {
        [0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]
    }

    fn trailer_before_record_count() -> Vec<u8> {
        let mut data = Vec::new();
        data.push(0x80); // zero attribute keys
        data.extend_from_slice(&1u64.to_le_bytes());
        data.push(0x80); // zero entries in the one-byte global AttrMap
        data.push(0x80); // zero-length dictionary
        data
    }

    fn trailer_with_fsts(path_fst: &[u8], block_fst: &[u8]) -> Vec<u8> {
        let mut data = trailer_before_record_count();
        data.push(0x80); // zero records
        data.extend_from_slice(&(path_fst.len() as u64).to_le_bytes());
        data.extend_from_slice(path_fst);
        data.extend_from_slice(&(block_fst.len() as u64).to_le_bytes());
        data.extend_from_slice(block_fst);
        data
    }

    fn writer_metadata(build: impl FnOnce(&mut BoxWriter)) -> BoxfsMetadata {
        let temp = tempfile::tempdir().expect("temporary archive directory");
        let archive_path = temp.path().join("kernel-root.box");
        let mut writer = BoxWriter::create(&archive_path).expect("create writer archive");
        build(&mut writer);
        writer.finish().expect("finish writer archive");

        let bytes = std::fs::read(&archive_path).expect("read writer archive");
        let header = parse_header(&bytes[..HEADER_SIZE]).expect("parse writer header");
        let trailer_offset = usize::try_from(header.trailer_offset).expect("trailer offset");
        let mut archive = parse_trailer(&bytes[trailer_offset..]).expect("parse writer trailer");
        archive.archive_size = bytes.len() as u64;

        let mut metadata = BoxfsMetadata::empty();
        metadata.add_archive(archive);
        metadata
    }

    // [spec:box:req:kernel-vfs.root.namespace/test/unit]
    #[test]
    fn writer_archives_use_non_aliasing_synthetic_root() {
        let config = CompressionConfig::new(WriterCompression::Stored);
        let files_only = writer_metadata(|writer| {
            writer
                .insert(
                    &config,
                    BoxPath::new("alpha.txt").unwrap(),
                    Cursor::new(b"alpha"),
                    WriterHashMap::new(),
                )
                .unwrap();
            writer
                .insert(
                    &config,
                    BoxPath::new("beta.txt").unwrap(),
                    Cursor::new(b"beta"),
                    WriterHashMap::new(),
                )
                .unwrap();
        });

        let root = files_only.root_index();
        assert_eq!(root, SYNTHETIC_ROOT_INDEX);
        assert!(files_only.get(root).is_some_and(Record::is_dir));
        assert_eq!(files_only.path_for_index(root), Some(&[][..]));
        assert!(
            files_only
                .merged_fst
                .prefix_iter(&[])
                .all(|(_, composite)| composite != root)
        );
        let mut names: Vec<_> = files_only
            .children(root)
            .into_iter()
            .map(|(_, record)| record.name.as_str())
            .collect();
        names.sort_unstable();
        assert_eq!(names, ["alpha.txt", "beta.txt"]);
        assert!(files_only.find_child(root, "alpha.txt").is_some());

        let mixed = writer_metadata(|writer| {
            writer
                .insert(
                    &config,
                    BoxPath::new("top.txt").unwrap(),
                    Cursor::new(b"top"),
                    WriterHashMap::new(),
                )
                .unwrap();
            writer
                .mkdir(BoxPath::new("dir").unwrap(), WriterHashMap::new())
                .unwrap();
            writer
                .insert(
                    &config,
                    BoxPath::new("dir/nested.txt").unwrap(),
                    Cursor::new(b"nested"),
                    WriterHashMap::new(),
                )
                .unwrap();
        });

        let root = mixed.root_index();
        let mut names: Vec<_> = mixed
            .children(root)
            .into_iter()
            .map(|(_, record)| record.name.as_str())
            .collect();
        names.sort_unstable();
        assert_eq!(names, ["dir", "top.txt"]);
        let directory = mixed.find_child(root, "dir").expect("top-level directory");
        assert!(mixed.find_child(directory, "nested.txt").is_some());
    }

    // [spec:box:syn:kernel-parser.root.trailer/test/unit]
    #[test]
    fn path_fst_magic_is_validated_during_trailer_parsing() {
        let invalid_magic = [0u8; 24];
        assert!(matches!(
            parse_trailer(&trailer_with_fsts(&invalid_magic, &[])),
            Err(KernelError::BadData)
        ));
    }

    // [spec:box:syn:kernel-parser.root.trailer/test/unit]
    #[test]
    fn block_fst_version_is_validated_during_trailer_parsing() {
        let mut invalid_version = [0u8; 24];
        invalid_version[..4].copy_from_slice(b"BFST");
        invalid_version[4] = 2;
        assert!(matches!(
            parse_trailer(&trailer_with_fsts(&[], &invalid_version)),
            Err(KernelError::BadData)
        ));
    }

    // [spec:box:syn:kernel-parser.root.trailer/test/unit]
    #[test]
    fn reserved_attribute_types_are_rejected() {
        for type_tag in [11, u8::MAX] {
            let data = [0x81, type_tag, 0x81, b'x'];
            assert!(matches!(parse_attr_keys(&data), Err(KernelError::BadData)));
        }
    }

    // [spec:box:def:attributes.root.standard-keys/test/unit]
    #[test]
    fn unix_mode_requires_exact_vu32() {
        let mut attrs = AttrMap::new();
        // FastVint Vu32 for 0100600: offset(3) + 0x4100.
        attrs.insert(0, Vec::from([0x20, 0x00, 0x41]).into_boxed_slice());
        let mut keys = Vec::from([AttrKey {
            name: String::from(ATTR_KEY_UNIX_MODE),
            attr_type: AttrType::Vu32,
        }]);

        assert_eq!(extract_mode(&attrs, &keys, 0o100644), 0o100600);

        attrs.insert(0, Vec::from([0x20, 0x00]).into_boxed_slice());
        assert_eq!(extract_mode(&attrs, &keys, 0o100644), 0o100644);

        attrs.insert(0, Vec::from([0x20, 0x00, 0x41, 0x80]).into_boxed_slice());
        assert_eq!(extract_mode(&attrs, &keys, 0o100644), 0o100644);

        attrs.insert(0, Vec::from([0x20, 0x00, 0x41]).into_boxed_slice());
        keys[0].attr_type = AttrType::Bytes;
        assert_eq!(extract_mode(&attrs, &keys, 0o100644), 0o100644);

        // 65536 is a valid Vu32 but cannot be represented by Record::mode.
        keys[0].attr_type = AttrType::Vu32;
        attrs.insert(0, Vec::from([0x20, 0x80, 0xbf]).into_boxed_slice());
        assert_eq!(extract_mode(&attrs, &keys, 0o100644), 0o100644);
    }

    // [spec:box:def:attributes.root.standard-keys/test/unit]
    #[test]
    fn modified_datetime_uses_standard_exact_vi64() {
        let mut attrs = AttrMap::new();
        // FastVint Vi64 for -9000 minutes: zigzag(−9000) = 17999.
        attrs.insert(0, Vec::from([0x20, 0xcf, 0x05]).into_boxed_slice());
        let mut keys = Vec::from([AttrKey {
            name: String::from(ATTR_KEY_MODIFIED),
            attr_type: AttrType::DateTime,
        }]);

        assert_eq!(extract_mtime(&attrs, &keys), -9000);

        keys[0].name = String::from("unix.mtime");
        assert_eq!(extract_mtime(&attrs, &keys), 0);

        keys[0].name = String::from(ATTR_KEY_MODIFIED);
        keys[0].attr_type = AttrType::Vi64;
        assert_eq!(extract_mtime(&attrs, &keys), 0);

        keys[0].attr_type = AttrType::DateTime;
        attrs.insert(0, Vec::from([0x20, 0xcf, 0x05, 0x80]).into_boxed_slice());
        assert_eq!(extract_mtime(&attrs, &keys), 0);
    }

    // [spec:box:syn:kernel-parser.root.trailer/test/unit]
    #[test]
    fn attrmap_parser_confines_entries_to_the_declared_envelope() {
        // count=1, key=2, value length=3, then the value. The final byte is
        // the start of the next trailer field and is not part of the map.
        let valid = encoded_attrmap(6, &[0x81, 0x82, 0x83, 1, 2, 3, 0xaa]);
        let (attrs, consumed) = parse_attrmap(&valid).expect("valid attribute map");
        assert_eq!(consumed, 14);
        assert_eq!(
            attrs.get(&2).map(|value| value.as_ref()),
            Some(&[1, 2, 3][..])
        );

        // The value bytes exist in the surrounding trailer, but lie beyond
        // the declared three-byte map envelope.
        let crossing = encoded_attrmap(3, &[0x81, 0x82, 0x82, 1, 2]);
        assert!(matches!(
            parse_attrmap(&crossing),
            Err(KernelError::BadData)
        ));

        // A declared envelope that is itself truncated is invalid.
        let truncated = encoded_attrmap(6, &[0x81, 0x82, 0x83, 1, 2]);
        assert!(matches!(
            parse_attrmap(&truncated),
            Err(KernelError::BadData)
        ));

        // Entries must consume the envelope exactly; padding is not accepted.
        let padded = encoded_attrmap(2, &[0x80, 0x80]);
        assert!(matches!(parse_attrmap(&padded), Err(KernelError::BadData)));

        // Even an empty map contains its encoded zero entry count.
        let missing_count = encoded_attrmap(0, &[0x80]);
        assert!(matches!(
            parse_attrmap(&missing_count),
            Err(KernelError::BadData)
        ));

        // Length and entry-count conversions must fail rather than wrap or
        // attempt an attacker-selected allocation.
        let oversized_envelope = encoded_attrmap(u64::MAX, &[]);
        assert!(matches!(
            parse_attrmap(&oversized_envelope),
            Err(KernelError::BadData)
        ));
        let oversized_count =
            encoded_attrmap(9, &[0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);
        assert!(matches!(
            parse_attrmap(&oversized_count),
            Err(KernelError::BadData)
        ));
    }

    // [spec:box:syn:kernel-parser.root.trailer/test/unit]
    #[test]
    fn attr_key_counts_are_bounded_before_allocation() {
        assert!(matches!(
            parse_attr_keys(&encoded_vu64_max()),
            Err(KernelError::BadData)
        ));

        // Two keys cannot fit in the zero bytes following this count.
        assert!(matches!(
            parse_attr_keys(&[0x82]),
            Err(KernelError::BadData)
        ));

        // The declared two-byte name has only one byte available.
        assert!(matches!(
            parse_attr_keys(&[0x81, 0x01, 0x82, b'a']),
            Err(KernelError::BadData)
        ));
    }

    // [spec:box:syn:kernel-parser.root.trailer/test/unit]
    #[test]
    fn dictionary_and_fst_lengths_reject_overflow_and_truncation() {
        assert!(matches!(
            skip_dictionary(&encoded_vu64_max()),
            Err(KernelError::BadData)
        ));
        assert!(matches!(
            skip_dictionary(&[0x83, 1, 2]),
            Err(KernelError::BadData)
        ));

        let oversized_fst = u64::MAX.to_le_bytes();
        assert!(matches!(
            skip_fst(&oversized_fst),
            Err(KernelError::BadData)
        ));
        assert!(matches!(
            parse_fst_data(&oversized_fst),
            Err(KernelError::BadData)
        ));

        assert!(matches!(skip_fst(&[0; 7]), Err(KernelError::BadData)));
        assert!(matches!(parse_fst_data(&[0; 7]), Err(KernelError::BadData)));

        let mut truncated_fst = Vec::new();
        truncated_fst.extend_from_slice(&3u64.to_le_bytes());
        truncated_fst.extend_from_slice(&[1, 2]);
        assert!(matches!(
            skip_fst(&truncated_fst),
            Err(KernelError::BadData)
        ));
        assert!(matches!(
            parse_fst_data(&truncated_fst),
            Err(KernelError::BadData)
        ));
    }

    // [spec:box:syn:kernel-parser.root.trailer/test/unit]
    #[test]
    fn record_counts_are_bounded_before_allocation() {
        let mut oversized = trailer_before_record_count();
        oversized.extend_from_slice(&encoded_vu64_max());
        assert!(matches!(
            parse_trailer(&oversized),
            Err(KernelError::BadData)
        ));

        let mut truncated = trailer_before_record_count();
        truncated.push(0);
        assert!(matches!(
            parse_trailer(&truncated),
            Err(KernelError::BadData)
        ));

        let mut impossible = trailer_before_record_count();
        impossible.push(0x81); // one record
        impossible.extend_from_slice(&0u64.to_le_bytes());
        impossible.extend_from_slice(&0u64.to_le_bytes());
        assert!(matches!(
            parse_trailer(&impossible),
            Err(KernelError::BadData)
        ));

        let mut empty = trailer_before_record_count();
        empty.push(0x80); // zero records
        empty.extend_from_slice(&0u64.to_le_bytes());
        empty.extend_from_slice(&0u64.to_le_bytes());
        assert!(parse_trailer(&empty).is_ok());
    }
}
