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
use crate::metadata::{AttrKey, AttrMap, AttrType, BoxfsMetadata, Compression, Record, RecordData};
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

    // Count leading zeros to determine length
    let leading_zeros = first.leading_zeros() as usize;
    let len = leading_zeros + 1;

    if data.len() < len {
        return Err(KernelError::BadData);
    }

    let value = match len {
        1 => (first & 0x7F) as u64,
        2 => {
            let b = [data[0] & 0x3F, data[1]];
            u16::from_be_bytes(b) as u64
        }
        3 => {
            let b = [0, data[0] & 0x1F, data[1], data[2]];
            u32::from_be_bytes(b) as u64
        }
        4 => {
            let b = [data[0] & 0x0F, data[1], data[2], data[3]];
            u32::from_be_bytes(b) as u64
        }
        5 => {
            let b = [0, 0, 0, data[0] & 0x07, data[1], data[2], data[3], data[4]];
            u64::from_be_bytes(b)
        }
        6 => {
            let b = [0, 0, data[0] & 0x03, data[1], data[2], data[3], data[4], data[5]];
            u64::from_be_bytes(b)
        }
        7 => {
            let b = [0, data[0] & 0x01, data[1], data[2], data[3], data[4], data[5], data[6]];
            u64::from_be_bytes(b)
        }
        8 => {
            // First byte is 0x00, next 7 bytes are value
            let b = [0, data[1], data[2], data[3], data[4], data[5], data[6], data[7]];
            u64::from_be_bytes(b)
        }
        9 => {
            // First byte is 0x00, next 8 bytes are full u64
            let b: [u8; 8] = data[1..9].try_into().unwrap();
            u64::from_be_bytes(b)
        }
        _ => return Err(KernelError::BadData),
    };

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
    let len = len as usize;
    let total = prefix_size + len;

    if data.len() < total {
        return Err(KernelError::BadData);
    }

    let s = core::str::from_utf8(&data[prefix_size..total])
        .map_err(|_| KernelError::BadData)?;
    Ok((s, total))
}

/// Parse a length-prefixed byte slice.
fn parse_bytes(data: &[u8]) -> Result<(&[u8], usize), KernelError> {
    let (len, prefix_size) = decode_vu64(data)?;
    let len = len as usize;
    let total = prefix_size + len;

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
    let byte_count = u64::from_le_bytes(data[0..8].try_into().unwrap()) as usize;

    // Total size is 8 (for byte_count) + byte_count
    let total = 8 + byte_count;
    if data.len() < total {
        return Err(KernelError::BadData);
    }

    if byte_count == 0 {
        return Ok((HashMap::new(), total));
    }

    // Parse the attrmap contents
    let mut pos = 8;
    let end = 8 + byte_count;

    // Read entry count
    let (count, consumed) = decode_vu64(&data[pos..])?;
    pos += consumed;

    let mut attrs = HashMap::with_capacity(count as usize);

    // Parse each entry: key_index(vu64) + value(bytes)
    for _ in 0..count {
        if pos >= end {
            return Err(KernelError::BadData);
        }

        let (key_index, consumed) = decode_vu64(&data[pos..])?;
        pos += consumed;

        let (value, consumed) = parse_bytes(&data[pos..])?;
        pos += consumed;

        attrs.insert(key_index as usize, value.to_vec().into_boxed_slice());
    }

    Ok((attrs, total))
}

/// Skip over an AttrMap without parsing it.
fn skip_attrmap(data: &[u8]) -> Result<usize, KernelError> {
    if data.len() < 8 {
        return Err(KernelError::BadData);
    }

    let byte_count = u64::from_le_bytes(data[0..8].try_into().unwrap()) as usize;
    let total = 8 + byte_count;
    if data.len() < total {
        return Err(KernelError::BadData);
    }

    Ok(total)
}

// ============================================================================
// RECORD PARSING
// ============================================================================

/// Attribute key names for mode and mtime
const ATTR_KEY_UNIX_MODE: &str = "unix.mode";
const ATTR_KEY_UNIX_MTIME: &str = "unix.mtime";

/// Extract mode from attrs if present, otherwise use default.
fn extract_mode(attrs: &AttrMap, attr_keys: &[AttrKey], default_mode: u16) -> u16 {
    // Find the unix.mode key index
    for (idx, key) in attr_keys.iter().enumerate() {
        if key.name == ATTR_KEY_UNIX_MODE {
            if let Some(value) = attrs.get(&idx) {
                // Mode is stored as u16
                if value.len() >= 2 {
                    return u16::from_le_bytes([value[0], value[1]]);
                }
            }
            break;
        }
    }
    default_mode
}

/// Extract mtime from attrs if present, otherwise use 0.
fn extract_mtime(attrs: &AttrMap, attr_keys: &[AttrKey]) -> i64 {
    // Find the unix.mtime key index
    for (idx, key) in attr_keys.iter().enumerate() {
        if key.name == ATTR_KEY_UNIX_MTIME {
            if let Some(value) = attrs.get(&idx) {
                // mtime is stored as vi64 (zigzag), decode it
                if let Ok((mtime, _)) = decode_vi64(value) {
                    return mtime;
                }
            }
            break;
        }
    }
    0
}

/// Parse a single record from the trailer.
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

            Ok((Record {
                name: String::from(name),
                data: RecordData::Directory { children: Vec::new() },
                mode,
                mtime,
                attrs,
            }, pos))
        }
        RECORD_TYPE_FILE => {
            // File: length(u64) + decompressed_length(u64) + data_offset(u64) + name + attrs
            if data.len() < pos + 24 {
                return Err(KernelError::BadData);
            }

            let compressed_size = u64::from_le_bytes(data[pos..pos+8].try_into().unwrap());
            pos += 8;
            let decompressed_size = u64::from_le_bytes(data[pos..pos+8].try_into().unwrap());
            pos += 8;
            let data_offset = u64::from_le_bytes(data[pos..pos+8].try_into().unwrap());
            pos += 8;

            let (name, consumed) = parse_str(&data[pos..])?;
            pos += consumed;

            let (attrs, consumed) = parse_attrmap(&data[pos..])?;
            pos += consumed;

            let mode = extract_mode(&attrs, attr_keys, 0o100644);
            let mtime = extract_mtime(&attrs, attr_keys);

            Ok((Record {
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
            }, pos))
        }
        RECORD_TYPE_CHUNKED_FILE => {
            // ChunkedFile: block_size(u32) + length(u64) + decompressed_length(u64) + data_offset(u64) + name + attrs
            if data.len() < pos + 28 {
                return Err(KernelError::BadData);
            }

            let block_size = u32::from_le_bytes(data[pos..pos+4].try_into().unwrap());
            pos += 4;
            let compressed_size = u64::from_le_bytes(data[pos..pos+8].try_into().unwrap());
            pos += 8;
            let decompressed_size = u64::from_le_bytes(data[pos..pos+8].try_into().unwrap());
            pos += 8;
            let data_offset = u64::from_le_bytes(data[pos..pos+8].try_into().unwrap());
            pos += 8;

            let (name, consumed) = parse_str(&data[pos..])?;
            pos += consumed;

            let (attrs, consumed) = parse_attrmap(&data[pos..])?;
            pos += consumed;

            let mode = extract_mode(&attrs, attr_keys, 0o100644);
            let mtime = extract_mtime(&attrs, attr_keys);

            Ok((Record {
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
            }, pos))
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

            Ok((Record {
                name: String::from(name),
                data: RecordData::InternalLink { target_index },
                mode,
                mtime,
                attrs,
            }, pos))
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

            Ok((Record {
                name: String::from(name),
                data: RecordData::ExternalLink { target: String::from(target) },
                mode,
                mtime,
                attrs,
            }, pos))
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

    let mut keys = Vec::with_capacity(count as usize);

    // Parse each key: type(1 byte) + name(string)
    for _ in 0..count {
        if data.len() <= pos {
            return Err(KernelError::BadData);
        }
        let type_byte = data[pos];
        pos += 1;

        let (name, consumed) = parse_str(&data[pos..])?;
        pos += consumed;

        keys.push(AttrKey {
            name: String::from(name),
            attr_type: AttrType::from(type_byte),
        });
    }

    Ok((keys, pos))
}

/// Skip over dictionary.
fn skip_dictionary(data: &[u8]) -> Result<usize, KernelError> {
    let (len, prefix_consumed) = decode_vu64(data)?;
    if len == 0 {
        return Ok(prefix_consumed);
    }
    let total = prefix_consumed + len as usize;
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

    let fst_len = u64::from_le_bytes(data[0..8].try_into().unwrap()) as usize;
    if fst_len == 0 {
        return Ok(8);
    }

    let total = 8 + fst_len;
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

    let fst_len = u64::from_le_bytes(data[0..8].try_into().unwrap()) as usize;
    if fst_len == 0 {
        return Ok(None);
    }

    let total = 8 + fst_len;
    if data.len() < total {
        return Err(KernelError::BadData);
    }

    let fst_bytes = data[8..total].to_vec().into_boxed_slice();
    Ok(Some(fst_bytes))
}

// ============================================================================
// FULL METADATA PARSING
// ============================================================================

/// Parse the complete trailer into metadata.
pub fn parse_trailer(data: &[u8], cache_capacity: usize) -> Result<BoxfsMetadata, KernelError> {
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

    // Parse all records
    let mut records = Vec::with_capacity(record_count as usize);
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

    // Find root directory index (first directory or index 0)
    let root_index = records.iter()
        .position(|r| r.is_dir())
        .map(|i| i as u64 + 1) // +1 because indices are 1-based
        .unwrap_or(1);

    Ok(BoxfsMetadata {
        records,
        root_index,
        archive_size: 0, // Set later by caller
        data_offset: 32, // Header size, data starts after
        fst_data,
        block_fst_data,
        block_cache: core::cell::RefCell::new(crate::metadata::BlockCache::new(cache_capacity)),
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
    let fst = match Fst::new(Cow::Borrowed(fst_bytes)) {
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
            records.iter()
                .position(|r| r.is_dir() && r.name.is_empty())
                .map(|i| i as u64 + 1)
        } else {
            fst.get(&parent_path)
        };

        if let Some(idx) = parent_index {
            if idx > 0 && (idx as usize) <= records.len() {
                if let RecordData::Directory { children: ref mut dir_children } = records[idx as usize - 1].data {
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
    let fst = Fst::new(Cow::Borrowed(fst_data)).ok()?;

    // Convert path to FST key format (using 0x1f as separator)
    let key: Vec<u8> = path.as_bytes()
        .iter()
        .map(|&b| if b == b'/' { 0x1f } else { b })
        .collect();

    fst.get(&key)
}

/// Get direct children of a directory from FST.
pub fn fst_children(fst_data: &[u8], parent_path: &str) -> Vec<(String, u64)> {
    let fst = match Fst::new(Cow::Borrowed(fst_data)) {
        Ok(f) => f,
        Err(_) => return Vec::new(),
    };

    // Build prefix for this directory
    let prefix: Vec<u8> = if parent_path.is_empty() {
        Vec::new()
    } else {
        let mut p: Vec<u8> = parent_path.as_bytes()
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
