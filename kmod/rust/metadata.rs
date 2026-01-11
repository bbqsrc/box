// SPDX-License-Identifier: GPL-2.0-only
//! Parsed box archive metadata
//!
//! This module holds the in-memory representation of the box archive
//! metadata that was parsed from the trailer.

use alloc::borrow::Cow;
use alloc::boxed::Box;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::cell::RefCell;

use box_fst::Fst;
use hashbrown::HashMap;

// ============================================================================
// BLOCK CACHE
// ============================================================================

/// Cache key: (record_index, block_logical_offset)
type CacheKey = (u64, u64);

/// Entry in the block cache with access tracking for LRU eviction.
struct CacheEntry {
    /// Decompressed block data
    data: Box<[u8]>,
    /// Access counter for LRU (higher = more recent)
    last_access: u64,
}

/// LRU cache for decompressed blocks.
///
/// Caches decompressed block data keyed by (record_index, block_logical_offset).
/// This avoids re-decompressing the same chunk for each page fault.
/// Capacity is in bytes, not block count, since blocks can vary in size.
pub struct BlockCache {
    entries: HashMap<CacheKey, CacheEntry>,
    /// Maximum bytes to cache
    capacity_bytes: usize,
    /// Current bytes used
    used_bytes: usize,
    access_counter: u64,
}

/// Default block cache capacity: 32MB
pub const DEFAULT_BLOCK_CACHE_BYTES: usize = 32 * 1024 * 1024;

impl BlockCache {
    /// Create a new block cache with the specified byte capacity.
    pub fn new(capacity_bytes: usize) -> Self {
        Self {
            entries: HashMap::new(),
            capacity_bytes,
            used_bytes: 0,
            access_counter: 0,
        }
    }

    /// Get a cached block if present, updating LRU order.
    pub fn get(&mut self, record_index: u64, block_offset: u64) -> Option<&[u8]> {
        let key = (record_index, block_offset);
        if let Some(entry) = self.entries.get_mut(&key) {
            self.access_counter += 1;
            entry.last_access = self.access_counter;
            Some(&entry.data)
        } else {
            None
        }
    }

    /// Insert a decompressed block into the cache.
    /// Evicts least recently used entries until there's room.
    pub fn insert(&mut self, record_index: u64, block_offset: u64, data: Box<[u8]>) {
        let key = (record_index, block_offset);
        let data_size = data.len();

        // Don't cache blocks larger than total capacity
        if data_size > self.capacity_bytes {
            return;
        }

        // Remove existing entry if present (will re-add with updated access)
        if let Some(old) = self.entries.remove(&key) {
            self.used_bytes -= old.data.len();
        }

        // Evict until we have room
        while self.used_bytes + data_size > self.capacity_bytes {
            if !self.evict_lru() {
                break;
            }
        }

        self.used_bytes += data_size;
        self.access_counter += 1;
        self.entries.insert(key, CacheEntry {
            data,
            last_access: self.access_counter,
        });
    }

    /// Evict the least recently used entry. Returns true if an entry was evicted.
    fn evict_lru(&mut self) -> bool {
        if self.entries.is_empty() {
            return false;
        }

        // Find entry with minimum last_access
        let lru_key = self.entries
            .iter()
            .min_by_key(|(_, entry)| entry.last_access)
            .map(|(key, _)| *key);

        if let Some(key) = lru_key {
            if let Some(entry) = self.entries.remove(&key) {
                self.used_bytes -= entry.data.len();
                return true;
            }
        }
        false
    }
}

impl Default for BlockCache {
    fn default() -> Self {
        Self::new(DEFAULT_BLOCK_CACHE_BYTES)
    }
}

/// Compression algorithm used for a file
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    /// No compression (stored)
    Stored,
    /// Zstandard compression
    Zstd,
    /// XZ/LZMA compression
    Xz,
    /// Unknown compression algorithm
    Unknown(u8),
}

/// Attribute value type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttrType {
    Bytes,
    String,
    Json,
    U8,
    Vi32,
    Vu32,
    Vi64,
    Vu64,
    U128,
    U256,
    DateTime,
    Unknown(u8),
}

impl From<u8> for AttrType {
    fn from(v: u8) -> Self {
        match v {
            0 => AttrType::Bytes,
            1 => AttrType::String,
            2 => AttrType::Json,
            3 => AttrType::U8,
            4 => AttrType::Vi32,
            5 => AttrType::Vu32,
            6 => AttrType::Vi64,
            7 => AttrType::Vu64,
            8 => AttrType::U128,
            9 => AttrType::U256,
            10 => AttrType::DateTime,
            _ => AttrType::Unknown(v),
        }
    }
}

/// Attribute key definition (from global attr_keys table)
#[derive(Debug, Clone)]
pub struct AttrKey {
    /// Attribute name (e.g., "linux.xattr.user.myattr")
    pub name: String,
    /// Attribute value type
    pub attr_type: AttrType,
}

/// Map of attribute key index -> raw value bytes
pub type AttrMap = HashMap<usize, Box<[u8]>>;

/// Type of record in the archive
#[derive(Debug, Clone)]
pub enum RecordData {
    /// Regular file (single compressed blob)
    File {
        /// Compression algorithm
        compression: Compression,
        /// Offset to compressed data in archive
        data_offset: u64,
        /// Compressed size in bytes
        compressed_size: u64,
        /// Decompressed size in bytes
        decompressed_size: u64,
    },
    /// Chunked file (multiple independently-compressed blocks)
    ChunkedFile {
        /// Compression algorithm used for all blocks
        compression: Compression,
        /// Uncompressed block size (last block may be smaller)
        block_size: u32,
        /// Offset to first block in archive
        data_offset: u64,
        /// Total compressed size in bytes
        compressed_size: u64,
        /// Total decompressed size in bytes
        decompressed_size: u64,
    },
    /// Directory
    Directory {
        /// Indices of child records (1-based)
        children: Vec<u64>,
    },
    /// Internal symlink (target is another record)
    InternalLink {
        /// Index of target record (1-based)
        target_index: u64,
    },
    /// External symlink (target is a path string)
    ExternalLink {
        /// Target path
        target: String,
    },
}

/// A single record in the archive
#[derive(Debug, Clone)]
pub struct Record {
    /// Name of this entry (filename, not full path)
    pub name: String,
    /// Record-specific data
    pub data: RecordData,
    /// Unix mode (permissions + type)
    pub mode: u16,
    /// Modification time (seconds since box epoch)
    pub mtime: i64,
    /// Extended attributes (key index -> raw bytes)
    pub attrs: AttrMap,
}

impl Record {
    /// Get the file type for dir_emit
    pub fn dtype(&self) -> u8 {
        match &self.data {
            RecordData::File { .. } | RecordData::ChunkedFile { .. } => 8,  // DT_REG
            RecordData::Directory { .. } => 4,  // DT_DIR
            RecordData::InternalLink { .. } | RecordData::ExternalLink { .. } => 10, // DT_LNK
        }
    }

    /// Check if this is a directory
    pub fn is_dir(&self) -> bool {
        matches!(&self.data, RecordData::Directory { .. })
    }

    /// Check if this is a regular file (including chunked)
    pub fn is_file(&self) -> bool {
        matches!(&self.data, RecordData::File { .. } | RecordData::ChunkedFile { .. })
    }

    /// Check if this is a symlink
    pub fn is_symlink(&self) -> bool {
        matches!(&self.data, RecordData::InternalLink { .. } | RecordData::ExternalLink { .. })
    }

    /// Get file size (decompressed size for files, 0 for others)
    pub fn size(&self) -> u64 {
        match &self.data {
            RecordData::File { decompressed_size, .. } => *decompressed_size,
            RecordData::ChunkedFile { decompressed_size, .. } => *decompressed_size,
            _ => 0,
        }
    }
}

/// Parsed metadata from a box archive
pub struct BoxfsMetadata {
    /// All records in the archive (0-indexed, but record indices are 1-based)
    pub records: Vec<Record>,
    /// Root record index (1-based)
    pub root_index: u64,
    /// Total archive size
    pub archive_size: u64,
    /// Offset to data section
    pub data_offset: u64,
    /// FST data for path lookups (optional)
    pub fst_data: Option<Box<[u8]>>,
    /// Block FST data for chunked file block lookups (optional)
    /// Keys: record_index(u64 BE) || logical_offset(u64 BE) = 16 bytes
    /// Values: physical offset within compressed data region
    pub block_fst_data: Option<Box<[u8]>>,
    /// Cache for decompressed blocks (interior mutability for shared access)
    pub block_cache: RefCell<BlockCache>,
    /// Global attribute keys table (name -> type)
    pub attr_keys: Vec<AttrKey>,
}

/// Prefix for Linux extended attributes in box format
pub const LINUX_XATTR_PREFIX: &str = "linux.xattr.";

impl BoxfsMetadata {
    /// Create empty metadata (for stub implementation)
    pub fn empty() -> Self {
        // Create a minimal root directory
        let root = Record {
            name: String::new(),
            data: RecordData::Directory { children: Vec::new() },
            mode: 0o40755, // S_IFDIR | 0755
            mtime: 0,
            attrs: HashMap::new(),
        };

        BoxfsMetadata {
            records: alloc::vec![root],
            root_index: 1,
            archive_size: 0,
            data_offset: 0,
            fst_data: None,
            block_fst_data: None,
            block_cache: RefCell::new(BlockCache::default()),
            attr_keys: Vec::new(),
        }
    }

    /// Get attribute key name by index
    pub fn attr_key_name(&self, index: usize) -> Option<&str> {
        self.attr_keys.get(index).map(|k| k.name.as_str())
    }

    /// Find attribute key index by name
    pub fn find_attr_key(&self, name: &str) -> Option<usize> {
        self.attr_keys.iter().position(|k| k.name == name)
    }

    /// Get an xattr value for a record by xattr name (e.g., "user.myattr")
    /// This looks up "linux.xattr.{name}" in the record's attrs
    pub fn get_xattr<'a>(&self, record: &'a Record, name: &str) -> Option<&'a [u8]> {
        // Construct the full key name
        let full_name = alloc::format!("{}{}", LINUX_XATTR_PREFIX, name);
        let key_idx = self.find_attr_key(&full_name)?;
        record.attrs.get(&key_idx).map(|v| v.as_ref())
    }

    /// List all xattr names for a record
    /// Returns iterator over xattr names (without "linux.xattr." prefix)
    pub fn list_xattrs<'a>(&'a self, record: &'a Record) -> impl Iterator<Item = &'a str> {
        record.attrs.keys().filter_map(move |&key_idx| {
            self.attr_key_name(key_idx)
                .and_then(|name| name.strip_prefix(LINUX_XATTR_PREFIX))
        })
    }

    /// Get a record by 1-based index
    pub fn get(&self, index: u64) -> Option<&Record> {
        if index == 0 {
            return None;
        }
        self.records.get(index as usize - 1)
    }

    /// Get record count
    pub fn record_count(&self) -> u64 {
        self.records.len() as u64
    }

    /// Find a child record by name within a directory
    pub fn find_child(&self, parent_index: u64, name: &str) -> Option<u64> {
        // First try FST lookup if available
        if let Some(ref fst_data) = self.fst_data {
            // Get parent path and append child name
            if let Some(parent_path) = self.path_for_index(parent_index) {
                let child_path = if parent_path.is_empty() {
                    name.to_string()
                } else {
                    alloc::format!("{}/{}", parent_path, name)
                };
                if let Some(idx) = crate::parser::fst_lookup(fst_data, &child_path) {
                    return Some(idx);
                }
            }
        }

        // Fall back to linear search in directory children
        let parent = self.get(parent_index)?;
        if let RecordData::Directory { children } = &parent.data {
            for &child_idx in children {
                if let Some(child) = self.get(child_idx) {
                    if child.name == name {
                        return Some(child_idx);
                    }
                }
            }
        }
        None
    }

    /// Get path for a record index (for symlink resolution)
    pub fn path_for_index(&self, index: u64) -> Option<String> {
        // If we have FST data, search through it
        if let Some(ref fst_data) = self.fst_data {
            if let Ok(fst) = Fst::new(Cow::Borrowed(fst_data.as_ref())) {
                for (key, idx) in fst.prefix_iter(&[]) {
                    if idx == index {
                        // Convert key to path string (0x1f -> /)
                        let path_bytes: Vec<u8> = key.iter()
                            .map(|&b| if b == 0x1f { b'/' } else { b })
                            .collect();
                        if let Ok(path) = core::str::from_utf8(&path_bytes) {
                            return Some(String::from(path));
                        }
                    }
                }
            }
        }

        // Simple fallback for root
        if index == self.root_index {
            return Some(String::new());
        }

        None
    }

    /// Get direct children of a directory
    pub fn children(&self, dir_index: u64) -> Vec<(u64, &Record)> {
        let mut result = Vec::new();

        // Try FST first
        if let Some(ref fst_data) = self.fst_data {
            if let Some(parent_path) = self.path_for_index(dir_index) {
                let children = crate::parser::fst_children(fst_data, &parent_path);
                for (_, idx) in children {
                    if let Some(record) = self.get(idx) {
                        result.push((idx, record));
                    }
                }
                if !result.is_empty() {
                    return result;
                }
            }
        }

        // Fall back to directory entries
        if let Some(dir) = self.get(dir_index) {
            if let RecordData::Directory { children } = &dir.data {
                for &child_idx in children {
                    if let Some(record) = self.get(child_idx) {
                        result.push((child_idx, record));
                    }
                }
            }
        }

        result
    }

    /// Find the block containing a logical offset for a chunked file.
    /// Returns (physical_offset, block_logical_offset) or None if not found.
    ///
    /// Uses predecessor query: finds largest key <= (record_index, logical_offset).
    pub fn find_block(&self, record_index: u64, logical_offset: u64) -> Option<(u64, u64)> {
        let block_fst_data = self.block_fst_data.as_ref()?;
        let fst = Fst::new(Cow::Borrowed(block_fst_data.as_ref())).ok()?;

        // Prefix is just the record_index (first 8 bytes)
        let prefix = record_index.to_be_bytes();

        // FST iteration gives keys in sorted order
        // Find the last key <= our query (predecessor)
        let mut result: Option<(u64, u64)> = None;
        for (k, physical_offset) in fst.prefix_iter(&prefix) {
            if k.len() != 16 {
                continue;
            }
            let block_logical = u64::from_be_bytes(k[8..16].try_into().ok()?);
            if block_logical <= logical_offset {
                result = Some((physical_offset, block_logical));
            } else {
                // Keys are sorted, stop once we pass our target
                break;
            }
        }
        result
    }

    /// Get the next block after a given logical offset for a chunked file.
    /// Returns (next_logical_offset, next_physical_offset) or None if no more blocks.
    pub fn next_block(&self, record_index: u64, current_logical: u64) -> Option<(u64, u64)> {
        let block_fst_data = self.block_fst_data.as_ref()?;
        let fst = Fst::new(Cow::Borrowed(block_fst_data.as_ref())).ok()?;

        let prefix = record_index.to_be_bytes();

        // Keys are sorted, find first block with logical > current_logical
        for (k, physical_offset) in fst.prefix_iter(&prefix) {
            if k.len() != 16 {
                continue;
            }
            let block_logical = u64::from_be_bytes(k[8..16].try_into().ok()?);
            if block_logical > current_logical {
                return Some((block_logical, physical_offset));
            }
        }
        None
    }

    /// Get all blocks for a chunked file record.
    /// Returns Vec of (logical_offset, physical_offset) in sorted order.
    pub fn blocks_for_record(&self, record_index: u64) -> Vec<(u64, u64)> {
        let Some(block_fst_data) = self.block_fst_data.as_ref() else {
            return Vec::new();
        };
        let Ok(fst) = Fst::new(Cow::Borrowed(block_fst_data.as_ref())) else {
            return Vec::new();
        };

        let prefix = record_index.to_be_bytes();

        // FST iteration is already sorted by key
        let mut blocks = Vec::new();
        for (k, physical_offset) in fst.prefix_iter(&prefix) {
            if k.len() == 16 {
                let block_logical = u64::from_be_bytes(k[8..16].try_into().unwrap());
                blocks.push((block_logical, physical_offset));
            }
        }
        blocks
    }
}
