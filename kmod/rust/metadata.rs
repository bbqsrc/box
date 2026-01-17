// SPDX-License-Identifier: GPL-2.0-only
//! Parsed box archive metadata
//!
//! This module holds the in-memory representation of the box archive
//! metadata that was parsed from the trailer. Supports mounting multiple
//! archives into a unified filesystem hierarchy.

use alloc::borrow::Cow;
use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;
use core::cell::RefCell;

use box_fst::{Fst, FstBuilder};
use hashbrown::HashMap;

// ============================================================================
// BLOCK CACHE
// ============================================================================

/// Cache key: (composite_index, block_logical_offset)
type CacheKey = (u128, u64);

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
    pub fn get(&mut self, composite: u128, block_offset: u64) -> Option<&[u8]> {
        let key = (composite, block_offset);
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
    pub fn insert(&mut self, composite: u128, block_offset: u64, data: Box<[u8]>) {
        let key = (composite, block_offset);
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

/// Per-archive metadata for multi-archive mounting.
pub struct ArchiveData {
    /// Archive identifier (used in composite indices)
    pub id: u64,
    /// All records in this archive (0-indexed, but record indices are 1-based)
    pub records: Vec<Record>,
    /// Root record index within this archive (1-based, local)
    pub root_index: u64,
    /// Total archive size
    pub archive_size: u64,
    /// Base offset of this archive's data in the block device
    pub data_offset_base: u64,
    /// FST data for path lookups within this archive (optional)
    pub fst_data: Option<Box<[u8]>>,
    /// Block FST data for chunked file block lookups (optional)
    /// Keys: record_index(u64 BE) || logical_offset(u64 BE) = 16 bytes
    /// Values: physical offset within compressed data region
    pub block_fst_data: Option<Box<[u8]>>,
    /// Global attribute keys table (name -> type)
    pub attr_keys: Vec<AttrKey>,
}

/// Parsed metadata supporting multiple box archives merged into one hierarchy.
///
/// Uses composite indices: `(archive_id << 64) | local_record_index`
pub struct BoxfsMetadata {
    /// Per-archive metadata, keyed by archive_id
    pub archives: HashMap<u64, ArchiveData>,
    /// Merged path→composite_index FST (in-memory, queryable)
    pub merged_fst: FstBuilder<u128>,
    /// Next archive ID to assign
    next_archive_id: u64,
    /// Cache for decompressed blocks (interior mutability for shared access)
    /// Keys use composite indices
    pub block_cache: RefCell<BlockCache>,
}

/// Prefix for Linux extended attributes in box format
pub const LINUX_XATTR_PREFIX: &str = "linux.xattr.";

impl BoxfsMetadata {
    /// Pack archive_id and local_index into a composite index.
    #[inline]
    pub fn pack_index(archive_id: u64, local_index: u64) -> u128 {
        ((archive_id as u128) << 64) | (local_index as u128)
    }

    /// Unpack composite index into (archive_id, local_index).
    #[inline]
    pub fn unpack_index(composite: u128) -> (u64, u64) {
        let archive_id = (composite >> 64) as u64;
        let local_index = composite as u64;
        (archive_id, local_index)
    }

    /// Create empty metadata (for initialization).
    pub fn empty() -> Self {
        BoxfsMetadata {
            archives: HashMap::new(),
            merged_fst: FstBuilder::new(),
            next_archive_id: 0,
            block_cache: RefCell::new(BlockCache::default()),
        }
    }

    /// Add an archive, merging its paths into the unified FST.
    ///
    /// Conflict policy:
    /// - Directories merge (both remain, children combine)
    /// - Files use last-wins (newer archive shadows older)
    pub fn add_archive(&mut self, mut archive: ArchiveData) -> u64 {
        let archive_id = self.next_archive_id;
        self.next_archive_id += 1;
        archive.id = archive_id;

        // Merge this archive's paths into the unified FST
        if let Some(ref fst_bytes) = archive.fst_data {
            if let Ok(fst) = Fst::<_, u64>::new(Cow::Borrowed(fst_bytes.as_ref())) {
                for (path, local_idx) in fst.prefix_iter(&[]) {
                    if local_idx == 0 {
                        continue;
                    }

                    let composite = Self::pack_index(archive_id, local_idx);

                    // Check for conflicts
                    if let Some(existing) = self.merged_fst.get(&path) {
                        let existing_record = self.get(existing);
                        let new_record = archive.records.get(local_idx as usize - 1);

                        // If both are directories, keep existing (children will merge via iteration)
                        if existing_record.map_or(false, |r| r.is_dir())
                            && new_record.map_or(false, |r| r.is_dir())
                        {
                            continue;
                        }
                        // Otherwise fall through to overwrite (last-wins for files)
                    }

                    // Insert or overwrite
                    self.merged_fst.insert_or_replace(&path, composite);
                }
            }
        }

        self.archives.insert(archive_id, archive);
        archive_id
    }

    /// Get the archive for a composite index.
    pub fn get_archive(&self, composite: u128) -> Option<&ArchiveData> {
        let (archive_id, _) = Self::unpack_index(composite);
        self.archives.get(&archive_id)
    }

    /// Get attribute key name by index from the appropriate archive.
    pub fn attr_key_name(&self, composite: u128, key_index: usize) -> Option<&str> {
        let archive = self.get_archive(composite)?;
        archive.attr_keys.get(key_index).map(|k| k.name.as_str())
    }

    /// Find attribute key index by name in the appropriate archive.
    pub fn find_attr_key(&self, composite: u128, name: &str) -> Option<usize> {
        let archive = self.get_archive(composite)?;
        archive.attr_keys.iter().position(|k| k.name == name)
    }

    /// Get an xattr value for a record by xattr name (e.g., "user.myattr")
    /// This looks up "linux.xattr.{name}" in the record's attrs
    pub fn get_xattr<'a>(&'a self, composite: u128, record: &'a Record, name: &str) -> Option<&'a [u8]> {
        let full_name = alloc::format!("{}{}", LINUX_XATTR_PREFIX, name);
        let key_idx = self.find_attr_key(composite, &full_name)?;
        record.attrs.get(&key_idx).map(|v| v.as_ref())
    }

    /// List all xattr names for a record
    /// Returns iterator over xattr names (without "linux.xattr." prefix)
    pub fn list_xattrs<'a>(&'a self, composite: u128, record: &'a Record) -> impl Iterator<Item = &'a str> {
        let archive = self.get_archive(composite);
        record.attrs.keys().filter_map(move |&key_idx| {
            archive
                .and_then(|a| a.attr_keys.get(key_idx))
                .map(|k| k.name.as_str())
                .and_then(|name| name.strip_prefix(LINUX_XATTR_PREFIX))
        })
    }

    /// Get a record by composite index.
    pub fn get(&self, composite: u128) -> Option<&Record> {
        let (archive_id, local_idx) = Self::unpack_index(composite);
        if local_idx == 0 {
            return None;
        }
        let archive = self.archives.get(&archive_id)?;
        archive.records.get(local_idx as usize - 1)
    }

    /// Get total record count across all archives.
    pub fn record_count(&self) -> u64 {
        self.archives.values().map(|a| a.records.len() as u64).sum()
    }

    /// Find a child record by name within a directory.
    /// Uses the merged FST for lookups.
    pub fn find_child(&self, parent_composite: u128, name: &str) -> Option<u128> {
        // Get parent path from merged FST
        if let Some(parent_path) = self.path_for_index(parent_composite) {
            // Build child path key (using 0x1f as separator)
            let child_key: Vec<u8> = if parent_path.is_empty() {
                name.as_bytes().to_vec()
            } else {
                let mut key = parent_path;
                key.push(0x1f);
                key.extend_from_slice(name.as_bytes());
                key
            };

            // Look up in merged FST
            if let Some(composite) = self.merged_fst.get(&child_key) {
                return Some(composite);
            }
        }

        // Fall back to linear search in directory children (for non-FST archives)
        let parent = self.get(parent_composite)?;
        if let RecordData::Directory { children } = &parent.data {
            let (archive_id, _) = Self::unpack_index(parent_composite);
            for &local_child_idx in children {
                let child_composite = Self::pack_index(archive_id, local_child_idx);
                if let Some(child) = self.get(child_composite) {
                    if child.name == name {
                        return Some(child_composite);
                    }
                }
            }
        }
        None
    }

    /// Get path (as FST key bytes) for a composite index.
    /// Returns None if not found in merged FST.
    pub fn path_for_index(&self, composite: u128) -> Option<Vec<u8>> {
        // Search merged FST for this composite index
        for (key, idx) in self.merged_fst.prefix_iter(&[]) {
            if idx == composite {
                return Some(key);
            }
        }

        // Check if this is a root of any archive
        for (archive_id, archive) in &self.archives {
            if Self::pack_index(*archive_id, archive.root_index) == composite {
                return Some(Vec::new()); // Root has empty path
            }
        }

        None
    }

    /// Get path as a string for a composite index (for symlink resolution).
    pub fn path_string_for_index(&self, composite: u128) -> Option<String> {
        let key = self.path_for_index(composite)?;
        // Convert key to path string (0x1f -> /)
        let path_bytes: Vec<u8> = key.iter()
            .map(|&b| if b == 0x1f { b'/' } else { b })
            .collect();
        core::str::from_utf8(&path_bytes).ok().map(String::from)
    }

    /// Get direct children of a directory from merged FST.
    /// Merges children from all archives for directories that exist in multiple.
    pub fn children(&self, dir_composite: u128) -> Vec<(u128, &Record)> {
        let mut result = Vec::new();
        let mut seen_names: HashMap<&str, u128> = HashMap::new();

        // Get parent path
        let parent_path = match self.path_for_index(dir_composite) {
            Some(p) => p,
            None => return result,
        };

        // Build prefix for children
        let prefix: Vec<u8> = if parent_path.is_empty() {
            Vec::new()
        } else {
            let mut p = parent_path;
            p.push(0x1f); // Add separator for children
            p
        };

        // Iterate merged FST for children
        for (key, composite) in self.merged_fst.prefix_iter(&prefix) {
            // Check if this is a direct child (no more separators after prefix)
            let suffix = &key[prefix.len()..];
            if suffix.contains(&0x1f) {
                continue; // Not a direct child
            }

            if let Some(record) = self.get(composite) {
                // For merged directories, use last-seen (which is last-mounted)
                seen_names.insert(&record.name, composite);
                result.push((composite, record));
            }
        }

        result
    }

    /// Find the block containing a logical offset for a chunked file.
    /// Returns (physical_offset, block_logical_offset) or None if not found.
    ///
    /// Note: For multi-archive, the physical_offset is relative to the archive's data section.
    /// Caller must add archive.data_offset_base.
    pub fn find_block(&self, composite: u128, logical_offset: u64) -> Option<(u64, u64)> {
        let (archive_id, local_idx) = Self::unpack_index(composite);
        let archive = self.archives.get(&archive_id)?;
        let block_fst_data = archive.block_fst_data.as_ref()?;
        let fst = Fst::<_, u64>::new(Cow::Borrowed(block_fst_data.as_ref())).ok()?;

        // Prefix is the local record_index (first 8 bytes)
        let prefix = local_idx.to_be_bytes();

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
    pub fn next_block(&self, composite: u128, current_logical: u64) -> Option<(u64, u64)> {
        let (archive_id, local_idx) = Self::unpack_index(composite);
        let archive = self.archives.get(&archive_id)?;
        let block_fst_data = archive.block_fst_data.as_ref()?;
        let fst = Fst::<_, u64>::new(Cow::Borrowed(block_fst_data.as_ref())).ok()?;

        let prefix = local_idx.to_be_bytes();

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
    pub fn blocks_for_record(&self, composite: u128) -> Vec<(u64, u64)> {
        let (archive_id, local_idx) = Self::unpack_index(composite);
        let Some(archive) = self.archives.get(&archive_id) else {
            return Vec::new();
        };
        let Some(block_fst_data) = archive.block_fst_data.as_ref() else {
            return Vec::new();
        };
        let Ok(fst) = Fst::<_, u64>::new(Cow::Borrowed(block_fst_data.as_ref())) else {
            return Vec::new();
        };

        let prefix = local_idx.to_be_bytes();

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

    /// Get the first archive's root as the filesystem root.
    /// Returns composite index or 0 if no archives.
    pub fn root_index(&self) -> u128 {
        // Return first archive's root
        if let Some((&archive_id, archive)) = self.archives.iter().next() {
            Self::pack_index(archive_id, archive.root_index)
        } else {
            0
        }
    }
}
