//! Multi-archive support for fusebox.
//!
//! This module provides the `MultiArchive` struct which allows mounting
//! multiple .box files into a unified filesystem hierarchy using the same
//! composite index scheme as the kernel module: `(archive_id << 64) | local_record_index`.

use std::collections::HashMap;

use box_format::sync::BoxReader;
use box_format::{BoxMetadata, Record, RecordIndex};
use box_fst::FstBuilder;

/// Per-archive wrapper holding a BoxReader.
pub struct ArchiveHandle {
    /// Archive ID (used in composite indices)
    pub id: u64,
    /// The reader for this archive
    pub reader: BoxReader,
}

/// Multi-archive container with merged FST for unified lookups.
///
/// Uses composite indices: `(archive_id << 64) | local_record_index`
/// to uniquely identify records across all mounted archives.
pub struct MultiArchive {
    /// Per-archive readers, keyed by archive_id
    pub archives: HashMap<u64, ArchiveHandle>,
    /// Merged path→composite_index FST (in-memory, queryable)
    pub merged_fst: FstBuilder<u128>,
    /// Reverse index: composite_index → path bytes for O(1) path lookups
    reverse_index: HashMap<u128, Vec<u8>>,
    /// Next archive ID to assign
    next_archive_id: u64,
}

impl MultiArchive {
    /// Create a new empty multi-archive container.
    pub fn new() -> Self {
        Self {
            archives: HashMap::new(),
            merged_fst: FstBuilder::new(),
            reverse_index: HashMap::new(),
            next_archive_id: 0,
        }
    }

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

    /// Add an archive to the multi-archive container.
    ///
    /// Merges the archive's paths into the unified FST.
    /// Conflict policy:
    /// - Directories merge (both remain, children combine)
    /// - Files use last-wins (newer archive shadows older)
    ///
    /// Returns the assigned archive ID.
    pub fn add_archive(&mut self, reader: BoxReader) -> u64 {
        let archive_id = self.next_archive_id;
        self.next_archive_id += 1;

        let metadata = reader.metadata();

        // Merge paths from this archive into merged_fst using the public iter() method
        for item in metadata.iter() {
            let local_idx = item.index().get();
            let composite = Self::pack_index(archive_id, local_idx);

            // Convert BoxPath to FST key (uses 0x1f as separator internally)
            let path: &[u8] = item.path.as_ref();

            // Check for conflicts
            if let Some(existing) = self.merged_fst.get(path) {
                let existing_record = self.get_record(existing);

                // If both are directories, keep existing (children will merge via iteration)
                if existing_record.map_or(false, |r| matches!(r, Record::Directory(_)))
                    && matches!(item.record, Record::Directory(_))
                {
                    // Still add to reverse_index so we can find this directory's path
                    self.reverse_index.insert(composite, path.to_vec());
                    continue;
                }
                // Otherwise fall through to overwrite (last-wins for files)
            }

            // Insert or overwrite in FST and reverse index
            self.merged_fst.insert_or_replace(path, composite);
            self.reverse_index.insert(composite, path.to_vec());
        }

        self.archives.insert(
            archive_id,
            ArchiveHandle {
                id: archive_id,
                reader,
            },
        );
        archive_id
    }

    /// Get the archive handle for a composite index.
    pub fn get_archive(&self, composite: u128) -> Option<&ArchiveHandle> {
        let (archive_id, _) = Self::unpack_index(composite);
        self.archives.get(&archive_id)
    }

    /// Get a mutable archive handle for a composite index.
    pub fn get_archive_mut(&mut self, composite: u128) -> Option<&mut ArchiveHandle> {
        let (archive_id, _) = Self::unpack_index(composite);
        self.archives.get_mut(&archive_id)
    }

    /// Get the metadata for a composite index.
    pub fn get_metadata(&self, composite: u128) -> Option<&BoxMetadata<'static>> {
        self.get_archive(composite).map(|a| a.reader.metadata())
    }

    /// Get a record by composite index.
    pub fn get_record(&self, composite: u128) -> Option<&Record<'static>> {
        let (archive_id, local_idx) = Self::unpack_index(composite);
        if local_idx == 0 {
            return None;
        }
        let archive = self.archives.get(&archive_id)?;
        let idx = RecordIndex::try_new(local_idx)?;
        archive.reader.metadata().record(idx)
    }

    /// Look up a child by name using direct FST access.
    /// This is O(1) for path lookup + O(key_len) for FST lookup.
    pub fn lookup_child(
        &self,
        parent_composite: Option<u128>,
        name: &str,
    ) -> Option<(u128, &Record<'static>)> {
        // Build the FST key for this child
        let key: Vec<u8> = match parent_composite {
            Some(parent) => {
                // Get parent path from reverse index (O(1))
                let mut parent_path = self.path_for_index(parent)?;
                parent_path.push(0x1f); // FST separator
                parent_path.extend_from_slice(name.as_bytes());
                parent_path
            }
            None => name.as_bytes().to_vec(), // Root-level lookup
        };

        // Direct FST lookup - O(key_len)
        let composite = self.merged_fst.get(&key)?;
        let record = self.get_record(composite)?;
        Some((composite, record))
    }

    /// Find a child record by name within a directory.
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
        let parent = self.get_record(parent_composite)?;
        if let Record::Directory(dir) = parent {
            let (archive_id, _) = Self::unpack_index(parent_composite);
            for child_idx in &dir.entries {
                let child_composite = Self::pack_index(archive_id, child_idx.get());
                if let Some(child) = self.get_record(child_composite) {
                    if child.name() == name {
                        return Some(child_composite);
                    }
                }
            }
        }
        None
    }

    /// Get path (as FST key bytes) for a composite index.
    /// O(1) lookup using the reverse index.
    pub fn path_for_index(&self, composite: u128) -> Option<Vec<u8>> {
        // O(1) lookup in reverse index
        self.reverse_index.get(&composite).cloned()
    }

    /// Get direct children of a directory from merged FST.
    /// Merges children from all archives for directories that exist in multiple.
    pub fn children(&self, dir_composite: u128) -> Vec<(u128, &Record<'static>)> {
        let mut result = Vec::new();
        let mut seen_names: HashMap<&str, usize> = HashMap::new();

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

            if let Some(record) = self.get_record(composite) {
                // Track by name to handle merged directories
                if let Some(&existing_idx) = seen_names.get(record.name()) {
                    // Update existing entry (last-wins for same name)
                    result[existing_idx] = (composite, record);
                } else {
                    seen_names.insert(record.name(), result.len());
                    result.push((composite, record));
                }
            }
        }

        result
    }

    /// Get root children (merged from all archives).
    pub fn root_children(&self) -> Vec<(u128, &Record<'static>)> {
        let mut result = Vec::new();
        let mut seen_names: HashMap<&str, usize> = HashMap::new();

        // Iterate merged FST for root-level entries (no separator in key)
        for (key, composite) in self.merged_fst.prefix_iter(&[]) {
            // Root-level entries have no separator
            if key.contains(&0x1f) {
                continue;
            }

            if let Some(record) = self.get_record(composite) {
                // Track by name to handle merged directories
                if let Some(&existing_idx) = seen_names.get(record.name()) {
                    // Update existing entry (last-wins for same name)
                    result[existing_idx] = (composite, record);
                } else {
                    seen_names.insert(record.name(), result.len());
                    result.push((composite, record));
                }
            }
        }

        result
    }

    /// Get the first archive's metadata (for archive-level attributes).
    pub fn first_metadata(&self) -> Option<&BoxMetadata<'static>> {
        self.archives.values().next().map(|a| a.reader.metadata())
    }

    /// Check if any archives are loaded.
    pub fn is_empty(&self) -> bool {
        self.archives.is_empty()
    }

    /// Get total record count across all archives.
    pub fn record_count(&self) -> u64 {
        self.archives
            .values()
            .map(|a| a.reader.metadata().iter().count() as u64)
            .sum()
    }

    /// Get total decompressed size across all archives.
    pub fn total_size(&self) -> u64 {
        self.archives
            .values()
            .flat_map(|a| a.reader.metadata().iter())
            .filter_map(|item| match item.record {
                Record::File(f) => Some(f.decompressed_length),
                Record::ChunkedFile(f) => Some(f.decompressed_length),
                _ => None,
            })
            .sum()
    }
}

impl Default for MultiArchive {
    fn default() -> Self {
        Self::new()
    }
}
