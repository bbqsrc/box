use std::ffi::OsStr;
use std::num::NonZeroUsize;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use lru::LruCache as LruCacheImpl;

use fastvint::ReadVintExt;
use fuser::{
    FileAttr, FileType, Filesystem, OpenFlags, ReplyAttr, ReplyData, ReplyDirectory,
    ReplyDirectoryPlus, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, ReplyXattr, Request,
};
use libc::{EACCES, ENODATA, ENOENT, ERANGE};

use box_format::sync::BoxReader;
use box_format::{BOX_EPOCH_UNIX, BoxMetadata, Record, RecordIndex, attrs};

mod multi;
pub use multi::MultiArchive;

// Re-export fuser types needed by consumers (avoids version mismatches)
pub use fuser::{MountOption, mount2};

/// Cache key for block-level caching
/// For regular files: (composite_index, 0)
/// For chunked files: (composite_index, block_index)
type CacheKey = (u128, u64);

/// LRU cache with size limit for decompressed data blocks
///
/// For regular files, caches the entire decompressed content.
/// For chunked files, caches individual 2MB blocks for efficient random access.
///
/// Uses the `lru` crate for O(1) get/put operations.
pub struct LruCache {
    inner: LruCacheImpl<CacheKey, Vec<u8>>,
    current_size: usize,
    max_size: usize,
}

impl LruCache {
    pub fn new(max_size_mb: usize) -> Self {
        let max_size = max_size_mb * 1024 * 1024;
        // Estimate max entries (assume avg 64KB per entry, minimum 1024 entries)
        let cap = NonZeroUsize::new((max_size / 65536).max(1024)).unwrap();
        Self {
            inner: LruCacheImpl::new(cap),
            current_size: 0,
            max_size,
        }
    }

    fn get(&mut self, key: &CacheKey) -> Option<&[u8]> {
        self.inner.get(key).map(|v| v.as_slice()) // O(1)
    }

    /// Check if key exists without updating LRU order (no mutation)
    fn contains(&self, key: &CacheKey) -> bool {
        self.inner.contains(key)
    }

    fn insert(&mut self, key: CacheKey, value: Vec<u8>) {
        let size = value.len();

        // If this single item is larger than max cache, don't cache it
        if size > self.max_size {
            return;
        }

        // Evict oldest entries until we have room (by size, not count)
        while self.current_size + size > self.max_size {
            if let Some((evicted_key, evicted)) = self.inner.pop_lru() {
                tracing::trace!(
                    composite = evicted_key.0,
                    block = evicted_key.1,
                    size = evicted.len(),
                    "cache evict"
                );
                self.current_size -= evicted.len();
            } else {
                break;
            }
        }

        // Remove existing entry if present (put returns old value)
        if let Some(old) = self.inner.put(key, value) {
            self.current_size -= old.len();
        }
        self.current_size += size;
    }

    /// Get current cache size in bytes
    pub fn size(&self) -> usize {
        self.current_size
    }

    /// Get number of cached entries
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Check if cache is empty
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

pub struct BoxFs {
    archives: MultiArchive,
    cache: LruCache,
}

impl BoxFs {
    pub fn new(archives: MultiArchive, cache: LruCache) -> Self {
        Self { archives, cache }
    }

    /// Add an archive to the multi-archive container.
    pub fn add_archive(&mut self, reader: BoxReader) -> u64 {
        self.archives.add_archive(reader)
    }
}

// Read-only filesystem - attributes never change, cache aggressively
const TTL: Duration = Duration::from_secs(3600);
const BLOCK_SIZE: u32 = 4096;

fn parse_archive_time(meta: &BoxMetadata, name: &str) -> Option<SystemTime> {
    let bytes = meta.file_attr(name)?;
    let (minutes, len) = fastvint::decode_vi64_slice(bytes);
    if len == 0 {
        return None;
    }
    let unix_secs = (minutes * 60 + BOX_EPOCH_UNIX) as u64;
    UNIX_EPOCH.checked_add(Duration::from_secs(unix_secs))
}

fn archive_uid(meta: &BoxMetadata) -> u32 {
    if let Some(bytes) = meta.file_attr(attrs::UNIX_UID) {
        let (uid, len) = fastvint::decode_vu32_slice(bytes);
        if len > 0 {
            return uid;
        }
    }
    unsafe { libc::getuid() }
}

fn archive_gid(meta: &BoxMetadata) -> u32 {
    if let Some(bytes) = meta.file_attr(attrs::UNIX_GID) {
        let (gid, len) = fastvint::decode_vu32_slice(bytes);
        if len > 0 {
            return gid;
        }
    }
    unsafe { libc::getgid() }
}

fn root_dir_attr(meta: &BoxMetadata) -> FileAttr {
    let ctime = parse_archive_time(meta, attrs::CREATED).unwrap_or(UNIX_EPOCH);

    FileAttr {
        ino: 1,
        size: 4096,
        blocks: 0,
        atime: ctime,
        mtime: ctime,
        ctime,
        crtime: ctime,
        kind: FileType::Directory,
        perm: 0o755,
        nlink: 2,
        uid: archive_uid(meta),
        gid: archive_gid(meta),
        rdev: 0,
        flags: 0,
        blksize: 0,
    }
}

trait RecordExt {
    fn fuse_file_type(&self) -> FileType;
    fn fuse_file_attr(&self, meta: &BoxMetadata, composite: u128) -> FileAttr;

    fn perm(&self, meta: &BoxMetadata) -> u16;
    fn uid(&self, meta: &BoxMetadata) -> u32;
    fn gid(&self, meta: &BoxMetadata) -> u32;
    fn ctime(&self, meta: &BoxMetadata) -> SystemTime;
    fn mtime(&self, meta: &BoxMetadata) -> SystemTime;
    fn atime(&self, meta: &BoxMetadata) -> SystemTime;
    fn parse_time_attr(&self, meta: &BoxMetadata, name: &str) -> Option<SystemTime>;
}

impl RecordExt for box_format::Record<'_> {
    fn fuse_file_type(&self) -> FileType {
        use box_format::Record::*;

        match self {
            File(_) | ChunkedFile(_) => FileType::RegularFile,
            Directory(_) => FileType::Directory,
            Link(_) | ExternalLink(_) => FileType::Symlink,
        }
    }

    fn fuse_file_attr(&self, meta: &BoxMetadata, composite: u128) -> FileAttr {
        let kind = self.fuse_file_type();
        let nlink = 1;
        let blocks = if kind == FileType::RegularFile { 1 } else { 0 };

        use box_format::Record::*;
        let size = match self {
            File(record) => record.decompressed_length,
            ChunkedFile(record) => record.decompressed_length,
            Directory(_) => 4096, // Standard directory size
            Link(record) => {
                // Internal link - size is the resolved path length (approximate with target name)
                meta.record(record.target)
                    .map(|r| r.name().len() as u64)
                    .unwrap_or(0)
            }
            ExternalLink(record) => record.target.as_ref().len() as u64,
        };

        let perm = self.perm(meta);
        let ctime = self.ctime(meta);
        let mtime = self.mtime(meta);
        let atime = self.atime(meta);

        FileAttr {
            ino: composite_to_ino(composite),
            size,
            blocks,
            atime,
            mtime,
            ctime,
            crtime: ctime,
            kind: self.fuse_file_type(),
            perm,
            nlink,
            uid: self.uid(meta),
            gid: self.gid(meta),
            rdev: 0,
            flags: 0,
            blksize: 0,
        }
    }

    fn perm(&self, meta: &BoxMetadata) -> u16 {
        match self.attr(meta, attrs::UNIX_MODE) {
            Some(bytes) => {
                let (mode, len) = fastvint::decode_vu32_slice(bytes);
                if len > 0 {
                    (mode & 0o7777) as u16
                } else {
                    use box_format::Record::*;
                    match self {
                        File(_) | ChunkedFile(_) => 0o644,
                        Directory(_) => 0o755,
                        Link(_) | ExternalLink(_) => 0o777,
                    }
                }
            }
            _ => {
                use box_format::Record::*;
                match self {
                    File(_) | ChunkedFile(_) => 0o644,
                    Directory(_) => 0o755,
                    Link(_) | ExternalLink(_) => 0o777,
                }
            }
        }
    }

    fn uid(&self, meta: &BoxMetadata) -> u32 {
        // Try record attribute
        if let Some(bytes) = self.attr(meta, attrs::UNIX_UID) {
            let (uid, len) = fastvint::decode_vu32_slice(bytes);
            if len > 0 {
                return uid;
            }
        }
        // Try archive-level default
        if let Some(bytes) = meta.file_attr(attrs::UNIX_UID) {
            let (uid, len) = fastvint::decode_vu32_slice(bytes);
            if len > 0 {
                return uid;
            }
        }
        // Fall back to current user
        unsafe { libc::getuid() }
    }

    fn gid(&self, meta: &BoxMetadata) -> u32 {
        // Try record attribute
        if let Some(bytes) = self.attr(meta, attrs::UNIX_GID) {
            let (gid, len) = fastvint::decode_vu32_slice(bytes);
            if len > 0 {
                return gid;
            }
        }
        // Try archive-level default
        if let Some(bytes) = meta.file_attr(attrs::UNIX_GID) {
            let (gid, len) = fastvint::decode_vu32_slice(bytes);
            if len > 0 {
                return gid;
            }
        }
        // Fall back to current group
        unsafe { libc::getgid() }
    }

    fn ctime(&self, meta: &BoxMetadata) -> SystemTime {
        self.parse_time_attr(meta, attrs::CREATED)
            .unwrap_or(UNIX_EPOCH)
    }

    fn mtime(&self, meta: &BoxMetadata) -> SystemTime {
        self.parse_time_attr(meta, attrs::MODIFIED)
            .or_else(|| self.parse_time_attr(meta, attrs::CREATED))
            .unwrap_or(UNIX_EPOCH)
    }

    fn atime(&self, meta: &BoxMetadata) -> SystemTime {
        self.parse_time_attr(meta, attrs::ACCESSED)
            .or_else(|| self.parse_time_attr(meta, attrs::MODIFIED))
            .or_else(|| self.parse_time_attr(meta, attrs::CREATED))
            .unwrap_or(UNIX_EPOCH)
    }

    fn parse_time_attr(&self, meta: &BoxMetadata, name: &str) -> Option<SystemTime> {
        let bytes = self.attr(meta, name)?;
        let mut cursor = std::io::Cursor::new(bytes);
        let minutes = cursor.read_vi64().ok()?;
        let unix_secs = (minutes * 60 + BOX_EPOCH_UNIX) as u64;
        UNIX_EPOCH.checked_add(Duration::from_secs(unix_secs))
    }
}

/// Convert inode to composite index.
/// Returns None for inode 1 (root).
///
/// FUSE uses u64 inodes, so we pack the composite index using:
/// `(archive_id << 48) | local_index` for the inode.
fn ino_to_composite(ino: u64) -> Option<u128> {
    if ino <= 1 {
        None
    } else {
        // Unpack the 48-bit scheme used for FUSE compatibility
        let packed = ino - 1;
        let archive_id = (packed >> 48) as u64;
        let local_index = packed & 0xFFFF_FFFF_FFFF;
        Some(MultiArchive::pack_index(archive_id, local_index))
    }
}

/// Convert composite index to inode.
///
/// FUSE uses u64 inodes, so we pack the composite index using:
/// `(archive_id << 48) | local_index` + 1 for the inode.
fn composite_to_ino(composite: u128) -> u64 {
    let (archive_id, local_index) = MultiArchive::unpack_index(composite);
    // Pack into u64 using 48-bit scheme for FUSE compatibility
    // This limits archive_id to 16 bits and local_index to 48 bits
    ((archive_id << 48) | (local_index & 0xFFFF_FFFF_FFFF)) + 1
}

impl Filesystem for BoxFs {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name = match name.to_str() {
            Some(v) => v,
            None => {
                tracing::debug!(parent, "lookup failed: invalid name encoding");
                reply.error(ENOENT);
                return;
            }
        };

        // Direct FST lookup - O(1) path lookup + O(key_len) FST lookup
        let parent_composite = ino_to_composite(parent);

        match self.archives.lookup_child(parent_composite, name) {
            Some((composite, record)) => {
                let ino = composite_to_ino(composite);
                tracing::trace!(parent, name, ino, "lookup");
                if let Some(meta) = self.archives.get_metadata(composite) {
                    reply.entry(&TTL, &record.fuse_file_attr(meta, composite), 0);
                } else {
                    reply.error(ENOENT);
                }
            }
            None => {
                tracing::trace!(parent, name, "lookup: not found");
                reply.error(ENOENT);
            }
        }
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, _flags: OpenFlags, reply: ReplyOpen) {
        // FOPEN_KEEP_CACHE tells the kernel to keep file data cached across open/close.
        // This is safe for a read-only filesystem where files never change.
        const FOPEN_KEEP_CACHE: u32 = 2;
        tracing::trace!(ino, "open");
        reply.opened(0, FOPEN_KEEP_CACHE);
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let composite = match ino_to_composite(ino) {
            Some(v) => v,
            None => {
                tracing::warn!(ino, "read: invalid inode (root)");
                reply.error(ENOENT);
                return;
            }
        };

        let offset = offset as u64;
        let size = size as u64;

        let record = match self.archives.get_record(composite) {
            Some(r) => r,
            None => {
                tracing::warn!(ino, "read: record not found");
                reply.error(ENOENT);
                return;
            }
        };

        // Get local index for decompression
        let (_, local_idx) = MultiArchive::unpack_index(composite);
        let local_index = match RecordIndex::try_new(local_idx) {
            Some(idx) => idx,
            None => {
                tracing::warn!(ino, "read: invalid local index");
                reply.error(ENOENT);
                return;
            }
        };

        match record {
            Record::File(f) => {
                // Regular files: cache entire decompressed content with key (composite, 0)
                let cache_key = (composite, 0u64);
                if let Some(cached) = self.cache.get(&cache_key) {
                    let end = (offset as usize + size as usize).min(cached.len());
                    let start = (offset as usize).min(end);
                    tracing::debug!(ino, offset, size, "read: cache hit (file)");
                    reply.data(&cached[start..end]);
                    return;
                }

                // Get the archive reader for decompression
                let archive = match self.archives.get_archive(composite) {
                    Some(a) => a,
                    None => {
                        tracing::warn!(ino, "read: archive not found");
                        reply.error(ENOENT);
                        return;
                    }
                };

                let mut buf = Vec::new();
                match archive.reader.decompress(f, &mut buf) {
                    Ok(_) => {
                        tracing::debug!(
                            ino,
                            offset,
                            size,
                            decompressed_size = buf.len(),
                            "read: cache miss, decompressed (file)"
                        );
                        let end = (offset as usize + size as usize).min(buf.len());
                        let start = (offset as usize).min(end);
                        reply.data(&buf[start..end]);
                        self.cache.insert(cache_key, buf);
                    }
                    Err(e) => {
                        tracing::error!(ino, error = %e, "read: decompression failed");
                        reply.error(ENOENT);
                    }
                }
            }
            Record::ChunkedFile(f) => {
                // Chunked files: use block-level caching for efficient random access
                let block_size = f.block_size as u64;
                let file_size = f.decompressed_length;

                // Clamp to file size
                let start_byte = offset.min(file_size);
                let end_byte = (offset + size).min(file_size);

                if start_byte >= end_byte {
                    reply.data(&[]);
                    return;
                }

                // Calculate which blocks we need
                let first_block_idx = start_byte / block_size;
                let last_block_idx = (end_byte.saturating_sub(1)) / block_size;

                tracing::debug!(
                    ino,
                    offset,
                    size,
                    first_block = first_block_idx,
                    last_block = last_block_idx,
                    "read: chunked file"
                );

                // Collect blocks - check cache first, decompress missing ones
                let mut output = Vec::with_capacity((end_byte - start_byte) as usize);
                let mut cache_hits = 0u64;
                let mut cache_misses = 0u64;

                for block_idx in first_block_idx..=last_block_idx {
                    let cache_key = (composite, block_idx);

                    // Check if block needs decompression (without updating LRU order)
                    if !self.cache.contains(&cache_key) {
                        cache_misses += 1;
                        // Decompress this single block
                        let archive = match self.archives.get_archive(composite) {
                            Some(a) => a,
                            None => {
                                tracing::error!(
                                    ino,
                                    "read: archive not found for block decompression"
                                );
                                reply.error(ENOENT);
                                return;
                            }
                        };
                        match archive
                            .reader
                            .decompress_chunked_block(f, local_index, block_idx)
                        {
                            Ok(data) => {
                                self.cache.insert(cache_key, data);
                            }
                            Err(e) => {
                                tracing::error!(
                                    ino,
                                    block = block_idx,
                                    error = %e,
                                    "read: block decompression failed"
                                );
                                reply.error(ENOENT);
                                return;
                            }
                        }
                    } else {
                        cache_hits += 1;
                    }

                    // Get from cache (updates LRU order) and slice directly - no clone needed
                    let block_data = self
                        .cache
                        .get(&cache_key)
                        .expect("block should be in cache");

                    // Calculate the slice of this block we need
                    let block_start_byte = block_idx * block_size;

                    // Offset within this block where our data starts
                    let slice_start = if block_idx == first_block_idx {
                        (start_byte - block_start_byte) as usize
                    } else {
                        0
                    };

                    // Offset within this block where our data ends
                    let slice_end = if block_idx == last_block_idx {
                        ((end_byte - block_start_byte) as usize).min(block_data.len())
                    } else {
                        block_data.len()
                    };

                    output.extend_from_slice(&block_data[slice_start..slice_end]);
                }

                tracing::debug!(
                    ino,
                    cache_hits,
                    cache_misses,
                    output_size = output.len(),
                    "read: chunked complete"
                );
                reply.data(&output);
            }
            _ => {
                tracing::warn!(ino, "read: not a file");
                reply.error(ENOENT);
            }
        }
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        // Don't evict cache on file close - let LRU naturally handle eviction.
        // This allows repeated reads of the same file to benefit from caching.
        tracing::trace!(ino, "release");
        reply.ok();
    }

    fn getattr(&mut self, _req: &Request, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        tracing::trace!(ino, "getattr");
        match ino_to_composite(ino) {
            Some(composite) => match self.archives.get_record(composite) {
                Some(record) => {
                    if let Some(meta) = self.archives.get_metadata(composite) {
                        let file_attr = record.fuse_file_attr(meta, composite);
                        reply.attr(&TTL, &file_attr);
                    } else {
                        reply.error(ENOENT);
                    }
                }
                None => {
                    tracing::warn!(ino, "getattr: record not found");
                    reply.error(ENOENT);
                }
            },
            None => {
                // Root inode
                if let Some(meta) = self.archives.first_metadata() {
                    reply.attr(&TTL, &root_dir_attr(meta));
                } else {
                    reply.error(ENOENT);
                }
            }
        }
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let children = match ino_to_composite(ino) {
            Some(composite) => self.archives.children(composite),
            None => self.archives.root_children(),
        };

        tracing::debug!(ino, offset, entries = children.len(), "readdir");

        for (i, (composite, record)) in children.iter().enumerate().skip(offset as usize) {
            let is_full = reply.add(
                composite_to_ino(*composite),
                i as i64 + 1,
                record.fuse_file_type(),
                record.name(),
            );
            if is_full {
                reply.ok();
                return;
            }
        }

        reply.ok();
    }

    fn readdirplus(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectoryPlus,
    ) {
        let children = match ino_to_composite(ino) {
            Some(composite) => self.archives.children(composite),
            None => self.archives.root_children(),
        };

        tracing::debug!(ino, offset, entries = children.len(), "readdirplus");

        for (i, &(composite, record)) in children.iter().enumerate().skip(offset as usize) {
            if let Some(meta) = self.archives.get_metadata(composite) {
                let attr = record.fuse_file_attr(meta, composite);
                let is_full = reply.add(
                    composite_to_ino(composite),
                    i as i64 + 1,
                    record.name(),
                    &TTL,
                    &attr,
                    0, // generation
                );
                if is_full {
                    reply.ok();
                    return;
                }
            }
        }

        reply.ok();
    }

    fn readlink(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyData) {
        tracing::trace!(ino, "readlink");
        let composite = match ino_to_composite(ino) {
            Some(v) => v,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        let record = match self.archives.get_record(composite) {
            Some(r) => r,
            None => {
                tracing::warn!(ino, "readlink: record not found");
                reply.error(ENOENT);
                return;
            }
        };

        match record {
            box_format::Record::Link(link) => {
                // Internal link - compute relative path from link location to target
                if let Some(meta) = self.archives.get_metadata(composite) {
                    // Get the link's own RecordIndex from composite
                    let (_, local_idx) = MultiArchive::unpack_index(composite);
                    let link_index = match RecordIndex::try_new(local_idx) {
                        Some(idx) => idx,
                        None => {
                            reply.error(ENOENT);
                            return;
                        }
                    };

                    // Get both paths
                    let link_path = meta.path_for_index(link_index);
                    let target_path = meta.path_for_index(link.target);

                    match (link_path, target_path) {
                        (Some(link_p), Some(target_p)) => {
                            // Compute relative path from link's parent directory to target
                            let link_parent =
                                link_p.parent().map(|p| p.to_path_buf()).unwrap_or_default();
                            let target_path_buf = target_p.to_path_buf();
                            let relative = pathdiff::diff_paths(&target_path_buf, &link_parent)
                                .unwrap_or(target_path_buf);
                            reply.data(relative.to_string_lossy().as_bytes());
                        }
                        _ => {
                            tracing::warn!(ino, "readlink: could not resolve internal link paths");
                            reply.error(ENOENT);
                        }
                    }
                } else {
                    tracing::warn!(ino, "readlink: archive not found");
                    reply.error(ENOENT);
                }
            }
            box_format::Record::ExternalLink(link) => {
                reply.data(link.target.as_bytes());
            }
            _ => {
                tracing::warn!(ino, "readlink: not a link");
                reply.error(ENOENT);
            }
        }
    }

    fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: ReplyStatfs) {
        // Count records and estimate total size across all archives
        let total_size = self.archives.total_size();
        let file_count = self.archives.record_count();
        let blocks = total_size.div_ceil(BLOCK_SIZE as u64);

        tracing::debug!(blocks, file_count, total_size, "statfs");

        reply.statfs(
            blocks,     // blocks: total blocks
            0,          // bfree: free blocks (read-only)
            0,          // bavail: available blocks (read-only)
            file_count, // files: total inodes
            0,          // ffree: free inodes (read-only)
            BLOCK_SIZE, // bsize: block size
            255,        // namelen: max filename length
            BLOCK_SIZE, // frsize: fragment size
        );
    }

    fn getxattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        name: &OsStr,
        size: u32,
        reply: ReplyXattr,
    ) {
        let name = match name.to_str() {
            Some(n) => n,
            None => {
                reply.error(ENODATA);
                return;
            }
        };

        tracing::trace!(ino, name, "getxattr");

        // Convert xattr name to box attribute name
        let box_attr_name = format!("{}{}", attrs::LINUX_XATTR_PREFIX, name);

        // Get the attribute value
        let value = match ino_to_composite(ino) {
            Some(composite) => {
                let record = match self.archives.get_record(composite) {
                    Some(r) => r,
                    None => {
                        reply.error(ENOENT);
                        return;
                    }
                };
                let meta = match self.archives.get_metadata(composite) {
                    Some(m) => m,
                    None => {
                        reply.error(ENOENT);
                        return;
                    }
                };
                record.attr(meta, &box_attr_name)
            }
            None => {
                // Root inode - check archive-level attributes from first archive
                self.archives
                    .first_metadata()
                    .and_then(|m| m.file_attr(&box_attr_name))
            }
        };

        match value {
            Some(data) => {
                if size == 0 {
                    // Return required size
                    reply.size(data.len() as u32);
                } else if data.len() > size as usize {
                    reply.error(ERANGE);
                } else {
                    reply.data(data);
                }
            }
            None => reply.error(ENODATA),
        }
    }

    fn listxattr(&mut self, _req: &Request<'_>, ino: u64, size: u32, reply: ReplyXattr) {
        tracing::trace!(ino, "listxattr");
        // Collect all xattr names for this inode
        let mut xattr_list = Vec::new();

        let add_xattrs = |attrs_iter: &mut dyn Iterator<Item = (&str, &[u8])>,
                          xattr_list: &mut Vec<u8>| {
            for (key, _value) in attrs_iter {
                if let Some(xattr_name) = key.strip_prefix(attrs::LINUX_XATTR_PREFIX) {
                    // Add null-terminated xattr name
                    xattr_list.extend_from_slice(xattr_name.as_bytes());
                    xattr_list.push(0);
                }
            }
        };

        match ino_to_composite(ino) {
            Some(composite) => {
                let record = match self.archives.get_record(composite) {
                    Some(r) => r,
                    None => {
                        reply.error(ENOENT);
                        return;
                    }
                };
                let meta = match self.archives.get_metadata(composite) {
                    Some(m) => m,
                    None => {
                        reply.error(ENOENT);
                        return;
                    }
                };
                let mut iter = record.attrs_iter(meta);
                add_xattrs(&mut iter, &mut xattr_list);
            }
            None => {
                // Root inode - list archive-level xattrs from first archive
                if let Some(meta) = self.archives.first_metadata() {
                    for key in meta.attr_keys() {
                        if let Some(xattr_name) = key.strip_prefix(attrs::LINUX_XATTR_PREFIX) {
                            xattr_list.extend_from_slice(xattr_name.as_bytes());
                            xattr_list.push(0);
                        }
                    }
                }
            }
        }

        if size == 0 {
            reply.size(xattr_list.len() as u32);
        } else if xattr_list.len() > size as usize {
            reply.error(ERANGE);
        } else {
            reply.data(&xattr_list);
        }
    }

    fn access(&mut self, req: &Request<'_>, ino: u64, mask: i32, reply: ReplyEmpty) {
        tracing::trace!(ino, mask, "access");
        // F_OK (existence check) is always successful if we got here
        if mask == libc::F_OK {
            reply.ok();
            return;
        }

        let (mode, file_uid, file_gid) = match ino_to_composite(ino) {
            Some(composite) => {
                let record = match self.archives.get_record(composite) {
                    Some(r) => r,
                    None => {
                        reply.error(ENOENT);
                        return;
                    }
                };
                let meta = match self.archives.get_metadata(composite) {
                    Some(m) => m,
                    None => {
                        reply.error(ENOENT);
                        return;
                    }
                };
                let mode = record.perm(meta) as u32;
                let uid = record.uid(meta);
                let gid = record.gid(meta);
                (mode, uid, gid)
            }
            None => {
                // Root directory
                if let Some(meta) = self.archives.first_metadata() {
                    let attr = root_dir_attr(meta);
                    (attr.perm as u32, attr.uid, attr.gid)
                } else {
                    reply.error(ENOENT);
                    return;
                }
            }
        };

        let req_uid = req.uid();
        let req_gid = req.gid();

        // Root can access anything
        if req_uid == 0 {
            reply.ok();
            return;
        }

        // Determine which permission bits to check
        let perm_bits = if req_uid == file_uid {
            (mode >> 6) & 0o7 // Owner bits
        } else if req_gid == file_gid {
            (mode >> 3) & 0o7 // Group bits
        } else {
            mode & 0o7 // Other bits
        };

        // Check requested permissions (read-only filesystem, so write always fails)
        let mut allowed = true;

        if (mask & libc::R_OK) != 0 && (perm_bits & 0o4) == 0 {
            allowed = false;
        }
        if (mask & libc::W_OK) != 0 {
            // Read-only filesystem
            allowed = false;
        }
        if (mask & libc::X_OK) != 0 && (perm_bits & 0o1) == 0 {
            allowed = false;
        }

        if allowed {
            reply.ok();
        } else {
            reply.error(EACCES);
        }
    }
}
