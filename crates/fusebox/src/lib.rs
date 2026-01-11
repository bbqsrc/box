use std::collections::{HashMap, VecDeque};
use std::ffi::OsStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fastvint::ReadVintExt;
use fuser::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyDirectoryPlus,
    ReplyEmpty, ReplyEntry, ReplyStatfs, ReplyXattr, Request,
};
use libc::{EACCES, ENODATA, ENOENT, ERANGE};

use box_format::sync::BoxReader;
use box_format::{BOX_EPOCH_UNIX, BoxMetadata, Record, RecordIndex};

/// LRU cache with size limit for decompressed file contents
pub struct LruCache {
    entries: HashMap<RecordIndex, Vec<u8>>,
    order: VecDeque<RecordIndex>,
    current_size: usize,
    max_size: usize,
}

impl LruCache {
    pub fn new(max_size_mb: usize) -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            current_size: 0,
            max_size: max_size_mb * 1024 * 1024,
        }
    }

    fn get(&mut self, key: &RecordIndex) -> Option<&[u8]> {
        if self.entries.contains_key(key) {
            // Move to front (most recently used)
            self.order.retain(|k| k != key);
            self.order.push_front(*key);
            self.entries.get(key).map(|v| v.as_slice())
        } else {
            None
        }
    }

    fn insert(&mut self, key: RecordIndex, value: Vec<u8>) {
        let size = value.len();

        // If this single item is larger than max cache, don't cache it
        if size > self.max_size {
            return;
        }

        // Evict oldest entries until we have room
        while self.current_size + size > self.max_size && !self.order.is_empty() {
            if let Some(oldest) = self.order.pop_back()
                && let Some(data) = self.entries.remove(&oldest)
            {
                self.current_size -= data.len();
            }
        }

        // Remove existing entry if present
        if let Some(old) = self.entries.remove(&key) {
            self.current_size -= old.len();
            self.order.retain(|k| k != &key);
        }

        self.entries.insert(key, value);
        self.order.push_front(key);
        self.current_size += size;
    }

    fn remove(&mut self, key: &RecordIndex) {
        if let Some(data) = self.entries.remove(key) {
            self.current_size -= data.len();
            self.order.retain(|k| k != key);
        }
    }
}

pub struct BoxFs {
    reader: BoxReader,
    cache: LruCache,
}

impl BoxFs {
    pub fn new(reader: BoxReader, cache: LruCache) -> Self {
        Self { reader, cache }
    }
}

const TTL: Duration = Duration::from_secs(1);
const XATTR_PREFIX: &str = "linux.xattr.";
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
    if let Some(bytes) = meta.file_attr("unix.uid") {
        let (uid, len) = fastvint::decode_vu32_slice(bytes);
        if len > 0 {
            return uid;
        }
    }
    unsafe { libc::getuid() }
}

fn archive_gid(meta: &BoxMetadata) -> u32 {
    if let Some(bytes) = meta.file_attr("unix.gid") {
        let (gid, len) = fastvint::decode_vu32_slice(bytes);
        if len > 0 {
            return gid;
        }
    }
    unsafe { libc::getgid() }
}

fn root_dir_attr(meta: &BoxMetadata) -> FileAttr {
    let ctime = parse_archive_time(meta, "created").unwrap_or(UNIX_EPOCH);

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
    fn fuse_file_attr(&self, meta: &BoxMetadata, index: RecordIndex) -> FileAttr;

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

    fn fuse_file_attr(&self, meta: &BoxMetadata, index: RecordIndex) -> FileAttr {
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

        let perm = self.perm(meta) & 0o0555;
        let ctime = self.ctime(meta);
        let mtime = self.mtime(meta);
        let atime = self.atime(meta);

        FileAttr {
            ino: index.get() + 1,
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
        match self.attr(meta, "unix.mode") {
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
        if let Some(bytes) = self.attr(meta, "unix.uid") {
            let (uid, len) = fastvint::decode_vu32_slice(bytes);
            if len > 0 {
                return uid;
            }
        }
        // Try archive-level default
        if let Some(bytes) = meta.file_attr("unix.uid") {
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
        if let Some(bytes) = self.attr(meta, "unix.gid") {
            let (gid, len) = fastvint::decode_vu32_slice(bytes);
            if len > 0 {
                return gid;
            }
        }
        // Try archive-level default
        if let Some(bytes) = meta.file_attr("unix.gid") {
            let (gid, len) = fastvint::decode_vu32_slice(bytes);
            if len > 0 {
                return gid;
            }
        }
        // Fall back to current group
        unsafe { libc::getgid() }
    }

    fn ctime(&self, meta: &BoxMetadata) -> SystemTime {
        self.parse_time_attr(meta, "created").unwrap_or(UNIX_EPOCH)
    }

    fn mtime(&self, meta: &BoxMetadata) -> SystemTime {
        self.parse_time_attr(meta, "modified")
            .or_else(|| self.parse_time_attr(meta, "created"))
            .unwrap_or(UNIX_EPOCH)
    }

    fn atime(&self, meta: &BoxMetadata) -> SystemTime {
        self.parse_time_attr(meta, "accessed")
            .or_else(|| self.parse_time_attr(meta, "modified"))
            .or_else(|| self.parse_time_attr(meta, "created"))
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

fn record_index(parent: u64) -> Option<RecordIndex> {
    RecordIndex::new(parent - 1).ok()
}

impl Filesystem for BoxFs {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name = match name.to_str() {
            Some(v) => v,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        let records = match record_index(parent) {
            Some(index) => self.reader.metadata().dir_records_by_index(index),
            None => self.reader.metadata().root_records(),
        };

        match records.iter().find(|(_, record)| record.name() == name) {
            Some((index, record)) => {
                reply.entry(
                    &TTL,
                    &record.fuse_file_attr(self.reader.metadata(), *index),
                    0,
                );
            }
            None => {
                reply.error(ENOENT);
            }
        }
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
        let index = match record_index(ino) {
            Some(v) => v,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        let offset = offset as usize;
        let size = size as usize;

        if let Some(cached) = self.cache.get(&index) {
            let end = (offset + size).min(cached.len());
            let start = offset.min(end);
            reply.data(&cached[start..end]);
            return;
        }

        let record = match self.reader.metadata().record(index) {
            Some(r) => r,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        let mut buf = Vec::new();
        let result = match record {
            Record::File(f) => self.reader.decompress(f, &mut buf),
            Record::ChunkedFile(f) => self.reader.decompress_chunked(f, index, &mut buf),
            _ => {
                reply.error(ENOENT);
                return;
            }
        };

        match result {
            Ok(_) => {
                let end = (offset + size).min(buf.len());
                let start = offset.min(end);
                reply.data(&buf[start..end]);
                self.cache.insert(index, buf);
            }
            Err(_) => {
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
        if let Some(index) = record_index(ino) {
            self.cache.remove(&index);
            reply.ok();
        } else {
            reply.error(ENOENT);
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        match record_index(ino) {
            Some(index) => match self.reader.metadata().record(index) {
                Some(record) => {
                    let file_attr = record.fuse_file_attr(self.reader.metadata(), index);
                    reply.attr(&TTL, &file_attr);
                }
                None => {
                    reply.error(ENOENT);
                }
            },
            None => {
                reply.attr(&TTL, &root_dir_attr(self.reader.metadata()));
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
        let records = match record_index(ino) {
            Some(index) => self.reader.metadata().dir_records_by_index(index),
            None => self.reader.metadata().root_records(),
        };

        for (i, (index, record)) in records.iter().enumerate().skip(offset as usize) {
            tracing::trace!("{:?}", record);

            let is_full = reply.add(
                index.get() + 1,
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
        let records = match record_index(ino) {
            Some(index) => self.reader.metadata().dir_records_by_index(index),
            None => self.reader.metadata().root_records(),
        };

        for (i, (index, record)) in records.iter().enumerate().skip(offset as usize) {
            let attr = record.fuse_file_attr(self.reader.metadata(), *index);
            let is_full = reply.add(
                index.get() + 1,
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

        reply.ok();
    }

    fn readlink(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyData) {
        let index = match record_index(ino) {
            Some(v) => v,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        let record = match self.reader.metadata().record(index) {
            Some(r) => r,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        match record {
            box_format::Record::Link(link) => {
                // Internal link - resolve to get the target path
                match self.reader.metadata().path_for_index(link.target) {
                    Some(path) => reply.data(path.as_ref()),
                    None => reply.error(ENOENT),
                }
            }
            box_format::Record::ExternalLink(link) => {
                reply.data(link.target.as_bytes());
            }
            _ => reply.error(ENOENT),
        }
    }

    fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: ReplyStatfs) {
        // Count records and estimate total size
        let mut total_size: u64 = 0;
        let mut file_count: u64 = 0;

        for item in self.reader.metadata().iter() {
            file_count += 1;
            if let Some(file) = item.record.as_file() {
                total_size += file.decompressed_length;
            } else if let Some(file) = item.record.as_chunked_file() {
                total_size += file.decompressed_length;
            }
        }

        let blocks = total_size.div_ceil(BLOCK_SIZE as u64);

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

        // Convert xattr name to box attribute name
        let box_attr_name = format!("{}{}", XATTR_PREFIX, name);

        // Get the attribute value
        let value = match record_index(ino) {
            Some(index) => match self.reader.metadata().record(index) {
                Some(record) => record.attr(self.reader.metadata(), &box_attr_name),
                None => {
                    reply.error(ENOENT);
                    return;
                }
            },
            None => {
                // Root inode - check archive-level attributes
                self.reader.metadata().file_attr(&box_attr_name)
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
        // Collect all xattr names for this inode
        let mut xattr_list = Vec::new();

        let add_xattrs = |attrs_iter: &mut dyn Iterator<Item = (&str, &[u8])>,
                          xattr_list: &mut Vec<u8>| {
            for (key, _value) in attrs_iter {
                if let Some(xattr_name) = key.strip_prefix(XATTR_PREFIX) {
                    // Add null-terminated xattr name
                    xattr_list.extend_from_slice(xattr_name.as_bytes());
                    xattr_list.push(0);
                }
            }
        };

        match record_index(ino) {
            Some(index) => match self.reader.metadata().record(index) {
                Some(record) => {
                    let mut iter = record.attrs_iter(self.reader.metadata());
                    add_xattrs(&mut iter, &mut xattr_list);
                }
                None => {
                    reply.error(ENOENT);
                    return;
                }
            },
            None => {
                // Root inode - list archive-level xattrs
                for key in self.reader.metadata().attr_keys() {
                    if let Some(xattr_name) = key.strip_prefix(XATTR_PREFIX) {
                        xattr_list.extend_from_slice(xattr_name.as_bytes());
                        xattr_list.push(0);
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
        // F_OK (existence check) is always successful if we got here
        if mask == libc::F_OK {
            reply.ok();
            return;
        }

        let (mode, file_uid, file_gid) = match record_index(ino) {
            Some(index) => match self.reader.metadata().record(index) {
                Some(record) => {
                    let mode = record.perm(self.reader.metadata()) as u32;
                    let uid = record.uid(self.reader.metadata());
                    let gid = record.gid(self.reader.metadata());
                    (mode, uid, gid)
                }
                None => {
                    reply.error(ENOENT);
                    return;
                }
            },
            None => {
                // Root directory
                let attr = root_dir_attr(self.reader.metadata());
                (attr.perm as u32, attr.uid, attr.gid)
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
