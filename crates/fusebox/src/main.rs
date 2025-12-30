use std::collections::{HashMap, VecDeque};
use std::ffi::OsStr;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clap::Parser;
use fastvint::ReadVintExt;
use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, Request,
};
use libc::ENOENT;
use tokio::runtime::Runtime;

use box_format::{BoxFileReader, BoxMetadata, RecordIndex};

/// Box epoch: 2020-01-01 00:00:00 UTC (seconds since Unix epoch)
const BOX_EPOCH_UNIX: i64 = 1577836800;

/// LRU cache with size limit for decompressed file contents
struct LruCache {
    entries: HashMap<RecordIndex, Vec<u8>>,
    order: VecDeque<RecordIndex>,
    current_size: usize,
    max_size: usize,
}

impl LruCache {
    fn new(max_size_mb: usize) -> Self {
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
            if let Some(oldest) = self.order.pop_back() {
                if let Some(data) = self.entries.remove(&oldest) {
                    self.current_size -= data.len();
                }
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

struct BoxFs {
    reader: BoxFileReader,
    cache: LruCache,
    runtime: Runtime,
}

const TTL: Duration = Duration::from_secs(1);

fn parse_archive_time(meta: &BoxMetadata, name: &str) -> Option<SystemTime> {
    let bytes = meta.file_attr(name)?;
    let mut cursor = std::io::Cursor::new(bytes.as_slice());
    let minutes = cursor.read_vi64().ok()?;
    let unix_secs = (minutes * 60 + BOX_EPOCH_UNIX) as u64;
    UNIX_EPOCH.checked_add(Duration::from_secs(unix_secs))
}

fn archive_uid(meta: &BoxMetadata) -> u32 {
    if let Some(bytes) = meta.file_attr("unix.uid") {
        let mut cursor = std::io::Cursor::new(bytes.as_slice());
        if let Ok(uid) = cursor.read_vu32() {
            return uid;
        }
    }
    unsafe { libc::getuid() }
}

fn archive_gid(meta: &BoxMetadata) -> u32 {
    if let Some(bytes) = meta.file_attr("unix.gid") {
        let mut cursor = std::io::Cursor::new(bytes.as_slice());
        if let Ok(gid) = cursor.read_vu32() {
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
            File(_) => FileType::RegularFile,
            Directory(_) => FileType::Directory,
            Link(_) => FileType::Symlink,
        }
    }

    fn fuse_file_attr(&self, meta: &BoxMetadata, index: RecordIndex) -> FileAttr {
        let kind = self.fuse_file_type();
        let nlink = 1;
        let blocks = if kind == FileType::RegularFile { 1 } else { 0 };

        use box_format::Record::*;
        let size = match self {
            File(record) => record.decompressed_length,
            Directory(_) => 4096, // Standard directory size
            Link(record) => record.target.as_ref().len() as u64,
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
                let mut cursor = std::io::Cursor::new(bytes);
                match cursor.read_vu32() {
                    Ok(mode) => (mode & 0o7777) as u16,
                    Err(_) => {
                        use box_format::Record::*;
                        match self {
                            File(_) => 0o644,
                            Directory(_) => 0o755,
                            Link(_) => 0o644,
                        }
                    }
                }
            }
            _ => {
                use box_format::Record::*;
                match self {
                    File(_) => 0o644,
                    Directory(_) => 0o755,
                    Link(_) => 0o644,
                }
            }
        }
    }

    fn uid(&self, meta: &BoxMetadata) -> u32 {
        // Try record attribute
        if let Some(bytes) = self.attr(meta, "unix.uid") {
            let mut cursor = std::io::Cursor::new(bytes);
            if let Ok(uid) = cursor.read_vu32() {
                return uid;
            }
        }
        // Try archive-level default
        if let Some(bytes) = meta.file_attr("unix.uid") {
            let mut cursor = std::io::Cursor::new(bytes);
            if let Ok(uid) = cursor.read_vu32() {
                return uid;
            }
        }
        // Fall back to current user
        unsafe { libc::getuid() }
    }

    fn gid(&self, meta: &BoxMetadata) -> u32 {
        // Try record attribute
        if let Some(bytes) = self.attr(meta, "unix.gid") {
            let mut cursor = std::io::Cursor::new(bytes);
            if let Ok(gid) = cursor.read_vu32() {
                return gid;
            }
        }
        // Try archive-level default
        if let Some(bytes) = meta.file_attr("unix.gid") {
            let mut cursor = std::io::Cursor::new(bytes);
            if let Ok(gid) = cursor.read_vu32() {
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

        let record = match self
            .reader
            .metadata()
            .record(index)
            .and_then(|x| x.as_file())
        {
            Some(v) => v,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        let mut buf = Vec::new();
        match self
            .runtime
            .block_on(self.reader.decompress(record, &mut buf))
        {
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
                return;
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

    fn readlink(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyData) {
        let index = match record_index(ino) {
            Some(v) => v,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        match self
            .reader
            .metadata()
            .record(index)
            .and_then(|r| r.as_link())
        {
            Some(link) => reply.data(link.target.as_ref()),
            None => reply.error(ENOENT),
        }
    }
}

#[derive(Debug, Parser)]
#[command(name = "fusebox", about = "Mount a .box archive as a FUSE filesystem")]
struct Options {
    /// Path to the .box archive file
    box_file: PathBuf,

    /// Mount point directory
    mountpoint: PathBuf,

    /// Maximum cache size in MB (default: 256)
    #[arg(long, default_value = "256")]
    cache_size: usize,
}

fn main() {
    tracing_subscriber::fmt::init();
    let opts = Options::parse();

    let runtime = Runtime::new().expect("Failed to create tokio runtime");
    let bf = runtime
        .block_on(BoxFileReader::open(opts.box_file))
        .unwrap();
    tracing::trace!("{:?}", &bf);

    let mount_opts = &[
        MountOption::RO,
        MountOption::FSName(bf.path().to_string_lossy().to_string()),
    ];

    let boxfs = BoxFs {
        reader: bf,
        cache: LruCache::new(opts.cache_size),
        runtime,
    };

    fuser::mount2(boxfs, &opts.mountpoint, mount_opts).unwrap();
}
