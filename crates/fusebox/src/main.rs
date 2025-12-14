use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, Request,
};
use libc::{ENOENT, ENOSYS};
use structopt::StructOpt;
use tokio::runtime::Runtime;

use box_format::{BoxFileReader, BoxMetadata, RecordIndex};

struct BoxFs {
    reader: BoxFileReader,
    cache: HashMap<RecordIndex, Vec<u8>>,
    runtime: Runtime,
}

const TTL: Duration = Duration::from_secs(1);

fn root_dir_attr(meta: &BoxMetadata) -> FileAttr {
    let ctime = match meta.file_attr("created") {
        Some(b) if b.len() == 8 => {
            let secs = u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]);
            UNIX_EPOCH
                .checked_add(Duration::from_secs(secs))
                .unwrap_or(UNIX_EPOCH)
        }
        _ => UNIX_EPOCH,
    };

    FileAttr {
        ino: 1,
        size: meta.root_records().len() as u64,
        blocks: 0,
        atime: ctime,
        mtime: ctime,
        ctime,
        crtime: ctime,
        kind: FileType::Directory,
        perm: 0o755,
        nlink: 2,
        uid: 501,
        gid: 20,
        rdev: 0,
        flags: 0,
        blksize: 0,
    }
}

trait RecordExt {
    fn fuse_file_type(&self) -> FileType;
    fn fuse_file_attr(&self, meta: &BoxMetadata, index: RecordIndex) -> FileAttr;

    fn perm(&self, meta: &BoxMetadata) -> u16;
    fn ctime(&self, meta: &BoxMetadata) -> SystemTime;
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
            Directory(record) => record.entries.len() as u64,
            Link(_) => 1,
        };

        let perm = self.perm(meta) & 0o0555;
        let ctime = self.ctime(meta);

        FileAttr {
            ino: index.get() + 1,
            size,
            blocks,
            atime: ctime,
            mtime: ctime,
            ctime,
            crtime: ctime,
            kind: self.fuse_file_type(),
            perm,
            nlink,
            uid: 501,
            gid: 20,
            rdev: 0,
            flags: 0,
            blksize: 0,
        }
    }

    fn perm(&self, meta: &BoxMetadata) -> u16 {
        match self.attr(meta, "unix.mode") {
            Some(bytes) if bytes.len() == 2 => u16::from_le_bytes([bytes[0], bytes[1]]),
            _ => {
                use box_format::Record::*;
                match self {
                    File(_) => 0o644,
                    Directory(_) => 0o755,
                    Link(_) => 0x644,
                }
            }
        }
    }

    fn ctime(&self, meta: &BoxMetadata) -> SystemTime {
        match self.attr(meta, "created") {
            Some(b) if b.len() == 8 => {
                let secs = u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]);
                UNIX_EPOCH
                    .checked_add(Duration::from_secs(secs))
                    .unwrap_or(UNIX_EPOCH)
            }
            _ => UNIX_EPOCH,
        }
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
            reply.data(&(&*cached)[offset..size + offset]);
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

        let mut buf = Vec::with_capacity(size as usize);
        match self
            .runtime
            .block_on(self.reader.decompress(record, &mut buf))
        {
            Ok(_) => {
                reply.data(&(&*buf)[offset..size + offset]);
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
            log::trace!("{:?}", record);

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

    fn readlink(&mut self, _req: &Request<'_>, _ino: u64, reply: ReplyData) {
        reply.error(ENOSYS);
    }
}

#[derive(Debug, StructOpt)]
struct Options {
    #[structopt(parse(from_os_str))]
    box_file: PathBuf,

    #[structopt(parse(from_os_str))]
    mountpoint: PathBuf,
}

fn main() {
    env_logger::init();
    let opts = Options::from_args();

    let runtime = Runtime::new().expect("Failed to create tokio runtime");
    let bf = runtime
        .block_on(BoxFileReader::open(opts.box_file))
        .unwrap();
    log::trace!("{:?}", &bf);

    let mount_opts = &[
        MountOption::RO,
        MountOption::FSName(bf.path().to_string_lossy().to_string()),
    ];

    let boxfs = BoxFs {
        reader: bf,
        cache: HashMap::new(),
        runtime,
    };

    fuser::mount2(boxfs, &opts.mountpoint, mount_opts).unwrap();
}
