use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::PathBuf;
use std::time::{Duration, UNIX_EPOCH};

use fuse::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    Request,
};
use libc::ENOENT;
use structopt::StructOpt;

use box_format::BoxFile;

struct BoxFs(BoxFile, HashMap<u64, Vec<u8>>);

const TTL: Duration = Duration::from_secs(1);

const ROOT_DIR_ATTR: FileAttr = FileAttr {
    ino: 1,
    size: 0,
    blocks: 0,
    atime: UNIX_EPOCH, // 1970-01-01 00:00:00
    mtime: UNIX_EPOCH,
    ctime: UNIX_EPOCH,
    crtime: UNIX_EPOCH,
    kind: FileType::Directory,
    perm: 0o755,
    nlink: 2,
    uid: 501,
    gid: 20,
    rdev: 0,
    flags: 0,
};

trait RecordExt {
    fn fuse_file_type(&self) -> FileType;
    fn fuse_file_attr(&self, index: u64) -> FileAttr;
}

impl RecordExt for box_format::Record {
    fn fuse_file_type(&self) -> FileType {
        use box_format::Record::*;

        match self {
            File(_) => FileType::RegularFile,
            Directory(_) => FileType::Directory,
        }
    }

    fn fuse_file_attr(&self, index: u64) -> FileAttr {
        let kind = self.fuse_file_type();
        let nlink = if kind == FileType::RegularFile { 1 } else { 2 };
        let blocks = if kind == FileType::RegularFile { 1 } else { 0 };

        use box_format::Record::*;
        let size = match self {
            File(record) => record.decompressed_length,
            Directory(_) => 0,
        };

        let perm = match self {
            File(_) => 0o644,
            Directory(_) => 0o755,
        };

        FileAttr {
            ino: index + 2,
            size,
            blocks,
            atime: UNIX_EPOCH,
            mtime: UNIX_EPOCH,
            ctime: UNIX_EPOCH,
            crtime: UNIX_EPOCH,
            kind: self.fuse_file_type(),
            perm,
            nlink,
            uid: 501,
            gid: 20,
            rdev: 0,
            flags: 0,
        }
    }
}

impl BoxFs {
    fn record(&self, ino: u64) -> Option<&box_format::Record> {
        self.0.metadata().records().get((ino - 2) as usize)
    }
}

impl Filesystem for BoxFs {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        log::debug!("HELLO");

        let records = self.0.metadata().records();

        if parent == 1 {
            // ROOT
            for (index, entry) in records.iter().enumerate() {
                log::debug!(
                    "{} {} {} -> {:?}",
                    index,
                    entry.name(),
                    entry.path().levels(),
                    &name
                );
                if entry.path().levels() == 0 && entry.name() == name.to_str().unwrap() {
                    reply.entry(&TTL, &entry.fuse_file_attr(index as u64), 0);
                    return;
                }
            }
            log::debug!("END LOOP");
        } else {
            let index = (parent - 2) as usize;
            log::debug!("Getting index {}", index);
            let record = match records.get(index) {
                Some(v) => v,
                None => {
                    reply.error(ENOENT);
                    return;
                }
            };

            let candidate = record.path().join(name.to_str().unwrap()).unwrap();
            for (i, record) in records.iter().enumerate() {
                if record.path() == &candidate {
                    reply.entry(&TTL, &record.fuse_file_attr(i as u64), 0);
                    return;
                }
            }
        }

        reply.error(ENOENT);
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        reply: ReplyData,
    ) {
        let offset = offset as usize;
        let size = size as usize;
        if let Some(cached) = self.1.get(&ino) {
            reply.data(&(&*cached)[offset..size + offset]);
            return;
        }

        let record = match self.record(ino).and_then(|x| x.as_file()) {
            Some(v) => v,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        let mut buf = Vec::with_capacity(size as usize);
        match self.0.decompress(record, &mut buf) {
            Ok(_) => {
                reply.data(&(&*buf)[offset..size + offset]);
                self.1.insert(ino, buf);
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
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        self.1.remove(&ino);
        reply.ok();
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        if ino == 1 {
            reply.attr(&TTL, &ROOT_DIR_ATTR);
            return;
        }

        // Otherwise we're looking in the file
        let index = (ino - 2) as usize;

        let record = match self.0.metadata().records().get(index) {
            Some(v) => v,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        let file_attr = record.fuse_file_attr(index as u64);
        reply.attr(&TTL, &file_attr);
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let records = self.0.metadata().records();

        if ino == 1 {
            // Root
            for (i, entry) in records.iter().enumerate().skip(offset as usize) {
                let i = i as i64 + 2;
                if entry.path().levels() == 0 {
                    let is_full = reply.add(i as u64, i + 1, entry.fuse_file_type(), entry.name());
                    if is_full {
                        reply.ok();
                        return;
                    }
                }
            }

            reply.ok();
            return;
        }

        let index = (ino - 2) as usize;
        let record = match records.get(index).and_then(|x| x.as_directory()) {
            Some(v) => v,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        let depth = record.path.levels() + 1;
        for (i, entry) in records.iter().enumerate().skip(offset as usize) {
            let i = i as i64 + 2;
            if entry.path().levels() == depth && entry.path().starts_with(&record.path) {
                log::debug!("{} {} -> {}", depth, entry.path(), &record.path);
                let is_full = reply.add(i as u64, i + 1, entry.fuse_file_type(), entry.name());
                if is_full {
                    reply.ok();
                    return;
                }
            }
        }

        reply.ok();
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
    let bf = BoxFile::open(opts.box_file).unwrap();
    let options = ["-o", "ro", "-o", "fsname=box"]
        .iter()
        .map(|o| o.as_ref())
        .collect::<Vec<&OsStr>>();
    fuse::mount(BoxFs(bf, HashMap::new()), &opts.mountpoint, &options).unwrap();
}
