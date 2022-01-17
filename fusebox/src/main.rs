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

use box_format::{BoxFileReader, BoxMetadata, Inode};

struct BoxFs(BoxFileReader, HashMap<Inode, Vec<u8>>);

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
    fn fuse_file_attr(&self, meta: &BoxMetadata, inode: Inode) -> FileAttr;

    fn perm(&self, meta: &BoxMetadata) -> u16;
    fn ctime(&self, meta: &BoxMetadata) -> SystemTime;
}

impl RecordExt for box_format::Record {
    fn fuse_file_type(&self) -> FileType {
        use box_format::Record::*;

        match self {
            File(_) => FileType::RegularFile,
            Directory(_) => FileType::Directory,
            Link(_) => FileType::Symlink,
        }
    }

    fn fuse_file_attr(&self, meta: &BoxMetadata, inode: Inode) -> FileAttr {
        let kind = self.fuse_file_type();
        let nlink = 1;
        let blocks = if kind == FileType::RegularFile { 1 } else { 0 };

        use box_format::Record::*;
        let size = match self {
            File(record) => record.decompressed_length,
            Directory(record) => record.inodes.len() as u64,
            Link(_) => 1,
        };

        let perm = self.perm(meta) & 0o0555;
        let ctime = self.ctime(meta);

        FileAttr {
            ino: inode.get() + 1,
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

fn inode(parent: u64) -> Option<Inode> {
    Inode::new(parent - 1).ok()
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

        let records = match inode(parent) {
            Some(inode) => match self
                .0
                .metadata()
                .record(inode)
                .and_then(|x| x.as_directory())
                .map(|x| self.0.metadata().records(x))
            {
                Some(v) => v,
                None => {
                    reply.error(ENOENT);
                    return;
                }
            },
            None => self.0.metadata().root_records(),
        };

        match records
            .iter()
            .find(|(_inode, record)| record.name() == name)
        {
            Some((inode, record)) => {
                reply.entry(&TTL, &record.fuse_file_attr(self.0.metadata(), *inode), 0);
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
        let inode = match inode(ino) {
            Some(v) => v,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        let offset = offset as usize;
        let size = size as usize;

        if let Some(cached) = self.1.get(&inode) {
            reply.data(&(&*cached)[offset..size + offset]);
            return;
        }

        let record = match self.0.metadata().record(inode).and_then(|x| x.as_file()) {
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
                self.1.insert(inode, buf);
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
        if let Some(inode) = inode(ino) {
            self.1.remove(&inode);
            reply.ok();
        } else {
            reply.error(ENOENT);
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        match inode(ino) {
            Some(inode) => match self.0.metadata().record(inode) {
                Some(record) => {
                    let file_attr = record.fuse_file_attr(self.0.metadata(), inode);
                    reply.attr(&TTL, &file_attr);
                }
                None => {
                    reply.error(ENOENT);
                }
            },
            None => {
                reply.attr(&TTL, &root_dir_attr(self.0.metadata()));
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
        let records = match inode(ino) {
            Some(inode) => match self
                .0
                .metadata()
                .record(inode)
                .and_then(|x| x.as_directory())
                .map(|x| self.0.metadata().records(x))
            {
                Some(v) => v,
                None => {
                    reply.error(ENOENT);
                    return;
                }
            },
            None => self.0.metadata().root_records(),
        };

        for (i, (inode, record)) in records.iter().enumerate().skip(offset as usize) {
            log::info!("{:?}", record);

            let is_full = reply.add(
                inode.get() + 1,
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
    let bf = BoxFileReader::open(opts.box_file).unwrap();
    log::info!("{:?}", &bf);
    let mount_opts = &[
        MountOption::RO,
        MountOption::FSName(bf.path().to_string_lossy().to_string()),
    ];
    fuser::mount2(BoxFs(bf, HashMap::new()), &opts.mountpoint, mount_opts).unwrap();
}
