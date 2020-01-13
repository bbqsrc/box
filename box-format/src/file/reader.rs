use std::fs::File;
use std::fs::OpenOptions;
use std::io::{prelude::*, BufReader, SeekFrom};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};

use comde::Decompress;
use memmap::MmapOptions;

use super::BoxMetadata;
use crate::{
    de::DeserializeOwned,
    header::BoxHeader,
    path::BoxPath,
    record::{FileRecord, LinkRecord, Record},
};

#[derive(Debug)]
pub struct BoxFileReader {
    pub(crate) file: BufReader<File>,
    pub(crate) path: PathBuf,
    pub(crate) header: BoxHeader,
    pub(crate) meta: BoxMetadata,
}

#[inline(always)]
pub(super) fn read_header<R: Read + Seek>(file: &mut R) -> std::io::Result<BoxHeader> {
    file.seek(SeekFrom::Start(0))?;
    BoxHeader::deserialize_owned(file)
}

#[inline(always)]
pub(super) fn read_trailer<R: Read + Seek>(
    file: &mut R,
    ptr: NonZeroU64,
) -> std::io::Result<BoxMetadata> {
    file.seek(SeekFrom::Start(ptr.get()))?;
    BoxMetadata::deserialize_owned(file)
}

use crate::file::Inode;
use std::collections::VecDeque;

pub struct FindRecord<'a> {
    meta: &'a BoxMetadata,
    query: VecDeque<String>,
    inodes: &'a [Inode],
}

impl<'a> FindRecord<'a> {
    pub(crate) fn new(
        meta: &'a BoxMetadata,
        query: VecDeque<String>,
        inodes: &'a [Inode],
    ) -> FindRecord<'a> {
        FindRecord {
            meta,
            query,
            inodes,
        }
    }
}

impl<'a> Iterator for FindRecord<'a> {
    type Item = Inode;

    fn next(&mut self) -> Option<Self::Item> {
        let candidate_name = match self.query.pop_front() {
            Some(v) => v,
            None => return None,
        };

        let result = self
            .inodes
            .iter()
            .map(|inode| (*inode, self.meta.record(*inode).unwrap()))
            .find(|x| x.1.name() == candidate_name);

        match result {
            Some(v) => {
                if self.query.is_empty() {
                    return Some(v.0);
                } else {
                    let mut tmp = VecDeque::new();
                    std::mem::swap(&mut self.query, &mut tmp);
                    FindRecord::new(self.meta, tmp, self.inodes).next()
                }
            }
            None => None,
        }
    }
}

pub struct Records<'a> {
    meta: &'a BoxMetadata,
    inodes: &'a [Inode],
    base_path: Option<BoxPath>,
    cur_inode: usize,
    cur_dir: Option<Box<Records<'a>>>,
}

impl<'a> Records<'a> {
    pub(crate) fn new(
        meta: &'a BoxMetadata,
        inodes: &'a [Inode],
        base_path: Option<BoxPath>,
    ) -> Records<'a> {
        Records {
            meta,
            inodes,
            base_path,
            cur_inode: 0,
            cur_dir: None,
        }
    }
}

#[non_exhaustive]
pub struct RecordsItem<'a> {
    pub(crate) inode: Inode,
    pub path: BoxPath,
    pub record: &'a Record,
}

impl<'a> Iterator for Records<'a> {
    type Item = RecordsItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        // If iterating a child iterator, do it here.
        if let Some(dir) = self.cur_dir.as_mut() {
            if let Some(record) = dir.next() {
                return Some(record);
            }

            // If nothing left, clear it here.
            self.cur_dir = None;
        }
        let inode = match self.inodes.get(self.cur_inode) {
            Some(i) => *i,
            None => return None,
        };

        let record = match self.meta.record(inode) {
            Some(v) => v,
            None => return None,
        };

        let base_path = self
            .base_path
            .as_ref()
            .map(|x| x.join(&record.name()).unwrap())
            .or_else(|| BoxPath::new(&record.name()).ok())
            .unwrap();

        if let Record::Directory(record) = record {
            self.cur_dir = Some(Box::new(Records::new(
                self.meta,
                &*record.inodes,
                Some(base_path.clone()),
            )));
        }

        self.cur_inode += 1;
        Some(RecordsItem {
            inode,
            path: base_path,
            record,
        })
    }
}

impl BoxFileReader {
    /// This will open an existing `.box` file for reading and writing, and error if the file is not valid.
    pub fn open<P: AsRef<Path>>(path: P) -> std::io::Result<BoxFileReader> {
        OpenOptions::new()
            .read(true)
            .open(path.as_ref())
            .map(|mut file| {
                // Try to load the header so we can easily rewrite it when saving.
                // If header is invalid, we're not even loading a .box file.
                let (header, meta) = {
                    let mut reader = BufReader::new(&mut file);
                    let header = read_header(&mut reader)?;
                    let ptr = header.trailer.ok_or_else(|| {
                        std::io::Error::new(std::io::ErrorKind::Other, "no trailer found")
                    })?;
                    let meta = read_trailer(&mut reader, ptr)?;
                    (header, meta)
                };

                let f = BoxFileReader {
                    file: BufReader::new(file),
                    path: path.as_ref().to_path_buf().canonicalize()?,
                    header,
                    meta,
                };

                Ok(f)
            })?
    }

    #[inline(always)]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[inline(always)]
    pub fn alignment(&self) -> u64 {
        self.header.alignment
    }

    #[inline(always)]
    pub fn version(&self) -> u32 {
        self.header.version
    }

    #[inline(always)]
    pub fn metadata(&self) -> &BoxMetadata {
        &self.meta
    }

    #[inline(always)]
    pub fn decompress_value<V: Decompress>(&self, record: &FileRecord) -> std::io::Result<V> {
        let mmap = unsafe { self.memory_map(record)? };
        record.compression.decompress(std::io::Cursor::new(mmap))
    }

    #[inline(always)]
    pub fn decompress<W: Write>(&self, record: &FileRecord, dest: W) -> std::io::Result<()> {
        let mmap = unsafe { self.memory_map(record)? };
        record
            .compression
            .decompress_write(std::io::Cursor::new(mmap), dest)
    }

    #[inline(always)]
    pub fn iter(&self) -> Records {
        Records::new(self.metadata(), &*self.metadata().root, None)
    }

    #[inline(always)]
    pub fn extract<P: AsRef<Path>>(&self, path: &BoxPath, output_path: P) -> std::io::Result<()> {
        let output_path = output_path.as_ref().canonicalize()?;
        let record = self.record(path).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Path not found in archive: {}", path),
            )
        })?;
        self.extract_inner(path, record, &output_path)
    }

    #[inline(always)]
    pub fn extract_all<P: AsRef<Path>>(&self, output_path: P) -> std::io::Result<()> {
        let output_path = output_path.as_ref().canonicalize()?;
        self.iter()
            .map(|RecordsItem { path, record, .. }| self.extract_inner(&path, record, &output_path))
            .collect()
    }

    #[inline(always)]
    fn record(&self, path: &BoxPath) -> Option<&Record> {
        let path_chunks = path.iter().map(str::to_string).collect();
        let mut finder = FindRecord::new(self.metadata(), path_chunks, &*self.metadata().root);
        finder.next().and_then(|x| self.meta.record(x))
    }

    #[inline(always)]
    pub fn attr<S: AsRef<str>>(&self, path: &BoxPath, key: S) -> Option<&Vec<u8>> {
        let key = self.meta.attr_key(key.as_ref())?;

        if let Some(record) = self.record(path) {
            record.attrs().get(&key)
        } else {
            None
        }
    }

    #[inline(always)]
    pub fn resolve_link(&self, link: &LinkRecord) -> std::io::Result<RecordsItem> {
        let result = FindRecord::new(
            self.metadata(),
            link.target.iter().map(str::to_string).collect(),
            &*self.metadata().root,
        )
        .next();

        match result {
            Some(inode) => Ok(RecordsItem {
                inode,
                path: link.target.to_owned(),
                record: self.meta.record(inode).unwrap(),
            }),
            None => Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("No inode for link target: {}", link.target),
            )),
        }
    }

    #[inline(always)]
    pub fn file_attr<S: AsRef<str>>(&self, key: S) -> Option<&Vec<u8>> {
        let key = self.metadata().attr_key(key.as_ref())?;

        self.meta.attrs.get(&key)
    }

    #[inline(always)]
    pub fn read_bytes(&self, record: &FileRecord) -> std::io::Result<std::io::Take<File>> {
        let mut file = OpenOptions::new().read(true).open(&self.path)?;

        file.seek(std::io::SeekFrom::Start(record.data.get()))?;
        Ok(file.take(record.length))
    }

    #[inline(always)]
    pub unsafe fn memory_map(&self, record: &FileRecord) -> std::io::Result<memmap::Mmap> {
        MmapOptions::new()
            .offset(record.data.get())
            .len(record.length as usize)
            .map(self.file.get_ref())
    }

    #[inline(always)]
    fn extract_inner(
        &self,
        path: &BoxPath,
        record: &Record,
        output_path: &Path,
    ) -> std::io::Result<()> {
        println!("{} -> {}: {:?}", path, output_path.display(), record);
        match record {
            Record::File(file) => {
                let out_file = std::fs::File::create(output_path.join(path.to_path_buf())).unwrap();
                let out_file = std::io::BufWriter::new(out_file);
                self.decompress(&file, out_file)
            }
            Record::Directory(dir) => std::fs::create_dir_all(output_path.join(path.to_path_buf())),
            #[cfg(unix)]
            Record::Link(link) => {
                let link_target = self.resolve_link(link)?;

                let source = output_path.join(path.to_path_buf());
                let destination = output_path.join(link_target.path.to_path_buf());

                std::os::unix::fs::symlink(&source, &destination)
            }
            #[cfg(windows)]
            Record::Link(link) => {
                let link_target = self.resolve_link(link)?;

                let source = output_path.join(path.to_path_buf());
                let destination = output_path.join(link_target.path.to_path_buf());

                if link_target.record.as_directory().is_some() {
                    std::os::windows::fs::symlink_dir(&source, &destination)
                } else {
                    std::os::windows::fs::symlink_file(&source, &destination)
                }
            }
        }
    }
}
