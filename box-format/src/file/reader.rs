use std::fs::File;
use std::fs::OpenOptions;
use std::io::{prelude::*, BufReader};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};

use comde::Decompress;
use memmap::MmapOptions;

use super::{read_header, read_trailer, BoxMetadata};
use crate::{header::BoxHeader, path::BoxPath, record::FileRecord};

#[derive(Debug)]
pub struct BoxFileReader {
    pub(crate) file: BufReader<File>,
    pub(crate) path: PathBuf,
    pub(crate) header: BoxHeader,
    pub(crate) meta: BoxMetadata,
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
    pub fn alignment(&self) -> Option<NonZeroU64> {
        self.header.alignment
    }

    #[inline(always)]
    pub fn version(&self) -> u32 {
        self.header.version
    }

    /// Will return the metadata for the `.box` if it has been provided.
    #[inline(always)]
    pub fn metadata(&self) -> &BoxMetadata {
        &self.meta
    }

    #[inline(always)]
    pub fn decompress_value<V: Decompress>(&self, record: &FileRecord) -> std::io::Result<V> {
        let mmap = unsafe { self.data(record)? };
        record.compression.decompress(std::io::Cursor::new(mmap))
    }

    #[inline(always)]
    pub fn decompress<W: Write>(&self, record: &FileRecord, dest: W) -> std::io::Result<()> {
        let mmap = unsafe { self.data(record)? };
        record
            .compression
            .decompress_write(std::io::Cursor::new(mmap), dest)
    }

    #[inline(always)]
    pub fn attr<S: AsRef<str>>(&self, path: &BoxPath, key: S) -> Option<&Vec<u8>> {
        let key = self.attr_key_for(key.as_ref())?;

        if let Some(record) = self.meta.records.iter().find(|r| r.path() == path) {
            record.attrs().get(&key)
        } else {
            None
        }
    }

    #[inline(always)]
    pub fn read_bytes(&self, record: &FileRecord) -> std::io::Result<std::io::Take<File>> {
         let mut file = OpenOptions::new()
            .read(true)
            .open(&self.path)?;

        file.seek(std::io::SeekFrom::Start(record.data.get()))?;
        Ok(file.take(record.length))
    }

    #[inline(always)]
    pub unsafe fn data(&self, record: &FileRecord) -> std::io::Result<memmap::Mmap> {
        MmapOptions::new()
            .offset(record.data.get())
            .len(record.length as usize)
            .map(self.file.get_ref())
    }

    #[inline(always)]
    pub(crate) fn attr_key_for(&self, key: &str) -> Option<u32> {
        self.meta
            .attr_keys
            .iter()
            .position(|r| r == key)
            .map(|v| v as u32)
    }
}
