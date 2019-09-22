use std::collections::HashMap;
use std::default::Default;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{prelude::*, BufReader, BufWriter, Result, SeekFrom};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};

use memmap::MmapOptions;

use crate::{
    compression::Compression,
    header::BoxHeader,
    path::BoxPath,
    record::{DirectoryRecord, FileRecord, Record},
    ser::Serialize,
};

use super::{read_header, read_trailer, BoxMetadata};

#[derive(Debug)]
pub struct BoxFileWriter {
    pub(crate) file: BufWriter<File>,
    pub(crate) path: PathBuf,
    pub(crate) header: BoxHeader,
    pub(crate) meta: BoxMetadata,
}

impl Drop for BoxFileWriter {
    fn drop(&mut self) {
        let _ = self.finish_inner();
    }
}

impl BoxFileWriter {
    #[inline(always)]
    fn write_header(&mut self) -> std::io::Result<()> {
        self.file.seek(SeekFrom::Start(0))?;
        self.header.write(&mut self.file)
    }

    #[inline(always)]
    fn finish_inner(&mut self) -> std::io::Result<u64> {
        let pos = self.next_write_addr().get();
        self.header.trailer = NonZeroU64::new(pos);
        self.write_header()?;
        self.file.seek(SeekFrom::Start(pos))?;
        self.meta.write(&mut self.file)?;

        let new_pos = self.file.seek(SeekFrom::Current(0))?;
        let file = self.file.get_mut();
        file.set_len(new_pos)?;
        Ok(new_pos)
    }

    pub fn finish(mut self) -> std::io::Result<u64> {
        self.finish_inner()
    }

    #[inline(always)]
    fn next_write_addr(&self) -> NonZeroU64 {
        let offset = self
            .meta
            .records
            .iter()
            .rev()
            .find_map(|r| r.as_file())
            .map(|r| r.data.get() + r.length)
            .unwrap_or(std::mem::size_of::<BoxHeader>() as u64);

        let v = match self.header.alignment {
            None => offset,
            Some(alignment) => {
                let alignment = alignment.get();
                let diff = offset % alignment;
                if diff == 0 {
                    offset
                } else {
                    offset + (alignment - diff)
                }
            }
        };

        NonZeroU64::new(v).unwrap()
    }

    /// This will open an existing `.box` file for writing, and error if the file is not valid.
    pub fn open<P: AsRef<Path>>(path: P) -> std::io::Result<BoxFileWriter> {
        OpenOptions::new()
            .read(true)
            .write(true)
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

                let f = BoxFileWriter {
                    file: BufWriter::new(file),
                    path: path.as_ref().to_path_buf().canonicalize()?,
                    header,
                    meta,
                };

                Ok(f)
            })?
    }

    /// This will create a new `.box` file for writing, and error if the file already exists.
    pub fn create<P: AsRef<Path>>(path: P) -> std::io::Result<BoxFileWriter> {
        let mut boxfile = OpenOptions::new()
            .write(true)
            .read(true)
            .create_new(true)
            .open(path.as_ref())
            .and_then(|file| {
                Ok(BoxFileWriter {
                    file: BufWriter::new(file),
                    path: path.as_ref().to_path_buf().canonicalize()?,
                    header: BoxHeader::default(),
                    meta: BoxMetadata::default(),
                })
            })?;

        boxfile.write_header()?;

        Ok(boxfile)
    }

    /// This will create a new `.box` file for reading and writing, and error if the file already exists.
    /// Will insert byte-aligned values based on provided `alignment` value. For best results, consider a power of 2.
    pub fn create_with_alignment<P: AsRef<Path>>(
        path: P,
        alignment: NonZeroU64,
    ) -> std::io::Result<BoxFileWriter> {
        let mut boxfile = OpenOptions::new()
            .write(true)
            .read(true)
            .create_new(true)
            .open(path.as_ref())
            .and_then(|file| {
                Ok(BoxFileWriter {
                    file: BufWriter::new(file),
                    path: path.as_ref().to_path_buf().canonicalize()?,
                    header: BoxHeader::with_alignment(alignment),
                    meta: BoxMetadata::default(),
                })
            })?;

        boxfile.write_header()?;

        Ok(boxfile)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn alignment(&self) -> Option<NonZeroU64> {
        self.header.alignment
    }

    pub fn version(&self) -> u32 {
        self.header.version
    }

    /// Will return the metadata for the `.box` if it has been provided.
    pub fn metadata(&self) -> &BoxMetadata {
        &self.meta
    }

    pub fn mkdir(&mut self, path: BoxPath, attrs: HashMap<String, Vec<u8>>) -> std::io::Result<()> {
        let attrs = attrs
            .into_iter()
            .map(|(k, v)| {
                let k = self.attr_key_for_mut(&k);
                (k, v)
            })
            .collect::<HashMap<_, _>>();

        self.meta
            .records
            .push(Record::Directory(DirectoryRecord { path, attrs }));
        Ok(())
    }

    pub fn insert<R: Read>(
        &mut self,
        compression: Compression,
        path: BoxPath,
        value: &mut R,
        attrs: HashMap<String, Vec<u8>>,
    ) -> std::io::Result<&FileRecord> {
        let data = self.next_write_addr();
        let bytes = self.write_data::<R>(compression, data.get(), value)?;
        let attrs = attrs
            .into_iter()
            .map(|(k, v)| {
                let k = self.attr_key_for_mut(&k);
                (k, v)
            })
            .collect::<HashMap<_, _>>();

        // Check there isn't already a record for this path
        if self.meta.records.iter().any(|x| x.path() == &path) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "path already found",
            ));
        }

        let record = FileRecord {
            compression,
            length: bytes.write,
            decompressed_length: bytes.read,
            path,
            data,
            attrs,
        };

        self.meta.records.push(Record::File(record));

        Ok(&self.meta.records.last().unwrap().as_file().unwrap())
    }

    pub unsafe fn data(&self, record: &FileRecord) -> std::io::Result<memmap::Mmap> {
        self.read_data(record)
    }

    #[inline(always)]
    fn write_data<R: Read>(
        &mut self,
        compression: Compression,
        pos: u64,
        reader: &mut R,
    ) -> std::io::Result<comde::com::ByteCount> {
        self.file.seek(SeekFrom::Start(pos))?;
        compression.compress(&mut self.file, reader)
    }

    #[inline(always)]
    pub(crate) fn attr_key_for_mut(&mut self, key: &str) -> u32 {
        match self.meta.attr_keys.iter().position(|r| r == key) {
            Some(v) => v as u32,
            None => {
                self.meta.attr_keys.push(key.to_string());
                (self.meta.attr_keys.len() - 1) as u32
            }
        }
    }

    pub fn set_attr<S: AsRef<str>>(
        &mut self,
        path: &BoxPath,
        key: S,
        value: Vec<u8>,
    ) -> Result<()> {
        let key = self.attr_key_for_mut(key.as_ref());

        if let Some(record) = self.meta.records.iter_mut().find(|r| r.path() == path) {
            record.attrs_mut().insert(key, value);
        }

        Ok(())
    }

    #[inline(always)]
    unsafe fn read_data(&self, header: &FileRecord) -> std::io::Result<memmap::Mmap> {
        MmapOptions::new()
            .offset(header.data.get())
            .len(header.length as usize)
            .map(&self.file.get_ref())
    }
}
