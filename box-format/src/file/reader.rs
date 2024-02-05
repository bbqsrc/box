use std::fs::File;
use std::fs::{self, OpenOptions};
use std::io::{self, prelude::*, BufReader, BufWriter, SeekFrom};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};

use comde::Decompress;
use memmap2::{Mmap, MmapOptions};

use super::meta::Records;
use super::{meta::RecordsItem, BoxMetadata};
use crate::path::IntoBoxPathError;
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
    pub(crate) offset: u64,
}

#[inline(always)]
pub(super) fn read_header<R: Read + Seek>(file: &mut R, offset: u64) -> io::Result<BoxHeader> {
    file.seek(SeekFrom::Start(offset))?;
    BoxHeader::deserialize_owned(file)
}

#[inline(always)]
pub(super) fn read_trailer<R: Read + Seek, P: AsRef<Path>>(
    reader: &mut R,
    ptr: NonZeroU64,
    _path: P,
    offset: u64,
) -> io::Result<BoxMetadata> {
    reader.seek(SeekFrom::Start(offset + ptr.get()))?;
    let meta = BoxMetadata::deserialize_owned(reader)?;

    Ok(meta)
}

#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    #[error("Could not find trailer (the end of the file is missing).")]
    MissingTrailer,

    #[error("Invalid trailer data (the data that describes where all the files are is invalid).")]
    InvalidTrailer(#[source] std::io::Error),

    #[error("Could not read header. Is this a valid Box archive?")]
    MissingHeader(#[source] std::io::Error),

    #[error("Invalid path to Box file. Path: '{}'", .1.display())]
    InvalidPath(#[source] std::io::Error, PathBuf),

    #[error("Failed to read Box file. Path: '{}'", .1.display())]
    ReadFailed(#[source] std::io::Error, PathBuf),
}

#[derive(Debug, thiserror::Error)]
pub enum ExtractError {
    #[error("Creating directory failed. Path: '{}'", .1.display())]
    CreateDirFailed(#[source] std::io::Error, PathBuf),

    #[error("Creating file failed. Path: '{}'", .1.display())]
    CreateFileFailed(#[source] std::io::Error, PathBuf),

    #[error("Path not found in archive. Path: '{}'", .0.display())]
    NotFoundInArchive(PathBuf),

    #[error("Decompressing file failed. Path: '{}'", .1.display())]
    DecompressionFailed(#[source] std::io::Error, PathBuf),

    #[error("Creating link failed. Path: '{}' -> '{}'", .1.display(), .2.display())]
    CreateLinkFailed(#[source] std::io::Error, PathBuf, PathBuf),

    #[error("Resolving link failed: Path: '{}' -> '{}'", .1.name, .1.target)]
    ResolveLinkFailed(#[source] std::io::Error, LinkRecord),

    #[error("Could not convert to a valid Box path. Path suffix: '{}'", .1)]
    ResolveBoxPathFailed(#[source] IntoBoxPathError, String),
}

impl BoxFileReader {
    /// This will open an existing `.box` file for reading and writing, and error if the file is not valid.
    pub fn open_at_offset<P: AsRef<Path>>(
        path: P,
        offset: u64,
    ) -> Result<BoxFileReader, OpenError> {
        let path = path.as_ref().to_path_buf();
        let path = path
            .canonicalize()
            .map_err(|e| OpenError::InvalidPath(e, path.to_path_buf()))?;

        let mut file = OpenOptions::new()
            .read(true)
            .open(&path)
            .map_err(|e| OpenError::ReadFailed(e, path.clone()))?;

        // Try to load the header so we can easily rewrite it when saving.
        // If header is invalid, we're not even loading a .box file.
        let (header, meta) = {
            let mut reader = BufReader::new(&mut file);
            let header = read_header(&mut reader, offset).map_err(OpenError::MissingHeader)?;
            let ptr = header.trailer.ok_or(OpenError::MissingTrailer)?;
            let meta =
                read_trailer(&mut reader, ptr, &path, offset).map_err(OpenError::InvalidTrailer)?;

            (header, meta)
        };

        let f = BoxFileReader {
            file: BufReader::new(file),
            path,
            header,
            meta,
            offset,
        };

        Ok(f)
    }

    /// This will open an existing `.box` file for reading and writing, and error if the file is not valid.
    #[inline]
    pub fn open<P: AsRef<Path>>(path: P) -> Result<BoxFileReader, OpenError> {
        Self::open_at_offset(path, 0)
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
    pub fn decompress_value<V: Decompress>(&self, record: &FileRecord) -> io::Result<V> {
        let mmap = unsafe { self.memory_map(record)? };
        record.compression.decompress(io::Cursor::new(mmap))
    }

    #[inline(always)]
    pub fn decompress<W: Write>(&self, record: &FileRecord, dest: W) -> io::Result<()> {
        let mmap = unsafe { self.memory_map(record)? };
        record
            .compression
            .decompress_write(io::Cursor::new(mmap), dest)
    }

    #[inline(always)]
    pub fn find(&self, path: &BoxPath) -> Result<&Record, ExtractError> {
        let record = self
            .meta
            .inode(path)
            .and_then(|x| self.meta.record(x))
            .ok_or_else(|| ExtractError::NotFoundInArchive(path.to_path_buf()))?;
        Ok(record)
    }

    #[inline(always)]
    pub fn extract<P: AsRef<Path>>(
        &self,
        path: &BoxPath,
        output_path: P,
    ) -> Result<(), ExtractError> {
        let output_path = output_path.as_ref();
        let record = self
            .meta
            .inode(path)
            .and_then(|x| self.meta.record(x))
            .ok_or_else(|| ExtractError::NotFoundInArchive(path.to_path_buf()))?;
        self.extract_inner(path, record, output_path)
    }

    #[inline(always)]
    pub fn extract_recursive<P: AsRef<Path>>(
        &self,
        path: &BoxPath,
        output_path: P,
    ) -> Result<(), ExtractError> {
        let output_path = output_path.as_ref();

        let inode = self
            .meta
            .inode(path)
            .ok_or_else(|| ExtractError::NotFoundInArchive(path.to_path_buf()))?;

        Records::new(&self.meta, &[inode], None).try_for_each(
            |RecordsItem { path, record, .. }| self.extract_inner(&path, record, output_path),
        )
    }

    #[inline(always)]
    pub fn extract_all<P: AsRef<Path>>(&self, output_path: P) -> Result<(), ExtractError> {
        let output_path = output_path.as_ref();
        self.meta
            .iter()
            .try_for_each(|RecordsItem { path, record, .. }| {
                self.extract_inner(&path, record, output_path)
            })
    }

    #[inline(always)]
    pub fn resolve_link(&self, link: &LinkRecord) -> io::Result<RecordsItem> {
        match self.meta.inode(&link.target) {
            Some(inode) => Ok(RecordsItem {
                inode,
                path: link.target.to_owned(),
                record: self.meta.record(inode).unwrap(),
            }),
            None => Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("No inode for link target: {}", link.target),
            )),
        }
    }

    #[inline(always)]
    pub fn read_bytes(&self, record: &FileRecord) -> io::Result<io::Take<File>> {
        let mut file = OpenOptions::new().read(true).open(&self.path)?;

        file.seek(io::SeekFrom::Start(self.offset + record.data.get()))?;
        Ok(file.take(record.length))
    }

    /// # Safety
    ///
    /// Use of memory maps is unsafe as modifications to the file could affect the operation
    /// of the application. Ensure that the Box being operated on is not mutated while a memory
    /// map is in use.
    #[inline(always)]
    pub unsafe fn memory_map(&self, record: &FileRecord) -> io::Result<Mmap> {
        MmapOptions::new()
            .offset(self.offset + record.data.get())
            .len(record.length as usize)
            .map(self.file.get_ref())
    }

    #[inline(always)]
    fn extract_inner(
        &self,
        path: &BoxPath,
        record: &Record,
        output_path: &Path,
    ) -> Result<(), ExtractError> {
        // println!("{} -> {}: {:?}", path, output_path.display(), record);
        match record {
            Record::File(file) => {
                fs::create_dir_all(&output_path)
                    .map_err(|e| ExtractError::CreateDirFailed(e, output_path.to_path_buf()))?;
                let out_path = output_path.join(path.to_path_buf());
                let mut out_file = std::fs::OpenOptions::new();

                #[cfg(unix)]
                {
                    use std::os::unix::fs::OpenOptionsExt;

                    let mode: Option<u32> = record
                        .attr(self.metadata(), "unix.mode")
                        .filter(|x| x.len() == 4)
                        .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]));

                    if let Some(mode) = mode {
                        out_file.mode(mode);
                    }
                }

                let out_file = out_file
                    .create(true)
                    .write(true)
                    .open(&out_path)
                    .map_err(|e| ExtractError::CreateFileFailed(e, out_path.to_path_buf()))?;

                let out_file = BufWriter::new(out_file);
                self.decompress(file, out_file)
                    .map_err(|e| ExtractError::DecompressionFailed(e, path.to_path_buf()))?;

                Ok(())
            }
            Record::Directory(_dir) => {
                fs::create_dir_all(&output_path)
                    .map_err(|e| ExtractError::CreateDirFailed(e, output_path.to_path_buf()))?;
                let new_dir = output_path.join(path.to_path_buf());
                fs::create_dir(&new_dir).map_err(|e| ExtractError::CreateDirFailed(e, new_dir))
            }
            #[cfg(unix)]
            Record::Link(link) => {
                let link_target = self
                    .resolve_link(link)
                    .map_err(|e| ExtractError::ResolveLinkFailed(e, link.clone()))?;

                let source = output_path.join(path.to_path_buf());
                let destination = output_path.join(link_target.path.to_path_buf());

                std::os::unix::fs::symlink(&source, &destination)
                    .map_err(|e| ExtractError::CreateLinkFailed(e, source, destination))
            }
            #[cfg(windows)]
            Record::Link(link) => {
                let link_target = self
                    .resolve_link(link)
                    .map_err(|e| ExtractError::ResolveLinkFailed(e, link.clone()))?;

                let source = output_path.join(path.to_path_buf());
                let destination = output_path.join(link_target.path.to_path_buf());

                if link_target.record.as_directory().is_some() {
                    std::os::windows::fs::symlink_dir(&source, &destination)
                        .map_err(|e| ExtractError::CreateLinkFailed(e, source, destination))
                } else {
                    std::os::windows::fs::symlink_file(&source, &destination)
                        .map_err(|e| ExtractError::CreateLinkFailed(e, source, destination))
                }
            }
        }
    }
}
