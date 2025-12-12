use std::io::SeekFrom;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};

use mmap_io::MemoryMappedFile;
use mmap_io::segment::Segment;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, BufReader};

use super::meta::Records;
use super::{BoxMetadata, meta::RecordsItem};
use crate::path::IntoBoxPathError;
use crate::{
    de::DeserializeOwned,
    header::BoxHeader,
    path::BoxPath,
    record::{FileRecord, LinkRecord, Record},
};

#[derive(Debug)]
pub struct BoxFileReader {
    pub(crate) path: PathBuf,
    pub(crate) header: BoxHeader,
    pub(crate) meta: BoxMetadata,
    pub(crate) offset: u64,
}

pub(super) async fn read_header<R: tokio::io::AsyncRead + tokio::io::AsyncSeek + Unpin + Send>(
    file: &mut R,
    offset: u64,
) -> std::io::Result<BoxHeader> {
    file.seek(SeekFrom::Start(offset)).await?;
    BoxHeader::deserialize_owned(file).await
}

pub(super) async fn read_trailer<R: tokio::io::AsyncRead + tokio::io::AsyncSeek + Unpin + Send>(
    reader: &mut R,
    ptr: NonZeroU64,
    offset: u64,
) -> std::io::Result<BoxMetadata> {
    reader.seek(SeekFrom::Start(offset + ptr.get())).await?;
    BoxMetadata::deserialize_owned(reader).await
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
    /// This will open an existing `.box` file for reading and error if the file is not valid.
    pub async fn open_at_offset<P: AsRef<Path>>(
        path: P,
        offset: u64,
    ) -> Result<BoxFileReader, OpenError> {
        let path = path.as_ref().to_path_buf();
        let path = tokio::fs::canonicalize(&path)
            .await
            .map_err(|e| OpenError::InvalidPath(e, path.to_path_buf()))?;

        let file = OpenOptions::new()
            .read(true)
            .open(&path)
            .await
            .map_err(|e| OpenError::ReadFailed(e, path.clone()))?;

        // Try to load the header so we can easily rewrite it when saving.
        // If header is invalid, we're not even loading a .box file.
        let (header, meta) = {
            let mut reader = BufReader::new(file);
            let header = read_header(&mut reader, offset)
                .await
                .map_err(OpenError::MissingHeader)?;
            let ptr = header.trailer.ok_or(OpenError::MissingTrailer)?;
            let meta = read_trailer(&mut reader, ptr, offset)
                .await
                .map_err(OpenError::InvalidTrailer)?;

            (header, meta)
        };

        let f = BoxFileReader {
            path,
            header,
            meta,
            offset,
        };

        Ok(f)
    }

    /// This will open an existing `.box` file for reading and error if the file is not valid.
    #[inline]
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<BoxFileReader, OpenError> {
        Self::open_at_offset(path, 0).await
    }

    #[inline(always)]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[inline(always)]
    pub fn alignment(&self) -> u32 {
        self.header.alignment
    }

    #[inline(always)]
    pub fn version(&self) -> u8 {
        self.header.version
    }

    #[inline(always)]
    pub fn metadata(&self) -> &BoxMetadata {
        &self.meta
    }

    pub async fn decompress<W: tokio::io::AsyncWrite + Unpin>(
        &self,
        record: &FileRecord,
        dest: W,
    ) -> std::io::Result<()> {
        let segment = self.memory_map(record)?;
        let data = segment.as_slice().map_err(std::io::Error::other)?;
        let cursor = std::io::Cursor::new(data);
        let buf_reader = tokio::io::BufReader::new(cursor);
        record.compression.decompress_write(buf_reader, dest).await
    }

    pub fn find(&self, path: &BoxPath) -> Result<&Record, ExtractError> {
        let record = self
            .meta
            .inode(path)
            .and_then(|x| self.meta.record(x))
            .ok_or_else(|| ExtractError::NotFoundInArchive(path.to_path_buf()))?;
        Ok(record)
    }

    pub async fn extract<P: AsRef<Path>>(
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
        self.extract_inner(path, record, output_path).await
    }

    pub async fn extract_recursive<P: AsRef<Path>>(
        &self,
        path: &BoxPath,
        output_path: P,
    ) -> Result<(), ExtractError> {
        let output_path = output_path.as_ref();

        let inode = self
            .meta
            .inode(path)
            .ok_or_else(|| ExtractError::NotFoundInArchive(path.to_path_buf()))?;

        for item in Records::new(&self.meta, &[inode], None) {
            self.extract_inner(&item.path, item.record, output_path)
                .await?;
        }
        Ok(())
    }

    pub async fn extract_all<P: AsRef<Path>>(&self, output_path: P) -> Result<(), ExtractError> {
        let output_path = output_path.as_ref();
        for item in self.meta.iter() {
            self.extract_inner(&item.path, item.record, output_path)
                .await?;
        }
        Ok(())
    }

    pub fn resolve_link(&self, link: &LinkRecord) -> std::io::Result<RecordsItem<'_>> {
        match self.meta.inode(&link.target) {
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

    pub async fn read_bytes(&self, record: &FileRecord) -> std::io::Result<tokio::io::Take<File>> {
        let mut file = OpenOptions::new().read(true).open(&self.path).await?;

        file.seek(SeekFrom::Start(self.offset + record.data.get()))
            .await?;
        Ok(file.take(record.length))
    }

    /// Memory-map the file and return a segment for the record's data.
    pub fn memory_map(&self, record: &FileRecord) -> std::io::Result<Segment> {
        let mmap = MemoryMappedFile::open_ro(&self.path).map_err(std::io::Error::other)?;
        let offset = self.offset + record.data.get();
        Segment::new(mmap.into(), offset, record.length).map_err(std::io::Error::other)
    }

    async fn extract_inner(
        &self,
        path: &BoxPath,
        record: &Record,
        output_path: &Path,
    ) -> Result<(), ExtractError> {
        match record {
            Record::File(file) => {
                fs::create_dir_all(output_path)
                    .await
                    .map_err(|e| ExtractError::CreateDirFailed(e, output_path.to_path_buf()))?;
                let out_path = output_path.join(path.to_path_buf());

                let out_file = fs::File::create(&out_path)
                    .await
                    .map_err(|e| ExtractError::CreateFileFailed(e, out_path.to_path_buf()))?;

                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;

                    let mode: Option<u32> = record
                        .attr(self.metadata(), "unix.mode")
                        .filter(|x| x.len() == 4)
                        .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]));

                    if let Some(mode) = mode {
                        let permissions = std::fs::Permissions::from_mode(mode);
                        fs::set_permissions(&out_path, permissions).await.ok();
                    }
                }

                let out_file = tokio::io::BufWriter::new(out_file);
                self.decompress(file, out_file)
                    .await
                    .map_err(|e| ExtractError::DecompressionFailed(e, path.to_path_buf()))?;

                Ok(())
            }
            Record::Directory(_dir) => {
                fs::create_dir_all(output_path)
                    .await
                    .map_err(|e| ExtractError::CreateDirFailed(e, output_path.to_path_buf()))?;
                let new_dir = output_path.join(path.to_path_buf());
                fs::create_dir(&new_dir)
                    .await
                    .map_err(|e| ExtractError::CreateDirFailed(e, new_dir))
            }
            #[cfg(unix)]
            Record::Link(link) => {
                let link_target = self
                    .resolve_link(link)
                    .map_err(|e| ExtractError::ResolveLinkFailed(e, link.clone()))?;

                let source = output_path.join(path.to_path_buf());
                let destination = output_path.join(link_target.path.to_path_buf());

                tokio::fs::symlink(&source, &destination)
                    .await
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
                    tokio::fs::symlink_dir(&source, &destination)
                        .await
                        .map_err(|e| ExtractError::CreateLinkFailed(e, source, destination))
                } else {
                    tokio::fs::symlink_file(&source, &destination)
                        .await
                        .map_err(|e| ExtractError::CreateLinkFailed(e, source, destination))
                }
            }
        }
    }
}
