use std::collections::HashMap;
use std::default::Default;
use std::io::SeekFrom;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};

use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufWriter};

use crate::{
    compression::{ByteCount, Compression},
    header::BoxHeader,
    path::BoxPath,
    record::{DirectoryRecord, FileRecord, LinkRecord, Record},
    ser::Serialize,
};

use super::{
    BoxMetadata,
    reader::{read_header, read_trailer},
};

pub struct BoxFileWriter {
    pub(crate) file: BufWriter<File>,
    pub(crate) path: PathBuf,
    pub(crate) header: BoxHeader,
    pub(crate) meta: BoxMetadata,
    finished: bool,
}

impl Drop for BoxFileWriter {
    fn drop(&mut self) {
        if !self.finished {
            // Can't do async in Drop, so we warn if not finished
            log::warn!(
                "BoxFileWriter dropped without calling finish(). \
                 Archive at {:?} may be incomplete.",
                self.path
            );
        }
    }
}

impl BoxFileWriter {
    async fn write_header(&mut self) -> std::io::Result<()> {
        self.file.seek(SeekFrom::Start(0)).await?;
        self.header.write(&mut self.file).await
    }

    async fn finish_inner(&mut self) -> std::io::Result<u64> {
        let pos = self.next_write_addr().get();
        self.header.trailer = NonZeroU64::new(pos);
        self.write_header().await?;
        self.file.seek(SeekFrom::Start(pos)).await?;
        self.meta.write(&mut self.file).await?;
        self.file.flush().await?;

        let new_pos = self.file.get_ref().metadata().await?.len();
        self.file.get_mut().set_len(new_pos).await?;
        self.finished = true;
        Ok(new_pos)
    }

    pub async fn finish(mut self) -> std::io::Result<u64> {
        self.finish_inner().await
    }

    fn next_write_addr(&self) -> NonZeroU64 {
        let offset = self
            .meta
            .inodes
            .iter()
            .rev()
            .find_map(|r| r.as_file())
            .map(|r| r.data.get() + r.length)
            .unwrap_or(std::mem::size_of::<BoxHeader>() as u64);

        let v = match self.header.alignment as u64 {
            0 => offset,
            alignment => {
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
    pub async fn open<P: AsRef<Path>>(path: P) -> std::io::Result<BoxFileWriter> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.as_ref())
            .await?;

        // Try to load the header so we can easily rewrite it when saving.
        // If header is invalid, we're not even loading a .box file.
        let mut reader = tokio::io::BufReader::new(file);
        let header = read_header(&mut reader, 0).await?;
        let ptr = header
            .trailer
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "no trailer found"))?;
        let meta = read_trailer(&mut reader, ptr, 0).await?;

        // Get the file back from the BufReader
        let file = reader.into_inner();

        let f = BoxFileWriter {
            file: BufWriter::new(file),
            path: tokio::fs::canonicalize(path.as_ref()).await?,
            header,
            meta,
            finished: false,
        };

        Ok(f)
    }

    /// This will create a new `.box` file for writing, and error if the file already exists.
    pub async fn create<P: AsRef<Path>>(path: P) -> std::io::Result<BoxFileWriter> {
        Self::create_inner(path, BoxHeader::default()).await
    }

    /// This will create a new `.box` file for reading and writing, and error if the file already exists.
    /// Will insert byte-aligned values based on provided `alignment` value. For best results, consider a power of 2.
    pub async fn create_with_alignment<P: AsRef<Path>>(
        path: P,
        alignment: u32,
    ) -> std::io::Result<BoxFileWriter> {
        Self::create_inner(path, BoxHeader::with_alignment(alignment)).await
    }

    async fn create_inner<P: AsRef<Path>>(
        path: P,
        header: BoxHeader,
    ) -> std::io::Result<BoxFileWriter> {
        let file = OpenOptions::new()
            .write(true)
            .read(true)
            .create_new(true)
            .open(path.as_ref())
            .await?;

        let mut boxfile = BoxFileWriter {
            file: BufWriter::new(file),
            path: tokio::fs::canonicalize(path.as_ref()).await?,
            header,
            meta: BoxMetadata::default(),
            finished: false,
        };

        boxfile.write_header().await?;

        Ok(boxfile)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn alignment(&self) -> u32 {
        self.header.alignment
    }

    pub fn version(&self) -> u8 {
        self.header.version
    }

    /// Will return the metadata for the `.box` if it has been provided.
    pub fn metadata(&self) -> &BoxMetadata {
        &self.meta
    }

    fn iter(&self) -> super::meta::Records<'_> {
        super::meta::Records::new(self.metadata(), &self.metadata().root, None)
    }

    fn convert_attrs(&mut self, attrs: HashMap<String, Vec<u8>>) -> HashMap<usize, Vec<u8>> {
        attrs
            .into_iter()
            .map(|(k, v)| (self.meta.attr_key_or_create(&k), v))
            .collect()
    }

    fn insert_inner(&mut self, path: BoxPath, record: Record) -> std::io::Result<()> {
        log::debug!("insert_inner path: {:?}", path);
        match path.parent() {
            Some(parent_path) => {
                log::debug!("insert_inner parent: {:?}", parent_path);

                match self.meta.inode(&parent_path) {
                    None => {
                        let err = std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("No inode found for path: {:?}", parent_path),
                        );
                        Err(err)
                    }
                    Some(parent_inode) => {
                        log::debug!(
                            "Inserting record into parent {:?}: {:?}",
                            &parent_inode,
                            &record
                        );
                        let new_inode = self.meta.insert_record(record);
                        log::debug!("Inserted with inode: {:?}", &new_inode);
                        let parent = self
                            .meta
                            .record_mut(parent_inode)
                            .unwrap()
                            .as_directory_mut()
                            .unwrap();
                        parent.inodes.push(new_inode);
                        Ok(())
                    }
                }
            }
            None => {
                log::debug!("Inserting record into root: {:?}", &record);
                let new_inode = self.meta.insert_record(record);
                self.meta.root.push(new_inode);
                Ok(())
            }
        }
    }

    pub fn mkdir(&mut self, path: BoxPath, attrs: HashMap<String, Vec<u8>>) -> std::io::Result<()> {
        log::debug!("mkdir: {}", path);

        let record = DirectoryRecord {
            name: path.filename(),
            inodes: vec![],
            attrs: self.convert_attrs(attrs),
        };

        self.insert_inner(path, record.into())
    }

    pub fn link(
        &mut self,
        path: BoxPath,
        target: BoxPath,
        attrs: HashMap<String, Vec<u8>>,
    ) -> std::io::Result<()> {
        let record = LinkRecord {
            name: path.filename(),
            target,
            attrs: self.convert_attrs(attrs),
        };

        self.insert_inner(path, record.into())
    }

    pub async fn insert<R: tokio::io::AsyncBufRead + Unpin>(
        &mut self,
        compression: Compression,
        path: BoxPath,
        value: R,
        attrs: HashMap<String, Vec<u8>>,
    ) -> std::io::Result<&FileRecord> {
        let attrs = self.convert_attrs(attrs);
        let next_addr = self.next_write_addr();
        let byte_count = self.write_data(compression, next_addr.get(), value).await?;

        let record = FileRecord {
            compression,
            length: byte_count.write,
            decompressed_length: byte_count.read,
            name: path.filename(),
            data: next_addr,
            attrs,
        };

        self.insert_inner(path, record.into())?;

        Ok(self.meta.inodes.last().unwrap().as_file().unwrap())
    }

    async fn write_data<R: tokio::io::AsyncBufRead + Unpin>(
        &mut self,
        compression: Compression,
        pos: u64,
        reader: R,
    ) -> std::io::Result<ByteCount> {
        self.file.seek(SeekFrom::Start(pos)).await?;
        compression.compress(&mut self.file, reader).await
    }

    pub fn set_attr<S: AsRef<str>>(
        &mut self,
        path: &BoxPath,
        key: S,
        value: Vec<u8>,
    ) -> std::io::Result<()> {
        let inode = match self.iter().find(|r| &r.path == path) {
            Some(v) => v.inode,
            None => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Path not found: {}", path),
                ));
            }
        };

        let key = self.meta.attr_key_or_create(key.as_ref());
        let record = self.meta.record_mut(inode).unwrap();
        record.attrs_mut().insert(key, value);

        Ok(())
    }

    pub fn set_file_attr<S: AsRef<str>>(&mut self, key: S, value: Vec<u8>) -> std::io::Result<()> {
        let key = self.meta.attr_key_or_create(key.as_ref());

        self.meta.attrs.insert(key, value);

        Ok(())
    }
}
