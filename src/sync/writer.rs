use std::borrow::Cow;
use crate::compat::HashMap;
use std::io::{BufRead, Read, Seek, SeekFrom, Write};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};

#[cfg(feature = "xz")]
use crate::compression::xz::XzCompressor;
#[cfg(feature = "zstd")]
use crate::compression::zstd::ZstdCompressor;
use crate::{
    compression::{
        ByteCount, Compression, CompressionConfig, StreamStatus, constants::DEFAULT_BLOCK_SIZE,
    },
    core::{ArchiveWriter, AttrType, BoxMetadata, RecordIndex},
    header::BoxHeader,
    path::BoxPath,
    record::{DirectoryRecord, ExternalLinkRecord, FileRecord, LinkRecord, Record},
};

/// 8MB buffer for efficient sequential writes
const WRITE_BUFFER_SIZE: usize = 8 * 1024 * 1024;

/// Sync writer for Box archives.
///
/// This is a frontend that wraps the sans-IO [`ArchiveWriter`] core,
/// providing sync I/O operations for writing archives.
pub struct BoxWriter {
    /// The sans-IO core that manages archive metadata
    pub(crate) core: ArchiveWriter,
    /// File handle for writing
    pub(crate) file: std::io::BufWriter<std::fs::File>,
    /// Path to the archive file
    pub(crate) path: PathBuf,
    /// Current file position (to avoid seek-induced buffer flushes)
    file_pos: u64,
    finished: bool,
}

impl Drop for BoxWriter {
    fn drop(&mut self) {
        if !self.finished {
            tracing::warn!(
                "BoxWriter dropped without calling finish(). \
                 Archive at {:?} may be incomplete.",
                self.path
            );
        }
    }
}

impl BoxWriter {
    fn write_header(&mut self) -> std::io::Result<()> {
        self.file.seek(SeekFrom::Start(0))?;
        let buf = self.core.encode_header();
        self.file.write_all(&buf)?;
        self.file_pos = BoxHeader::SIZE as u64;
        Ok(())
    }

    fn finish_inner(&mut self) -> std::io::Result<u64> {
        self.file.flush()?;

        let (trailer_offset, meta_bytes) = self.core.finish()?;

        self.write_header()?;

        self.file.seek(SeekFrom::Start(trailer_offset))?;
        self.file_pos = trailer_offset;

        self.file.write_all(&meta_bytes)?;

        self.file.flush()?;

        let new_pos = self.file.get_ref().metadata()?.len();
        self.file.get_mut().set_len(new_pos)?;
        self.finished = true;
        Ok(new_pos)
    }

    pub fn finish(mut self) -> std::io::Result<u64> {
        self.finish_inner()
    }

    #[inline]
    fn next_write_addr(&self) -> NonZeroU64 {
        self.core.next_write_addr()
    }

    /// This will open an existing `.box` file for writing, and error if the file is not valid.
    pub fn open<P: AsRef<Path>>(path: P) -> std::io::Result<BoxWriter> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.as_ref())?;

        let mut reader = std::io::BufReader::new(file);

        // Read header
        reader.seek(SeekFrom::Start(0))?;
        let mut buf = [0u8; 32];
        reader.read_exact(&mut buf)?;
        let (header_data, _) = crate::parse::parse_header(&buf)?;
        let header = BoxHeader {
            version: header_data.version,
            allow_external_symlinks: header_data.allow_external_symlinks,
            allow_escapes: header_data.allow_escapes,
            alignment: header_data.alignment,
            trailer: std::num::NonZeroU64::new(header_data.trailer_offset),
        };

        let ptr = header
            .trailer
            .ok_or_else(|| std::io::Error::other("no trailer found"))?;

        // Read trailer
        reader.seek(SeekFrom::Start(ptr.get()))?;
        let mut trailer_buf = Vec::new();
        reader.read_to_end(&mut trailer_buf)?;

        let meta = match header.version {
            0 => {
                let mut pos = 0;
                crate::de::v0::deserialize_metadata_borrowed(&trailer_buf, &mut pos)?.into_owned()
            }
            _ => {
                let (meta, _) = crate::parse::parse_metadata_v1(&trailer_buf)?;
                meta.into_owned()
            }
        };

        let file = reader.into_inner();

        let next_write_pos = meta
            .records
            .iter()
            .rev()
            .find_map(|r| r.as_file())
            .map(|r| r.data.get() + r.length)
            .unwrap_or(BoxHeader::SIZE as u64);

        let core = ArchiveWriter::from_existing(header, meta, next_write_pos);

        Ok(BoxWriter {
            core,
            file: std::io::BufWriter::with_capacity(WRITE_BUFFER_SIZE, file),
            path: std::fs::canonicalize(path.as_ref())?,
            file_pos: 0,
            finished: false,
        })
    }

    /// This will create a new `.box` file for writing, and error if the file already exists.
    pub fn create<P: AsRef<Path>>(path: P) -> std::io::Result<BoxWriter> {
        Self::create_inner(path, BoxHeader::default())
    }

    /// This will create a new `.box` file for reading and writing, and error if the file already exists.
    pub fn create_with_alignment<P: AsRef<Path>>(
        path: P,
        alignment: u32,
    ) -> std::io::Result<BoxWriter> {
        Self::create_inner(path, BoxHeader::with_alignment(alignment))
    }

    /// This will create a new `.box` file that allows `\xNN` escape sequences in paths.
    pub fn create_with_escapes<P: AsRef<Path>>(path: P) -> std::io::Result<BoxWriter> {
        Self::create_inner(path, BoxHeader::with_escapes(true))
    }

    /// This will create a new `.box` file with custom alignment and escape settings.
    pub fn create_with_options<P: AsRef<Path>>(
        path: P,
        alignment: u32,
        allow_escapes: bool,
        allow_external_symlinks: bool,
    ) -> std::io::Result<BoxWriter> {
        Self::create_inner(
            path,
            BoxHeader::with_options(alignment, allow_escapes, allow_external_symlinks),
        )
    }

    fn create_inner<P: AsRef<Path>>(path: P, header: BoxHeader) -> std::io::Result<BoxWriter> {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .create_new(true)
            .open(path.as_ref())?;

        let core =
            ArchiveWriter::from_existing(header, BoxMetadata::default(), BoxHeader::SIZE as u64);

        let mut boxfile = BoxWriter {
            core,
            file: std::io::BufWriter::with_capacity(WRITE_BUFFER_SIZE, file),
            path: std::fs::canonicalize(path.as_ref())?,
            file_pos: 0,
            finished: false,
        };

        boxfile.write_header()?;

        Ok(boxfile)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn alignment(&self) -> u32 {
        self.core.alignment()
    }

    pub fn version(&self) -> u8 {
        self.core.version()
    }

    pub fn allow_escapes(&self) -> bool {
        self.core.allow_escapes()
    }

    fn make_box_path<P: AsRef<Path>>(
        &self,
        path: P,
    ) -> std::result::Result<BoxPath<'static>, crate::path::IntoBoxPathError> {
        if self.core.allow_escapes() {
            BoxPath::new_with_escapes(path)
        } else {
            BoxPath::new(path)
        }
    }

    pub fn metadata(&self) -> &BoxMetadata<'static> {
        self.core.metadata()
    }

    fn convert_attrs(
        &mut self,
        attrs: HashMap<String, Vec<u8>>,
    ) -> std::io::Result<HashMap<usize, Box<[u8]>>> {
        // Set archive-level uid/gid defaults from first file if not already set
        if let Some(uid) = attrs.get("unix.uid") {
            let uid_key = self
                .core
                .meta
                .attr_key_or_create("unix.uid", AttrType::Vu32)?;
            self.core
                .meta
                .attrs
                .entry(uid_key)
                .or_insert_with(|| uid.clone().into_boxed_slice());
        }
        if let Some(gid) = attrs.get("unix.gid") {
            let gid_key = self
                .core
                .meta
                .attr_key_or_create("unix.gid", AttrType::Vu32)?;
            self.core
                .meta
                .attrs
                .entry(gid_key)
                .or_insert_with(|| gid.clone().into_boxed_slice());
        }

        let attrs: Vec<_> = {
            let default_uid = self
                .core
                .meta
                .attr_key("unix.uid")
                .and_then(|k| self.core.meta.attrs.get(&k).map(|v| &**v));
            let default_gid = self
                .core
                .meta
                .attr_key("unix.gid")
                .and_then(|k| self.core.meta.attrs.get(&k).map(|v| &**v));

            attrs
                .into_iter()
                .filter(|(k, v)| {
                    if k == "unix.uid" && default_uid.is_some_and(|d| v.as_slice() == d) {
                        return false;
                    }
                    if k == "unix.gid" && default_gid.is_some_and(|d| v.as_slice() == d) {
                        return false;
                    }
                    true
                })
                .collect()
        };

        let mut result = HashMap::new();
        for (k, v) in attrs {
            let attr_type = match k.as_str() {
                "unix.mode" | "unix.uid" | "unix.gid" => AttrType::Vu32,
                "created" | "modified" | "accessed" => AttrType::DateTime,
                "created.seconds" | "modified.seconds" | "accessed.seconds" => AttrType::U8,
                "created.nanoseconds" | "modified.nanoseconds" | "accessed.nanoseconds" => {
                    AttrType::Vu64
                }
                "blake3" => AttrType::U256,
                _ => AttrType::Bytes,
            };
            let key = self.core.meta.attr_key_or_create(&k, attr_type)?;
            result.insert(key, v.into_boxed_slice());
        }

        Ok(result)
    }

    fn insert_inner(
        &mut self,
        path: BoxPath<'_>,
        record: Record<'static>,
    ) -> std::io::Result<RecordIndex> {
        self.insert_inner_with_parent(path, record, None)
    }

    fn insert_inner_with_parent(
        &mut self,
        path: BoxPath<'_>,
        record: Record<'static>,
        cached_parent: Option<RecordIndex>,
    ) -> std::io::Result<RecordIndex> {
        match path.parent() {
            Some(parent_path) => {
                let parent_index = match cached_parent {
                    Some(idx) => idx,
                    None => self.core.meta.index(&parent_path).ok_or_else(|| {
                        std::io::Error::other(format!(
                            "No record found for path: {:?}",
                            parent_path
                        ))
                    })?,
                };

                let new_index = self.core.meta.insert_record(record);
                let parent = self
                    .core
                    .meta
                    .record_mut(parent_index)
                    .unwrap()
                    .as_directory_mut()
                    .unwrap();
                parent.entries.push(new_index);
                Ok(new_index)
            }
            None => {
                let new_index = self.core.meta.insert_record(record);
                self.core.meta.root.push(new_index);
                Ok(new_index)
            }
        }
    }

    pub fn mkdir(
        &mut self,
        path: BoxPath<'_>,
        attrs: HashMap<String, Vec<u8>>,
    ) -> std::io::Result<()> {
        let record = DirectoryRecord {
            name: Cow::Owned(path.filename().to_string()),
            entries: vec![],
            attrs: self.convert_attrs(attrs)?,
        };

        self.insert_inner(path, record.into())?;
        Ok(())
    }

    pub fn mkdir_all(
        &mut self,
        path: BoxPath<'_>,
        attrs: HashMap<String, Vec<u8>>,
    ) -> std::io::Result<()> {
        if let Some(parent) = path.parent()
            && self.core.meta.index(&parent).is_none()
        {
            self.mkdir_all(parent.into_owned(), HashMap::new())?;
        }

        if self.core.meta.index(&path).is_none() {
            self.mkdir(path, attrs)?;
        }

        Ok(())
    }

    pub fn link(
        &mut self,
        path: BoxPath<'_>,
        target: RecordIndex,
        attrs: HashMap<String, Vec<u8>>,
    ) -> std::io::Result<RecordIndex> {
        if self.core.meta.record(target).is_none() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Symlink target index {} does not exist in archive",
                    target.get()
                ),
            ));
        }

        let record = LinkRecord {
            name: Cow::Owned(path.filename().to_string()),
            target,
            attrs: self.convert_attrs(attrs)?,
        };

        self.insert_inner(path, record.into())
    }

    pub fn external_link(
        &mut self,
        path: BoxPath<'_>,
        target: &str,
        attrs: HashMap<String, Vec<u8>>,
    ) -> std::io::Result<RecordIndex> {
        self.core.header.allow_external_symlinks = true;

        let record = ExternalLinkRecord {
            name: Cow::Owned(path.filename().to_string()),
            target: Cow::Owned(target.to_string()),
            attrs: self.convert_attrs(attrs)?,
        };

        self.insert_inner(path, record.into())
    }

    pub fn insert<R: BufRead>(
        &mut self,
        config: &CompressionConfig,
        path: BoxPath<'_>,
        value: R,
        attrs: HashMap<String, Vec<u8>>,
    ) -> std::io::Result<&FileRecord<'static>> {
        let attrs = self.convert_attrs(attrs)?;
        let next_addr = self.next_write_addr();
        let byte_count = self.write_data(config, next_addr.get(), value)?;

        self.core.advance_position(byte_count.write);

        let record = FileRecord {
            compression: config.compression,
            length: byte_count.write,
            decompressed_length: byte_count.read,
            name: Cow::Owned(path.filename().to_string()),
            data: next_addr,
            attrs,
        };

        let index = self.insert_inner(path, record.into())?;

        Ok(self.core.meta.record(index).unwrap().as_file().unwrap())
    }

    fn write_data<R: BufRead>(
        &mut self,
        config: &CompressionConfig,
        pos: u64,
        mut reader: R,
    ) -> std::io::Result<ByteCount> {
        if self.file_pos != pos {
            self.file.seek(SeekFrom::Start(pos))?;
            self.file_pos = pos;
        }

        let byte_count = match config.compression {
            Compression::Stored => {
                let mut buf = vec![0u8; DEFAULT_BLOCK_SIZE as usize];
                let mut total_read = 0u64;
                let mut total_write = 0u64;
                loop {
                    let n = reader.read(&mut buf)?;
                    if n == 0 {
                        break;
                    }
                    total_read += n as u64;
                    self.file.write_all(&buf[..n])?;
                    total_write += n as u64;
                }
                ByteCount {
                    read: total_read,
                    write: total_write,
                }
            }
            #[cfg(feature = "zstd")]
            Compression::Zstd => {
                let level = config
                    .get_i32("level")
                    .unwrap_or(zstd::DEFAULT_COMPRESSION_LEVEL);
                let mut compressor = match &config.dictionary {
                    Some(dict) => ZstdCompressor::with_dictionary(level, dict)?,
                    None => ZstdCompressor::new(level)?,
                };

                let mut in_buf = vec![0u8; DEFAULT_BLOCK_SIZE as usize];
                let mut out_buf = vec![0u8; zstd_safe::compress_bound(DEFAULT_BLOCK_SIZE as usize)];
                let mut total_read = 0u64;
                let mut total_write = 0u64;

                loop {
                    let n = reader.read(&mut in_buf)?;
                    if n == 0 {
                        break;
                    }
                    total_read += n as u64;

                    let mut in_pos = 0;
                    while in_pos < n {
                        let status = compressor.compress(&in_buf[in_pos..n], &mut out_buf)?;
                        in_pos += status.bytes_consumed();
                        if status.bytes_produced() > 0 {
                            self.file.write_all(&out_buf[..status.bytes_produced()])?;
                            total_write += status.bytes_produced() as u64;
                        }
                    }
                }

                loop {
                    match compressor.finish(&mut out_buf)? {
                        StreamStatus::Done { bytes_produced, .. } => {
                            if bytes_produced > 0 {
                                self.file.write_all(&out_buf[..bytes_produced])?;
                                total_write += bytes_produced as u64;
                            }
                            break;
                        }
                        StreamStatus::Progress { bytes_produced, .. } => {
                            if bytes_produced > 0 {
                                self.file.write_all(&out_buf[..bytes_produced])?;
                                total_write += bytes_produced as u64;
                            }
                        }
                    }
                }

                ByteCount {
                    read: total_read,
                    write: total_write,
                }
            }
            #[cfg(feature = "xz")]
            Compression::Xz => {
                let level = config.get_i32("level").unwrap_or(6) as u32;
                let mut compressor = XzCompressor::new(level)?;

                let mut in_buf = vec![0u8; DEFAULT_BLOCK_SIZE as usize];
                let mut out_buf = vec![0u8; DEFAULT_BLOCK_SIZE as usize + 1024];
                let mut total_read = 0u64;
                let mut total_write = 0u64;

                loop {
                    let n = reader.read(&mut in_buf)?;
                    if n == 0 {
                        break;
                    }
                    total_read += n as u64;

                    let mut in_pos = 0;
                    while in_pos < n {
                        let status = compressor.compress(&in_buf[in_pos..n], &mut out_buf)?;
                        in_pos += status.bytes_consumed();
                        if status.bytes_produced() > 0 {
                            self.file.write_all(&out_buf[..status.bytes_produced()])?;
                            total_write += status.bytes_produced() as u64;
                        }
                        if status.bytes_consumed() == 0 && status.bytes_produced() == 0 {
                            break;
                        }
                    }
                }

                loop {
                    match compressor.finish(&mut out_buf)? {
                        StreamStatus::Done { bytes_produced, .. } => {
                            if bytes_produced > 0 {
                                self.file.write_all(&out_buf[..bytes_produced])?;
                                total_write += bytes_produced as u64;
                            }
                            break;
                        }
                        StreamStatus::Progress { bytes_produced, .. } => {
                            if bytes_produced > 0 {
                                self.file.write_all(&out_buf[..bytes_produced])?;
                                total_write += bytes_produced as u64;
                            }
                        }
                    }
                }

                ByteCount {
                    read: total_read,
                    write: total_write,
                }
            }
            Compression::Unknown(id) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Unknown compression id: {}", id),
                ));
            }
        };

        self.file_pos += byte_count.write;
        Ok(byte_count)
    }

    /// Insert a file from the filesystem.
    pub fn insert_file<P: AsRef<Path>>(
        &mut self,
        config: &CompressionConfig,
        fs_path: P,
        box_path: BoxPath<'static>,
        attrs: HashMap<String, Vec<u8>>,
    ) -> std::io::Result<&FileRecord<'static>> {
        let file = std::fs::File::open(fs_path.as_ref())?;
        let reader = std::io::BufReader::new(file);
        self.insert(config, box_path, reader, attrs)
    }
}
