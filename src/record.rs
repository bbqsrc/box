use std::borrow::Cow;
use std::num::NonZeroU64;

use crate::AttrValue;
use crate::core::{BoxMetadata, RecordIndex};
use crate::{AttrMap, compression::Compression};

#[derive(Debug, Clone)]
pub enum Record<'a> {
    File(FileRecord<'a>),
    ChunkedFile(ChunkedFileRecord<'a>),
    Directory(DirectoryRecord<'a>),
    Link(LinkRecord<'a>),
    ExternalLink(ExternalLinkRecord<'a>),
}

impl<'a> Record<'a> {
    #[inline(always)]
    pub fn as_file(&self) -> Option<&FileRecord<'a>> {
        match self {
            Record::File(file) => Some(file),
            _ => None,
        }
    }

    #[inline(always)]
    pub fn as_file_mut(&mut self) -> Option<&mut FileRecord<'a>> {
        match self {
            Record::File(file) => Some(file),
            _ => None,
        }
    }

    #[inline(always)]
    pub fn as_directory(&self) -> Option<&DirectoryRecord<'a>> {
        match self {
            Record::Directory(dir) => Some(dir),
            _ => None,
        }
    }

    #[inline(always)]
    pub fn as_directory_mut(&mut self) -> Option<&mut DirectoryRecord<'a>> {
        match self {
            Record::Directory(dir) => Some(dir),
            _ => None,
        }
    }

    #[inline(always)]
    pub fn as_link(&self) -> Option<&LinkRecord<'a>> {
        match self {
            Record::Link(link) => Some(link),
            _ => None,
        }
    }

    #[inline(always)]
    pub fn as_link_mut(&mut self) -> Option<&mut LinkRecord<'a>> {
        match self {
            Record::Link(link) => Some(link),
            _ => None,
        }
    }

    #[inline(always)]
    pub fn as_external_link(&self) -> Option<&ExternalLinkRecord<'a>> {
        match self {
            Record::ExternalLink(link) => Some(link),
            _ => None,
        }
    }

    #[inline(always)]
    pub fn as_external_link_mut(&mut self) -> Option<&mut ExternalLinkRecord<'a>> {
        match self {
            Record::ExternalLink(link) => Some(link),
            _ => None,
        }
    }

    #[inline(always)]
    pub fn as_chunked_file(&self) -> Option<&ChunkedFileRecord<'a>> {
        match self {
            Record::ChunkedFile(file) => Some(file),
            _ => None,
        }
    }

    #[inline(always)]
    pub fn as_chunked_file_mut(&mut self) -> Option<&mut ChunkedFileRecord<'a>> {
        match self {
            Record::ChunkedFile(file) => Some(file),
            _ => None,
        }
    }

    #[inline(always)]
    pub fn name(&self) -> &str {
        match self {
            Record::File(file) => &file.name,
            Record::ChunkedFile(file) => &file.name,
            Record::Directory(dir) => &dir.name,
            Record::Link(link) => &link.name,
            Record::ExternalLink(link) => &link.name,
        }
    }

    #[inline(always)]
    pub fn attr<S: AsRef<str>>(&self, metadata: &BoxMetadata<'_>, key: S) -> Option<&[u8]> {
        let key = metadata.attr_key(key.as_ref())?;
        self.attrs().get(&key).map(|x| &**x)
    }

    /// Get a typed attribute value.
    #[inline(always)]
    pub fn attr_value<S: AsRef<str>>(
        &self,
        metadata: &BoxMetadata<'_>,
        key: S,
    ) -> Option<AttrValue<'_>> {
        let key_idx = metadata.attr_key(key.as_ref())?;
        let attr_type = metadata.attr_key_type(key_idx)?;
        let raw = self.attrs().get(&key_idx)?;
        Some(metadata.parse_attr_value(raw, attr_type))
    }

    #[inline(always)]
    pub(crate) fn attrs(&self) -> &AttrMap {
        match self {
            Record::Directory(dir) => &dir.attrs,
            Record::File(file) => &file.attrs,
            Record::ChunkedFile(file) => &file.attrs,
            Record::Link(link) => &link.attrs,
            Record::ExternalLink(link) => &link.attrs,
        }
    }

    #[inline(always)]
    pub(crate) fn attrs_mut(&mut self) -> &mut AttrMap {
        match self {
            Record::Directory(dir) => &mut dir.attrs,
            Record::File(file) => &mut file.attrs,
            Record::ChunkedFile(file) => &mut file.attrs,
            Record::Link(link) => &mut link.attrs,
            Record::ExternalLink(link) => &mut link.attrs,
        }
    }

    /// Iterate over attributes with resolved key names.
    pub fn attrs_iter<'b>(
        &'b self,
        metadata: &'b BoxMetadata<'_>,
    ) -> impl Iterator<Item = (&'b str, &'b [u8])> {
        self.attrs().iter().filter_map(move |(key_idx, value)| {
            metadata
                .attr_key_name(*key_idx)
                .map(|name| (name, &**value))
        })
    }

    pub fn into_owned(self) -> Record<'static> {
        match self {
            Record::File(file) => Record::File(file.into_owned()),
            Record::ChunkedFile(file) => Record::ChunkedFile(file.into_owned()),
            Record::Directory(dir) => Record::Directory(dir.into_owned()),
            Record::Link(link) => Record::Link(link.into_owned()),
            Record::ExternalLink(link) => Record::ExternalLink(link.into_owned()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LinkRecord<'a> {
    pub name: Cow<'a, str>,

    /// The target record index. During extraction, this is resolved to compute
    /// the relative path from the link's location to the target's location.
    pub target: RecordIndex,

    /// Optional attributes for the given paths, such as Windows or Unix ACLs, last accessed time, etc.
    pub attrs: AttrMap,
}

impl LinkRecord<'_> {
    #[inline(always)]
    pub fn attr<S: AsRef<str>>(&self, metadata: &BoxMetadata<'_>, key: S) -> Option<&[u8]> {
        let key = metadata.attr_key(key.as_ref())?;
        self.attrs.get(&key).map(|x| &**x)
    }

    pub fn into_owned(self) -> LinkRecord<'static> {
        LinkRecord {
            name: Cow::Owned(self.name.into_owned()),
            target: self.target,
            attrs: self.attrs,
        }
    }
}

impl<'a> From<LinkRecord<'a>> for Record<'a> {
    fn from(link: LinkRecord<'a>) -> Self {
        Record::Link(link)
    }
}

/// A symlink that points outside the archive (external target).
#[derive(Debug, Clone)]
pub struct ExternalLinkRecord<'a> {
    pub name: Cow<'a, str>,

    /// The target path, normalized and relative (e.g., "../../../etc/environment").
    /// Uses `/` as separator.
    pub target: Cow<'a, str>,

    /// Optional attributes for the given paths, such as Windows or Unix ACLs, last accessed time, etc.
    pub attrs: AttrMap,
}

impl ExternalLinkRecord<'_> {
    #[inline(always)]
    pub fn attr<S: AsRef<str>>(&self, metadata: &BoxMetadata<'_>, key: S) -> Option<&[u8]> {
        let key = metadata.attr_key(key.as_ref())?;
        self.attrs.get(&key).map(|x| &**x)
    }

    pub fn into_owned(self) -> ExternalLinkRecord<'static> {
        ExternalLinkRecord {
            name: Cow::Owned(self.name.into_owned()),
            target: Cow::Owned(self.target.into_owned()),
            attrs: self.attrs,
        }
    }
}

impl<'a> From<ExternalLinkRecord<'a>> for Record<'a> {
    fn from(link: ExternalLinkRecord<'a>) -> Self {
        Record::ExternalLink(link)
    }
}

#[derive(Debug, Clone)]
pub struct DirectoryRecord<'a> {
    /// The name of the directory
    pub name: Cow<'a, str>,

    /// List of child record indices
    pub entries: Vec<RecordIndex>,

    /// Optional attributes for the given paths, such as Windows or Unix ACLs, last accessed time, etc.
    pub attrs: AttrMap,
}

impl DirectoryRecord<'static> {
    pub fn new(name: String) -> DirectoryRecord<'static> {
        DirectoryRecord {
            name: Cow::Owned(name),
            entries: vec![],
            attrs: AttrMap::new(),
        }
    }
}

impl DirectoryRecord<'_> {
    #[inline(always)]
    pub fn attr<S: AsRef<str>>(&self, metadata: &BoxMetadata<'_>, key: S) -> Option<&[u8]> {
        let key = metadata.attr_key(key.as_ref())?;
        self.attrs.get(&key).map(|x| &**x)
    }

    pub fn into_owned(self) -> DirectoryRecord<'static> {
        DirectoryRecord {
            name: Cow::Owned(self.name.into_owned()),
            entries: self.entries,
            attrs: self.attrs,
        }
    }
}

impl<'a> From<DirectoryRecord<'a>> for Record<'a> {
    fn from(dir: DirectoryRecord<'a>) -> Self {
        Record::Directory(dir)
    }
}

#[derive(Debug, Clone)]
pub struct FileRecord<'a> {
    /// a bytestring representing the type of compression being used, always 8 bytes.
    pub compression: Compression,

    /// The exact length of the data as written, ignoring any padding.
    pub length: u64,

    /// A hint for the size of the content when decompressed. Do not trust in absolute terms.
    pub decompressed_length: u64,

    /// The position of the data in the file
    pub data: NonZeroU64,

    /// The name of the file
    pub name: Cow<'a, str>,

    /// Optional attributes for the given paths, such as Windows or Unix ACLs, last accessed time, etc.
    pub attrs: AttrMap,
}

impl FileRecord<'_> {
    #[inline(always)]
    pub fn compression(&self) -> Compression {
        self.compression
    }

    #[inline(always)]
    pub fn attr<S: AsRef<str>>(&self, metadata: &BoxMetadata<'_>, key: S) -> Option<&[u8]> {
        let key = metadata.attr_key(key.as_ref())?;
        self.attrs.get(&key).map(|x| &**x)
    }

    pub fn into_owned(self) -> FileRecord<'static> {
        FileRecord {
            compression: self.compression,
            length: self.length,
            decompressed_length: self.decompressed_length,
            data: self.data,
            name: Cow::Owned(self.name.into_owned()),
            attrs: self.attrs,
        }
    }
}

impl<'a> From<FileRecord<'a>> for Record<'a> {
    fn from(file: FileRecord<'a>) -> Self {
        Record::File(file)
    }
}

/// A file record for large files stored as independently-compressed blocks.
/// This allows random access and parallel decompression.
#[derive(Debug, Clone)]
pub struct ChunkedFileRecord<'a> {
    /// The compression algorithm used for each block.
    pub compression: Compression,

    /// The size of each block before compression (last block may be smaller).
    pub block_size: u32,

    /// The exact length of the compressed data as written.
    pub length: u64,

    /// The total decompressed size of all blocks combined.
    pub decompressed_length: u64,

    /// The position of the first block's data in the file.
    pub data: NonZeroU64,

    /// The name of the file.
    pub name: Cow<'a, str>,

    /// Optional attributes for the given paths, such as Windows or Unix ACLs, last accessed time, etc.
    pub attrs: AttrMap,
}

impl ChunkedFileRecord<'_> {
    #[inline(always)]
    pub fn compression(&self) -> Compression {
        self.compression
    }

    #[inline(always)]
    pub fn attr<S: AsRef<str>>(&self, metadata: &BoxMetadata<'_>, key: S) -> Option<&[u8]> {
        let key = metadata.attr_key(key.as_ref())?;
        self.attrs.get(&key).map(|x| &**x)
    }

    /// Returns the number of blocks in this chunked file.
    pub fn block_count(&self) -> u64 {
        if self.decompressed_length == 0 {
            0
        } else {
            (self.decompressed_length + self.block_size as u64 - 1) / self.block_size as u64
        }
    }

    pub fn into_owned(self) -> ChunkedFileRecord<'static> {
        ChunkedFileRecord {
            compression: self.compression,
            block_size: self.block_size,
            length: self.length,
            decompressed_length: self.decompressed_length,
            data: self.data,
            name: Cow::Owned(self.name.into_owned()),
            attrs: self.attrs,
        }
    }
}

impl<'a> From<ChunkedFileRecord<'a>> for Record<'a> {
    fn from(file: ChunkedFileRecord<'a>) -> Self {
        Record::ChunkedFile(file)
    }
}
