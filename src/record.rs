use std::borrow::Cow;
use std::num::NonZeroU64;

use crate::file::{BoxMetadata, RecordIndex};
use crate::{AttrMap, compression::Compression};

#[derive(Debug, Clone)]
pub enum Record<'a> {
    File(FileRecord<'a>),
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
    pub fn name(&self) -> &str {
        match self {
            Record::File(file) => &file.name,
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

    #[inline(always)]
    pub(crate) fn attrs(&self) -> &AttrMap {
        match self {
            Record::Directory(dir) => &dir.attrs,
            Record::File(file) => &file.attrs,
            Record::Link(link) => &link.attrs,
            Record::ExternalLink(link) => &link.attrs,
        }
    }

    #[inline(always)]
    pub(crate) fn attrs_mut(&mut self) -> &mut AttrMap {
        match self {
            Record::Directory(dir) => &mut dir.attrs,
            Record::File(file) => &mut file.attrs,
            Record::Link(link) => &mut link.attrs,
            Record::ExternalLink(link) => &mut link.attrs,
        }
    }

    pub fn into_owned(self) -> Record<'static> {
        match self {
            Record::File(file) => Record::File(file.into_owned()),
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
