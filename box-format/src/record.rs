use std::borrow::Cow;
use std::num::NonZeroU64;

use crate::file::{BoxMetadata, RecordIndex};
use crate::{AttrMap, compression::Compression, path::BoxPath};

#[derive(Debug, Clone)]
pub enum Record<'a> {
    File(FileRecord<'a>),
    Directory(DirectoryRecord<'a>),
    Link(LinkRecord<'a>),
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
    pub fn name(&self) -> &str {
        match self {
            Record::File(file) => &file.name,
            Record::Directory(dir) => &dir.name,
            Record::Link(link) => &link.name,
        }
    }

    #[inline(always)]
    pub fn attr<S: AsRef<str>>(&self, metadata: &BoxMetadata, key: S) -> Option<&[u8]> {
        let key = metadata.attr_key(key.as_ref())?;
        self.attrs().get(&key).map(|x| &**x)
    }

    #[inline(always)]
    pub(crate) fn attrs(&self) -> &AttrMap {
        match self {
            Record::Directory(dir) => &dir.attrs,
            Record::File(file) => &file.attrs,
            Record::Link(link) => &link.attrs,
        }
    }

    #[inline(always)]
    pub(crate) fn attrs_mut(&mut self) -> &mut AttrMap {
        match self {
            Record::Directory(dir) => &mut dir.attrs,
            Record::File(file) => &mut file.attrs,
            Record::Link(link) => &mut link.attrs,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LinkRecord<'a> {
    pub name: Cow<'a, str>,

    /// The target path of the symbolic link, which is the place the link points to. A path is always relative (no leading separator),
    /// always delimited by a `UNIT SEPARATOR U+001F` (`"\x1f"`), and may not contain
    /// any `.` or `..` path chunks.
    pub target: BoxPath<'a>,

    /// Optional attributes for the given paths, such as Windows or Unix ACLs, last accessed time, etc.
    pub attrs: AttrMap,
}

impl LinkRecord<'_> {
    #[inline(always)]
    pub fn attr<S: AsRef<str>>(&self, metadata: &BoxMetadata, key: S) -> Option<&[u8]> {
        let key = metadata.attr_key(key.as_ref())?;
        self.attrs.get(&key).map(|x| &**x)
    }
}

impl<'a> From<LinkRecord<'a>> for Record<'a> {
    fn from(link: LinkRecord<'a>) -> Self {
        Record::Link(link)
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
    pub fn attr<S: AsRef<str>>(&self, metadata: &BoxMetadata, key: S) -> Option<&[u8]> {
        let key = metadata.attr_key(key.as_ref())?;
        self.attrs.get(&key).map(|x| &**x)
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
    pub fn attr<S: AsRef<str>>(&self, metadata: &BoxMetadata, key: S) -> Option<&[u8]> {
        let key = metadata.attr_key(key.as_ref())?;
        self.attrs.get(&key).map(|x| &**x)
    }
}

impl<'a> From<FileRecord<'a>> for Record<'a> {
    fn from(file: FileRecord<'a>) -> Self {
        Record::File(file)
    }
}
