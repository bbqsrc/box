use crate::{compression::Compression, path::BoxPath, AttrMap};

use crate::file::{BoxMetadata, Inode};
use std::num::NonZeroU64;

#[derive(Debug, Clone)]
pub enum Record {
    File(FileRecord),
    Directory(DirectoryRecord),
    Link(LinkRecord),
}

impl Record {
    #[inline(always)]
    pub fn as_file(&self) -> Option<&FileRecord> {
        match self {
            Record::File(file) => Some(file),
            _ => None,
        }
    }

    #[inline(always)]
    pub fn as_file_mut(&mut self) -> Option<&mut FileRecord> {
        match self {
            Record::File(file) => Some(file),
            _ => None,
        }
    }

    #[inline(always)]
    pub fn as_directory(&self) -> Option<&DirectoryRecord> {
        match self {
            Record::Directory(dir) => Some(dir),
            _ => None,
        }
    }

    #[inline(always)]
    pub fn as_directory_mut(&mut self) -> Option<&mut DirectoryRecord> {
        match self {
            Record::Directory(dir) => Some(dir),
            _ => None,
        }
    }

    #[inline(always)]
    pub fn as_link(&self) -> Option<&LinkRecord> {
        match self {
            Record::Link(link) => Some(link),
            _ => None,
        }
    }

    #[inline(always)]
    pub fn as_link_mut(&mut self) -> Option<&mut LinkRecord> {
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
pub struct LinkRecord {
    pub name: String,

    /// The target path of the symbolic link, which is the place the link points to. A path is always relative (no leading separator),
    /// always delimited by a `UNIT SEPARATOR U+001F` (`"\x1f"`), and may not contain
    /// any `.` or `..` path chunks.
    pub target: BoxPath,

    /// Optional attributes for the given paths, such as Windows or Unix ACLs, last accessed time, etc.
    pub attrs: AttrMap,
}

impl LinkRecord {
    #[inline(always)]
    pub fn attr<S: AsRef<str>>(&self, metadata: &BoxMetadata, key: S) -> Option<&[u8]> {
        let key = metadata.attr_key(key.as_ref())?;
        self.attrs.get(&key).map(|x| &**x)
    }

    #[inline(always)]
    pub fn upcast(self) -> Record {
        Record::Link(self)
    }
}

#[derive(Debug, Clone)]
pub struct DirectoryRecord {
    /// The name of the directory
    pub name: String, // TODO: BoxName

    /// List of inodes
    pub inodes: Vec<Inode>,

    /// Optional attributes for the given paths, such as Windows or Unix ACLs, last accessed time, etc.
    pub attrs: AttrMap,
}

impl DirectoryRecord {
    pub fn new(name: String) -> DirectoryRecord {
        DirectoryRecord {
            name,
            inodes: vec![],
            attrs: AttrMap::new(),
        }
    }

    #[inline(always)]
    pub fn attr<S: AsRef<str>>(&self, metadata: &BoxMetadata, key: S) -> Option<&[u8]> {
        let key = metadata.attr_key(key.as_ref())?;
        self.attrs.get(&key).map(|x| &**x)
    }

    #[inline(always)]
    pub fn upcast(self) -> Record {
        Record::Directory(self)
    }
}

#[derive(Debug, Clone)]
pub struct FileRecord {
    /// a bytestring representing the type of compression being used, always 8 bytes.
    pub compression: Compression,

    /// The exact length of the data as written, ignoring any padding.
    pub length: u64,

    /// A hint for the size of the content when decompressed. Do not trust in absolute terms.
    pub decompressed_length: u64,

    /// The position of the data in the file
    pub data: NonZeroU64,

    /// The name of the file
    pub name: String, // TODO: add BoxName

    /// Optional attributes for the given paths, such as Windows or Unix ACLs, last accessed time, etc.
    pub attrs: AttrMap,
}

impl FileRecord {
    #[inline(always)]
    pub fn compression(&self) -> Compression {
        self.compression
    }

    #[inline(always)]
    pub fn attr<S: AsRef<str>>(&self, metadata: &BoxMetadata, key: S) -> Option<&[u8]> {
        let key = metadata.attr_key(key.as_ref())?;
        self.attrs.get(&key).map(|x| &**x)
    }

    #[inline(always)]
    pub fn upcast(self) -> Record {
        Record::File(self)
    }
}
