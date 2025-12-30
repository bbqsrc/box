//! Record types for Box archive entries.

use core::num::NonZeroU64;

#[cfg(feature = "alloc")]
use alloc::borrow::Cow;
#[cfg(feature = "alloc")]
use alloc::string::String;
#[cfg(feature = "alloc")]
use alloc::vec::Vec;

#[cfg(feature = "alloc")]
use crate::compression::Compression;
#[cfg(feature = "alloc")]
use crate::path::BoxPath;
#[cfg(feature = "alloc")]
use crate::AttrMap;

/// A 1-based index into the records array.
///
/// Uses NonZeroU64 internally to allow Option<RecordIndex> to be the same size.
/// This also provides compatibility with platforms like Linux that reserve 0.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct RecordIndex(NonZeroU64);

impl RecordIndex {
    /// Create a new RecordIndex from a u64 value.
    ///
    /// Returns an error if the value is zero.
    pub fn new(value: u64) -> Result<RecordIndex, RecordIndexError> {
        match NonZeroU64::new(value) {
            Some(v) => Ok(RecordIndex(v)),
            None => Err(RecordIndexError::Zero),
        }
    }

    /// Get the raw u64 value.
    pub fn get(self) -> u64 {
        self.0.get()
    }

    /// Convert to array index (0-based).
    pub fn to_array_index(self) -> usize {
        (self.0.get() - 1) as usize
    }
}

/// Error when creating a RecordIndex.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordIndexError {
    /// Record index cannot be zero.
    Zero,
}

impl core::fmt::Display for RecordIndexError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            RecordIndexError::Zero => write!(f, "record index must not be zero"),
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for RecordIndexError {}

/// A record in the Box archive.
#[derive(Debug, Clone)]
#[cfg(feature = "alloc")]
pub enum Record<'a> {
    File(FileRecord<'a>),
    Directory(DirectoryRecord<'a>),
    Link(LinkRecord<'a>),
}

#[cfg(feature = "alloc")]
impl<'a> Record<'a> {
    /// Get as a file record, if this is a file.
    #[inline(always)]
    pub fn as_file(&self) -> Option<&FileRecord<'a>> {
        match self {
            Record::File(file) => Some(file),
            _ => None,
        }
    }

    /// Get as a mutable file record, if this is a file.
    #[inline(always)]
    pub fn as_file_mut(&mut self) -> Option<&mut FileRecord<'a>> {
        match self {
            Record::File(file) => Some(file),
            _ => None,
        }
    }

    /// Get as a directory record, if this is a directory.
    #[inline(always)]
    pub fn as_directory(&self) -> Option<&DirectoryRecord<'a>> {
        match self {
            Record::Directory(dir) => Some(dir),
            _ => None,
        }
    }

    /// Get as a mutable directory record, if this is a directory.
    #[inline(always)]
    pub fn as_directory_mut(&mut self) -> Option<&mut DirectoryRecord<'a>> {
        match self {
            Record::Directory(dir) => Some(dir),
            _ => None,
        }
    }

    /// Get as a link record, if this is a link.
    #[inline(always)]
    pub fn as_link(&self) -> Option<&LinkRecord<'a>> {
        match self {
            Record::Link(link) => Some(link),
            _ => None,
        }
    }

    /// Get as a mutable link record, if this is a link.
    #[inline(always)]
    pub fn as_link_mut(&mut self) -> Option<&mut LinkRecord<'a>> {
        match self {
            Record::Link(link) => Some(link),
            _ => None,
        }
    }

    /// Get the name of this record.
    #[inline(always)]
    pub fn name(&self) -> &str {
        match self {
            Record::File(file) => &file.name,
            Record::Directory(dir) => &dir.name,
            Record::Link(link) => &link.name,
        }
    }

    /// Get the attributes map for this record.
    #[inline(always)]
    pub fn attrs(&self) -> &AttrMap {
        match self {
            Record::Directory(dir) => &dir.attrs,
            Record::File(file) => &file.attrs,
            Record::Link(link) => &link.attrs,
        }
    }

    /// Get the mutable attributes map for this record.
    #[inline(always)]
    pub fn attrs_mut(&mut self) -> &mut AttrMap {
        match self {
            Record::Directory(dir) => &mut dir.attrs,
            Record::File(file) => &mut file.attrs,
            Record::Link(link) => &mut link.attrs,
        }
    }

    /// Convert to an owned record with 'static lifetime.
    pub fn into_owned(self) -> Record<'static> {
        match self {
            Record::File(file) => Record::File(file.into_owned()),
            Record::Directory(dir) => Record::Directory(dir.into_owned()),
            Record::Link(link) => Record::Link(link.into_owned()),
        }
    }
}

/// A file record.
#[derive(Debug, Clone)]
#[cfg(feature = "alloc")]
pub struct FileRecord<'a> {
    /// The compression type used for this file's data.
    pub compression: Compression,

    /// The exact length of the compressed data.
    pub length: u64,

    /// A hint for the decompressed size. Do not trust absolutely.
    pub decompressed_length: u64,

    /// The offset of the file data in the archive.
    pub data: NonZeroU64,

    /// The name of the file.
    pub name: Cow<'a, str>,

    /// Optional attributes (permissions, timestamps, etc.).
    pub attrs: AttrMap,
}

#[cfg(feature = "alloc")]
impl FileRecord<'_> {
    /// Get the compression type.
    #[inline(always)]
    pub fn compression(&self) -> Compression {
        self.compression
    }

    /// Convert to an owned record with 'static lifetime.
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

#[cfg(feature = "alloc")]
impl<'a> From<FileRecord<'a>> for Record<'a> {
    fn from(file: FileRecord<'a>) -> Self {
        Record::File(file)
    }
}

/// A directory record.
#[derive(Debug, Clone)]
#[cfg(feature = "alloc")]
pub struct DirectoryRecord<'a> {
    /// The name of the directory.
    pub name: Cow<'a, str>,

    /// List of child record indices.
    pub entries: Vec<RecordIndex>,

    /// Optional attributes (permissions, timestamps, etc.).
    pub attrs: AttrMap,
}

#[cfg(feature = "alloc")]
impl DirectoryRecord<'static> {
    /// Create a new directory record with the given name.
    pub fn new(name: String) -> DirectoryRecord<'static> {
        DirectoryRecord {
            name: Cow::Owned(name),
            entries: Vec::new(),
            attrs: AttrMap::new(),
        }
    }
}

#[cfg(feature = "alloc")]
impl DirectoryRecord<'_> {
    /// Convert to an owned record with 'static lifetime.
    pub fn into_owned(self) -> DirectoryRecord<'static> {
        DirectoryRecord {
            name: Cow::Owned(self.name.into_owned()),
            entries: self.entries,
            attrs: self.attrs,
        }
    }
}

#[cfg(feature = "alloc")]
impl<'a> From<DirectoryRecord<'a>> for Record<'a> {
    fn from(dir: DirectoryRecord<'a>) -> Self {
        Record::Directory(dir)
    }
}

/// A symbolic link record.
#[derive(Debug, Clone)]
#[cfg(feature = "alloc")]
pub struct LinkRecord<'a> {
    /// The name of the link.
    pub name: Cow<'a, str>,

    /// The target path of the symbolic link.
    pub target: BoxPath<'a>,

    /// Optional attributes (permissions, timestamps, etc.).
    pub attrs: AttrMap,
}

#[cfg(feature = "alloc")]
impl LinkRecord<'_> {
    /// Convert to an owned record with 'static lifetime.
    pub fn into_owned(self) -> LinkRecord<'static> {
        LinkRecord {
            name: Cow::Owned(self.name.into_owned()),
            target: self.target.into_owned(),
            attrs: self.attrs,
        }
    }
}

#[cfg(feature = "alloc")]
impl<'a> From<LinkRecord<'a>> for Record<'a> {
    fn from(link: LinkRecord<'a>) -> Self {
        Record::Link(link)
    }
}
