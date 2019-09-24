use crate::{compression::Compression, path::BoxPath, AttrMap, BoxFileReader};
use std::num::NonZeroU64;

#[derive(Debug)]
pub enum Record {
    File(FileRecord),
    Directory(DirectoryRecord),
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
    pub fn as_directory(&self) -> Option<&DirectoryRecord> {
        match self {
            Record::Directory(dir) => Some(dir),
            _ => None,
        }
    }

    #[inline(always)]
    pub fn path(&self) -> &BoxPath {
        match self {
            Record::File(file) => file.path(),
            Record::Directory(dir) => dir.path(),
        }
    }

    #[inline(always)]
    pub fn name(&self) -> String {
        self.path()
            .to_path_buf()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string()
    }

    #[inline(always)]
    pub fn attr<S: AsRef<str>>(&self, boxfile: &BoxFileReader, key: S) -> Option<&Vec<u8>> {
        let key = boxfile.attr_key_for(key.as_ref())?;
        self.attrs().get(&key)
    }

    #[inline(always)]
    pub(crate) fn attrs(&self) -> &AttrMap {
        match self {
            Record::Directory(dir) => &dir.attrs,
            Record::File(file) => &file.attrs,
        }
    }

    #[inline(always)]
    pub(crate) fn attrs_mut(&mut self) -> &mut AttrMap {
        match self {
            Record::Directory(dir) => &mut dir.attrs,
            Record::File(file) => &mut file.attrs,
        }
    }
}

#[derive(Debug)]
pub struct DirectoryRecord {
    /// The path of the directory. A path is always relative (no leading separator),
    /// always delimited by a `UNIT SEPARATOR U+001F` (`"\x1f"`), and may not contain
    /// any `.` or `..` path chunks.
    pub path: BoxPath,

    /// Optional attributes for the given paths, such as Windows or Unix ACLs, last accessed time, etc.
    pub attrs: AttrMap,
}

impl DirectoryRecord {
    #[inline(always)]
    pub fn path(&self) -> &BoxPath {
        &self.path
    }

    #[inline(always)]
    pub fn attr<S: AsRef<str>>(&self, boxfile: &BoxFileReader, key: S) -> Option<&Vec<u8>> {
        let key = boxfile.attr_key_for(key.as_ref())?;
        self.attrs.get(&key)
    }
}

#[derive(Debug)]
pub struct FileRecord {
    /// a bytestring representing the type of compression being used, always 8 bytes.
    pub compression: Compression,

    /// The exact length of the data as written, ignoring any padding.
    pub length: u64,

    /// A hint for the size of the content when decompressed. Do not trust in absolute terms.
    pub decompressed_length: u64,

    /// The position of the data in the file
    pub data: NonZeroU64,

    /// The path of the file. A path is always relative (no leading separator),
    /// always delimited by a `UNIT SEPARATOR U+001F` (`"\x1f"`), and may not contain
    /// any `.` or `..` path chunks.
    pub path: BoxPath,

    /// Optional attributes for the given paths, such as Windows or Unix ACLs, last accessed time, etc.
    pub attrs: AttrMap,
}

impl FileRecord {
    #[inline(always)]
    pub fn compression(&self) -> Compression {
        self.compression
    }

    #[inline(always)]
    pub fn path(&self) -> &BoxPath {
        &self.path
    }

    #[inline(always)]
    pub fn attr<S: AsRef<str>>(&self, boxfile: &BoxFileReader, key: S) -> Option<&Vec<u8>> {
        let key = boxfile.attr_key_for(key.as_ref())?;
        self.attrs.get(&key)
    }
}
