//! BoxItem: Wrapper around Box records for FSKit.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use box_format::{BOX_EPOCH_UNIX, BoxMetadata, Record, RecordIndex};

use crate::bindings::{FSItemIdentifier, FSItemType};

/// Wrapper around Box records that provides FSKit-compatible metadata.
#[derive(Debug)]
pub struct BoxItem {
    /// The record index in the Box archive.
    index: RecordIndex,
}

impl BoxItem {
    /// Create a new BoxItem for a record.
    pub fn new(index: RecordIndex) -> Self {
        Self { index }
    }

    /// Get the FSKit item identifier.
    ///
    /// We use index + 2 because:
    /// - 0 is invalid
    /// - 1 is reserved for root directory
    /// - 2+ are actual records
    pub fn identifier(&self) -> FSItemIdentifier {
        FSItemIdentifier::new(self.index.get() + 2)
    }

    /// Get the record index.
    pub fn record_index(&self) -> RecordIndex {
        self.index
    }

    /// Convert an FSKit identifier back to a record index.
    pub fn index_from_identifier(id: FSItemIdentifier) -> Option<RecordIndex> {
        if id.raw() < 2 {
            None
        } else {
            RecordIndex::new(id.raw() - 2).ok()
        }
    }

    /// Get the FSItemType for a Box record.
    pub fn item_type(record: &Record<'_>) -> FSItemType {
        match record {
            Record::File(_) => FSItemType::File,
            Record::Directory(_) => FSItemType::Directory,
            Record::Link(_) => FSItemType::SymLink,
        }
    }

    /// Get the file mode from a Box record.
    pub fn mode(record: &Record<'_>, meta: &BoxMetadata) -> u32 {
        match record.attr(meta, "unix.mode") {
            Some(bytes) => {
                let (mode, len) = fastvint::decode_vu32_slice(bytes);
                if len > 0 {
                    mode & 0o7777
                } else {
                    Self::default_mode(record)
                }
            }
            None => Self::default_mode(record),
        }
    }

    fn default_mode(record: &Record<'_>) -> u32 {
        match record {
            Record::File(_) => 0o644,
            Record::Directory(_) => 0o755,
            Record::Link(_) => 0o777,
        }
    }

    /// Get the uid from a Box record.
    pub fn uid(record: &Record<'_>, meta: &BoxMetadata) -> u32 {
        match record.attr(meta, "unix.uid") {
            Some(bytes) => {
                let (v, len) = fastvint::decode_vu32_slice(bytes);
                if len > 0 { v } else { 501 }
            }
            None => 501, // Default to current user
        }
    }

    /// Get the gid from a Box record.
    pub fn gid(record: &Record<'_>, meta: &BoxMetadata) -> u32 {
        match record.attr(meta, "unix.gid") {
            Some(bytes) => {
                let (v, len) = fastvint::decode_vu32_slice(bytes);
                if len > 0 { v } else { 20 }
            }
            None => 20, // Default staff group
        }
    }

    /// Get the creation time from a Box record.
    pub fn ctime(record: &Record<'_>, meta: &BoxMetadata) -> SystemTime {
        Self::get_time(record, meta, "created")
    }

    /// Get the modification time from a Box record.
    pub fn mtime(record: &Record<'_>, meta: &BoxMetadata) -> SystemTime {
        Self::get_time(record, meta, "modified")
    }

    /// Get the access time from a Box record.
    pub fn atime(record: &Record<'_>, meta: &BoxMetadata) -> SystemTime {
        Self::get_time(record, meta, "accessed")
    }

    fn get_time(record: &Record<'_>, meta: &BoxMetadata, attr_name: &str) -> SystemTime {
        match record.attr(meta, attr_name) {
            Some(bytes) => {
                let (minutes, len) = fastvint::decode_vi64_slice(bytes);
                if len > 0 {
                    let unix_secs = (minutes * 60 + BOX_EPOCH_UNIX) as u64;
                    UNIX_EPOCH
                        .checked_add(Duration::from_secs(unix_secs))
                        .unwrap_or(UNIX_EPOCH)
                } else {
                    UNIX_EPOCH
                }
            }
            None => UNIX_EPOCH,
        }
    }

    /// Get the file size from a Box record.
    pub fn size(record: &Record<'_>) -> u64 {
        match record {
            Record::File(f) => f.decompressed_length,
            Record::Directory(d) => d.entries.len() as u64,
            Record::Link(_) => 0,
        }
    }
}
