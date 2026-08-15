//! BoxItem: Wrapper around Box records for FSKit.

use box_format::{AttrValue, BOX_EPOCH_UNIX, BoxMetadata, Record, RecordIndex, attrs};

use crate::bindings::{FSItemIdentifier, FSItemType, FSKitTimeSpec};

fn box_timestamp_to_timespec(minutes: i64, seconds: u8, nanoseconds: u64) -> FSKitTimeSpec {
    let unix_seconds = i128::from(minutes)
        .saturating_mul(60)
        .saturating_add(i128::from(BOX_EPOCH_UNIX))
        .saturating_add(i128::from(seconds))
        .clamp(i64::MIN as i128, i64::MAX as i128) as i64;

    FSKitTimeSpec {
        tv_sec: unix_seconds,
        tv_nsec: nanoseconds.min(999_999_999) as i64,
    }
}

/// Wrapper around Box records that provides FSKit-compatible metadata.
#[derive(Debug)]
// [spec:box:req:fskit-extension.root]
// [spec:box:req:fskit-extension.root.record-mapping-and-cache]
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
            Record::File(_) | Record::ChunkedFile(_) => FSItemType::File,
            Record::Directory(_) => FSItemType::Directory,
            Record::Link(_) | Record::ExternalLink(_) => FSItemType::SymLink,
        }
    }

    /// Get the file mode from a Box record.
    pub fn mode(record: &Record<'_>, meta: &BoxMetadata) -> u32 {
        match record.attr(meta, attrs::UNIX_MODE) {
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
            Record::File(_) | Record::ChunkedFile(_) => 0o644,
            Record::Directory(_) => 0o755,
            Record::Link(_) | Record::ExternalLink(_) => 0o777,
        }
    }

    /// Get the uid from a Box record.
    pub fn uid(record: &Record<'_>, meta: &BoxMetadata) -> u32 {
        match record.attr(meta, attrs::UNIX_UID) {
            Some(bytes) => {
                let (v, len) = fastvint::decode_vu32_slice(bytes);
                if len > 0 { v } else { 501 }
            }
            None => 501, // Default to current user
        }
    }

    /// Get the gid from a Box record.
    pub fn gid(record: &Record<'_>, meta: &BoxMetadata) -> u32 {
        match record.attr(meta, attrs::UNIX_GID) {
            Some(bytes) => {
                let (v, len) = fastvint::decode_vu32_slice(bytes);
                if len > 0 { v } else { 20 }
            }
            None => 20, // Default staff group
        }
    }

    /// Get the creation time from a Box record.
    pub fn ctime(record: &Record<'_>, meta: &BoxMetadata) -> FSKitTimeSpec {
        Self::get_time(
            record,
            meta,
            attrs::CREATED,
            attrs::CREATED_SECONDS,
            attrs::CREATED_NANOSECONDS,
        )
    }

    /// Get the modification time from a Box record.
    pub fn mtime(record: &Record<'_>, meta: &BoxMetadata) -> FSKitTimeSpec {
        Self::get_time(
            record,
            meta,
            attrs::MODIFIED,
            attrs::MODIFIED_SECONDS,
            attrs::MODIFIED_NANOSECONDS,
        )
    }

    /// Get the access time from a Box record.
    pub fn atime(record: &Record<'_>, meta: &BoxMetadata) -> FSKitTimeSpec {
        Self::get_time(
            record,
            meta,
            attrs::ACCESSED,
            attrs::ACCESSED_SECONDS,
            attrs::ACCESSED_NANOSECONDS,
        )
    }

    fn attr_value<'a>(
        record: &'a Record<'_>,
        meta: &'a BoxMetadata,
        attr_name: &str,
    ) -> Option<AttrValue<'a>> {
        record.attr_value(meta, attr_name).or_else(|| {
            let key = meta.attr_key(attr_name)?;
            let attr_type = meta.attr_key_type(key)?;
            let raw = meta.file_attr(attr_name)?;
            Some(meta.parse_attr_value(raw, attr_type))
        })
    }

    fn get_time(
        record: &Record<'_>,
        meta: &BoxMetadata,
        minutes_key: &str,
        seconds_key: &str,
        nanoseconds_key: &str,
    ) -> FSKitTimeSpec {
        let minutes = match Self::attr_value(record, meta, minutes_key) {
            Some(AttrValue::DateTime(minutes)) => minutes,
            _ => return FSKitTimeSpec::default(),
        };
        let seconds = match Self::attr_value(record, meta, seconds_key) {
            Some(AttrValue::U8(seconds @ 0..=59)) => seconds,
            _ => 0,
        };
        let nanoseconds = match Self::attr_value(record, meta, nanoseconds_key) {
            Some(AttrValue::Vu64(nanoseconds @ 0..=999_999_999)) => nanoseconds,
            _ => 0,
        };
        box_timestamp_to_timespec(minutes, seconds, nanoseconds)
    }

    /// Get the file size from a Box record.
    pub fn size(record: &Record<'_>) -> u64 {
        match record {
            Record::File(f) => f.decompressed_length,
            Record::ChunkedFile(f) => f.decompressed_length,
            Record::Directory(d) => d.entries.len() as u64,
            Record::Link(_) | Record::ExternalLink(_) => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use box_format::{BoxPath, Compression, CompressionConfig, HashMap};

    // [spec:box:req:fskit-extension.root.record-mapping-and-cache/test/unit]
    #[test]
    fn timestamp_arithmetic_is_saturating() {
        assert_eq!(
            box_timestamp_to_timespec(0, 7, 123),
            FSKitTimeSpec {
                tv_sec: BOX_EPOCH_UNIX + 7,
                tv_nsec: 123,
            }
        );
        assert_eq!(box_timestamp_to_timespec(i64::MIN, 0, 0).tv_sec, i64::MIN);
        assert_eq!(
            box_timestamp_to_timespec(i64::MAX, 59, u64::MAX).tv_sec,
            i64::MAX
        );
        assert_eq!(
            box_timestamp_to_timespec(i64::MAX, 59, u64::MAX).tv_nsec,
            999_999_999
        );
    }

    #[test]
    fn record_timestamp_mapping_uses_typed_precision_components() {
        let temp = tempfile::tempdir().unwrap();
        let archive = temp.path().join("fskit-timestamps.box");
        let mut writer = box_format::sync::BoxWriter::create(&archive).unwrap();
        let mut file_attrs = HashMap::new();
        file_attrs.insert(
            attrs::MODIFIED.to_string(),
            fastvint::Vi64::new(12).bytes().to_vec(),
        );
        file_attrs.insert(attrs::MODIFIED_SECONDS.to_string(), vec![7]);
        file_attrs.insert(
            attrs::MODIFIED_NANOSECONDS.to_string(),
            fastvint::Vu64::new(123_456_789).bytes().to_vec(),
        );
        writer
            .insert(
                &CompressionConfig::new(Compression::Stored),
                BoxPath::new("timestamped.bin").unwrap(),
                std::io::Cursor::new(b"timestamped"),
                file_attrs,
            )
            .unwrap();
        writer.finish().unwrap();

        let reader = box_format::sync::BoxReader::open(archive).unwrap();
        let record = reader
            .metadata()
            .record(
                reader
                    .metadata()
                    .index(&BoxPath::new("timestamped.bin").unwrap())
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(
            BoxItem::mtime(record, reader.metadata()),
            FSKitTimeSpec {
                tv_sec: BOX_EPOCH_UNIX + 12 * 60 + 7,
                tv_nsec: 123_456_789,
            }
        );
    }
}
