//! Time conversion utilities for Box epoch to Windows FILETIME.

use box_format::{AttrValue, BoxMetadata, Record};

pub use box_format::BOX_EPOCH_UNIX;

/// Difference between Unix epoch (1970-01-01) and Windows epoch (1601-01-01)
/// in 100-nanosecond intervals.
const UNIX_TO_WINDOWS_EPOCH_DIFF: i64 = 116444736000000000;

const HUNDRED_NANOSECONDS_PER_SECOND: i128 = 10_000_000;
const SECONDS_PER_MINUTE: i128 = 60;

// [spec:box:def:attributes.root.standard-keys]
pub(crate) fn record_timestamp_minutes(
    metadata: &BoxMetadata<'_>,
    record: &Record<'_>,
    attr: &str,
) -> Option<i64> {
    if let Some(AttrValue::DateTime(minutes)) = record.attr_value(metadata, attr) {
        return Some(minutes);
    }

    let key = metadata.attr_key(attr)?;
    let attr_type = metadata.attr_key_type(key)?;
    let bytes = metadata.file_attr(attr)?;
    match metadata.parse_attr_value(bytes, attr_type) {
        AttrValue::DateTime(minutes) => Some(minutes),
        _ => None,
    }
}

/// Convert Box timestamp (minutes since Box epoch as VLQ i64) to Windows FILETIME.
///
/// FILETIME is a 64-bit value representing 100-nanosecond intervals since
/// January 1, 1601 (UTC).
#[inline]
// [spec:box:req:projfs-provider.root.placeholders]
pub fn box_to_filetime(box_minutes: i64) -> i64 {
    let filetime = i128::from(box_minutes) * SECONDS_PER_MINUTE * HUNDRED_NANOSECONDS_PER_SECOND
        + i128::from(BOX_EPOCH_UNIX) * HUNDRED_NANOSECONDS_PER_SECOND
        + i128::from(UNIX_TO_WINDOWS_EPOCH_DIFF);

    filetime.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
}

/// Get the current time as a Windows FILETIME value.
#[inline]
pub fn now_as_filetime() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    let unix_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);

    unix_secs * 10_000_000 + UNIX_TO_WINDOWS_EPOCH_DIFF
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::num::NonZeroU64;

    use box_format::{AttrMap, AttrType, Compression, FileRecord, attrs};

    use super::*;

    fn timestamp_record(
        attr_type: AttrType,
        bytes: Vec<u8>,
    ) -> (BoxMetadata<'static>, Record<'static>) {
        let mut metadata = BoxMetadata::default();
        let key = metadata
            .attr_key_or_create(attrs::CREATED, attr_type)
            .unwrap();
        let mut attrs = AttrMap::default();
        attrs.insert(key, bytes.into_boxed_slice());
        let record = Record::File(FileRecord {
            compression: Compression::Stored,
            length: 0,
            decompressed_length: 0,
            data: NonZeroU64::new(32).unwrap(),
            name: Cow::Borrowed("file"),
            attrs,
        });
        (metadata, record)
    }

    #[test]
    fn test_box_epoch_to_filetime() {
        // Box epoch (2026-01-01 00:00:00 UTC) should convert to a valid FILETIME
        let ft = box_to_filetime(0);
        // Should be positive and reasonable
        assert!(ft > UNIX_TO_WINDOWS_EPOCH_DIFF);
    }

    // [spec:box:req:projfs-provider.root.placeholders/test]
    #[test]
    fn test_positive_minutes() {
        // 60 minutes after box epoch
        let ft = box_to_filetime(60);
        let ft0 = box_to_filetime(0);
        // Should be 1 hour (3600 seconds * 10_000_000) later
        assert_eq!(ft - ft0, 3600 * 10_000_000);
    }

    // [spec:box:req:projfs-provider.root.placeholders/test/unit]
    #[test]
    fn extreme_box_timestamps_saturate_without_panicking() {
        assert_eq!(box_to_filetime(i64::MIN), i64::MIN);
        assert_eq!(box_to_filetime(i64::MAX), i64::MAX);
    }

    #[test]
    // [spec:box:def:attributes.root.standard-keys/test/unit]
    // [spec:box:req:projfs-provider.root.placeholders/test/unit]
    fn timestamp_lookup_rejects_the_wrong_attribute_type() {
        let (metadata, record) = timestamp_record(AttrType::U8, vec![7]);

        assert_eq!(
            record_timestamp_minutes(&metadata, &record, attrs::CREATED),
            None
        );
    }

    #[test]
    // [spec:box:def:attributes.root.standard-keys/test/unit]
    // [spec:box:req:projfs-provider.root.placeholders/test/unit]
    fn timestamp_lookup_requires_an_exact_datetime_payload() {
        let encoded = fastvint::encode_vi64(-42);
        let mut trailing = encoded.bytes().to_vec();
        trailing.push(0);
        let (metadata, record) = timestamp_record(AttrType::DateTime, trailing);

        assert_eq!(
            record_timestamp_minutes(&metadata, &record, attrs::CREATED),
            None
        );

        let (metadata, record) = timestamp_record(AttrType::DateTime, encoded.bytes().to_vec());
        assert_eq!(
            record_timestamp_minutes(&metadata, &record, attrs::CREATED),
            Some(-42)
        );
    }
}
