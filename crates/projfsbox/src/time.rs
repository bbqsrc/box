//! Time conversion utilities for Box epoch to Windows FILETIME.

/// Box epoch: 2020-01-01 00:00:00 UTC (seconds since Unix epoch)
pub const BOX_EPOCH_UNIX: i64 = 1577836800;

/// Difference between Unix epoch (1970-01-01) and Windows epoch (1601-01-01)
/// in 100-nanosecond intervals.
const UNIX_TO_WINDOWS_EPOCH_DIFF: i64 = 116444736000000000;

/// Convert Box timestamp (minutes since Box epoch as VLQ i64) to Windows FILETIME.
///
/// FILETIME is a 64-bit value representing 100-nanosecond intervals since
/// January 1, 1601 (UTC).
#[inline]
pub fn box_to_filetime(box_minutes: i64) -> i64 {
    let unix_secs = box_minutes * 60 + BOX_EPOCH_UNIX;
    // Convert to 100-nanosecond intervals since Windows epoch
    unix_secs * 10_000_000 + UNIX_TO_WINDOWS_EPOCH_DIFF
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
    use super::*;

    #[test]
    fn test_box_epoch_to_filetime() {
        // Box epoch (2020-01-01 00:00:00 UTC) should convert to a valid FILETIME
        let ft = box_to_filetime(0);
        // Should be positive and reasonable
        assert!(ft > UNIX_TO_WINDOWS_EPOCH_DIFF);
    }

    #[test]
    fn test_positive_minutes() {
        // 60 minutes after box epoch
        let ft = box_to_filetime(60);
        let ft0 = box_to_filetime(0);
        // Should be 1 hour (3600 seconds * 10_000_000) later
        assert_eq!(ft - ft0, 3600 * 10_000_000);
    }
}
