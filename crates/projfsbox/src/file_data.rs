use std::ops::Range;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FileSizeOutOfRange(pub(crate) u64);

pub(crate) fn checked_file_size(length: u64) -> Result<i64, FileSizeOutOfRange> {
    i64::try_from(length).map_err(|_| FileSizeOutOfRange(length))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FileDataRangeError {
    OffsetPastEnd,
    IntegerOverflow,
}

pub(crate) fn checked_file_data_range(
    byte_offset: u64,
    length: u32,
    file_len: usize,
) -> Result<Range<usize>, FileDataRangeError> {
    let file_len = u64::try_from(file_len).map_err(|_| FileDataRangeError::IntegerOverflow)?;

    if byte_offset > file_len {
        return Err(FileDataRangeError::OffsetPastEnd);
    }

    let requested_end = byte_offset
        .checked_add(u64::from(length))
        .ok_or(FileDataRangeError::IntegerOverflow)?;
    let end = requested_end.min(file_len);

    let start = usize::try_from(byte_offset).map_err(|_| FileDataRangeError::IntegerOverflow)?;
    let end = usize::try_from(end).map_err(|_| FileDataRangeError::IntegerOverflow)?;

    Ok(start..end)
}

pub(crate) fn logical_file_buffer(length: u64) -> std::io::Result<Vec<u8>> {
    let capacity = usize::try_from(length).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("logical file length {length} does not fit in memory"),
        )
    })?;

    let mut buffer = Vec::new();
    buffer.try_reserve_exact(capacity).map_err(|error| {
        std::io::Error::new(
            std::io::ErrorKind::OutOfMemory,
            format!("cannot reserve {capacity} bytes for logical file data: {error}"),
        )
    })?;
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;

    // [spec:box:req:projfs-provider.root.placeholders/test/unit]
    #[test]
    fn checks_file_size_at_the_projfs_abi_boundary() {
        assert_eq!(checked_file_size(i64::MAX as u64), Ok(i64::MAX));
        assert_eq!(
            checked_file_size(i64::MAX as u64 + 1),
            Err(FileSizeOutOfRange(i64::MAX as u64 + 1))
        );
        assert_eq!(
            checked_file_size(u64::MAX),
            Err(FileSizeOutOfRange(u64::MAX))
        );
    }

    // [spec:box:req:projfs-provider.root.file-data-and-errors/test/unit]
    #[test]
    fn preserves_an_in_range_request() {
        assert_eq!(checked_file_data_range(3, 4, 10), Ok(3..7));
    }

    #[test]
    fn clamps_a_request_at_end_of_file() {
        assert_eq!(checked_file_data_range(8, 10, 10), Ok(8..10));
    }

    #[test]
    fn accepts_an_empty_request_at_end_of_file() {
        assert_eq!(checked_file_data_range(10, 0, 10), Ok(10..10));
    }

    #[test]
    fn rejects_an_offset_past_end_of_file() {
        assert_eq!(
            checked_file_data_range(11, 1, 10),
            Err(FileDataRangeError::OffsetPastEnd)
        );
    }

    #[test]
    fn rejects_a_negative_abi_value_encoded_as_u64() {
        assert_eq!(
            checked_file_data_range(u64::MAX, 1, 10),
            Err(FileDataRangeError::OffsetPastEnd)
        );
    }

    #[test]
    fn clamps_an_oversized_request_without_overflow() {
        assert_eq!(checked_file_data_range(4, u32::MAX, 10), Ok(4..10));
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn rejects_end_arithmetic_overflow() {
        assert_eq!(
            checked_file_data_range(u64::MAX, 1, usize::MAX),
            Err(FileDataRangeError::IntegerOverflow)
        );
    }

    #[test]
    fn rejects_an_unrepresentable_logical_file_capacity() {
        let error = logical_file_buffer(u64::MAX).unwrap_err();
        assert!(matches!(
            error.kind(),
            std::io::ErrorKind::InvalidData | std::io::ErrorKind::OutOfMemory
        ));
    }
}
