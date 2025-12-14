//! Checksum trait and implementations for archive checksums.

use digest::{Digest, FixedOutput, HashMarker, Output, OutputSizeUser, Reset, Update, typenum::U0};

/// A checksum algorithm with a named attribute.
///
/// Implementors of this trait can be used with `insert_streaming` to compute
/// checksums while data is being compressed. The `NAME` constant determines
/// the attribute name under which the checksum is stored.
pub trait Checksum: Digest + Default + Unpin {
    /// Attribute name for storing the checksum.
    ///
    /// If empty, no attribute is stored.
    const NAME: &'static str;
}

impl Checksum for blake3::Hasher {
    const NAME: &'static str = "blake3";
}

/// A no-op checksum that does nothing.
///
/// Use this when checksums are disabled (`--no-checksum`).
/// All operations are no-ops and the output is zero-length.
#[derive(Default, Clone)]
pub struct NullChecksum;

impl Checksum for NullChecksum {
    const NAME: &'static str = "";
}

impl OutputSizeUser for NullChecksum {
    type OutputSize = U0;
}

impl Update for NullChecksum {
    fn update(&mut self, _data: &[u8]) {
        // No-op
    }
}

impl FixedOutput for NullChecksum {
    fn finalize_into(self, _out: &mut Output<Self>) {
        // No-op - output is zero-length
    }
}

impl Reset for NullChecksum {
    fn reset(&mut self) {
        // No-op
    }
}

impl HashMarker for NullChecksum {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null_checksum() {
        let mut hasher = NullChecksum;
        Digest::update(&mut hasher, b"hello world");
        let result = hasher.finalize();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_blake3_name() {
        assert_eq!(blake3::Hasher::NAME, "blake3");
    }

    #[test]
    fn test_null_name() {
        assert_eq!(NullChecksum::NAME, "");
    }
}
