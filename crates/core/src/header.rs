//! Box file header types and constants.

use core::num::NonZeroU64;

/// The magic bytes at the start of every Box file.
/// Uses 0xFF to break UTF-8 parsing, followed by "BOX".
pub const MAGIC_BYTES: &[u8; 4] = b"\xffBOX";

/// Current Box format version.
pub const VERSION: u8 = 1;

/// The Box file header (32 bytes fixed size).
#[derive(Debug, Clone, Copy)]
pub struct BoxHeader {
    /// Magic bytes identifying this as a Box file.
    pub magic_bytes: [u8; 4],

    /// Format version number.
    pub version: u8,

    /// Alignment for data entries (0 = no alignment).
    pub alignment: u32,

    /// Offset to the trailer section (None if no trailer yet).
    pub trailer: Option<NonZeroU64>,
}

impl BoxHeader {
    /// Size of the header in bytes.
    pub const SIZE: usize = 32;

    /// Create a new header with the given trailer offset.
    pub const fn new(trailer: Option<NonZeroU64>) -> BoxHeader {
        BoxHeader {
            magic_bytes: *MAGIC_BYTES,
            version: VERSION,
            alignment: 0,
            trailer,
        }
    }

    /// Create a new header with the specified alignment.
    pub const fn with_alignment(alignment: u32) -> BoxHeader {
        BoxHeader {
            magic_bytes: *MAGIC_BYTES,
            version: VERSION,
            alignment,
            trailer: None,
        }
    }

    /// Check if the magic bytes are valid.
    pub fn is_valid_magic(&self) -> bool {
        &self.magic_bytes == MAGIC_BYTES
    }
}

impl Default for BoxHeader {
    fn default() -> Self {
        BoxHeader::new(None)
    }
}
