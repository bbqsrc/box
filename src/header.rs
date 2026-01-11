use crate::compat::NonZeroU64;

/// Box archive header (32 bytes on disk).
#[derive(Debug, Clone)]
pub struct BoxHeader {
    /// Format version (currently 1).
    pub version: u8,
    /// Allow `\xNN` escape sequences in paths.
    pub allow_escapes: bool,
    /// Allow external symlinks pointing outside the archive.
    pub allow_external_symlinks: bool,
    /// Alignment for file data (0 = no alignment).
    pub alignment: u32,
    /// Offset to the trailer (metadata), or None if not yet written.
    pub trailer: Option<NonZeroU64>,
}

impl BoxHeader {
    pub const SIZE: usize = 32;
}

pub const VERSION: u8 = 1;

impl BoxHeader {
    /// Create a new header with just the trailer offset.
    pub fn new(trailer: Option<NonZeroU64>) -> BoxHeader {
        BoxHeader {
            version: VERSION,
            allow_escapes: false,
            allow_external_symlinks: false,
            alignment: 0,
            trailer,
        }
    }

    /// Create a header with specific alignment.
    pub fn with_alignment(alignment: u32) -> BoxHeader {
        BoxHeader {
            alignment,
            ..Default::default()
        }
    }

    /// Create a header that allows escape sequences in paths.
    pub fn with_escapes(allow_escapes: bool) -> BoxHeader {
        BoxHeader {
            allow_escapes,
            ..Default::default()
        }
    }

    /// Create a header with custom options.
    pub fn with_options(
        alignment: u32,
        allow_escapes: bool,
        allow_external_symlinks: bool,
    ) -> BoxHeader {
        BoxHeader {
            alignment,
            allow_escapes,
            allow_external_symlinks,
            ..Default::default()
        }
    }
}

impl Default for BoxHeader {
    fn default() -> Self {
        BoxHeader::new(None)
    }
}
