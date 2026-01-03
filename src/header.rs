use std::num::NonZeroU64;

#[derive(Debug)]
pub(crate) struct BoxHeader {
    pub(crate) magic_bytes: [u8; 4],
    pub(crate) version: u8,
    pub(crate) allow_escapes: bool,
    pub(crate) alignment: u32,
    pub(crate) trailer: Option<NonZeroU64>,
}

impl BoxHeader {
    pub const SIZE: usize = 32;
}

// Make some attempt to not accidentally load plain text files,
// and also make it break almost immediately in any UTF-8 compliant text parser.
pub(crate) const MAGIC_BYTES: &[u8; 4] = b"\xffBOX";
pub const VERSION: u8 = 1;

impl BoxHeader {
    pub(crate) fn new(trailer: Option<NonZeroU64>) -> BoxHeader {
        BoxHeader {
            magic_bytes: *MAGIC_BYTES,
            version: VERSION,
            allow_escapes: false,
            alignment: 0,
            trailer,
        }
    }

    pub(crate) fn with_alignment(alignment: u32) -> BoxHeader {
        BoxHeader {
            alignment,
            ..Default::default()
        }
    }

    pub(crate) fn with_escapes(allow_escapes: bool) -> BoxHeader {
        BoxHeader {
            allow_escapes,
            ..Default::default()
        }
    }

    pub(crate) fn with_options(alignment: u32, allow_escapes: bool) -> BoxHeader {
        BoxHeader {
            alignment,
            allow_escapes,
            ..Default::default()
        }
    }
}

impl Default for BoxHeader {
    fn default() -> Self {
        BoxHeader::new(None)
    }
}
