use std::num::NonZeroU64;

#[derive(Debug)]
pub(crate) struct BoxHeader {
    pub(crate) magic_bytes: [u8; 4],
    pub(crate) version: u32,
    pub(crate) alignment: u64,
    pub(crate) trailer: Option<NonZeroU64>,
}

// Make some attempt to not accidentally load plain text files,
// and also make it break almost immediately in any UTF-8 compliant text parser.
pub(crate) const MAGIC_BYTES: &[u8; 4] = b"\xffBOX";

impl BoxHeader {
    pub(crate) fn new(trailer: Option<NonZeroU64>) -> BoxHeader {
        BoxHeader {
            magic_bytes: *MAGIC_BYTES,
            version: 0x0,
            alignment: 0,
            trailer,
        }
    }

    pub(crate) fn with_alignment(alignment: u64) -> BoxHeader {
        let mut header = BoxHeader::default();
        header.alignment = alignment;
        header
    }
}

impl Default for BoxHeader {
    fn default() -> Self {
        BoxHeader::new(None)
    }
}
