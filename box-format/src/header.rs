use std::num::NonZeroU64;

#[derive(Debug)]
pub(crate) struct BoxHeader {
    pub(crate) magic_bytes: [u8; 4],
    pub(crate) version: u32,
    pub(crate) alignment: Option<NonZeroU64>,
    pub(crate) trailer: Option<NonZeroU64>,
}

impl BoxHeader {
    pub(crate) fn new(trailer: Option<NonZeroU64>) -> BoxHeader {
        BoxHeader {
            magic_bytes: *b"BOX\0",
            version: 0x0,
            alignment: None,
            trailer,
        }
    }

    pub(crate) fn with_alignment(alignment: NonZeroU64) -> BoxHeader {
        let mut header = BoxHeader::default();
        header.alignment = Some(alignment);
        header
    }
}

impl Default for BoxHeader {
    fn default() -> Self {
        BoxHeader::new(None)
    }
}
