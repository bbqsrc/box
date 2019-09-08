use std::borrow::Cow;

use cursed::{FromForeign, ToForeign};

use crate::BoxFile;

struct BoxFileResultMarshaler;

impl ToForeign<std::io::Result<BoxFile>, *const libc::c_void> for BoxFileResultMarshaler {
    type Error = std::io::Error;

    fn to_foreign(result: std::io::Result<BoxFile>) -> Result<*const libc::c_void, Self::Error> {
        match result {
            Ok(v) => Ok(Box::into_raw(Box::new(v)) as *const _),
            Err(e) => Err(e)
        }
    }
}

#[cthulhu::invoke(return_marshaler = "BoxFileResultMarshaler")]
pub fn box_file_open(path: Cow<str>) -> std::io::Result<BoxFile> {
    BoxFile::open(&*path)
}

#[cthulhu::invoke(return_marshaler = "BoxFileResultMarshaler")]
pub fn box_file_create(path: Cow<str>) -> std::io::Result<BoxFile> {
    BoxFile::create(&*path)
}
