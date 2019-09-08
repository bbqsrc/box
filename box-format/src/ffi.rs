use std::borrow::Cow;

use cursed::{FromForeign, ToForeign};

use crate::BoxFile;

struct BoxFileResultMarshaler;

impl ToForeign<BoxFile, *const libc::c_void> for BoxFileResultMarshaler {
    type Error = std::io::Error;

    fn to_foreign(file: BoxFile) -> Result<*const libc::c_void, Self::Error> {
        Ok(Box::into_raw(Box::new(file)) as *const _)
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
