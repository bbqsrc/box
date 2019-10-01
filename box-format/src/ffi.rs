use cursed::{FromForeign, ReturnType, ToForeign};

use crate::BoxFileReader;
use std::ffi::CStr;
use std::path::Path;

use std::io;

#[inline(always)]
fn null_ptr_error() -> Box<io::Error> {
    Box::new(io::Error::new(io::ErrorKind::InvalidData, "null pointer"))
}

pub struct BoxFileReaderResultMarshaler;

impl ToForeign<std::io::Result<BoxFileReader>, *const libc::c_void>
    for BoxFileReaderResultMarshaler
{
    type Error = std::io::Error;

    #[inline(always)]
    fn to_foreign(
        file: std::io::Result<BoxFileReader>,
    ) -> Result<*const libc::c_void, Self::Error> {
        match file {
            Ok(file) => Ok(Box::into_raw(Box::new(file)).cast()),
            Err(err) => Err(err),
        }
    }
}

pub struct BoxFileReaderMarshaler;

impl<'a> FromForeign<*const libc::c_void, &'a BoxFileReader> for BoxFileReaderMarshaler {
    type Error = Box<std::io::Error>;

    #[inline(always)]
    fn from_foreign(foreign: *const libc::c_void) -> Result<&'a BoxFileReader, Self::Error> {
        if foreign.is_null() {
            return Err(null_ptr_error());
        }

        Ok(unsafe { &*foreign.cast() })
    }
}

pub struct PathMarshaler;

#[cfg(unix)]
impl<'a> FromForeign<*const libc::c_void, &'a Path> for PathMarshaler {
    type Error = Box<std::io::Error>;

    #[inline(always)]
    fn from_foreign(foreign: *const libc::c_void) -> Result<&'a Path, Self::Error> {
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt;

        if foreign.is_null() {
            return Err(null_ptr_error());
        }

        let c_str = unsafe { CStr::from_ptr(foreign.cast()) };
        let os_str = OsStr::from_bytes(c_str.to_bytes());
        Ok(Path::new(os_str))
    }
}

pub struct UnitMarshaler;

impl ReturnType for UnitMarshaler {
    type Foreign = ();

    #[inline(always)]
    fn foreign_default() -> () {
        ()
    }
}

impl ReturnType for PathMarshaler {
    type Foreign = *const std::ffi::c_void;

    #[inline(always)]
    fn foreign_default() -> Self::Foreign {
        std::ptr::null()
    }
}

impl ReturnType for BoxFileReaderMarshaler {
    type Foreign = *const std::ffi::c_void;

    #[inline(always)]
    fn foreign_default() -> Self::Foreign {
        std::ptr::null()
    }
}

impl ReturnType for BoxFileReaderResultMarshaler {
    type Foreign = *const libc::c_void;

    #[inline(always)]
    fn foreign_default() -> Self::Foreign {
        std::ptr::null()
    }
}

impl<E> ToForeign<Result<(), E>, ()> for UnitMarshaler {
    type Error = E;

    #[inline(always)]
    fn to_foreign(result: Result<(), E>) -> Result<(), Self::Error> {
        result
    }
}

#[cthulhu::invoke(return_marshaler = "BoxFileReaderResultMarshaler")]
pub extern "C" fn box_file_reader_open(path: &Path) -> std::io::Result<BoxFileReader> {
    BoxFileReader::open(path)
}

#[cthulhu::invoke(return_marshaler = "UnitMarshaler")]
pub extern "C" fn box_file_reader_extract_all(
    handle: &BoxFileReader,
    path: &Path,
) -> std::io::Result<()> {
    handle.extract_all(path)
}
