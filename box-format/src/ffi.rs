use cffi::{FromForeign, ToForeign};

use crate::BoxFileReader;
use std::path::PathBuf;

#[cffi::marshal(return_marshaler = "cffi::BoxMarshaler::<BoxFileReader>")]
pub extern "C" fn box_file_reader_open(
    #[marshal(cffi::PathBufMarshaler)] path: PathBuf,
) -> Result<Box<BoxFileReader>, Box<dyn std::error::Error>> {
    BoxFileReader::open(path)
        .map(Box::new)
        .map_err(|err| Box::new(err) as _)
}

#[cffi::marshal(return_marshaler = "cffi::UnitMarshaler")]
pub extern "C" fn box_file_reader_extract_all(
    #[marshal(cffi::BoxRefMarshaler::<BoxFileReader>)] handle: &BoxFileReader,
    #[marshal(cffi::PathBufMarshaler)] path: PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    handle.extract_all(path).map_err(|err| Box::new(err) as _)
}
