use cursed::{FromForeign, ToForeign};

use crate::BoxFileReader;
use std::path::PathBuf;

#[cthulhu::invoke(return_marshaler = "cursed::BoxMarshaler::<BoxFileReader>")]
pub extern "C" fn box_file_reader_open(
    #[marshal(cursed::PathMarshaler)] path: PathBuf,
) -> Result<Box<BoxFileReader>, Box<dyn std::error::Error>> {
    BoxFileReader::open(path)
        .map(|x| Box::new(x))
        .map_err(|err| Box::new(err) as _)
}

#[cthulhu::invoke(return_marshaler = "cursed::UnitMarshaler")]
pub extern "C" fn box_file_reader_extract_all(
    #[marshal(cursed::BoxRefMarshaler::<BoxFileReader>)] handle: &BoxFileReader,
    #[marshal(cursed::PathMarshaler)] path: PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    handle.extract_all(path).map_err(|err| Box::new(err) as _)
}
