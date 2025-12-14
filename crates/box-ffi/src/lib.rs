use std::path::PathBuf;

use box_format::BoxFileReader;
use cffi::{FromForeign, ToForeign};

fn runtime() -> &'static tokio::runtime::Runtime {
    use std::sync::OnceLock;
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime"))
}

#[cffi::marshal(return_marshaler = "cffi::BoxMarshaler::<BoxFileReader>")]
pub extern "C" fn box_file_reader_open(
    #[marshal(cffi::PathBufMarshaler)] path: PathBuf,
) -> Result<Box<BoxFileReader>, Box<dyn std::error::Error>> {
    runtime()
        .block_on(BoxFileReader::open(path))
        .map(Box::new)
        .map_err(|err| Box::new(err) as _)
}

#[cffi::marshal(return_marshaler = "cffi::UnitMarshaler")]
pub extern "C" fn box_file_reader_extract_all(
    #[marshal(cffi::BoxRefMarshaler::<BoxFileReader>)] handle: &BoxFileReader,
    #[marshal(cffi::PathBufMarshaler)] path: PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    runtime()
        .block_on(handle.extract_all(path))
        .map_err(|err| Box::new(err) as _)
}
