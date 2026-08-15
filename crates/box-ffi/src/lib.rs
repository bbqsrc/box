// The cffi-generated entry shims validate and marshal raw foreign pointers
// before constructing the annotated Rust references below.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use std::path::PathBuf;

use box_format::BoxFileReader;
use cffi::{FromForeign, ToForeign};

// [spec:box:req:c-api.root.runtime-and-ownership]
fn runtime() -> &'static tokio::runtime::Runtime {
    use std::sync::OnceLock;
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime"))
}

fn open_reader(path: PathBuf) -> Result<Box<BoxFileReader>, Box<dyn std::error::Error>> {
    runtime()
        .block_on(BoxFileReader::open(path))
        .map(Box::new)
        .map_err(|err| Box::new(err) as _)
}

fn extract_all(handle: &BoxFileReader, path: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    runtime()
        .block_on(handle.extract_all(path))
        .map_err(|err| Box::new(err) as _)
}

#[cffi::marshal(return_marshaler = "cffi::BoxMarshaler::<BoxFileReader>")]
// [spec:box:req:c-api.root]
// [spec:box:req:c-api.root.runtime-and-ownership]
pub extern "C" fn box_file_reader_open(
    #[marshal(cffi::PathBufMarshaler)] path: PathBuf,
) -> Result<Box<BoxFileReader>, Box<dyn std::error::Error>> {
    open_reader(path)
}

#[cffi::marshal(return_marshaler = "cffi::UnitMarshaler")]
// [spec:box:req:c-api.root]
// [spec:box:req:c-api.root.runtime-and-ownership]
pub extern "C" fn box_file_reader_extract_all(
    #[marshal(cffi::BoxRefMarshaler::<BoxFileReader>)] handle: &BoxFileReader,
    #[marshal(cffi::PathBufMarshaler)] path: PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    extract_all(handle, path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use box_format::{BoxFileWriter, BoxPath, Compression, CompressionConfig, HashMap};

    // [spec:box:req:c-api.root/test/integration]
    // [spec:box:req:c-api.root.runtime-and-ownership/test/integration]
    #[test]
    fn c_api_extract_all_flushes_compressed_output() {
        let temp = tempfile::tempdir().unwrap();
        let archive = temp.path().join("ffi-compressed.box");
        let output = temp.path().join("output");
        let payload = vec![b'F'; 4096];

        runtime().block_on(async {
            let mut writer = BoxFileWriter::create(&archive).await.unwrap();
            writer
                .insert(
                    &CompressionConfig::new(Compression::Zstd),
                    BoxPath::new("ffi.bin").unwrap(),
                    std::io::Cursor::new(payload.clone()),
                    HashMap::new(),
                )
                .await
                .unwrap();
            writer.finish().await.unwrap();
        });

        let reader = open_reader(archive).unwrap();
        extract_all(reader.as_ref(), output.clone()).unwrap();
        assert_eq!(std::fs::read(output.join("ffi.bin")).unwrap(), payload);
    }
}
