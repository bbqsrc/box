#[cfg(test)]
mod tests {
    use crate::compat::HashMap;
    use crate::compression::{Compression, CompressionConfig};
    use crate::path::BoxPath;
    use crate::sync::{BoxReader, BoxWriter};

    fn create_test_box_sync(filename: &str) {
        let _ = std::fs::remove_file(filename);

        let mut cursor = std::io::Cursor::new(b"hello\0\0\0".to_vec());

        let mut writer = BoxWriter::create(filename).unwrap();
        writer
            .insert(
                &CompressionConfig::new(Compression::Stored),
                BoxPath::new("hello.txt").unwrap(),
                &mut cursor,
                HashMap::new(),
            )
            .unwrap();
        writer.finish().unwrap();
    }

    #[test]
    fn sync_create_box_file() {
        create_test_box_sync("./sync_smoketest.box");
    }

    #[test]
    fn sync_read_file() {
        let filename = "./sync_read.box";
        create_test_box_sync(filename);

        let bf = BoxReader::open(filename).unwrap();
        let trailer = bf.metadata();
        let segment = bf
            .memory_map(trailer.records[0].as_file().unwrap())
            .unwrap();
        let file_data = segment.as_slice().unwrap();
        assert_eq!(file_data, b"hello\0\0\0");
    }

    #[test]
    fn sync_decompress() {
        let filename = "./sync_decompress.box";
        create_test_box_sync(filename);

        let bf = BoxReader::open(filename).unwrap();
        let record = bf.metadata().records[0].as_file().unwrap();
        let mut buf = Vec::new();
        bf.decompress(record, &mut buf).unwrap();
        assert_eq!(buf, b"hello\0\0\0");
    }

    #[test]
    fn sync_zstd_roundtrip() {
        let filename = "./sync_zstd.box";
        let _ = std::fs::remove_file(filename);

        let data = "This is compressible data data data data data!\n";

        let mut writer = BoxWriter::create(filename).unwrap();
        writer
            .mkdir(BoxPath::new("test").unwrap(), HashMap::new())
            .unwrap();
        writer
            .insert(
                &CompressionConfig::new(Compression::Zstd),
                BoxPath::new("test/file.txt").unwrap(),
                &mut std::io::Cursor::new(data),
                HashMap::new(),
            )
            .unwrap();
        writer.finish().unwrap();

        let reader = BoxReader::open(filename).unwrap();
        let record = reader.metadata().records[1].as_file().unwrap();
        let mut buf = Vec::new();
        reader.decompress(record, &mut buf).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), data);
    }

    #[test]
    fn sync_extract_all() {
        let filename = "./sync_extract.box";
        let _ = std::fs::remove_file(filename);

        let mut writer = BoxWriter::create(filename).unwrap();
        writer
            .mkdir(BoxPath::new("dir").unwrap(), HashMap::new())
            .unwrap();
        writer
            .insert(
                &CompressionConfig::new(Compression::Stored),
                BoxPath::new("dir/file.txt").unwrap(),
                &mut std::io::Cursor::new(b"content"),
                HashMap::new(),
            )
            .unwrap();
        writer.finish().unwrap();

        let reader = BoxReader::open(filename).unwrap();
        let tmp = tempfile::tempdir().unwrap();
        reader.extract_all(tmp.path()).unwrap();

        let extracted = std::fs::read_to_string(tmp.path().join("dir/file.txt")).unwrap();
        assert_eq!(extracted, "content");
    }
}
