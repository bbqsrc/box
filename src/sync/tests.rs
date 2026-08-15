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
    fn invalid_trailer_error_identifies_record_field_and_offsets() {
        use std::error::Error as _;

        let temp_dir = tempfile::tempdir().unwrap();
        let filename = temp_dir.path().join("truncated.box");
        create_test_box_sync(filename.to_str().unwrap());

        let header = std::fs::read(&filename).unwrap();
        let trailer_offset = u64::from_le_bytes(header[0x10..0x18].try_into().unwrap());

        // Keep the record header and fixed file fields, then cut the file name
        // after four of its nine declared bytes.
        std::fs::OpenOptions::new()
            .write(true)
            .open(&filename)
            .unwrap()
            .set_len(trailer_offset + 42)
            .unwrap();

        let error = BoxReader::open(&filename).unwrap_err();
        let mut chain = vec![error.to_string()];
        let mut source = error.source();
        while let Some(next) = source {
            chain.push(next.to_string());
            source = next.source();
        }
        let diagnostic = chain.join("\n");

        assert!(diagnostic.contains("Box header is valid"));
        assert!(diagnostic.contains("Box format version 1 metadata trailer starts"));
        assert!(diagnostic.contains("record 1 of 1"));
        assert!(diagnostic.contains("file record body"));
        assert!(diagnostic.contains("file name at trailer byte 37 (0x25)"));
        assert!(diagnostic.contains("declares 9 bytes, but only 4 remain"));
        assert!(diagnostic.contains("5 bytes missing"));
        assert!(error.diagnostic_help().contains("header is valid"));
        assert!(!error.diagnostic_help().contains("Is this a valid"));
    }

    #[test]
    fn trailer_pointer_beyond_eof_is_reported_without_panicking() {
        use std::error::Error as _;

        let temp_dir = tempfile::tempdir().unwrap();
        let filename = temp_dir.path().join("bad-pointer.box");
        let header = crate::encode::encode_header_array(
            &crate::encode::HeaderConfig::new().with_trailer(1_024),
        );
        std::fs::write(&filename, header).unwrap();

        let error = BoxReader::open(&filename).unwrap_err();
        let source = error.source().unwrap().to_string();

        assert!(source.contains("trailer at file byte 1024 (0x400)"));
        assert!(source.contains("beyond EOF at byte 32"));
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
