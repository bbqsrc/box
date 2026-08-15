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

    fn replace_block_entries(
        reader: &mut BoxReader,
        record_index: crate::core::RecordIndex,
        entries: &[(u64, u64)],
    ) {
        let mut builder = box_fst::FstBuilder::<u64>::new();
        for &(logical_offset, physical_offset) in entries {
            let mut key = [0u8; 16];
            key[..8].copy_from_slice(&record_index.get().to_be_bytes());
            key[8..].copy_from_slice(&logical_offset.to_be_bytes());
            builder.insert(&key, physical_offset).unwrap();
        }
        let bytes = builder.finish().unwrap();
        reader.core.meta.block_fst = Some(
            box_fst::Fst::new(std::borrow::Cow::Owned(bytes))
                .expect("test block FST should be well-formed"),
        );
    }

    // [spec:box:sem:sync-io.root/test/integration]
    // [spec:box:sem:sync-io.root.write/test/integration]
    #[test]
    fn sync_create_box_file() {
        create_test_box_sync("./sync_smoketest.box");
    }

    // [spec:box:req:sans-io.root.hierarchy/test]
    #[test]
    fn sync_writer_rejects_invalid_parents() {
        let temp = tempfile::tempdir().unwrap();
        let archive = temp.path().join("invalid-parent.box");
        let mut writer = BoxWriter::create(&archive).unwrap();

        let missing_child = BoxPath::new("missing/child").unwrap();
        let error = writer
            .mkdir(missing_child.clone(), HashMap::new())
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(writer.metadata().records.len(), 0);
        assert!(writer.metadata().index(&missing_child).is_none());

        writer
            .insert(
                &CompressionConfig::new(Compression::Stored),
                BoxPath::new("parent").unwrap(),
                std::io::Cursor::new(b"not a directory"),
                HashMap::new(),
            )
            .unwrap();
        let record_count = writer.metadata().records.len();
        let nested_child = BoxPath::new("parent/child").unwrap();

        let error = writer
            .mkdir(nested_child.clone(), HashMap::new())
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("not a directory"));
        assert_eq!(writer.metadata().records.len(), record_count);
        assert!(writer.metadata().index(&nested_child).is_none());

        writer.finish().unwrap();
        let reader = BoxReader::open(&archive).unwrap();
        assert_eq!(reader.metadata().records.len(), record_count);
        assert!(reader.metadata().index(&nested_child).is_none());
    }

    // [spec:box:sem:sync-io.root.open/test/integration]
    // [spec:box:sem:sync-io.root.read/test/integration]
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

    // [spec:box:sem:sync-io.root.read/test/unit]
    #[test]
    fn sync_regular_mapping_rejects_embedded_offset_overflow() {
        let temp = tempfile::tempdir().unwrap();
        let archive = temp.path().join("regular-offset-overflow.box");
        create_test_box_sync(archive.to_str().unwrap());

        let mut reader = BoxReader::open(&archive).unwrap();
        let mut record = reader.metadata().records[0].as_file().unwrap().clone();
        record.data = std::num::NonZeroU64::new(u64::MAX).unwrap();
        reader.core.offset = 1;

        assert_eq!(
            reader.memory_map(&record).unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
        );
    }

    // [spec:box:sem:sync-io.root.open/test/integration]
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

    // [spec:box:sem:sync-io.root.read/test/integration]
    // [spec:box:sem:sync-io.root.write/test/integration]
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

    // [spec:box:sem:chunked-io.root.sync-range/test/integration]
    #[tokio::test]
    async fn sync_chunk_ranges_use_logical_and_physical_offsets() {
        let temp = tempfile::tempdir().unwrap();
        let archive = temp.path().join("sync-chunked-range.box");
        let payload: Vec<u8> = (0..300u32).map(|value| (value % 251) as u8).collect();

        let mut writer = crate::BoxFileWriter::create(&archive).await.unwrap();
        writer
            .insert_chunked(
                BoxPath::new("payload.bin").unwrap(),
                std::io::Cursor::new(payload.clone()),
                64,
                Compression::Zstd,
                HashMap::new(),
            )
            .await
            .unwrap();
        writer.finish().await.unwrap();

        let mut reader = BoxReader::open(&archive).unwrap();
        let path = BoxPath::new("payload.bin").unwrap();
        let record_index = reader.metadata().index(&path).unwrap();
        let record = reader
            .metadata()
            .record(record_index)
            .unwrap()
            .as_chunked_file()
            .unwrap();

        assert_eq!(
            reader
                .decompress_chunked_range(record, record_index, 60, 141)
                .unwrap(),
            payload[60..141]
        );
        assert_eq!(
            reader
                .decompress_chunked_range(record, record_index, 290, 999)
                .unwrap(),
            payload[290..]
        );
        assert!(
            reader
                .decompress_chunked_range(record, record_index, 999, 1_000)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            reader
                .decompress_chunked_block(record, record_index, 2)
                .unwrap(),
            payload[128..192]
        );

        let data_start = record.data.get();
        let data_end = data_start + record.length;

        replace_block_entries(&mut reader, record_index, &[(0, data_start - 1)]);
        let record = reader
            .metadata()
            .record(record_index)
            .unwrap()
            .as_chunked_file()
            .unwrap();
        let error = reader
            .decompress_chunked_range(record, record_index, 0, 1)
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);

        replace_block_entries(&mut reader, record_index, &[(0, data_end)]);
        let record = reader
            .metadata()
            .record(record_index)
            .unwrap()
            .as_chunked_file()
            .unwrap();
        let error = reader
            .decompress_chunked_block(record, record_index, 0)
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);

        replace_block_entries(
            &mut reader,
            record_index,
            &[(0, data_start + 1), (64, data_start)],
        );
        let record = reader
            .metadata()
            .record(record_index)
            .unwrap()
            .as_chunked_file()
            .unwrap();
        let error = reader
            .decompress_chunked(record, record_index, Vec::new())
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    }

    // [spec:box:req:records.root.references.resolution/test]
    #[test]
    fn sync_extraction_rejects_unindexed_link_target() {
        let temp = tempfile::tempdir().unwrap();
        let archive = temp.path().join("unindexed-link-target.box");
        let target_path = BoxPath::new("target.txt").unwrap();
        let link_path = BoxPath::new("link.txt").unwrap();

        let mut writer = BoxWriter::create(&archive).unwrap();
        writer
            .insert(
                &CompressionConfig::new(Compression::Stored),
                target_path.clone(),
                std::io::Cursor::new(b"target"),
                HashMap::new(),
            )
            .unwrap();
        let target_index = writer.metadata().index(&target_path).unwrap();
        let link_index = writer
            .link(link_path.clone(), target_index, HashMap::new())
            .unwrap();
        writer.finish().unwrap();

        let mut reader = BoxReader::open(&archive).unwrap();
        let mut builder = box_fst::FstBuilder::<u64>::new();
        builder
            .insert(link_path.as_ref(), link_index.get())
            .unwrap();
        let bytes = builder.finish().unwrap();
        reader.core.meta.fst = Some(
            box_fst::Fst::new(std::borrow::Cow::Owned(bytes))
                .expect("test path FST should be well-formed"),
        );
        reader.core.meta.root.clear();

        assert!(reader.core.record(target_index).is_some());
        assert!(reader.core.path_for_index(target_index).is_none());

        let output = temp.path().join("out");
        let error = reader.extract(&link_path, &output).unwrap_err();
        match error {
            crate::aio::ExtractError::ResolveLinkFailed(source, link) => {
                assert_eq!(source.kind(), std::io::ErrorKind::NotFound);
                assert_eq!(link.target, target_index);
            }
            other => panic!("expected unindexed link target error, got {other:?}"),
        }
        assert!(!output.join("link.txt").exists());
    }

    // [spec:box:sem:sync-io.root.extraction-validation+2/test/unit]
    #[test]
    fn recursive_extraction_rejects_legacy_self_child_cycles() {
        let temp = tempfile::tempdir().unwrap();
        let archive = temp.path().join("legacy-self-child-cycle.box");
        let directory_path = BoxPath::new("loop").unwrap();

        let mut writer = BoxWriter::create(&archive).unwrap();
        writer
            .mkdir(directory_path.clone(), HashMap::new())
            .unwrap();
        writer.finish().unwrap();

        let mut reader = BoxReader::open(&archive).unwrap();
        let directory_index = reader.metadata().index(&directory_path).unwrap();
        reader.core.meta.fst = None;
        reader.core.meta.root = vec![directory_index];
        reader
            .core
            .meta
            .record_mut(directory_index)
            .unwrap()
            .as_directory_mut()
            .unwrap()
            .entries = vec![directory_index];

        let output = temp.path().join("out");
        let error = reader
            .extract_recursive(&directory_path, &output)
            .unwrap_err();
        match error {
            crate::aio::ExtractError::InvalidArchiveHierarchy(source, path) => {
                assert_eq!(source.kind(), std::io::ErrorKind::InvalidData);
                assert!(
                    source
                        .to_string()
                        .contains(&directory_index.get().to_string())
                );
                assert_eq!(path, std::path::PathBuf::from("loop/loop"));
            }
            other => panic!("expected invalid archive hierarchy, got {other:?}"),
        }
        assert!(output.join("loop").is_dir());
    }

    // [spec:box:req:paths.root.extraction-gates+1/test/unit]
    // [spec:box:sem:sync-io.root.extraction-validation+2/test/unit]
    // [spec:box:req:records.root.references.resolution/test/unit]
    #[test]
    fn extraction_rejects_hostile_legacy_names_before_write() {
        let temp = tempfile::tempdir().unwrap();
        let archive = temp.path().join("hostile-legacy-name.box");
        let root_path = BoxPath::new("root").unwrap();
        let safe_path = BoxPath::new("root/safe.txt").unwrap();
        let link_path = BoxPath::new("root/link").unwrap();

        let mut writer = BoxWriter::create(&archive).unwrap();
        writer.mkdir(root_path.clone(), HashMap::new()).unwrap();
        writer
            .insert(
                &CompressionConfig::new(Compression::Stored),
                safe_path.clone(),
                std::io::Cursor::new(b"payload"),
                HashMap::new(),
            )
            .unwrap();
        let root_index = writer.metadata().index(&root_path).unwrap();
        let file_index = writer.metadata().index(&safe_path).unwrap();
        let link_index = writer
            .link(link_path.clone(), file_index, HashMap::new())
            .unwrap();
        writer.finish().unwrap();

        let mut reader = BoxReader::open(&archive).unwrap();
        reader.core.meta.fst = None;
        reader.core.meta.root = vec![root_index];
        reader
            .core
            .meta
            .record_mut(root_index)
            .unwrap()
            .as_directory_mut()
            .unwrap()
            .entries = vec![file_index, link_index];
        reader
            .core
            .meta
            .record_mut(file_index)
            .unwrap()
            .as_file_mut()
            .unwrap()
            .name = std::borrow::Cow::Owned("sub/../../../outside.txt".to_string());

        let serial_output = temp.path().join("serial-out");
        assert!(matches!(
            reader
                .extract_all_with_options(&serial_output, crate::aio::ExtractOptions::default())
                .unwrap_err(),
            crate::aio::ExtractError::InvalidArchiveHierarchy(_, _)
        ));

        let recursive_output = temp.path().join("recursive-out");
        assert!(matches!(
            reader
                .extract_recursive(&root_path, &recursive_output)
                .unwrap_err(),
            crate::aio::ExtractError::InvalidArchiveHierarchy(_, _)
        ));

        let link_output = temp.path().join("link-out");
        assert!(matches!(
            reader.extract(&link_path, &link_output).unwrap_err(),
            crate::aio::ExtractError::ResolveLinkFailed(_, _)
        ));

        assert!(!temp.path().join("outside.txt").exists());
        assert!(!link_output.join("root/link").exists());
    }
}
