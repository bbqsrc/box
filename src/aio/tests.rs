#[cfg(test)]
mod tests {
    use crate::compat::HashMap;
    use crate::{compression::Compression, *};
    use std::path::Path;

    async fn create_test_box<F: AsRef<Path>>(filename: F) {
        let _ = std::fs::remove_file(filename.as_ref());

        let mut cursor = std::io::Cursor::new(b"hello\0\0\0".to_vec());

        let mut writer = BoxFileWriter::create(filename).await.unwrap();
        writer
            .insert(
                &CompressionConfig::new(Compression::Stored),
                BoxPath::new("hello.txt").unwrap(),
                &mut cursor,
                HashMap::new(),
            )
            .await
            .unwrap();
        writer.finish().await.unwrap();
    }

    // [spec:box:sem:async-io.root/test/integration]
    // [spec:box:sem:async-io.root.writer-lifecycle/test/integration]
    #[tokio::test]
    async fn create_box_file() {
        create_test_box("./smoketest.box").await;
    }

    // [spec:box:req:sans-io.root.hierarchy/test]
    #[tokio::test]
    async fn async_writer_rejects_invalid_parents() {
        let temp = tempfile::tempdir().unwrap();
        let archive = temp.path().join("invalid-parent.box");
        let mut writer = BoxFileWriter::create(&archive).await.unwrap();

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
            .await
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

        writer.finish().await.unwrap();
        let reader = BoxFileReader::open(&archive).await.unwrap();
        assert_eq!(reader.metadata().records.len(), record_count);
        assert!(reader.metadata().index(&nested_child).is_none());
    }

    // [spec:box:sem:chunked-io.root.block-cache/test/unit]
    #[test]
    fn block_cache_enforces_capacity_keying_and_lru() {
        use crate::aio::BlockCache;

        assert!(std::panic::catch_unwind(|| BlockCache::new(0)).is_err());

        let mut keyed = BlockCache::new(2);
        keyed.insert(1, 0, vec![1].into_boxed_slice());
        keyed.insert(2, 0, vec![2].into_boxed_slice());
        assert_eq!(keyed.get(1, 0), Some([1].as_slice()));
        assert_eq!(keyed.get(2, 0), Some([2].as_slice()));

        let mut cache = BlockCache::default();
        for offset in 0..8 {
            cache.insert(7, offset * 64, vec![offset as u8].into_boxed_slice());
        }
        assert_eq!(cache.len(), 8);

        // Refresh the oldest entry, then force one eviction. The next-oldest
        // entry must be evicted while the refreshed entry remains resident.
        assert_eq!(cache.get(7, 0), Some([0].as_slice()));
        cache.insert(7, 8 * 64, vec![8].into_boxed_slice());
        assert!(cache.contains(7, 0));
        assert!(!cache.contains(7, 64));
        assert!(cache.contains(7, 8 * 64));
        assert_eq!(cache.len(), 8);

        cache.clear();
        assert!(cache.is_empty());
    }

    #[tokio::test]
    async fn parallel_small_file_zstd_roundtrip() {
        let temp = tempfile::tempdir().unwrap();
        let archive = temp.path().join("small-files.box");
        let mut expected = Vec::new();
        let mut jobs = Vec::new();

        for index in 0..64 {
            let name = format!("file-{index}.txt");
            let contents = format!(
                "small package file {index}: repeated repeated repeated repeated repeated\n"
            )
            .repeat(4)
            .into_bytes();
            let fs_path = temp.path().join(&name);
            std::fs::write(&fs_path, &contents).unwrap();
            expected.push((name.clone(), contents));
            jobs.push(FileJob {
                fs_path,
                box_path: BoxPath::new(name).unwrap(),
                config: CompressionConfig::new(Compression::Zstd),
                attrs: HashMap::new(),
            });
        }

        let mut writer = BoxFileWriter::create(&archive).await.unwrap();
        let stats = writer
            .add_paths_parallel(jobs, true, false, false, usize::MAX)
            .await
            .unwrap();
        assert_eq!(stats.files_added, expected.len() as u64);
        writer.finish().await.unwrap();

        let reader = BoxFileReader::open(&archive).await.unwrap();
        for (name, contents) in expected {
            let index = reader
                .metadata()
                .index(&BoxPath::new(name).unwrap())
                .unwrap();
            let record = reader.metadata().record(index).unwrap().as_file().unwrap();
            let mut actual = Vec::new();
            reader.decompress(record, &mut actual).await.unwrap();
            assert_eq!(actual, contents);
        }
    }

    // [spec:box:sem:async-io.root.open/test/integration]
    #[tokio::test]
    async fn read_garbage() {
        let filename = "./read_garbage.box";
        create_test_box(filename).await;

        let bf = BoxFileReader::open(filename).await.unwrap();
        let trailer = bf.metadata();
        println!("{:?}", bf.core.header);
        println!("{:?}", &trailer);
        let segment = bf
            .memory_map(trailer.records[0].as_file().unwrap())
            .unwrap();
        let file_data = segment.as_slice().unwrap();
        println!("{:?}", &file_data);
        assert_eq!(file_data, b"hello\0\0\0")
    }

    #[tokio::test]
    async fn create_garbage() {
        let filename = "./create_garbage.box";
        let _ = std::fs::remove_file(filename);
        let bf = BoxFileWriter::create(filename).await.expect("Mah box");
        bf.finish().await.unwrap();
    }

    // [spec:box:sem:async-io.root.read/test/integration]
    #[tokio::test]
    async fn read_bytes() {
        let filename = "./read_bytes.box";
        create_test_box(filename).await;
        let bf = BoxFileReader::open(filename).await.unwrap();
        let record = bf
            .metadata()
            .records
            .first()
            .map(|f| f.as_file().unwrap())
            .unwrap();
        let mut reader = bf.read_bytes(record).await.unwrap();
        let mut vec = vec![];
        tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut vec)
            .await
            .unwrap();
        assert_eq!(vec, b"hello\0\0\0")
    }

    // [spec:box:sem:async-io.root.read/test/unit]
    #[tokio::test]
    async fn async_regular_reads_reject_embedded_offset_overflow() {
        let temp = tempfile::tempdir().unwrap();
        let archive = temp.path().join("regular-offset-overflow.box");
        create_test_box(&archive).await;

        let mut reader = BoxFileReader::open(&archive).await.unwrap();
        let mut record = reader.metadata().records[0].as_file().unwrap().clone();
        record.data = std::num::NonZeroU64::new(u64::MAX).unwrap();
        reader.core.offset = 1;

        assert_eq!(
            reader.memory_map(&record).unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
        );
        assert_eq!(
            reader.read_bytes(&record).await.unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
        );
    }

    async fn insert_impl(filename: &str, mut bf: BoxFileWriter) {
        let v =
            "This, this, this, this, this is a compressable string string string string string.\n"
                .to_string();

        {
            use std::time::SystemTime;
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs()
                .to_le_bytes();

            let mut dir_attrs = HashMap::new();
            dir_attrs.insert(crate::attrs::CREATED.into(), now.to_vec());
            dir_attrs.insert(
                crate::attrs::UNIX_MODE.into(),
                0o755u16.to_le_bytes().to_vec(),
            );

            let mut attrs = HashMap::new();
            attrs.insert(crate::attrs::CREATED.into(), now.to_vec());
            attrs.insert(
                crate::attrs::UNIX_MODE.into(),
                0o644u16.to_le_bytes().to_vec(),
            );

            bf.mkdir(BoxPath::new("test").unwrap(), dir_attrs).unwrap();

            bf.insert(
                &CompressionConfig::new(Compression::Zstd),
                BoxPath::new("test/string.txt").unwrap(),
                &mut std::io::Cursor::new(v.clone()),
                attrs.clone(),
            )
            .await
            .unwrap();
            bf.insert(
                &CompressionConfig::new(Compression::Zstd),
                BoxPath::new("test/string2.txt").unwrap(),
                &mut std::io::Cursor::new(v.clone()),
                attrs.clone(),
            )
            .await
            .unwrap();
            bf.finish().await.unwrap();
        }

        let bf = BoxFileReader::open(filename).await.expect("Mah box");
        println!("{:#?}", &bf);

        let mut buf1 = Vec::new();
        bf.decompress(bf.core.meta.records[1].as_file().unwrap(), &mut buf1)
            .await
            .unwrap();
        assert_eq!(v, String::from_utf8(buf1).unwrap());

        let mut buf2 = Vec::new();
        bf.decompress(bf.core.meta.records[2].as_file().unwrap(), &mut buf2)
            .await
            .unwrap();
        assert_eq!(v, String::from_utf8(buf2).unwrap());
    }

    #[tokio::test]
    async fn insert() {
        let _ = std::fs::remove_file("./insert_garbage.box");
        insert_impl(
            "./insert_garbage.box",
            BoxFileWriter::create("./insert_garbage.box").await.unwrap(),
        )
        .await;

        let _ = std::fs::remove_file("./insert_garbage_align8.box");
        insert_impl(
            "./insert_garbage_align8.box",
            BoxFileWriter::create_with_alignment("./insert_garbage_align8.box", 8)
                .await
                .unwrap(),
        )
        .await;

        let _ = std::fs::remove_file("./insert_garbage_align7.box");
        insert_impl(
            "./insert_garbage_align7.box",
            BoxFileWriter::create_with_alignment("./insert_garbage_align7.box", 7)
                .await
                .unwrap(),
        )
        .await;
    }

    // [spec:box:sem:chunked-io.root/test/integration]
    // [spec:box:sem:chunked-io.root.explicit-insert/test/integration]
    // [spec:box:syn:chunked-io.root.block-index-entry/test/integration]
    // [spec:box:sem:chunked-io.root.block-decompression/test/integration]
    #[tokio::test]
    async fn chunked_file_roundtrip() {
        let filename = "./chunked_roundtrip.box";
        let _ = std::fs::remove_file(filename);

        // Create content large enough for multiple blocks (use small block size for testing)
        let block_size: u32 = 64;
        let content = "ABCD".repeat(100); // 400 bytes = ~6 blocks at 64 bytes each
        let content_bytes = content.as_bytes();

        // Write chunked file
        {
            let mut writer = BoxFileWriter::create(filename).await.unwrap();
            let reader = std::io::Cursor::new(content_bytes.to_vec());

            writer
                .insert_chunked(
                    BoxPath::new("chunked.txt").unwrap(),
                    reader,
                    block_size,
                    Compression::Zstd,
                    HashMap::new(),
                )
                .await
                .unwrap();
            writer.finish().await.unwrap();
        }

        // Read and verify
        let bf = BoxFileReader::open(filename).await.unwrap();

        // Verify block FST is present
        assert!(
            bf.metadata().block_fst.is_some(),
            "block FST should be built for chunked files"
        );

        // Get the chunked file record
        let path = BoxPath::new("chunked.txt").unwrap();
        let record_index = bf.metadata().index(&path).expect("record should exist");
        let record = bf
            .metadata()
            .record(record_index)
            .expect("record should be accessible");
        let chunked = record.as_chunked_file().expect("should be chunked file");

        // Verify blocks are tracked
        let blocks = bf.metadata().blocks_for_record(record_index);
        assert!(!blocks.is_empty(), "chunked file should have block entries");
        assert!(
            blocks.len() >= 2,
            "with 400 bytes and 64-byte blocks, should have multiple blocks (got {})",
            blocks.len()
        );

        // Extract and compare
        let mut extracted = Vec::new();
        bf.decompress_chunked(chunked, record_index, &mut extracted)
            .await
            .unwrap();

        assert_eq!(
            extracted.len(),
            content_bytes.len(),
            "extracted size should match original"
        );
        assert_eq!(
            extracted, content_bytes,
            "extracted content should match original"
        );

        // Cleanup
        let _ = std::fs::remove_file(filename);
    }

    // [spec:box:sem:chunked-io.root.block-queries/test/integration]
    // [spec:box:sem:chunked-io.root.async-range/test/integration]
    #[tokio::test]
    async fn chunked_random_access() {
        let filename = "./chunked_random_access.box";
        let _ = std::fs::remove_file(filename);

        // Create content with recognizable pattern - 400 bytes total
        let block_size: u32 = 64;
        let content = "ABCD".repeat(100); // 400 bytes = ~6 blocks at 64 bytes each
        let content_bytes = content.as_bytes();

        // Write chunked file
        {
            let mut writer = BoxFileWriter::create(filename).await.unwrap();
            let reader = std::io::Cursor::new(content_bytes.to_vec());

            writer
                .insert_chunked(
                    BoxPath::new("chunked.txt").unwrap(),
                    reader,
                    block_size,
                    Compression::Zstd,
                    HashMap::new(),
                )
                .await
                .unwrap();
            writer.finish().await.unwrap();
        }

        // Open and test random access
        let bf = BoxFileReader::open(filename).await.unwrap();
        let path = BoxPath::new("chunked.txt").unwrap();
        let record_index = bf.metadata().index(&path).expect("record should exist");
        let record = bf
            .metadata()
            .record(record_index)
            .expect("record should be accessible");
        let chunked = record.as_chunked_file().expect("should be chunked file");

        // Test 1: Read from start (first block partial)
        let data = bf
            .read_chunked_range(chunked, record_index, 0, 10)
            .await
            .unwrap();
        assert_eq!(&data, &content_bytes[0..10]);

        // Test 2: Read from middle (may cross block boundary)
        let data = bf
            .read_chunked_range(chunked, record_index, 60, 20)
            .await
            .unwrap();
        assert_eq!(&data, &content_bytes[60..80]);

        // Test 3: Read exact block size
        let data = bf
            .read_chunked_range(chunked, record_index, 64, 64)
            .await
            .unwrap();
        assert_eq!(&data, &content_bytes[64..128]);

        // Test 4: Read across multiple blocks
        let data = bf
            .read_chunked_range(chunked, record_index, 50, 100)
            .await
            .unwrap();
        assert_eq!(&data, &content_bytes[50..150]);

        // Test 5: Read from end (last block partial)
        let data = bf
            .read_chunked_range(chunked, record_index, 390, 10)
            .await
            .unwrap();
        assert_eq!(&data, &content_bytes[390..400]);

        // Test 6: Empty read
        let data = bf
            .read_chunked_range(chunked, record_index, 100, 0)
            .await
            .unwrap();
        assert!(data.is_empty());

        // Test 7: Read entire file
        let data = bf
            .read_chunked_range(chunked, record_index, 0, 400)
            .await
            .unwrap();
        assert_eq!(&data, content_bytes);

        // Cleanup
        let _ = std::fs::remove_file(filename);
    }

    #[tokio::test]
    async fn chunked_slice_access() {
        let filename = "./chunked_slice_access.box";
        let _ = std::fs::remove_file(filename);

        let block_size: u32 = 64;
        let content = "Hello, World! ".repeat(30); // 420 bytes
        let content_bytes = content.as_bytes();

        // Write chunked file
        {
            let mut writer = BoxFileWriter::create(filename).await.unwrap();
            let reader = std::io::Cursor::new(content_bytes.to_vec());

            writer
                .insert_chunked(
                    BoxPath::new("test.txt").unwrap(),
                    reader,
                    block_size,
                    Compression::Zstd,
                    HashMap::new(),
                )
                .await
                .unwrap();
            writer.finish().await.unwrap();
        }

        // Open and test ChunkedSlice
        let bf = BoxFileReader::open(filename).await.unwrap();
        let path = BoxPath::new("test.txt").unwrap();
        let record_index = bf.metadata().index(&path).expect("record should exist");
        let record = bf
            .metadata()
            .record(record_index)
            .expect("record should be accessible");
        let chunked = record.as_chunked_file().expect("should be chunked file");

        // Get ChunkedSlice
        let slice = bf.chunked_slice(chunked, record_index).await.unwrap();

        // Test Deref to &[u8]
        let data: &[u8] = &*slice;
        assert_eq!(data.len(), content_bytes.len());
        assert_eq!(data, content_bytes);

        // Test AsRef
        let data: &[u8] = slice.as_ref();
        assert_eq!(data, content_bytes);

        // Test len() and is_empty()
        assert_eq!(slice.len(), content_bytes.len());
        assert!(!slice.is_empty());

        // Test indexing (via Deref)
        assert_eq!(slice[0], b'H');
        assert_eq!(&slice[0..5], b"Hello");

        // Test into_vec
        let vec = slice.into_vec();
        assert_eq!(vec, content_bytes);

        // Cleanup
        let _ = std::fs::remove_file(filename);
    }

    // [spec:box:sem:chunked-io.root.slice-extraction/test/integration]
    #[tokio::test]
    async fn chunked_slice_and_extraction_reconstruct_payload() {
        let temp = tempfile::tempdir().unwrap();
        let archive = temp.path().join("chunked-slice-extraction.box");
        let payload = b"slice and extraction payload".repeat(40);

        let mut writer = BoxFileWriter::create(&archive).await.unwrap();
        writer
            .mkdir(BoxPath::new("tree").unwrap(), HashMap::new())
            .unwrap();
        writer
            .insert_chunked(
                BoxPath::new("tree/payload.bin").unwrap(),
                std::io::Cursor::new(payload.clone()),
                64,
                Compression::Zstd,
                HashMap::new(),
            )
            .await
            .unwrap();
        writer.finish().await.unwrap();

        let reader = BoxFileReader::open(&archive).await.unwrap();
        let path = BoxPath::new("tree/payload.bin").unwrap();
        let record_index = reader.metadata().index(&path).unwrap();
        let record = reader
            .metadata()
            .record(record_index)
            .unwrap()
            .as_chunked_file()
            .unwrap();
        let slice = reader.chunked_slice(record, record_index).await.unwrap();
        assert_eq!(slice.as_ref(), payload);
        assert_eq!(slice.into_vec(), payload);

        let async_output = temp.path().join("async-output");
        reader.extract_all(&async_output).await.unwrap();
        assert_eq!(
            std::fs::read(async_output.join("tree/payload.bin")).unwrap(),
            payload
        );

        let sync_reader = crate::sync::BoxReader::open(&archive).unwrap();
        let sync_output = temp.path().join("sync-output");
        sync_reader.extract_all(&sync_output).unwrap();
        assert_eq!(
            std::fs::read(sync_output.join("tree/payload.bin")).unwrap(),
            payload
        );
    }

    // [spec:box:sem:chunked-io.root.async-range/test/integration]
    #[tokio::test]
    async fn chunked_range_boundary_errors() {
        let filename = "./chunked_range_errors.box";
        let _ = std::fs::remove_file(filename);

        let block_size: u32 = 64;
        let content = "Test".repeat(25); // 100 bytes

        // Write chunked file
        {
            let mut writer = BoxFileWriter::create(filename).await.unwrap();
            let reader = std::io::Cursor::new(content.as_bytes().to_vec());

            writer
                .insert_chunked(
                    BoxPath::new("test.txt").unwrap(),
                    reader,
                    block_size,
                    Compression::Zstd,
                    HashMap::new(),
                )
                .await
                .unwrap();
            writer.finish().await.unwrap();
        }

        let bf = BoxFileReader::open(filename).await.unwrap();
        let path = BoxPath::new("test.txt").unwrap();
        let record_index = bf.metadata().index(&path).unwrap();
        let record = bf.metadata().record(record_index).unwrap();
        let chunked = record.as_chunked_file().unwrap();

        // Test: Read past end of file
        let result = bf.read_chunked_range(chunked, record_index, 90, 20).await;
        assert!(result.is_err());

        // Test: Offset past end of file
        let result = bf.read_chunked_range(chunked, record_index, 200, 10).await;
        assert!(result.is_err());

        // Cleanup
        let _ = std::fs::remove_file(filename);
    }

    // [spec:box:sem:chunked-io.root.seek-reader/test/integration]
    #[tokio::test]
    async fn chunked_reader_sequential() {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};

        let filename = "./chunked_reader_sequential.box";
        let _ = std::fs::remove_file(filename);

        let block_size: u32 = 64;
        let content = "ABCD".repeat(100); // 400 bytes = ~6 blocks at 64 bytes each
        let content_bytes = content.as_bytes();

        // Write chunked file
        {
            let mut writer = BoxFileWriter::create(filename).await.unwrap();
            let reader = std::io::Cursor::new(content_bytes.to_vec());

            writer
                .insert_chunked(
                    BoxPath::new("chunked.txt").unwrap(),
                    reader,
                    block_size,
                    Compression::Zstd,
                    HashMap::new(),
                )
                .await
                .unwrap();
            writer.finish().await.unwrap();
        }

        // Open and test ChunkedReader
        let bf = BoxFileReader::open(filename).await.unwrap();
        let path = BoxPath::new("chunked.txt").unwrap();
        let record_index = bf.metadata().index(&path).expect("record should exist");
        let record = bf
            .metadata()
            .record(record_index)
            .expect("record should be accessible");
        let chunked = record.as_chunked_file().expect("should be chunked file");

        let mut reader = bf.chunked_reader(chunked, record_index).unwrap();

        // Test 1: Read first 10 bytes
        let mut buf = vec![0u8; 10];
        reader.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, &content_bytes[0..10]);

        // Test 2: Read next 20 bytes (sequential)
        let mut buf = vec![0u8; 20];
        reader.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, &content_bytes[10..30]);

        // Test 3: Read across block boundary (should trigger new block load)
        let mut buf = vec![0u8; 50];
        reader.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, &content_bytes[30..80]);

        // Test 4: Seek to a position
        reader.seek(std::io::SeekFrom::Start(200)).await.unwrap();
        let mut buf = vec![0u8; 50];
        reader.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, &content_bytes[200..250]);

        // Test 5: Seek from current
        reader.seek(std::io::SeekFrom::Current(-50)).await.unwrap();
        let mut buf = vec![0u8; 50];
        reader.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, &content_bytes[200..250]);

        // Test 6: Read to end
        reader.seek(std::io::SeekFrom::Start(0)).await.unwrap();
        let mut all = Vec::new();
        reader.read_to_end(&mut all).await.unwrap();
        assert_eq!(all, content_bytes);

        // Cleanup
        let _ = std::fs::remove_file(filename);
    }

    // [spec:box:sem:chunked-io.root.seek-reader/test/integration]
    #[tokio::test]
    async fn chunked_reader_random_access() {
        let filename = "./chunked_reader_random.box";
        let _ = std::fs::remove_file(filename);

        let block_size: u32 = 64;
        // Create pattern where each position has unique content
        let content: Vec<u8> = (0..400u32).map(|i| (i % 256) as u8).collect();

        // Write chunked file
        {
            let mut writer = BoxFileWriter::create(filename).await.unwrap();
            let reader = std::io::Cursor::new(content.clone());

            writer
                .insert_chunked(
                    BoxPath::new("data.bin").unwrap(),
                    reader,
                    block_size,
                    Compression::Zstd,
                    HashMap::new(),
                )
                .await
                .unwrap();
            writer.finish().await.unwrap();
        }

        let bf = BoxFileReader::open(filename).await.unwrap();
        let path = BoxPath::new("data.bin").unwrap();
        let record_index = bf.metadata().index(&path).unwrap();
        let record = bf.metadata().record(record_index).unwrap();
        let chunked = record.as_chunked_file().unwrap();

        let mut reader = bf.chunked_reader(chunked, record_index).unwrap();

        // True random access with read_at - no sequential state
        let mut buf = [0u8; 20];

        // Jump around randomly
        reader.read_at(350, &mut buf).await.unwrap();
        assert_eq!(&buf, &content[350..370]);

        reader.read_at(50, &mut buf).await.unwrap();
        assert_eq!(&buf, &content[50..70]);

        reader.read_at(200, &mut buf).await.unwrap();
        assert_eq!(&buf, &content[200..220]);

        reader.read_at(0, &mut buf).await.unwrap();
        assert_eq!(&buf, &content[0..20]);

        // Single byte access at arbitrary positions
        let mut byte = [0u8; 1];
        reader.read_at(137, &mut byte).await.unwrap();
        assert_eq!(byte[0], content[137]);

        reader.read_at(299, &mut byte).await.unwrap();
        assert_eq!(byte[0], content[299]);

        // Cross block boundary reads
        let mut buf = [0u8; 20];
        reader.read_at(60, &mut buf).await.unwrap(); // crosses 64-byte boundary
        assert_eq!(&buf, &content[60..80]);

        reader.read_at(120, &mut buf).await.unwrap();
        assert_eq!(&buf, &content[120..140]);

        // Verify position wasn't changed by read_at
        assert_eq!(reader.position(), 0);

        // Cleanup
        let _ = std::fs::remove_file(filename);
    }

    // [spec:box:sem:chunked-io.root.explicit-insert/test]
    #[tokio::test]
    async fn chunked_writer_rejects_zero_block_and_empty_input() {
        let temp = tempfile::tempdir().unwrap();
        let archive = temp.path().join("invalid-empty-chunked.box");
        let mut writer = BoxFileWriter::create(&archive).await.unwrap();

        for (path, block_size) in [("zero-block.bin", 0), ("empty.bin", 8)] {
            let error = writer
                .insert_chunked(
                    BoxPath::new(path).unwrap(),
                    std::io::Cursor::new(Vec::<u8>::new()),
                    block_size,
                    Compression::Stored,
                    HashMap::new(),
                )
                .await
                .unwrap_err();
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        }

        assert!(writer.metadata().records.is_empty());
        writer.finish().await.unwrap();
        let reader = BoxFileReader::open(&archive).await.unwrap();
        assert!(reader.metadata().records.is_empty());
    }

    // [spec:box:sem:validation.root.parallel/test]
    // [spec:box:sem:extraction.root.parallel-ordering/test]
    #[tokio::test]
    async fn parallel_workflows_safely_bound_extreme_concurrency() {
        let temp = tempfile::tempdir().unwrap();
        let archive = temp.path().join("zero-validation-workers.box");
        create_test_box(&archive).await;

        let reader = BoxFileReader::open(&archive).await.unwrap();
        let stats = reader.validate_all_parallel(0).await.unwrap();
        assert_eq!(stats.files_checked, 1);

        let output = temp.path().join("zero-extraction-workers");
        let stats = reader
            .extract_all_parallel(&output, ExtractOptions::default(), 0)
            .await
            .unwrap();
        assert_eq!(stats.files_extracted, 1);
        assert_eq!(
            tokio::fs::read(output.join("hello.txt")).await.unwrap(),
            b"hello\0\0\0"
        );

        assert_eq!(
            reader
                .validate_all_parallel(usize::MAX)
                .await
                .unwrap()
                .files_checked,
            1
        );
        let output = temp.path().join("huge-extraction-workers");
        assert_eq!(
            reader
                .extract_all_parallel(&output, ExtractOptions::default(), usize::MAX)
                .await
                .unwrap()
                .files_extracted,
            1
        );
    }

    // [spec:box:req:records.root.references.resolution/test]
    #[cfg(unix)]
    #[tokio::test]
    async fn async_extraction_rejects_unrecorded_fst_link_target() {
        let temp = tempfile::tempdir().unwrap();
        let archive = temp.path().join("dangling-fst-link.box");
        let target_path = BoxPath::new("target.txt").unwrap();
        let link_path = BoxPath::new("link.txt").unwrap();

        let mut writer = BoxFileWriter::create(&archive).await.unwrap();
        writer
            .insert(
                &CompressionConfig::new(Compression::Stored),
                target_path,
                std::io::Cursor::new(b"target"),
                HashMap::new(),
            )
            .await
            .unwrap();
        let target_index = writer
            .metadata()
            .index(&BoxPath::new("target.txt").unwrap())
            .unwrap();
        let link_index = writer
            .link(link_path.clone(), target_index, HashMap::new())
            .unwrap();
        writer.finish().await.unwrap();

        let mut reader = BoxFileReader::open(&archive).await.unwrap();
        let invalid_target = RecordIndex::new(999).unwrap();
        reader
            .core
            .meta
            .record_mut(link_index)
            .unwrap()
            .as_link_mut()
            .unwrap()
            .target = invalid_target;

        let mut builder = box_fst::FstBuilder::<u64>::new();
        builder
            .insert(link_path.as_ref(), link_index.get())
            .unwrap();
        builder
            .insert(b"phantom.txt", invalid_target.get())
            .unwrap();
        reader.core.meta.fst =
            Some(box_fst::Fst::new(std::borrow::Cow::Owned(builder.finish().unwrap())).unwrap());

        assert!(reader.core.record(invalid_target).is_none());
        assert!(reader.core.path_for_index(invalid_target).is_none());

        let output = temp.path().join("out");
        let error = reader.extract(&link_path, &output).await.unwrap_err();
        assert!(matches!(error, ExtractError::ResolveLinkFailed(_, _)));
        assert!(!output.join("link.txt").exists());
    }

    // [spec:box:sem:extraction.root.selection+2/test/unit]
    #[tokio::test]
    async fn recursive_extraction_rejects_fst_alias_cycles() {
        let temp = tempfile::tempdir().unwrap();
        let archive = temp.path().join("fst-alias-cycle.box");
        let directory_path = BoxPath::new("loop").unwrap();
        let alias_path = BoxPath::new("loop/loop").unwrap();

        let mut writer = BoxFileWriter::create(&archive).await.unwrap();
        writer
            .mkdir(directory_path.clone(), HashMap::new())
            .unwrap();
        writer.finish().await.unwrap();

        let mut reader = BoxFileReader::open(&archive).await.unwrap();
        let directory_index = reader.metadata().index(&directory_path).unwrap();
        let mut builder = box_fst::FstBuilder::<u64>::new();
        builder
            .insert(directory_path.as_ref(), directory_index.get())
            .unwrap();
        builder
            .insert(alias_path.as_ref(), directory_index.get())
            .unwrap();
        reader.core.meta.fst =
            Some(box_fst::Fst::new(std::borrow::Cow::Owned(builder.finish().unwrap())).unwrap());
        reader.core.meta.root.clear();

        let output = temp.path().join("out");
        let error = reader
            .extract_recursive(&directory_path, &output)
            .await
            .unwrap_err();
        match error {
            ExtractError::InvalidArchiveHierarchy(source, path) => {
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
    // [spec:box:sem:extraction.root.selection+2/test/unit]
    // [spec:box:req:extraction.root.internal-symlink/test/unit]
    #[tokio::test]
    async fn extraction_rejects_hostile_fst_paths_before_write() {
        let temp = tempfile::tempdir().unwrap();
        let archive = temp.path().join("hostile-fst-path.box");
        let root_path = BoxPath::new("root").unwrap();
        let safe_path = BoxPath::new("root/safe.txt").unwrap();
        let link_path = BoxPath::new("root/link").unwrap();

        let mut writer = BoxFileWriter::create(&archive).await.unwrap();
        writer.mkdir(root_path.clone(), HashMap::new()).unwrap();
        writer
            .insert(
                &CompressionConfig::new(Compression::Stored),
                safe_path.clone(),
                std::io::Cursor::new(b"payload"),
                HashMap::new(),
            )
            .await
            .unwrap();
        let root_index = writer.metadata().index(&root_path).unwrap();
        let file_index = writer.metadata().index(&safe_path).unwrap();
        let link_index = writer
            .link(link_path.clone(), file_index, HashMap::new())
            .unwrap();
        writer.finish().await.unwrap();

        let mut reader = BoxFileReader::open(&archive).await.unwrap();
        reader
            .core
            .meta
            .record_mut(file_index)
            .unwrap()
            .as_file_mut()
            .unwrap()
            .name = std::borrow::Cow::Owned("aaa/../../../outside.txt".to_string());

        let mut mismatched = box_fst::FstBuilder::<u64>::new();
        mismatched
            .insert(root_path.as_ref(), root_index.get())
            .unwrap();
        mismatched
            .insert(link_path.as_ref(), link_index.get())
            .unwrap();
        mismatched
            .insert(safe_path.as_ref(), file_index.get())
            .unwrap();
        reader.core.meta.fst =
            Some(box_fst::Fst::new(std::borrow::Cow::Owned(mismatched.finish().unwrap())).unwrap());
        reader.core.meta.root.clear();

        let recursive_output = temp.path().join("recursive-out");
        assert!(matches!(
            reader
                .extract_recursive(&root_path, &recursive_output)
                .await
                .unwrap_err(),
            ExtractError::InvalidArchiveHierarchy(_, _)
        ));

        let hostile_path = b"root\x1faaa/../../../outside.txt";
        let mut hostile = box_fst::FstBuilder::<u64>::new();
        hostile
            .insert(root_path.as_ref(), root_index.get())
            .unwrap();
        hostile.insert(hostile_path, file_index.get()).unwrap();
        hostile
            .insert(link_path.as_ref(), link_index.get())
            .unwrap();
        reader.core.meta.fst =
            Some(box_fst::Fst::new(std::borrow::Cow::Owned(hostile.finish().unwrap())).unwrap());

        let serial_output = temp.path().join("serial-out");
        assert!(matches!(
            reader
                .extract_all_with_options(&serial_output, ExtractOptions::default())
                .await
                .unwrap_err(),
            ExtractError::InvalidArchiveHierarchy(_, _)
        ));

        let parallel_output = temp.path().join("parallel-out");
        assert!(matches!(
            reader
                .extract_all_parallel(&parallel_output, ExtractOptions::default(), 2)
                .await
                .unwrap_err(),
            ExtractError::InvalidArchiveHierarchy(_, _)
        ));

        let link_output = temp.path().join("link-out");
        assert!(matches!(
            reader.extract(&link_path, &link_output).await.unwrap_err(),
            ExtractError::ResolveLinkFailed(_, _)
        ));

        assert!(!temp.path().join("outside.txt").exists());
        assert!(!link_output.join("root/link").exists());
    }
}
