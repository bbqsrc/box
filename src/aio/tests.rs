#[cfg(test)]
mod tests {
    use crate::{compression::Compression, *};
    use crate::compat::HashMap;
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

    #[tokio::test]
    async fn create_box_file() {
        create_test_box("./smoketest.box").await;
    }

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
            dir_attrs.insert("created".into(), now.to_vec());
            dir_attrs.insert("unix.mode".into(), 0o755u16.to_le_bytes().to_vec());

            let mut attrs = HashMap::new();
            attrs.insert("created".into(), now.to_vec());
            attrs.insert("unix.mode".into(), 0o644u16.to_le_bytes().to_vec());

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
}
