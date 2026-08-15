use box_format::aio::{BoxFileReader, BoxFileWriter};
use box_format::{BoxPath, Compression, HashMap};

// [spec:box:sem:chunked-io.root.block-cache/test/integration]
#[tokio::test]
async fn chunked_reader_survives_cache_hits_and_evictions() {
    let temp = tempfile::tempdir().unwrap();
    let archive = temp.path().join("chunk-cache.box");
    let block_size = 32u32;
    let payload: Vec<u8> = (0..(block_size as usize * 10))
        .map(|offset| ((offset * 17) % 251) as u8)
        .collect();

    let mut writer = BoxFileWriter::create(&archive).await.unwrap();
    writer
        .insert_chunked(
            BoxPath::new("cached.bin").unwrap(),
            std::io::Cursor::new(payload.clone()),
            block_size,
            Compression::Zstd,
            HashMap::new(),
        )
        .await
        .unwrap();
    writer.finish().await.unwrap();

    let reader = BoxFileReader::open(&archive).await.unwrap();
    let index = reader
        .metadata()
        .index(&BoxPath::new("cached.bin").unwrap())
        .unwrap();
    let record = reader
        .metadata()
        .record(index)
        .unwrap()
        .as_chunked_file()
        .unwrap();
    let mut chunked = reader.chunked_reader(record, index).unwrap();

    // The default cache holds eight blocks. Fill it, refresh its oldest entry,
    // and force an eviction through the public archive reader.
    let mut byte = [0u8; 1];
    for block in 0..8u64 {
        let offset = block * u64::from(block_size);
        assert_eq!(chunked.read_at(offset, &mut byte).await.unwrap(), 1);
        assert_eq!(byte[0], payload[offset as usize]);
    }
    assert_eq!(chunked.cached_block_count(), 8);
    for block in 0..8u64 {
        assert!(chunked.is_block_cached(block * u64::from(block_size)));
    }

    // A hit on block zero refreshes its LRU position. Loading block eight must
    // therefore evict block one rather than the refreshed block zero.
    assert_eq!(chunked.read_at(0, &mut byte).await.unwrap(), 1);
    assert_eq!(byte[0], payload[0]);
    let new_offset = 8 * u64::from(block_size);
    assert_eq!(chunked.read_at(new_offset, &mut byte).await.unwrap(), 1);
    assert_eq!(byte[0], payload[new_offset as usize]);

    assert_eq!(chunked.cached_block_count(), 8);
    assert!(chunked.is_block_cached(0));
    assert!(!chunked.is_block_cached(u64::from(block_size)));
    assert!(chunked.is_block_cached(new_offset));
}
