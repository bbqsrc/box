use std::path::Path;

use box_format::aio::{BoxFileReader, BoxFileWriter};
use box_format::sync::{BoxReader, BoxWriter};
use box_format::{AttrValue, BoxPath, Compression, CompressionConfig, FileJob, HashMap, attrs};

const REGULAR_PATH: &str = "existing.txt";
const CHUNKED_PATH: &str = "existing-chunked.bin";
const EXISTING_DIR: &str = "existing-dir";
const APPENDED_PATH: &str = "appended.txt";
const AUTO_CHUNKED_PATH: &str = "automatic-appended-chunked.bin";
const APPENDED_DIR: &str = "existing-dir/appended-dir";
const NESTED_APPENDED_PATH: &str = "existing-dir/appended-dir/appended-child.txt";
const REGULAR_BYTES: &[u8] = b"regular data written before append";
const APPENDED_BYTES: &[u8] = b"root data written by the reopened writer";
const NESTED_APPENDED_BYTES: &[u8] = b"child data written beneath an existing v1 directory";
const BLOCK_SIZE: u32 = 16;
const DEFAULT_BLOCK_SIZE: u32 = 2_097_152;

fn chunked_bytes() -> Vec<u8> {
    (0..113).map(|offset| ((offset * 29) % 251) as u8).collect()
}

fn box_path(path: &str) -> BoxPath<'static> {
    BoxPath::new(path).unwrap()
}

async fn create_initial_archive(path: &Path) {
    let mut writer = BoxFileWriter::create(path).await.unwrap();
    writer
        .insert(
            &CompressionConfig::new(Compression::Stored),
            box_path(REGULAR_PATH),
            std::io::Cursor::new(REGULAR_BYTES),
            HashMap::new(),
        )
        .await
        .unwrap();
    writer
        .insert_chunked(
            box_path(CHUNKED_PATH),
            std::io::Cursor::new(chunked_bytes()),
            BLOCK_SIZE,
            Compression::Stored,
            HashMap::new(),
        )
        .await
        .unwrap();
    writer
        .mkdir(box_path(EXISTING_DIR), HashMap::new())
        .unwrap();
    writer.finish().await.unwrap();
}

async fn async_regular_bytes(reader: &BoxFileReader, path: &str) -> Vec<u8> {
    let index = reader.metadata().index(&box_path(path)).unwrap();
    let record = reader.metadata().record(index).unwrap().as_file().unwrap();
    let mut bytes = Vec::new();
    reader.decompress(record, &mut bytes).await.unwrap();
    bytes
}

fn sync_regular_bytes(reader: &BoxReader, path: &str) -> Vec<u8> {
    let index = reader.metadata().index(&box_path(path)).unwrap();
    let record = reader.metadata().record(index).unwrap().as_file().unwrap();
    let mut bytes = Vec::new();
    reader.decompress(record, &mut bytes).unwrap();
    bytes
}

// [spec:box:sem:async-io.root.writer-lifecycle/test/integration]
// [spec:box:sem:async-io.root.parallel-compression+1/test/integration]
// [spec:box:sem:sans-io.root.finalization/test/integration]
// [spec:box:syn:chunked-io.root.block-index-entry/test/integration]
// [spec:box:req:chunked-io.root.automatic-creation/test/integration]
// [spec:box:sem:chunked-io.root.block-queries/test/integration]
// [spec:box:sem:chunked-io.root.async-range/test/integration]
// [spec:box:req:checksums.root.attachment/test/integration]
// [spec:box:sem:dictionaries.root/test/integration]
#[tokio::test]
async fn async_reopen_preserves_paths_payloads_and_chunks() {
    let temp = tempfile::tempdir().unwrap();
    let archive = temp.path().join("async-append.box");
    create_initial_archive(&archive).await;

    let initial = BoxFileReader::open(&archive).await.unwrap();
    let chunk_path = box_path(CHUNKED_PATH);
    let original_chunk_index = initial.metadata().index(&chunk_path).unwrap();
    let original_blocks = initial.metadata().blocks_for_record(original_chunk_index);
    assert!(original_blocks.len() > 1);
    drop(initial);

    let mut writer = BoxFileWriter::open(&archive).await.unwrap();
    let appended_source = temp.path().join("parallel-appended-source.txt");
    let automatic_source = temp.path().join("automatic-appended-source.bin");
    let dictionary = b"append integration dictionary for repeated chunk payloads".to_vec();
    let automatic_len = DEFAULT_BLOCK_SIZE as usize * 3 + 73;
    let mut automatic_bytes = dictionary.repeat(automatic_len / dictionary.len() + 1);
    automatic_bytes.truncate(automatic_len);
    tokio::fs::write(&appended_source, APPENDED_BYTES)
        .await
        .unwrap();
    tokio::fs::write(&automatic_source, &automatic_bytes)
        .await
        .unwrap();
    let mut automatic_config =
        CompressionConfig::with_dictionary(Compression::Zstd, dictionary.clone());
    automatic_config.set_option("level", "1");
    writer
        .add_paths_parallel(
            [
                FileJob {
                    fs_path: appended_source,
                    box_path: box_path(APPENDED_PATH),
                    config: CompressionConfig::new(Compression::Stored),
                    attrs: HashMap::new(),
                },
                FileJob {
                    fs_path: automatic_source,
                    box_path: box_path(AUTO_CHUNKED_PATH),
                    config: automatic_config,
                    attrs: HashMap::new(),
                },
            ],
            true,
            false,
            false,
            2,
        )
        .await
        .unwrap();
    writer
        .mkdir(box_path(APPENDED_DIR), HashMap::new())
        .unwrap();
    writer
        .insert(
            &CompressionConfig::new(Compression::Stored),
            box_path(NESTED_APPENDED_PATH),
            std::io::Cursor::new(NESTED_APPENDED_BYTES),
            HashMap::new(),
        )
        .await
        .unwrap();
    writer.finish().await.unwrap();

    let reader = BoxFileReader::open(&archive).await.unwrap();
    assert_eq!(
        async_regular_bytes(&reader, REGULAR_PATH).await,
        REGULAR_BYTES
    );
    assert_eq!(
        async_regular_bytes(&reader, APPENDED_PATH).await,
        APPENDED_BYTES
    );
    assert_eq!(
        async_regular_bytes(&reader, NESTED_APPENDED_PATH).await,
        NESTED_APPENDED_BYTES
    );

    let chunk_index = reader.metadata().index(&chunk_path).unwrap();
    assert_eq!(chunk_index, original_chunk_index);
    assert_eq!(
        reader.metadata().blocks_for_record(chunk_index),
        original_blocks
    );
    let chunk_record = reader
        .metadata()
        .record(chunk_index)
        .unwrap()
        .as_chunked_file()
        .unwrap();
    let mut restored = Vec::new();
    reader
        .decompress_chunked(chunk_record, chunk_index, &mut restored)
        .await
        .unwrap();
    assert_eq!(restored, chunked_bytes());
    assert_eq!(
        reader
            .read_chunked_range(chunk_record, chunk_index, 13, 37)
            .await
            .unwrap(),
        chunked_bytes()[13..50]
    );

    assert_eq!(reader.metadata().dictionary(), Some(dictionary.as_slice()));
    let automatic_path = box_path(AUTO_CHUNKED_PATH);
    let automatic_index = reader.metadata().index(&automatic_path).unwrap();
    assert_ne!(automatic_index, chunk_index);
    let automatic_record = reader
        .metadata()
        .record(automatic_index)
        .unwrap()
        .as_chunked_file()
        .unwrap();
    assert_eq!(automatic_record.block_size, DEFAULT_BLOCK_SIZE);
    assert_eq!(automatic_record.block_count(), 4);
    let automatic_blocks = reader.metadata().blocks_for_record(automatic_index);
    assert_eq!(
        automatic_blocks
            .iter()
            .map(|(logical, _)| *logical)
            .collect::<Vec<_>>(),
        vec![
            0,
            u64::from(DEFAULT_BLOCK_SIZE),
            u64::from(DEFAULT_BLOCK_SIZE) * 2,
            u64::from(DEFAULT_BLOCK_SIZE) * 3,
        ]
    );
    assert_eq!(automatic_blocks[0].1, automatic_record.data.get());
    assert!(
        automatic_blocks
            .windows(2)
            .all(|blocks| blocks[0].1 < blocks[1].1)
    );
    assert!(
        automatic_blocks.last().unwrap().1 < automatic_record.data.get() + automatic_record.length
    );
    assert!(original_blocks.last().unwrap().1 < automatic_blocks[0].1);
    let checksum = match reader
        .metadata()
        .record(automatic_index)
        .unwrap()
        .attr_value(reader.metadata(), attrs::BLAKE3)
    {
        Some(AttrValue::U256(checksum)) => checksum,
        other => panic!("expected appended chunked-file Blake3 checksum, got {other:?}"),
    };
    assert_eq!(checksum, blake3::hash(&automatic_bytes).as_bytes());

    let mut automatic_restored = Vec::new();
    reader
        .decompress_chunked(automatic_record, automatic_index, &mut automatic_restored)
        .await
        .unwrap();
    assert_eq!(automatic_restored, automatic_bytes);
    let range_start = u64::from(DEFAULT_BLOCK_SIZE) - 23;
    assert_eq!(
        reader
            .read_chunked_range(automatic_record, automatic_index, range_start, 80)
            .await
            .unwrap(),
        automatic_bytes[range_start as usize..range_start as usize + 80]
    );
}

// [spec:box:sem:sync-io.root.open/test/integration]
// [spec:box:sem:sync-io.root.write/test/integration]
// [spec:box:sem:sans-io.root.finalization/test/integration]
// [spec:box:syn:chunked-io.root.block-index-entry/test/integration]
#[tokio::test]
async fn sync_reopen_preserves_paths_payloads_and_chunks() {
    let temp = tempfile::tempdir().unwrap();
    let archive = temp.path().join("sync-append.box");
    create_initial_archive(&archive).await;

    let initial = BoxReader::open(&archive).unwrap();
    let chunk_path = box_path(CHUNKED_PATH);
    let original_chunk_index = initial.metadata().index(&chunk_path).unwrap();
    let original_blocks = initial.metadata().blocks_for_record(original_chunk_index);
    assert!(original_blocks.len() > 1);
    drop(initial);

    let mut writer = BoxWriter::open(&archive).unwrap();
    writer
        .insert(
            &CompressionConfig::new(Compression::Stored),
            box_path(APPENDED_PATH),
            std::io::Cursor::new(APPENDED_BYTES),
            HashMap::new(),
        )
        .unwrap();
    writer
        .mkdir(box_path(APPENDED_DIR), HashMap::new())
        .unwrap();
    writer
        .insert(
            &CompressionConfig::new(Compression::Stored),
            box_path(NESTED_APPENDED_PATH),
            std::io::Cursor::new(NESTED_APPENDED_BYTES),
            HashMap::new(),
        )
        .unwrap();
    writer.finish().unwrap();

    let reader = BoxReader::open(&archive).unwrap();
    assert_eq!(sync_regular_bytes(&reader, REGULAR_PATH), REGULAR_BYTES);
    assert_eq!(sync_regular_bytes(&reader, APPENDED_PATH), APPENDED_BYTES);
    assert_eq!(
        sync_regular_bytes(&reader, NESTED_APPENDED_PATH),
        NESTED_APPENDED_BYTES
    );

    let chunk_index = reader.metadata().index(&chunk_path).unwrap();
    assert_eq!(chunk_index, original_chunk_index);
    assert_eq!(
        reader.metadata().blocks_for_record(chunk_index),
        original_blocks
    );
    let chunk_record = reader
        .metadata()
        .record(chunk_index)
        .unwrap()
        .as_chunked_file()
        .unwrap();
    let mut restored = Vec::new();
    reader
        .decompress_chunked(chunk_record, chunk_index, &mut restored)
        .unwrap();
    assert_eq!(restored, chunked_bytes());
    assert_eq!(
        reader
            .decompress_chunked_range(chunk_record, chunk_index, 13, 50)
            .unwrap(),
        chunked_bytes()[13..50]
    );
}
