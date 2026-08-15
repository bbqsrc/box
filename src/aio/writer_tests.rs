use super::*;
use crate::aio::BoxFileReader;

// [spec:box:req:sans-io.root.hierarchy/test]
// [spec:box:sem:chunked-io.root.explicit-insert/test]
// [spec:box:syn:chunked-io.root.block-index-entry/test]
#[tokio::test]
async fn invalid_parent_chunk_insert_recovers_for_retry() {
    let temp = tempfile::tempdir().unwrap();
    let archive = temp.path().join("invalid-parent-chunk.box");
    let mut writer = BoxFileWriter::create(&archive).await.unwrap();

    let parent_path = BoxPath::new("parent").unwrap();
    writer
        .insert(
            &CompressionConfig::new(Compression::Stored),
            parent_path.clone(),
            std::io::Cursor::new(b"not a directory".to_vec()),
            HashMap::new(),
        )
        .await
        .unwrap();

    let mkdir_error = writer
        .mkdir_all(parent_path.clone(), HashMap::new())
        .unwrap_err();
    assert_eq!(mkdir_error.kind(), std::io::ErrorKind::InvalidInput);

    let invalid_path = BoxPath::new("parent/invalid.bin").unwrap();
    let record_count = writer.core.record_count();
    let attr_key_count = writer.metadata().attr_keys().len();
    let block_entry_count = writer.core.block_entries.len();
    let next_write_addr = writer.next_write_addr();
    let file_pos = writer.file_pos;
    let mut invalid_attrs = HashMap::new();
    invalid_attrs.insert("invalid-attempt-only".to_string(), vec![1]);

    let error = writer
        .insert_chunked(
            invalid_path.clone(),
            std::io::Cursor::new(b"nonempty invalid payload".to_vec()),
            4,
            Compression::Stored,
            invalid_attrs,
        )
        .await
        .unwrap_err();

    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert_eq!(writer.core.record_count(), record_count);
    assert_eq!(writer.metadata().attr_keys().len(), attr_key_count);
    assert_eq!(writer.core.block_entries.len(), block_entry_count);
    assert_eq!(writer.next_write_addr(), next_write_addr);
    assert_eq!(writer.file_pos, file_pos);
    assert!(writer.metadata().index(&invalid_path).is_none());

    let valid_path = BoxPath::new("recovered.bin").unwrap();
    let valid_payload = b"valid payload spanning blocks".to_vec();
    let mut failed_attrs = HashMap::new();
    failed_attrs.insert("failed-compression-only".to_string(), vec![1]);
    let error = writer
        .insert_chunked(
            valid_path.clone(),
            std::io::Cursor::new(valid_payload.clone()),
            5,
            Compression::Unknown(0xfe),
            failed_attrs,
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert_eq!(writer.core.record_count(), record_count);
    assert_eq!(writer.metadata().attr_keys().len(), attr_key_count);
    assert_eq!(writer.core.block_entries.len(), block_entry_count);
    assert_eq!(writer.next_write_addr(), next_write_addr);
    assert!(writer.metadata().index(&valid_path).is_none());

    let valid_data = writer
        .insert_chunked(
            valid_path.clone(),
            std::io::Cursor::new(valid_payload.clone()),
            5,
            Compression::Stored,
            HashMap::new(),
        )
        .await
        .unwrap()
        .data;
    assert_eq!(valid_data, next_write_addr);

    writer.finish().await.unwrap();

    let reader = BoxFileReader::open(&archive).await.unwrap();
    assert!(reader.metadata().index(&invalid_path).is_none());
    let valid_index = reader.metadata().index(&valid_path).unwrap();
    let valid_record = reader
        .metadata()
        .record(valid_index)
        .unwrap()
        .as_chunked_file()
        .unwrap();
    let mut restored = Vec::new();
    reader
        .decompress_chunked(valid_record, valid_index, &mut restored)
        .await
        .unwrap();
    assert_eq!(restored, valid_payload);
}

// [spec:box:req:chunked-io.root.automatic-creation/test/unit]
#[tokio::test]
async fn add_path_chunks_only_large_sources() {
    let temp = tempfile::tempdir().unwrap();
    let source_dir = temp.path().join("automatic");
    std::fs::create_dir(&source_dir).unwrap();
    let large_path = source_dir.join("large.bin");
    let exact_path = source_dir.join("exact.bin");
    let empty_path = source_dir.join("empty.bin");
    let large_payload = vec![b'L'; DEFAULT_BLOCK_SIZE as usize * 2 + 37];
    std::fs::write(&large_path, &large_payload).unwrap();
    std::fs::write(&exact_path, vec![b'E'; DEFAULT_BLOCK_SIZE as usize]).unwrap();
    std::fs::write(&empty_path, []).unwrap();

    let archive = temp.path().join("automatic.box");
    let mut writer = BoxFileWriter::create(&archive).await.unwrap();
    let stats = writer
        .add_path(
            &source_dir,
            AddOptions {
                config: CompressionConfig::new(Compression::Zstd),
                ..AddOptions::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(stats.files_added, 3);
    assert_eq!(
        stats.bytes_original,
        large_payload.len() as u64 + DEFAULT_BLOCK_SIZE as u64
    );
    writer.finish().await.unwrap();

    let reader = BoxFileReader::open(&archive).await.unwrap();
    let large_box_path = BoxPath::new(&large_path).unwrap();
    let large_index = reader.metadata().index(&large_box_path).unwrap();
    let large_record = reader
        .metadata()
        .record(large_index)
        .unwrap()
        .as_chunked_file()
        .unwrap();
    assert_eq!(large_record.block_size, DEFAULT_BLOCK_SIZE);
    assert_eq!(large_record.block_count(), 3);
    let blocks = reader.metadata().blocks_for_record(large_index);
    assert_eq!(
        blocks
            .iter()
            .map(|(logical, _)| *logical)
            .collect::<Vec<_>>(),
        vec![
            0,
            u64::from(DEFAULT_BLOCK_SIZE),
            u64::from(DEFAULT_BLOCK_SIZE) * 2,
        ]
    );
    assert_eq!(blocks[0].1, large_record.data.get());
    assert!(blocks.windows(2).all(|pair| pair[0].1 < pair[1].1));
    assert!(blocks.last().unwrap().1 < large_record.data.get() + large_record.length);
    let checksum = reader
        .metadata()
        .record(large_index)
        .unwrap()
        .attr(reader.metadata(), crate::attrs::BLAKE3)
        .unwrap();
    assert_eq!(checksum, blake3::hash(&large_payload).as_bytes());
    let mut restored = Vec::new();
    reader
        .decompress_chunked(large_record, large_index, &mut restored)
        .await
        .unwrap();
    assert_eq!(restored, large_payload);

    for path in [&exact_path, &empty_path] {
        let index = reader
            .metadata()
            .index(&BoxPath::new(path).unwrap())
            .unwrap();
        assert!(reader.metadata().record(index).unwrap().as_file().is_some());
    }
}

// [spec:box:req:chunked-io.root.automatic-creation/test/unit]
// [spec:box:sem:async-io.root.parallel-compression+1/test/unit]
#[tokio::test]
async fn parallel_chunk_preparation_is_transactional() {
    let temp = tempfile::tempdir().unwrap();
    let large_path = temp.path().join("large-dictionary.bin");
    let ordinary_path = temp.path().join("ordinary.bin");
    let dictionary = b"automatic chunk dictionary with repeated payload tokens".to_vec();
    let large_payload = dictionary.repeat(DEFAULT_BLOCK_SIZE as usize / dictionary.len() * 2 + 3);
    let ordinary_payload = vec![b'x'; 512];
    std::fs::write(&large_path, &large_payload).unwrap();
    std::fs::write(&ordinary_path, &ordinary_payload).unwrap();

    let mut chunk_config =
        CompressionConfig::with_dictionary(Compression::Zstd, dictionary.clone());
    chunk_config.set_option("level", "1");
    let large_box_path = BoxPath::new("large-dictionary.bin").unwrap();
    let ordinary_box_path = BoxPath::new("ordinary.bin").unwrap();
    let archive = temp.path().join("transactional.box");
    let mut writer = BoxFileWriter::create(&archive).await.unwrap();
    let initial_position = writer.next_write_addr();

    let error = writer
        .add_paths_parallel(
            [
                FileJob {
                    fs_path: large_path.clone(),
                    box_path: large_box_path.clone(),
                    config: chunk_config.clone(),
                    attrs: HashMap::new(),
                },
                FileJob {
                    fs_path: ordinary_path.clone(),
                    box_path: ordinary_box_path.clone(),
                    config: CompressionConfig::new(Compression::Unknown(0xfe)),
                    attrs: HashMap::new(),
                },
            ],
            true,
            false,
            false,
            2,
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert_eq!(writer.next_write_addr(), initial_position);
    assert_eq!(writer.core.record_count(), 0);
    assert!(writer.core.block_entries.is_empty());
    assert!(writer.metadata().dictionary().is_none());
    assert!(writer.metadata().index(&large_box_path).is_none());
    assert!(writer.metadata().index(&ordinary_box_path).is_none());

    let jobs = 2;
    let probe = Arc::new(CompressionTestProbe::default());
    probe.defer_until_another_preparation(ordinary_box_path.clone());
    writer.compression_test_probe = Some(probe.clone());
    let (progress_tx, mut progress_rx) = tokio::sync::mpsc::unbounded_channel();
    let stats = writer
        .add_paths_parallel_with_progress(
            [
                FileJob {
                    fs_path: ordinary_path,
                    box_path: ordinary_box_path.clone(),
                    config: CompressionConfig::new(Compression::Stored),
                    attrs: HashMap::new(),
                },
                FileJob {
                    fs_path: large_path,
                    box_path: large_box_path.clone(),
                    config: chunk_config,
                    attrs: HashMap::new(),
                },
            ],
            true,
            false,
            false,
            jobs,
            Some(progress_tx),
        )
        .await
        .unwrap();
    assert_eq!(stats.files_added, 2);
    assert_eq!(
        stats.bytes_original,
        (ordinary_payload.len() + large_payload.len()) as u64
    );
    assert_eq!(
        probe.preparation_order(),
        [large_box_path.clone(), ordinary_box_path.clone()]
    );
    assert_eq!(probe.max_active(), jobs);
    assert!(probe.max_active() <= jobs);
    let work_paths = probe.work_paths();
    assert!(work_paths.contains(&ordinary_box_path));
    assert!(
        work_paths
            .iter()
            .filter(|path| *path == &large_box_path)
            .count()
            >= 2
    );

    let ordinary_index = writer.metadata().index(&ordinary_box_path).unwrap();
    let pending_index = writer.metadata().index(&large_box_path).unwrap();
    assert!(ordinary_index < pending_index);
    let ordinary_record = writer
        .metadata()
        .record(ordinary_index)
        .unwrap()
        .as_file()
        .unwrap();
    let pending_record = writer
        .metadata()
        .record(pending_index)
        .unwrap()
        .as_chunked_file()
        .unwrap();
    assert!(ordinary_record.data < pending_record.data);
    assert_eq!(
        stats.bytes_compressed,
        ordinary_record.length + pending_record.length
    );
    let mut progress = Vec::new();
    while let Some(event) = progress_rx.recv().await {
        progress.push(event);
    }
    assert_eq!(
        progress
            .iter()
            .filter(|event| matches!(event, ParallelProgress::Compressing { .. }))
            .count(),
        2
    );
    assert_eq!(
        progress
            .iter()
            .filter(|event| matches!(event, ParallelProgress::Compressed { .. }))
            .count(),
        2
    );
    assert!(matches!(
        progress.first(),
        Some(ParallelProgress::Started { total_files: 2 })
    ));
    assert!(matches!(progress.last(), Some(ParallelProgress::Finished)));
    let compressed_paths: Vec<_> = progress
        .iter()
        .filter_map(|event| match event {
            ParallelProgress::Compressed { path } => Some(path.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        compressed_paths,
        [large_box_path.clone(), ordinary_box_path.clone()]
    );
    let written_paths: Vec<_> = progress
        .iter()
        .filter_map(|event| match event {
            ParallelProgress::Written { path, .. } => Some(path.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        written_paths,
        [ordinary_box_path.clone(), large_box_path.clone()]
    );
    assert_eq!(writer.metadata().dictionary(), Some(dictionary.as_slice()));
    writer.finish().await.unwrap();

    let reader = BoxFileReader::open(&archive).await.unwrap();
    assert_eq!(reader.metadata().dictionary(), Some(dictionary.as_slice()));
    let index = reader.metadata().index(&large_box_path).unwrap();
    let record = reader
        .metadata()
        .record(index)
        .unwrap()
        .as_chunked_file()
        .unwrap();
    let mut restored = Vec::new();
    reader
        .decompress_chunked(record, index, &mut restored)
        .await
        .unwrap();
    assert_eq!(restored, large_payload);
}

// [spec:box:sem:async-io.root.writer-lifecycle/test/unit]
#[tokio::test]
async fn open_rejects_payload_offsets_outside_the_existing_trailer() {
    let temp = tempfile::tempdir().unwrap();
    let archive = temp.path().join("hostile-resume-cursor.box");
    let mut writer = BoxFileWriter::create(&archive).await.unwrap();
    let index = writer.core.meta.insert_record(Record::File(FileRecord {
        compression: Compression::Stored,
        length: 0,
        decompressed_length: 0,
        name: Cow::Borrowed("hostile"),
        data: NonZeroU64::new(u64::MAX).unwrap(),
        attrs: Default::default(),
    }));
    writer.core.meta.root.push(index);
    writer.finish().await.unwrap();

    let error = BoxFileWriter::open(&archive)
        .await
        .err()
        .expect("payload start after the trailer must be rejected");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("pre-trailer payload envelope"));
}
