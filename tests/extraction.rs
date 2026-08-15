use std::collections::HashSet;
use std::path::Path;

use box_format::{
    AttrValue, BoxFileReader, BoxFileWriter, BoxPath, Compression, CompressionConfig, ExtractError,
    ExtractOptions, ExtractProgress, HashMap, attrs,
};

async fn insert_file(
    writer: &mut BoxFileWriter,
    path: &str,
    payload: &[u8],
    attrs: HashMap<String, Vec<u8>>,
) {
    writer
        .insert(
            &CompressionConfig::new(Compression::Stored),
            BoxPath::new(path).unwrap(),
            std::io::Cursor::new(payload.to_vec()),
            attrs,
        )
        .await
        .unwrap();
}

fn assert_empty_directory(path: &Path) {
    assert!(
        std::fs::read_dir(path).unwrap().next().is_none(),
        "safety gate materialized an archive entry in {}",
        path.display()
    );
}

// [spec:box:req:extraction.root.safety-options/test/integration]
// [spec:box:req:records.root.references.external-header-flag/test/integration]
#[tokio::test]
async fn extraction_gates_escaped_and_external_links() {
    let temp = tempfile::tempdir().unwrap();

    let escaped_archive = temp.path().join("escaped.box");
    let mut escaped_writer = BoxFileWriter::create_with_options(&escaped_archive, 8, true, false)
        .await
        .unwrap();
    insert_file(
        &mut escaped_writer,
        "escaped.txt",
        b"escaped archive payload",
        HashMap::new(),
    )
    .await;
    escaped_writer.finish().await.unwrap();

    let escaped_reader = BoxFileReader::open(&escaped_archive).await.unwrap();
    let escaped_output = temp.path().join("escaped-output");
    std::fs::create_dir(&escaped_output).unwrap();
    let error = escaped_reader
        .extract_all_with_options(&escaped_output, ExtractOptions::default())
        .await
        .unwrap_err();
    assert!(matches!(error, ExtractError::AllowEscapesRequired));
    assert_empty_directory(&escaped_output);

    let single_error = escaped_reader
        .extract(&BoxPath::new("escaped.txt").unwrap(), &escaped_output)
        .await
        .unwrap_err();
    assert!(matches!(single_error, ExtractError::AllowEscapesRequired));
    assert_empty_directory(&escaped_output);

    let escaped_options = ExtractOptions {
        allow_escapes: true,
        ..ExtractOptions::default()
    };
    escaped_reader
        .extract_all_with_options(&escaped_output, escaped_options)
        .await
        .unwrap();
    assert_eq!(
        std::fs::read(escaped_output.join("escaped.txt")).unwrap(),
        b"escaped archive payload"
    );

    let external_archive = temp.path().join("external.box");
    let mut external_writer = BoxFileWriter::create(&external_archive).await.unwrap();
    insert_file(
        &mut external_writer,
        "ordinary.txt",
        b"must remain gated",
        HashMap::new(),
    )
    .await;
    external_writer
        .external_link(
            BoxPath::new("external-link").unwrap(),
            "../missing-target",
            HashMap::new(),
        )
        .unwrap();
    external_writer.finish().await.unwrap();

    let external_reader = BoxFileReader::open(&external_archive).await.unwrap();
    let external_output = temp.path().join("external-output");
    std::fs::create_dir(&external_output).unwrap();
    let error = external_reader
        .extract_all_with_options(&external_output, ExtractOptions::default())
        .await
        .unwrap_err();
    assert!(matches!(error, ExtractError::ExternalSymlinksRequired));
    assert_empty_directory(&external_output);
}

// [spec:box:sem:extraction.root.selection+2/test/integration]
#[tokio::test]
async fn extraction_selects_one_record_or_one_recursive_subtree() {
    let temp = tempfile::tempdir().unwrap();
    let archive = temp.path().join("selection.box");
    let mut writer = BoxFileWriter::create(&archive).await.unwrap();
    writer
        .mkdir(BoxPath::new("selected").unwrap(), HashMap::new())
        .unwrap();
    insert_file(
        &mut writer,
        "selected/nested.txt",
        b"selected payload",
        HashMap::new(),
    )
    .await;
    insert_file(
        &mut writer,
        "other.txt",
        b"unselected payload",
        HashMap::new(),
    )
    .await;
    writer.finish().await.unwrap();

    let reader = BoxFileReader::open(&archive).await.unwrap();
    let one_output = temp.path().join("one");
    reader
        .extract(&BoxPath::new("other.txt").unwrap(), &one_output)
        .await
        .unwrap();
    assert_eq!(
        std::fs::read(one_output.join("other.txt")).unwrap(),
        b"unselected payload"
    );
    assert!(!one_output.join("selected").exists());

    let recursive_output = temp.path().join("recursive");
    let stats = reader
        .extract_recursive_with_options(
            &BoxPath::new("selected").unwrap(),
            &recursive_output,
            ExtractOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(stats.dirs_created, 1);
    assert_eq!(stats.files_extracted, 1);
    assert_eq!(
        std::fs::read(recursive_output.join("selected/nested.txt")).unwrap(),
        b"selected payload"
    );
    assert!(!recursive_output.join("other.txt").exists());
}

// [spec:box:req:extraction.root.materialization/test/integration]
#[tokio::test]
async fn extraction_materializes_and_truncates_file_kinds() {
    let temp = tempfile::tempdir().unwrap();
    let archive = temp.path().join("materialization.box");
    let regular_payload = b"short regular payload";
    let chunked_payload = b"chunked materialization payload".repeat(40);

    #[cfg(all(target_os = "linux", feature = "xattr"))]
    let xattrs_supported = {
        let probe = temp.path().join("xattr-probe");
        std::fs::write(&probe, b"probe").unwrap();
        xattr::set(&probe, "user.box-probe", b"supported").is_ok()
    };

    let mut writer = BoxFileWriter::create(&archive).await.unwrap();
    writer
        .mkdir(BoxPath::new("tree").unwrap(), HashMap::new())
        .unwrap();
    let regular_path = BoxPath::new("regular.txt").unwrap();
    insert_file(&mut writer, "regular.txt", regular_payload, HashMap::new()).await;
    writer
        .set_attr(&regular_path, attrs::UNIX_MODE, AttrValue::Vu32(0o100600))
        .unwrap();

    let chunked_path = BoxPath::new("tree/chunked.bin").unwrap();
    writer
        .insert_chunked(
            chunked_path.clone(),
            std::io::Cursor::new(chunked_payload.clone()),
            64,
            Compression::Zstd,
            HashMap::new(),
        )
        .await
        .unwrap();
    writer
        .set_attr(&chunked_path, attrs::UNIX_MODE, AttrValue::Vu32(0o100640))
        .unwrap();

    #[cfg(all(target_os = "linux", feature = "xattr"))]
    {
        writer
            .set_attr(
                &regular_path,
                "linux.xattr.user.box-test",
                AttrValue::Bytes(b"regular-xattr"),
            )
            .unwrap();
        writer
            .set_attr(
                &chunked_path,
                "linux.xattr.user.box-test",
                AttrValue::Bytes(b"chunked-xattr"),
            )
            .unwrap();
    }
    writer.finish().await.unwrap();

    let output = temp.path().join("output");
    std::fs::create_dir(&output).unwrap();
    std::fs::write(
        output.join("regular.txt"),
        b"this stale file is deliberately much longer than the replacement payload",
    )
    .unwrap();

    let reader = BoxFileReader::open(&archive).await.unwrap();
    let options = ExtractOptions {
        xattrs: cfg!(all(target_os = "linux", feature = "xattr")),
        ..ExtractOptions::default()
    };
    let stats = reader
        .extract_all_parallel(&output, options, 2)
        .await
        .unwrap();
    assert_eq!(stats.dirs_created, 1);
    assert_eq!(stats.files_extracted, 2);
    assert_eq!(
        stats.bytes_written,
        (regular_payload.len() + chunked_payload.len()) as u64
    );
    assert_eq!(
        std::fs::read(output.join("regular.txt")).unwrap(),
        regular_payload
    );
    assert_eq!(
        std::fs::read(output.join("tree/chunked.bin")).unwrap(),
        chunked_payload
    );

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        assert_eq!(
            std::fs::metadata(output.join("regular.txt"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert_eq!(
            std::fs::metadata(output.join("tree/chunked.bin"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o640
        );
    }

    #[cfg(all(target_os = "linux", feature = "xattr"))]
    if xattrs_supported {
        assert_eq!(
            xattr::get(output.join("regular.txt"), "user.box-test").unwrap(),
            Some(b"regular-xattr".to_vec())
        );
        assert_eq!(
            xattr::get(output.join("tree/chunked.bin"), "user.box-test").unwrap(),
            Some(b"chunked-xattr".to_vec())
        );
    }
}

// [spec:box:req:extraction.root.external-symlink/test/integration]
#[cfg(unix)]
#[tokio::test]
async fn external_symlink_preserves_dangling_target() {
    let temp = tempfile::tempdir().unwrap();
    let archive = temp.path().join("external-link.box");
    let mut writer = BoxFileWriter::create(&archive).await.unwrap();
    writer
        .external_link(
            BoxPath::new("links/dangling").unwrap(),
            "../../not-installed-yet",
            HashMap::new(),
        )
        .unwrap_err();
    writer
        .mkdir(BoxPath::new("links").unwrap(), HashMap::new())
        .unwrap();
    writer
        .external_link(
            BoxPath::new("links/dangling").unwrap(),
            "../../not-installed-yet",
            HashMap::new(),
        )
        .unwrap();
    writer.finish().await.unwrap();

    let reader = BoxFileReader::open(&archive).await.unwrap();
    let output = temp.path().join("external-link-output");
    let options = ExtractOptions {
        allow_external_symlinks: true,
        ..ExtractOptions::default()
    };
    let stats = reader
        .extract_all_parallel(&output, options, 2)
        .await
        .unwrap();
    assert_eq!(stats.links_created, 1);
    assert_eq!(
        std::fs::read_link(output.join("links/dangling")).unwrap(),
        Path::new("../../not-installed-yet")
    );
    assert!(!output.join("links/dangling").exists());
}

// [spec:box:sem:extraction.root.parallel-ordering/test/integration]
#[tokio::test]
async fn parallel_extraction_orders_directories_files_and_links() {
    let temp = tempfile::tempdir().unwrap();
    let archive = temp.path().join("ordering.box");
    let mut writer = BoxFileWriter::create(&archive).await.unwrap();
    writer
        .mkdir(BoxPath::new("tree").unwrap(), HashMap::new())
        .unwrap();
    insert_file(
        &mut writer,
        "tree/target.txt",
        b"ordered payload",
        HashMap::new(),
    )
    .await;
    let target = writer
        .metadata()
        .index(&BoxPath::new("tree/target.txt").unwrap())
        .unwrap();
    writer
        .link(
            BoxPath::new("tree/link.txt").unwrap(),
            target,
            HashMap::new(),
        )
        .unwrap();
    writer.finish().await.unwrap();

    let reader = BoxFileReader::open(&archive).await.unwrap();
    let output = temp.path().join("ordering-output");
    let (progress_tx, mut progress_rx) = tokio::sync::mpsc::unbounded_channel();
    reader
        .extract_all_parallel_with_progress(
            &output,
            ExtractOptions::default(),
            2,
            Some(progress_tx),
        )
        .await
        .unwrap();

    let mut events = Vec::new();
    while let Some(event) = progress_rx.recv().await {
        events.push(event);
    }
    let directory = events
        .iter()
        .position(|event| matches!(event, ExtractProgress::DirectoryCreated { .. }))
        .unwrap();
    let extracting = events
        .iter()
        .position(|event| matches!(event, ExtractProgress::Extracting { .. }))
        .unwrap();
    let extracted = events
        .iter()
        .position(|event| matches!(event, ExtractProgress::Extracted { .. }))
        .unwrap();
    let linked = events
        .iter()
        .position(|event| matches!(event, ExtractProgress::LinkCreated { .. }))
        .unwrap();
    assert!(directory < extracting);
    assert!(extracting < extracted);
    assert!(extracted < linked);
    assert_eq!(
        std::fs::read(output.join("tree/link.txt")).unwrap(),
        b"ordered payload"
    );
}

// [spec:box:req:extraction.root.checksum-verification/test/integration]
// [spec:box:sem:checksums.root.verification.extraction-statistics/test/integration]
#[tokio::test]
async fn extraction_counts_checksum_mismatches_without_failure() {
    let temp = tempfile::tempdir().unwrap();
    let archive = temp.path().join("checksum-extraction.box");
    let regular_payload = b"regular checksum mismatch";
    let chunked_payload = b"chunked checksum mismatch".repeat(32);
    let mut bad_attrs = HashMap::new();
    bad_attrs.insert(attrs::BLAKE3.to_string(), vec![0x5A; 32]);

    let mut writer = BoxFileWriter::create(&archive).await.unwrap();
    insert_file(
        &mut writer,
        "regular.txt",
        regular_payload,
        bad_attrs.clone(),
    )
    .await;
    writer
        .insert_chunked(
            BoxPath::new("chunked.bin").unwrap(),
            std::io::Cursor::new(chunked_payload.clone()),
            64,
            Compression::Zstd,
            bad_attrs,
        )
        .await
        .unwrap();
    writer.finish().await.unwrap();

    let reader = BoxFileReader::open(&archive).await.unwrap();
    let verified_output = temp.path().join("verified");
    let verified = reader
        .extract_all_parallel(&verified_output, ExtractOptions::default(), 2)
        .await
        .unwrap();
    assert_eq!(verified.files_extracted, 2);
    assert_eq!(verified.checksum_failures, 2);
    assert_eq!(
        std::fs::read(verified_output.join("regular.txt")).unwrap(),
        regular_payload
    );
    assert_eq!(
        std::fs::read(verified_output.join("chunked.bin")).unwrap(),
        chunked_payload
    );

    let unchecked_output = temp.path().join("unchecked");
    let unchecked = reader
        .extract_all_parallel(
            &unchecked_output,
            ExtractOptions {
                verify_checksums: false,
                ..ExtractOptions::default()
            },
            2,
        )
        .await
        .unwrap();
    assert_eq!(unchecked.checksum_failures, 0);
}

// [spec:box:sem:extraction.root.progress/test/integration]
#[tokio::test]
async fn parallel_extraction_reports_totals_paths_counts_and_finished() {
    let temp = tempfile::tempdir().unwrap();
    let archive = temp.path().join("progress.box");
    let mut writer = BoxFileWriter::create(&archive).await.unwrap();
    writer
        .mkdir(BoxPath::new("tree").unwrap(), HashMap::new())
        .unwrap();
    for (path, payload) in [
        ("tree/one.txt", b"one".as_slice()),
        ("tree/two.txt", b"two".as_slice()),
    ] {
        insert_file(&mut writer, path, payload, HashMap::new()).await;
    }
    let target = writer
        .metadata()
        .index(&BoxPath::new("tree/one.txt").unwrap())
        .unwrap();
    writer
        .link(
            BoxPath::new("tree/link.txt").unwrap(),
            target,
            HashMap::new(),
        )
        .unwrap();
    writer.finish().await.unwrap();

    let reader = BoxFileReader::open(&archive).await.unwrap();
    let (progress_tx, mut progress_rx) = tokio::sync::mpsc::unbounded_channel();
    reader
        .extract_all_parallel_with_progress(
            temp.path().join("progress-output"),
            ExtractOptions::default(),
            2,
            Some(progress_tx),
        )
        .await
        .unwrap();

    let mut events = Vec::new();
    while let Some(event) = progress_rx.recv().await {
        events.push(event);
    }

    assert!(matches!(
        events.first(),
        Some(ExtractProgress::Started {
            total_files: 2,
            total_dirs: 1,
            total_links: 1,
        })
    ));
    assert!(matches!(events.last(), Some(ExtractProgress::Finished)));

    let directories: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            ExtractProgress::DirectoryCreated { path } => Some(path.to_string()),
            _ => None,
        })
        .collect();
    assert_eq!(directories, ["tree"]);

    let extracting: HashSet<_> = events
        .iter()
        .filter_map(|event| match event {
            ExtractProgress::Extracting { path } => Some(path.to_string()),
            _ => None,
        })
        .collect();
    assert_eq!(
        extracting,
        HashSet::from(["tree/one.txt".to_string(), "tree/two.txt".to_string()])
    );

    let mut completed = Vec::new();
    for event in &events {
        if let ExtractProgress::Extracted {
            files_extracted,
            total_files,
            ..
        } = event
        {
            assert_eq!(*total_files, 2);
            completed.push(*files_extracted);
        }
    }
    completed.sort_unstable();
    assert_eq!(completed, [1, 2]);
    assert!(events.iter().any(|event| matches!(
        event,
        ExtractProgress::LinkCreated { path } if path.to_string() == "tree/link.txt"
    )));
}

// [spec:box:req:extraction.root.materialization/test/integration]
// [spec:box:req:extraction.root.checksum-verification/test/integration]
// [spec:box:sem:sync-io.root.extraction-validation+2/test/integration]
#[tokio::test]
async fn serial_extraction_flushes_before_verification() {
    let temp = tempfile::tempdir().unwrap();
    let archive = temp.path().join("buffered-extraction.box");
    let zstd_payload = vec![b'Z'; 4096];
    let xz_payload = vec![b'X'; 6144];

    let mut writer = BoxFileWriter::create(&archive).await.unwrap();
    writer
        .mkdir(BoxPath::new("selected").unwrap(), HashMap::new())
        .unwrap();
    for (path, payload, compression) in [
        ("zstd.bin", zstd_payload.as_slice(), Compression::Zstd),
        ("selected/xz.bin", xz_payload.as_slice(), Compression::Xz),
    ] {
        let mut file_attrs = HashMap::new();
        file_attrs.insert(
            attrs::BLAKE3.to_string(),
            blake3::hash(payload).as_bytes().to_vec(),
        );
        writer
            .insert(
                &CompressionConfig::new(compression),
                BoxPath::new(path).unwrap(),
                std::io::Cursor::new(payload.to_vec()),
                file_attrs,
            )
            .await
            .unwrap();
    }
    writer.finish().await.unwrap();

    let reader = BoxFileReader::open(&archive).await.unwrap();
    let serial_output = temp.path().join("async-serial");
    let stats = reader
        .extract_all_with_options(&serial_output, ExtractOptions::default())
        .await
        .unwrap();
    assert_eq!(stats.files_extracted, 2);
    assert_eq!(stats.checksum_failures, 0);
    assert_eq!(
        std::fs::read(serial_output.join("zstd.bin")).unwrap(),
        zstd_payload
    );
    assert_eq!(
        std::fs::read(serial_output.join("selected/xz.bin")).unwrap(),
        xz_payload
    );

    let selected_output = temp.path().join("async-selected");
    reader
        .extract_recursive(&BoxPath::new("selected").unwrap(), &selected_output)
        .await
        .unwrap();
    assert_eq!(
        std::fs::read(selected_output.join("selected/xz.bin")).unwrap(),
        xz_payload
    );
    assert!(!selected_output.join("zstd.bin").exists());

    let sync_reader = box_format::sync::BoxReader::open(&archive).unwrap();
    let sync_output = temp.path().join("sync-verified");
    let sync_stats = sync_reader
        .extract_all_with_options(&sync_output, ExtractOptions::default())
        .unwrap();
    assert_eq!(sync_stats.checksum_failures, 0);
    assert_eq!(
        std::fs::read(sync_output.join("zstd.bin")).unwrap(),
        zstd_payload
    );
    assert_eq!(
        std::fs::read(sync_output.join("selected/xz.bin")).unwrap(),
        xz_payload
    );
}
