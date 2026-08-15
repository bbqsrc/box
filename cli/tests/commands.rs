use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use box_format::{
    AttrValue, BoxFileReader, BoxFileWriter, BoxPath, Compression, CompressionConfig, HashMap,
    Record, attrs,
};

const DEFAULT_BLOCK_SIZE: u32 = 2_097_152;

fn box_command() -> Command {
    Command::new(env!("CARGO_BIN_EXE_box"))
}

fn output_text(output: &Output) -> String {
    format!(
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn assert_success(output: &Output) {
    assert!(output.status.success(), "{}", output_text(output));
}

fn create_input_tree(root: &Path) {
    std::fs::create_dir_all(root.join("input/nested")).unwrap();
    std::fs::write(root.join("input/nested/file.txt"), b"nested payload\n").unwrap();
    std::fs::write(root.join("input/sibling.txt"), b"sibling payload\n").unwrap();
    std::fs::write(root.join("input/compressible.bin"), vec![b'z'; 8192]).unwrap();
}

fn create_cli_archive(root: &Path) -> PathBuf {
    create_input_tree(root);
    let archive = root.join("archive.box");
    let output = box_command()
        .current_dir(root)
        .args(["create", "--quiet", "archive.box", "input"])
        .output()
        .unwrap();
    assert_success(&output);
    archive
}

// [spec:box:req:cli-commands.root/test/integration]
#[test]
fn command_surface_requires_one_command_and_retains_aliases() {
    let missing = box_command().output().unwrap();
    assert!(!missing.status.success(), "a command must be required");
    assert!(
        String::from_utf8_lossy(&missing.stderr).contains("Usage:"),
        "{}",
        output_text(&missing)
    );

    let help = box_command().arg("--help").output().unwrap();
    assert_success(&help);
    let help = String::from_utf8_lossy(&help.stdout);
    for command in ["create", "extract", "list", "info", "validate"] {
        assert!(help.contains(command), "missing {command} in help:\n{help}");
    }

    for alias in ["c", "x", "l", "ls", "t", "test"] {
        let output = box_command().args([alias, "--help"]).output().unwrap();
        assert_success(&output);
    }

    let failed_handler = box_command()
        .args(["info", "definitely-does-not-exist.box"])
        .output()
        .unwrap();
    assert!(
        !failed_handler.status.success(),
        "an async command-handler error must reach the process status"
    );
    assert!(
        !failed_handler.stderr.is_empty(),
        "{}",
        output_text(&failed_handler)
    );
}

// [spec:box:req:cli-commands.root.create+1/test/integration]
// [spec:box:req:checksums.root/test/integration]
// [spec:box:req:checksums.root.attachment/test/integration]
// [spec:box:req:checksums.root.disabled/test/integration]
// [spec:box:req:chunked-io.root.explicit-creation/test/integration]
// [spec:box:sem:cli-selection.root.representation/test/integration]
#[test]
fn create_builds_a_finished_checksummed_archive_with_hierarchy() {
    let temp = tempfile::tempdir().unwrap();
    create_input_tree(temp.path());
    let large_payload = vec![b'B'; DEFAULT_BLOCK_SIZE as usize * 2 + 257];
    std::fs::write(temp.path().join("input/large.bin"), &large_payload).unwrap();
    let default_large_payload = vec![b'R'; DEFAULT_BLOCK_SIZE as usize + 91];
    std::fs::write(
        temp.path().join("input/default-large.bin"),
        &default_large_payload,
    )
    .unwrap();
    let selected_small_payload = vec![b'C'; 4096];
    std::fs::write(
        temp.path().join("input/selected-small.bin"),
        &selected_small_payload,
    )
    .unwrap();
    std::fs::write(
        temp.path().join("input/exact-block.bin"),
        vec![b'E'; DEFAULT_BLOCK_SIZE as usize],
    )
    .unwrap();
    std::fs::write(temp.path().join("input/empty.bin"), []).unwrap();

    let missing_input = box_command()
        .current_dir(temp.path())
        .args(["create", "missing-input.box"])
        .output()
        .unwrap();
    assert!(
        !missing_input.status.success(),
        "create must require at least one input operand"
    );

    let archive = temp.path().join("archive.box");
    let output = box_command()
        .current_dir(temp.path())
        .args([
            "create",
            "--quiet",
            "archive.box",
            "--zstd-chunked",
            "input/large.bin",
            "input/selected-small.bin",
            "--zstd",
            "input",
        ])
        .output()
        .unwrap();
    assert_success(&output);

    let reader = box_format::sync::BoxReader::open(&archive).unwrap();
    assert!(reader.file_attrs().contains_key("created"));

    let file_path = BoxPath::new("input/nested/file.txt").unwrap();
    let file_index = reader.metadata().index(&file_path).unwrap();
    let record = reader.metadata().record(file_index).unwrap();
    assert!(matches!(record, Record::File(_)));
    let checksum = match record.attr_value(reader.metadata(), attrs::BLAKE3) {
        Some(AttrValue::U256(checksum)) => checksum,
        other => panic!("expected a U256 Blake3 checksum, got {other:?}"),
    };
    assert_eq!(checksum, blake3::hash(b"nested payload\n").as_bytes());

    let mut payload = Vec::new();
    reader
        .decompress(record.as_file().unwrap(), &mut payload)
        .unwrap();
    assert_eq!(payload, b"nested payload\n");

    let compressible_path = BoxPath::new("input/compressible.bin").unwrap();
    let compressible_record = reader
        .metadata()
        .record(reader.metadata().index(&compressible_path).unwrap())
        .unwrap();
    let compressible_file = compressible_record.as_file().unwrap();
    assert_eq!(compressible_file.compression, Compression::Zstd);
    assert!(compressible_file.length < compressible_file.decompressed_length);
    let compressible_checksum =
        match compressible_record.attr_value(reader.metadata(), attrs::BLAKE3) {
            Some(AttrValue::U256(checksum)) => checksum,
            other => panic!("expected a U256 Blake3 checksum, got {other:?}"),
        };
    assert_eq!(
        compressible_checksum,
        blake3::hash(&vec![b'z'; 8192]).as_bytes()
    );

    let large_path = BoxPath::new("input/large.bin").unwrap();
    let large_index = reader.metadata().index(&large_path).unwrap();
    let large_record = reader
        .metadata()
        .record(large_index)
        .unwrap()
        .as_chunked_file()
        .expect("--zstd-chunked must explicitly select chunked representation");
    assert_eq!(large_record.block_size, DEFAULT_BLOCK_SIZE);
    assert_eq!(large_record.block_count(), 3);
    assert_eq!(large_record.decompressed_length, large_payload.len() as u64);
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
    let large_checksum = match reader
        .metadata()
        .record(large_index)
        .unwrap()
        .attr_value(reader.metadata(), attrs::BLAKE3)
    {
        Some(AttrValue::U256(checksum)) => checksum,
        other => panic!("expected a whole-file Blake3 checksum, got {other:?}"),
    };
    assert_eq!(large_checksum, blake3::hash(&large_payload).as_bytes());
    let mut large_roundtrip = Vec::new();
    reader
        .decompress_chunked(large_record, large_index, &mut large_roundtrip)
        .unwrap();
    assert_eq!(large_roundtrip, large_payload);

    let selected_small_path = BoxPath::new("input/selected-small.bin").unwrap();
    let selected_small_index = reader.metadata().index(&selected_small_path).unwrap();
    let selected_small_record = reader
        .metadata()
        .record(selected_small_index)
        .unwrap()
        .as_chunked_file()
        .expect("explicit chunk selection must not depend on source size");
    assert_eq!(selected_small_record.compression, Compression::Zstd);
    assert_eq!(selected_small_record.block_count(), 1);
    let mut selected_small_roundtrip = Vec::new();
    reader
        .decompress_chunked(
            selected_small_record,
            selected_small_index,
            &mut selected_small_roundtrip,
        )
        .unwrap();
    assert_eq!(selected_small_roundtrip, selected_small_payload);

    let default_large_path = BoxPath::new("input/default-large.bin").unwrap();
    let default_large_record = reader
        .metadata()
        .record(reader.metadata().index(&default_large_path).unwrap())
        .unwrap();
    assert!(
        matches!(default_large_record, Record::File(_)),
        "source size must not implicitly select chunked representation"
    );
    let mut default_large_roundtrip = Vec::new();
    reader
        .decompress(
            default_large_record.as_file().unwrap(),
            &mut default_large_roundtrip,
        )
        .unwrap();
    assert_eq!(default_large_roundtrip, default_large_payload);

    let empty_index = reader
        .metadata()
        .index(&BoxPath::new("input/empty.bin").unwrap())
        .unwrap();
    let empty_record = reader.metadata().record(empty_index).unwrap();
    assert!(matches!(empty_record, Record::File(file) if file.decompressed_length == 0));
    let exact_index = reader
        .metadata()
        .index(&BoxPath::new("input/exact-block.bin").unwrap())
        .unwrap();
    assert!(matches!(
        reader.metadata().record(exact_index),
        Some(Record::File(file)) if file.decompressed_length == u64::from(DEFAULT_BLOCK_SIZE)
    ));

    let unchecked_archive = temp.path().join("unchecked.box");
    let output = box_command()
        .current_dir(temp.path())
        .args([
            "create",
            "--quiet",
            "--no-checksum",
            "unchecked.box",
            "--zstd-chunked",
            "input/large.bin",
            "--stored",
            "input",
        ])
        .output()
        .unwrap();
    assert_success(&output);
    let unchecked = box_format::sync::BoxReader::open(unchecked_archive).unwrap();
    let unchecked_record = unchecked
        .metadata()
        .record(unchecked.metadata().index(&file_path).unwrap())
        .unwrap();
    assert!(matches!(unchecked_record, Record::File(_)));
    assert!(
        unchecked_record
            .attr_value(unchecked.metadata(), attrs::BLAKE3)
            .is_none(),
        "--no-checksum must omit the Blake3 attribute"
    );
    let unchecked_large = unchecked
        .metadata()
        .record(unchecked.metadata().index(&large_path).unwrap())
        .unwrap();
    assert!(matches!(unchecked_large, Record::ChunkedFile(_)));
    assert!(
        unchecked_large
            .attr_value(unchecked.metadata(), attrs::BLAKE3)
            .is_none(),
        "--no-checksum must omit the whole-file chunked checksum"
    );

    #[cfg(unix)]
    std::os::unix::fs::symlink("../not-in-archive", temp.path().join("input/external-link"))
        .unwrap();

    let aligned_archive = temp.path().join("aligned.box");
    let output = box_command()
        .current_dir(temp.path())
        .args([
            "create",
            "--align",
            "32",
            "--allow-escapes",
            "--allow-external-symlinks",
            "--serial",
            "aligned.box",
            "--zstd-chunked",
            "input/large.bin",
            "--stored",
            "input",
        ])
        .output()
        .unwrap();
    assert_success(&output);
    let summary = String::from_utf8_lossy(&output.stdout);
    assert!(summary.contains("Created"), "{summary}");
    assert!(summary.contains("files"), "{summary}");
    assert!(summary.contains("compression"), "{summary}");

    let aligned = box_format::sync::BoxReader::open(aligned_archive).unwrap();
    assert_eq!(aligned.alignment(), 32);
    assert!(aligned.allow_escapes());
    assert!(aligned.allow_external_symlinks());
    let aligned_large = aligned
        .metadata()
        .record(aligned.metadata().index(&large_path).unwrap())
        .unwrap();
    assert!(
        matches!(aligned_large, Record::ChunkedFile(file) if file.compression == Compression::Zstd),
        "serial CLI creation must honor explicit chunk selection"
    );
    assert!(
        aligned
            .metadata()
            .index(&BoxPath::new("input/nested").unwrap())
            .is_some(),
        "parent directories must be present before their files"
    );
    #[cfg(unix)]
    assert!(matches!(
        aligned.metadata().record(
            aligned
                .metadata()
                .index(&BoxPath::new("input/external-link").unwrap())
                .unwrap()
        ),
        Some(Record::ExternalLink(_))
    ));
}

// [spec:box:req:cli-commands.root.create+1/test/integration]
// [spec:box:req:cli-safety.root/test/integration]
#[cfg(unix)]
#[test]
fn create_resolves_collected_internal_symlinks() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::create_dir(temp.path().join("input")).unwrap();
    std::fs::write(temp.path().join("input/target.bin"), b"internal target").unwrap();
    std::os::unix::fs::symlink("target.bin", temp.path().join("input/link.bin")).unwrap();

    let archive = temp.path().join("internal-link.box");
    let output = box_command()
        .current_dir(temp.path())
        .args(["create", "--quiet", "internal-link.box", "input"])
        .output()
        .unwrap();
    assert_success(&output);

    let reader = box_format::sync::BoxReader::open(archive).unwrap();
    assert!(!reader.allow_external_symlinks());
    let target = reader
        .metadata()
        .index(&BoxPath::new("input/target.bin").unwrap())
        .unwrap();
    let link = reader
        .metadata()
        .record(
            reader
                .metadata()
                .index(&BoxPath::new("input/link.bin").unwrap())
                .unwrap(),
        )
        .unwrap();
    assert!(matches!(link, Record::Link(link) if link.target == target));
}

// [spec:box:req:cli-commands.root.create+1/test/integration]
// [spec:box:def:attributes.root.standard-keys/test/integration]
#[cfg(target_os = "linux")]
#[test]
fn parallel_create_preserves_requested_file_xattrs() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::create_dir(temp.path().join("input")).unwrap();
    let input = temp.path().join("input/xattr.bin");
    std::fs::write(&input, b"parallel xattr payload").unwrap();
    if xattr::set(&input, "user.box-create", b"preserved").is_err() {
        return;
    }

    let archive = temp.path().join("xattr.box");
    let output = box_command()
        .current_dir(temp.path())
        .args([
            "create",
            "--quiet",
            "--xattrs",
            "--jobs",
            "2",
            "xattr.box",
            "input",
        ])
        .output()
        .unwrap();
    assert_success(&output);

    let reader = box_format::sync::BoxReader::open(archive).unwrap();
    let record = reader
        .metadata()
        .record(
            reader
                .metadata()
                .index(&BoxPath::new("input/xattr.bin").unwrap())
                .unwrap(),
        )
        .unwrap();
    assert!(matches!(
        record.attr_value(reader.metadata(), "linux.xattr.user.box-create"),
        Some(AttrValue::Bytes(value)) if value == b"preserved"
    ));
}

// [spec:box:req:cli-commands.root.extract/test/integration]
#[test]
fn extract_handles_archive_and_recursive_selection() {
    let temp = tempfile::tempdir().unwrap();
    let archive = create_cli_archive(temp.path());
    let whole = temp.path().join("whole");
    let selected = temp.path().join("selected");

    let output = box_command()
        .args(["extract", "--quiet", "--jobs", "2"])
        .arg(&archive)
        .arg("--output")
        .arg(&whole)
        .output()
        .unwrap();
    assert_success(&output);
    assert_eq!(
        std::fs::read(whole.join("input/nested/file.txt")).unwrap(),
        b"nested payload\n"
    );
    assert_eq!(
        std::fs::read(whole.join("input/sibling.txt")).unwrap(),
        b"sibling payload\n"
    );

    let output = box_command()
        .args(["extract", "--quiet", "--serial"])
        .arg(&archive)
        .arg("--output")
        .arg(&selected)
        .arg("input/nested")
        .output()
        .unwrap();
    assert_success(&output);
    assert_eq!(
        std::fs::read(selected.join("input/nested/file.txt")).unwrap(),
        b"nested payload\n"
    );
    assert!(!selected.join("input/sibling.txt").exists());
}

// [spec:box:req:cli-commands.root.inspect/test/integration]
#[tokio::test]
async fn list_and_info_report_archive_and_record_views() {
    let temp = tempfile::tempdir().unwrap();
    let archive = temp.path().join("inspect.box");
    let regular_payload = b"ordinary inspect payload";
    let chunked_payload = b"chunked inspect payload".repeat(20);

    let mut writer = BoxFileWriter::create_with_options(&archive, 32, false, true)
        .await
        .unwrap();
    writer
        .set_file_attr("created", AttrValue::DateTime(0))
        .unwrap();
    writer
        .set_file_attr("fixture", AttrValue::String("rich archive"))
        .unwrap();
    writer
        .mkdir(BoxPath::new("dir").unwrap(), HashMap::new())
        .unwrap();

    let regular_path = BoxPath::new("dir/file.txt").unwrap();
    writer
        .insert(
            &CompressionConfig::new(Compression::Stored),
            regular_path.clone(),
            std::io::Cursor::new(regular_payload),
            HashMap::new(),
        )
        .await
        .unwrap();
    writer
        .set_attr(&regular_path, "description", AttrValue::String("ordinary"))
        .unwrap();
    writer
        .set_attr(
            &regular_path,
            attrs::BLAKE3,
            AttrValue::U256(blake3::hash(regular_payload).as_bytes()),
        )
        .unwrap();
    let target = writer.metadata().index(&regular_path).unwrap();

    let chunked_path = BoxPath::new("chunked.bin").unwrap();
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
        .set_attr(
            &chunked_path,
            attrs::BLAKE3,
            AttrValue::U256(blake3::hash(&chunked_payload).as_bytes()),
        )
        .unwrap();
    writer
        .link(
            BoxPath::new("internal-link").unwrap(),
            target,
            HashMap::new(),
        )
        .unwrap();
    writer
        .external_link(
            BoxPath::new("external-link").unwrap(),
            "../outside",
            HashMap::new(),
        )
        .unwrap();
    writer.finish().await.unwrap();

    let compact = box_command().arg("list").arg(&archive).output().unwrap();
    assert_success(&compact);
    let compact = String::from_utf8_lossy(&compact.stdout);
    for path in [
        "dir/",
        "dir/file.txt",
        "chunked.bin",
        "internal-link",
        "external-link",
    ] {
        assert!(compact.contains(path), "missing {path}:\n{compact}");
    }
    assert!(compact.contains("-> [dir/file.txt]"), "{compact}");
    assert!(compact.contains("-> ../outside (external)"), "{compact}");

    let long = box_command()
        .arg("list")
        .arg(&archive)
        .arg("--long")
        .output()
        .unwrap();
    assert_success(&long);
    let checksum_prefix = blake3::hash(regular_payload)
        .as_bytes()
        .iter()
        .take(8)
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    assert!(String::from_utf8_lossy(&long.stdout).contains(&checksum_prefix));

    let json = box_command()
        .arg("list")
        .arg(&archive)
        .arg("--json")
        .output()
        .unwrap();
    assert_success(&json);
    let pretty_json = String::from_utf8_lossy(&json.stdout);
    assert!(
        pretty_json.starts_with("[\n  {") && pretty_json.contains("\n    \"path\":"),
        "JSON list output must remain pretty-printed:\n{pretty_json}"
    );
    let json: serde_json::Value = serde_json::from_slice(&json.stdout).unwrap();
    let entries = json.as_array().unwrap();
    let entry = |path: &str| {
        entries
            .iter()
            .find(|entry| entry["path"] == path)
            .unwrap_or_else(|| panic!("missing JSON entry {path}: {json}"))
    };
    assert_eq!(entry("dir")["type"], "directory");
    assert_eq!(entry("dir/file.txt")["type"], "file");
    assert_eq!(entry("dir/file.txt")["compression"], "stored");
    assert_eq!(entry("chunked.bin")["type"], "chunked_file");
    assert_eq!(entry("chunked.bin")["compression"], "zstd");
    assert_eq!(entry("internal-link")["type"], "link");
    assert_eq!(entry("internal-link")["target"], "dir/file.txt");
    assert_eq!(entry("external-link")["type"], "external_link");
    assert_eq!(entry("external-link")["target"], "../outside");

    let archive_info = box_command().arg("info").arg(&archive).output().unwrap();
    assert_success(&archive_info);
    let archive_info = String::from_utf8_lossy(&archive_info.stdout);
    assert!(archive_info.contains("Version:"));
    assert!(archive_info.contains("Alignment:   32 bytes"));
    assert!(archive_info.contains("Files:       2"));
    assert!(archive_info.contains("Directories: 1"));
    assert!(archive_info.contains("Links:       2"));
    assert!(archive_info.contains("Original:"));
    assert!(archive_info.contains("Compressed:"));
    assert!(archive_info.contains("Attribute keys:"));
    assert!(archive_info.contains("fixture[str]: rich archive"));

    let info = |path: &str| {
        let output = box_command()
            .arg("info")
            .arg(&archive)
            .arg(path)
            .output()
            .unwrap();
        assert_success(&output);
        String::from_utf8(output.stdout).unwrap()
    };
    let file_info = info("dir/file.txt");
    assert!(file_info.contains("Type:  file"));
    assert!(file_info.contains("Compression: stored"));
    assert!(file_info.contains("Size:"));
    assert!(file_info.contains("Offset:"));
    assert!(file_info.contains("description[str]: ordinary"));
    assert!(file_info.contains("blake3[u256]:"));

    let chunked_info = info("chunked.bin");
    assert!(chunked_info.contains("Type:  chunked file"));
    assert!(chunked_info.contains("Compression: zstd"));
    assert!(chunked_info.contains("Block size:"));
    assert!(chunked_info.contains("Size:"));
    assert!(chunked_info.contains("Blocks:"));

    let directory_info = info("dir");
    assert!(directory_info.contains("Type:  directory"));
    assert!(directory_info.contains("Entries:"));

    let internal_info = info("internal-link");
    assert!(internal_info.contains("Type:   symlink"));
    assert!(internal_info.contains("Target: dir/file.txt"));

    let external_info = info("external-link");
    assert!(external_info.contains("Type:   external symlink"));
    assert!(external_info.contains("Target: ../outside (external)"));
}

// [spec:box:req:cli-commands.root.validate/test/integration]
// [spec:box:req:checksums.root.verification/test/integration]
// [spec:box:sem:checksums.root.verification.checksum-less/test/integration]
// [spec:box:req:checksums.root.verification.cli-failure/test/integration]
// [spec:box:def:checksums.root.logical-content-domain/test/integration]
// [spec:box:req:validation.root/test/integration]
// [spec:box:sem:validation.root.payload-hash/test/integration]
// [spec:box:sem:validation.root.results/test/integration]
// [spec:box:sem:validation.root.parallel/test/integration]
#[tokio::test]
async fn validate_counts_chunked_checksums_serial_and_parallel() {
    let temp = tempfile::tempdir().unwrap();
    let archive = temp.path().join("checksums.box");
    let chunked_payload = b"chunked checksum payload".repeat(32);
    let ordinary_payload = b"ordinary checksum payload".to_vec();
    let valid_chunked_payload = b"valid logical chunk checksum".repeat(24);

    let mut writer = BoxFileWriter::create(&archive).await.unwrap();
    let mut chunked_attrs = HashMap::new();
    chunked_attrs.insert(attrs::BLAKE3.to_string(), vec![0xA5; 32]);
    writer
        .insert_chunked(
            BoxPath::new("chunked.bin").unwrap(),
            std::io::Cursor::new(chunked_payload),
            64,
            Compression::Zstd,
            chunked_attrs,
        )
        .await
        .unwrap();

    let mut valid_chunked_attrs = HashMap::new();
    valid_chunked_attrs.insert(
        attrs::BLAKE3.to_string(),
        blake3::hash(&valid_chunked_payload).as_bytes().to_vec(),
    );
    writer
        .insert_chunked(
            BoxPath::new("valid-chunked.bin").unwrap(),
            std::io::Cursor::new(valid_chunked_payload),
            64,
            Compression::Zstd,
            valid_chunked_attrs,
        )
        .await
        .unwrap();

    let mut ordinary_attrs = HashMap::new();
    ordinary_attrs.insert(
        attrs::BLAKE3.to_string(),
        blake3::hash(&ordinary_payload).as_bytes().to_vec(),
    );
    writer
        .insert(
            &CompressionConfig::new(Compression::Stored),
            BoxPath::new("good.txt").unwrap(),
            std::io::Cursor::new(ordinary_payload),
            ordinary_attrs,
        )
        .await
        .unwrap();
    writer
        .insert_chunked(
            BoxPath::new("unchecked.bin").unwrap(),
            std::io::Cursor::new(b"checksum-less chunked payload".repeat(12)),
            64,
            Compression::Stored,
            HashMap::new(),
        )
        .await
        .unwrap();
    writer.finish().await.unwrap();

    let reader = BoxFileReader::open(&archive).await.unwrap();
    for stats in [
        reader.validate_all().await.unwrap(),
        reader.validate_all_parallel(2).await.unwrap(),
    ] {
        assert_eq!(stats.files_checked, 4);
        assert_eq!(stats.files_without_checksum, 1);
        assert_eq!(stats.checksum_failures, 1);
    }

    let (progress_tx, mut progress_rx) = tokio::sync::mpsc::unbounded_channel();
    let progress_stats = reader
        .validate_all_parallel_with_progress(2, Some(progress_tx))
        .await
        .unwrap();
    assert_eq!(progress_stats.files_checked, 4);
    let mut progress = Vec::new();
    while let Some(event) = progress_rx.recv().await {
        progress.push(event);
    }
    assert!(matches!(
        progress.first(),
        Some(box_format::ValidateProgress::Started { total_files: 3 })
    ));
    assert_eq!(
        progress
            .iter()
            .filter(|event| matches!(event, box_format::ValidateProgress::Validating { .. }))
            .count(),
        3
    );
    let validated: Vec<_> = progress
        .iter()
        .filter_map(|event| match event {
            box_format::ValidateProgress::Validated {
                path,
                files_checked,
                total_files,
                success,
            } => Some((path.to_string(), *files_checked, *total_files, *success)),
            _ => None,
        })
        .collect();
    assert_eq!(validated.len(), 3);
    assert_eq!(
        validated
            .iter()
            .map(|(_, files_checked, _, _)| *files_checked)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert!(
        validated
            .iter()
            .all(|(_, _, total_files, _)| *total_files == 3)
    );
    assert_eq!(
        validated
            .iter()
            .map(|(path, _, _, _)| path.as_str())
            .collect::<std::collections::HashSet<_>>(),
        std::collections::HashSet::from(["chunked.bin", "valid-chunked.bin", "good.txt"])
    );
    assert_eq!(
        validated
            .iter()
            .filter(|(_, _, _, success)| *success)
            .count(),
        2
    );
    assert!(matches!(
        progress.last(),
        Some(box_format::ValidateProgress::Finished)
    ));

    for mode in [vec!["--serial"], vec!["--jobs", "2"]] {
        let output = box_command()
            .arg("validate")
            .args(mode)
            .arg(&archive)
            .output()
            .unwrap();
        assert!(
            !output.status.success(),
            "a bad checksum must fail validation"
        );
        assert!(
            String::from_utf8_lossy(&output.stdout)
                .contains("Validated 4 files (1 without checksum, 1 failures)"),
            "{}",
            output_text(&output)
        );
    }
}
