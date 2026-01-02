//! Tests for symlink handling in box archives.
//!
//! These tests verify that symlinks are correctly stored as RecordIndex references
//! and that extraction computes the correct relative paths.

use box_format::{BoxFileWriter, BoxPath, Compression, CompressionConfig, RecordIndex};
use std::collections::HashMap;
use tempfile::TempDir;
use tokio::fs;

/// Helper to create a test archive and return the temp dir + archive path
async fn create_test_archive() -> (TempDir, std::path::PathBuf) {
    let temp_dir = TempDir::new().unwrap();
    let archive_path = temp_dir.path().join("test.box");
    (temp_dir, archive_path)
}

/// Test basic symlink: link and target in same directory
#[tokio::test]
async fn test_symlink_same_directory() {
    let (temp_dir, archive_path) = create_test_archive().await;

    // Create archive with a file and a symlink to it
    {
        let mut writer = BoxFileWriter::create(&archive_path).await.unwrap();

        // Add a file
        let config = CompressionConfig::new(Compression::Stored);
        let file_path = BoxPath::new("target.txt").unwrap();
        let content = b"hello world";
        let mut cursor = std::io::Cursor::new(content.to_vec());
        writer
            .insert(&config, file_path.clone(), &mut cursor, HashMap::new())
            .await
            .unwrap();

        // Get the file's index
        let target_index = writer.metadata().index(&file_path).unwrap();

        // Add a symlink pointing to the file
        let link_path = BoxPath::new("link.txt").unwrap();
        writer
            .link(link_path, target_index, HashMap::new())
            .unwrap();

        writer.finish().await.unwrap();
    }

    // Extract and verify
    let extract_dir = temp_dir.path().join("extracted");
    fs::create_dir_all(&extract_dir).await.unwrap();

    {
        let reader = box_format::BoxFileReader::open(&archive_path)
            .await
            .unwrap();
        reader.extract_all(&extract_dir).await.unwrap();
    }

    // Verify the symlink exists and points to the correct target
    let link_path = extract_dir.join("link.txt");
    assert!(link_path.is_symlink(), "link.txt should be a symlink");

    let target = fs::read_link(&link_path).await.unwrap();
    assert_eq!(target.to_str().unwrap(), "target.txt");

    // Verify the symlink resolves correctly
    let content = fs::read_to_string(&link_path).await.unwrap();
    assert_eq!(content, "hello world");
}

/// Test symlink to file in subdirectory
#[tokio::test]
async fn test_symlink_to_subdirectory() {
    let (temp_dir, archive_path) = create_test_archive().await;

    {
        let mut writer = BoxFileWriter::create(&archive_path).await.unwrap();

        // Create subdirectory
        writer
            .mkdir(BoxPath::new("subdir").unwrap(), HashMap::new())
            .unwrap();

        // Add a file in the subdirectory
        let config = CompressionConfig::new(Compression::Stored);
        let file_path = BoxPath::new("subdir/target.txt").unwrap();
        let content = b"nested content";
        let mut cursor = std::io::Cursor::new(content.to_vec());
        writer
            .insert(&config, file_path.clone(), &mut cursor, HashMap::new())
            .await
            .unwrap();

        // Get the file's index
        let target_index = writer.metadata().index(&file_path).unwrap();

        // Add a symlink at root pointing to the subdirectory file
        let link_path = BoxPath::new("link.txt").unwrap();
        writer
            .link(link_path, target_index, HashMap::new())
            .unwrap();

        writer.finish().await.unwrap();
    }

    // Extract and verify
    let extract_dir = temp_dir.path().join("extracted");
    fs::create_dir_all(&extract_dir).await.unwrap();

    {
        let reader = box_format::BoxFileReader::open(&archive_path)
            .await
            .unwrap();
        reader.extract_all(&extract_dir).await.unwrap();
    }

    let link_path = extract_dir.join("link.txt");
    assert!(link_path.is_symlink());

    let target = fs::read_link(&link_path).await.unwrap();
    assert_eq!(target.to_str().unwrap(), "subdir/target.txt");

    // Verify resolution
    let content = fs::read_to_string(&link_path).await.unwrap();
    assert_eq!(content, "nested content");
}

/// Test symlink to file in parent directory (uses ..)
#[tokio::test]
async fn test_symlink_to_parent_directory() {
    let (temp_dir, archive_path) = create_test_archive().await;

    {
        let mut writer = BoxFileWriter::create(&archive_path).await.unwrap();

        // Add a file at root
        let config = CompressionConfig::new(Compression::Stored);
        let file_path = BoxPath::new("root_file.txt").unwrap();
        let content = b"root content";
        let mut cursor = std::io::Cursor::new(content.to_vec());
        writer
            .insert(&config, file_path.clone(), &mut cursor, HashMap::new())
            .await
            .unwrap();

        let target_index = writer.metadata().index(&file_path).unwrap();

        // Create subdirectory
        writer
            .mkdir(BoxPath::new("subdir").unwrap(), HashMap::new())
            .unwrap();

        // Add a symlink in the subdirectory pointing to the root file
        let link_path = BoxPath::new("subdir/link.txt").unwrap();
        writer
            .link(link_path, target_index, HashMap::new())
            .unwrap();

        writer.finish().await.unwrap();
    }

    // Extract and verify
    let extract_dir = temp_dir.path().join("extracted");
    fs::create_dir_all(&extract_dir).await.unwrap();

    {
        let reader = box_format::BoxFileReader::open(&archive_path)
            .await
            .unwrap();
        reader.extract_all(&extract_dir).await.unwrap();
    }

    let link_path = extract_dir.join("subdir/link.txt");
    assert!(link_path.is_symlink());

    let target = fs::read_link(&link_path).await.unwrap();
    assert_eq!(target.to_str().unwrap(), "../root_file.txt");

    // Verify resolution
    let content = fs::read_to_string(&link_path).await.unwrap();
    assert_eq!(content, "root content");
}

/// Test symlink with multiple levels of parent directory traversal
#[tokio::test]
async fn test_symlink_multiple_parent_levels() {
    let (temp_dir, archive_path) = create_test_archive().await;

    {
        let mut writer = BoxFileWriter::create(&archive_path).await.unwrap();

        // Create deep directory structure: a/b/c
        writer
            .mkdir_all(BoxPath::new("a/b/c").unwrap(), HashMap::new())
            .unwrap();

        // Add a file at root level in directory 'x'
        writer
            .mkdir(BoxPath::new("x").unwrap(), HashMap::new())
            .unwrap();

        let config = CompressionConfig::new(Compression::Stored);
        let file_path = BoxPath::new("x/target.txt").unwrap();
        let content = b"deep target";
        let mut cursor = std::io::Cursor::new(content.to_vec());
        writer
            .insert(&config, file_path.clone(), &mut cursor, HashMap::new())
            .await
            .unwrap();

        let target_index = writer.metadata().index(&file_path).unwrap();

        // Add a symlink deep in a/b/c pointing to x/target.txt
        let link_path = BoxPath::new("a/b/c/link.txt").unwrap();
        writer
            .link(link_path, target_index, HashMap::new())
            .unwrap();

        writer.finish().await.unwrap();
    }

    // Extract and verify
    let extract_dir = temp_dir.path().join("extracted");
    fs::create_dir_all(&extract_dir).await.unwrap();

    {
        let reader = box_format::BoxFileReader::open(&archive_path)
            .await
            .unwrap();
        reader.extract_all(&extract_dir).await.unwrap();
    }

    let link_path = extract_dir.join("a/b/c/link.txt");
    assert!(link_path.is_symlink());

    let target = fs::read_link(&link_path).await.unwrap();
    // From a/b/c to x/target.txt requires ../../../x/target.txt
    assert_eq!(target.to_str().unwrap(), "../../../x/target.txt");

    // Verify resolution
    let content = fs::read_to_string(&link_path).await.unwrap();
    assert_eq!(content, "deep target");
}

/// Test symlink to sibling directory file
#[tokio::test]
async fn test_symlink_to_sibling_directory() {
    let (temp_dir, archive_path) = create_test_archive().await;

    {
        let mut writer = BoxFileWriter::create(&archive_path).await.unwrap();

        // Create two sibling directories
        writer
            .mkdir(BoxPath::new("dir_a").unwrap(), HashMap::new())
            .unwrap();
        writer
            .mkdir(BoxPath::new("dir_b").unwrap(), HashMap::new())
            .unwrap();

        // Add a file in dir_b
        let config = CompressionConfig::new(Compression::Stored);
        let file_path = BoxPath::new("dir_b/target.txt").unwrap();
        let content = b"sibling content";
        let mut cursor = std::io::Cursor::new(content.to_vec());
        writer
            .insert(&config, file_path.clone(), &mut cursor, HashMap::new())
            .await
            .unwrap();

        let target_index = writer.metadata().index(&file_path).unwrap();

        // Add a symlink in dir_a pointing to dir_b/target.txt
        let link_path = BoxPath::new("dir_a/link.txt").unwrap();
        writer
            .link(link_path, target_index, HashMap::new())
            .unwrap();

        writer.finish().await.unwrap();
    }

    // Extract and verify
    let extract_dir = temp_dir.path().join("extracted");
    fs::create_dir_all(&extract_dir).await.unwrap();

    {
        let reader = box_format::BoxFileReader::open(&archive_path)
            .await
            .unwrap();
        reader.extract_all(&extract_dir).await.unwrap();
    }

    let link_path = extract_dir.join("dir_a/link.txt");
    assert!(link_path.is_symlink());

    let target = fs::read_link(&link_path).await.unwrap();
    // From dir_a to dir_b/target.txt requires ../dir_b/target.txt
    assert_eq!(target.to_str().unwrap(), "../dir_b/target.txt");

    // Verify resolution
    let content = fs::read_to_string(&link_path).await.unwrap();
    assert_eq!(content, "sibling content");
}

/// Test symlink to a directory
#[tokio::test]
async fn test_symlink_to_directory() {
    let (temp_dir, archive_path) = create_test_archive().await;

    {
        let mut writer = BoxFileWriter::create(&archive_path).await.unwrap();

        // Create a directory
        writer
            .mkdir(BoxPath::new("target_dir").unwrap(), HashMap::new())
            .unwrap();

        let target_index = writer
            .metadata()
            .index(&BoxPath::new("target_dir").unwrap())
            .unwrap();

        // Add a symlink pointing to the directory
        let link_path = BoxPath::new("link_dir").unwrap();
        writer
            .link(link_path, target_index, HashMap::new())
            .unwrap();

        writer.finish().await.unwrap();
    }

    // Extract and verify
    let extract_dir = temp_dir.path().join("extracted");
    fs::create_dir_all(&extract_dir).await.unwrap();

    {
        let reader = box_format::BoxFileReader::open(&archive_path)
            .await
            .unwrap();
        reader.extract_all(&extract_dir).await.unwrap();
    }

    let link_path = extract_dir.join("link_dir");
    assert!(link_path.is_symlink());

    let target = fs::read_link(&link_path).await.unwrap();
    assert_eq!(target.to_str().unwrap(), "target_dir");
}

/// Test the real-world case: compiler-rt symlinks like LLVM uses
/// lib/clang/21/lib/linux/libclang_rt.builtins-x86_64.a -> ../x86_64-unknown-linux-musl/libclang_rt.builtins.a
#[tokio::test]
async fn test_llvm_style_symlinks() {
    let (temp_dir, archive_path) = create_test_archive().await;

    {
        let mut writer = BoxFileWriter::create(&archive_path).await.unwrap();

        // Create the directory structure
        writer
            .mkdir_all(
                BoxPath::new("lib/clang/21/lib/linux").unwrap(),
                HashMap::new(),
            )
            .unwrap();
        writer
            .mkdir_all(
                BoxPath::new("lib/clang/21/lib/x86_64-unknown-linux-musl").unwrap(),
                HashMap::new(),
            )
            .unwrap();

        // Add the actual file
        let config = CompressionConfig::new(Compression::Stored);
        let file_path =
            BoxPath::new("lib/clang/21/lib/x86_64-unknown-linux-musl/libclang_rt.builtins.a")
                .unwrap();
        let content = b"fake archive content";
        let mut cursor = std::io::Cursor::new(content.to_vec());
        writer
            .insert(&config, file_path.clone(), &mut cursor, HashMap::new())
            .await
            .unwrap();

        let target_index = writer.metadata().index(&file_path).unwrap();

        // Add the symlink (like LLVM's linux/ symlinks)
        let link_path =
            BoxPath::new("lib/clang/21/lib/linux/libclang_rt.builtins-x86_64.a").unwrap();
        writer
            .link(link_path, target_index, HashMap::new())
            .unwrap();

        writer.finish().await.unwrap();
    }

    // Extract and verify
    let extract_dir = temp_dir.path().join("extracted");
    fs::create_dir_all(&extract_dir).await.unwrap();

    {
        let reader = box_format::BoxFileReader::open(&archive_path)
            .await
            .unwrap();
        reader.extract_all(&extract_dir).await.unwrap();
    }

    let link_path = extract_dir.join("lib/clang/21/lib/linux/libclang_rt.builtins-x86_64.a");
    assert!(link_path.is_symlink(), "LLVM-style symlink should exist");

    let target = fs::read_link(&link_path).await.unwrap();
    // From lib/clang/21/lib/linux to lib/clang/21/lib/x86_64-unknown-linux-musl
    assert_eq!(
        target.to_str().unwrap(),
        "../x86_64-unknown-linux-musl/libclang_rt.builtins.a"
    );

    // Verify the symlink resolves and contains the right content
    let content = fs::read(&link_path).await.unwrap();
    assert_eq!(content, b"fake archive content");
}

/// Test multiple symlinks pointing to the same target
#[tokio::test]
async fn test_multiple_symlinks_same_target() {
    let (temp_dir, archive_path) = create_test_archive().await;

    {
        let mut writer = BoxFileWriter::create(&archive_path).await.unwrap();

        // Add a file
        let config = CompressionConfig::new(Compression::Stored);
        let file_path = BoxPath::new("target.txt").unwrap();
        let content = b"shared content";
        let mut cursor = std::io::Cursor::new(content.to_vec());
        writer
            .insert(&config, file_path.clone(), &mut cursor, HashMap::new())
            .await
            .unwrap();

        let target_index = writer.metadata().index(&file_path).unwrap();

        // Add multiple symlinks to the same target
        writer
            .link(
                BoxPath::new("link1.txt").unwrap(),
                target_index,
                HashMap::new(),
            )
            .unwrap();
        writer
            .link(
                BoxPath::new("link2.txt").unwrap(),
                target_index,
                HashMap::new(),
            )
            .unwrap();
        writer
            .link(
                BoxPath::new("link3.txt").unwrap(),
                target_index,
                HashMap::new(),
            )
            .unwrap();

        writer.finish().await.unwrap();
    }

    // Extract and verify
    let extract_dir = temp_dir.path().join("extracted");
    fs::create_dir_all(&extract_dir).await.unwrap();

    {
        let reader = box_format::BoxFileReader::open(&archive_path)
            .await
            .unwrap();
        reader.extract_all(&extract_dir).await.unwrap();
    }

    // All symlinks should point to target.txt
    for link_name in ["link1.txt", "link2.txt", "link3.txt"] {
        let link_path = extract_dir.join(link_name);
        assert!(link_path.is_symlink());
        let target = fs::read_link(&link_path).await.unwrap();
        assert_eq!(target.to_str().unwrap(), "target.txt");
    }
}

/// Test parallel extraction with symlinks
#[tokio::test]
async fn test_parallel_extraction_with_symlinks() {
    let (temp_dir, archive_path) = create_test_archive().await;

    {
        let mut writer = BoxFileWriter::create(&archive_path).await.unwrap();

        // Create several files and symlinks
        let config = CompressionConfig::new(Compression::Stored);

        for i in 0..10 {
            let file_path = BoxPath::new(&format!("file{}.txt", i)).unwrap();
            let content = format!("content {}", i);
            let mut cursor = std::io::Cursor::new(content.into_bytes());
            writer
                .insert(&config, file_path.clone(), &mut cursor, HashMap::new())
                .await
                .unwrap();

            let target_index = writer.metadata().index(&file_path).unwrap();

            let link_path = BoxPath::new(&format!("link{}.txt", i)).unwrap();
            writer
                .link(link_path, target_index, HashMap::new())
                .unwrap();
        }

        writer.finish().await.unwrap();
    }

    // Extract in parallel
    let extract_dir = temp_dir.path().join("extracted");
    fs::create_dir_all(&extract_dir).await.unwrap();

    {
        let reader = box_format::BoxFileReader::open(&archive_path)
            .await
            .unwrap();
        reader
            .extract_all_parallel(&extract_dir, Default::default(), 4)
            .await
            .unwrap();
    }

    // Verify all symlinks
    for i in 0..10 {
        let link_path = extract_dir.join(format!("link{}.txt", i));
        assert!(link_path.is_symlink());
        let target = fs::read_link(&link_path).await.unwrap();
        assert_eq!(target.to_str().unwrap(), format!("file{}.txt", i));

        let content = fs::read_to_string(&link_path).await.unwrap();
        assert_eq!(content, format!("content {}", i));
    }
}

/// Test resolve_link method
#[tokio::test]
async fn test_resolve_link() {
    let (_temp_dir, archive_path) = create_test_archive().await;

    {
        let mut writer = BoxFileWriter::create(&archive_path).await.unwrap();

        // Add a file
        let config = CompressionConfig::new(Compression::Stored);
        let file_path = BoxPath::new("target.txt").unwrap();
        let content = b"content";
        let mut cursor = std::io::Cursor::new(content.to_vec());
        writer
            .insert(&config, file_path.clone(), &mut cursor, HashMap::new())
            .await
            .unwrap();

        let target_index = writer.metadata().index(&file_path).unwrap();

        // Add a symlink
        let link_path = BoxPath::new("link.txt").unwrap();
        writer
            .link(link_path, target_index, HashMap::new())
            .unwrap();

        writer.finish().await.unwrap();
    }

    // Open and test resolve_link
    let reader = box_format::BoxFileReader::open(&archive_path)
        .await
        .unwrap();

    let link_index = reader
        .metadata()
        .index(&BoxPath::new("link.txt").unwrap())
        .unwrap();
    let link_record = reader.metadata().record(link_index).unwrap();

    if let box_format::Record::Link(link) = link_record {
        let resolved = reader.resolve_link(link).unwrap();
        assert_eq!(resolved.path.to_string(), "target.txt");
        assert!(resolved.record.as_file().is_some());
    } else {
        panic!("Expected link record");
    }
}

// ============================================================================
// ERROR CASES - Tests that verify proper error handling
// ============================================================================

/// Test that creating a symlink with an invalid (non-existent) RecordIndex fails at creation time
#[tokio::test]
async fn test_symlink_invalid_target_index_creation_fails() {
    let (_temp_dir, archive_path) = create_test_archive().await;

    let mut writer = BoxFileWriter::create(&archive_path).await.unwrap();

    // Add a file (so the archive isn't empty)
    let config = CompressionConfig::new(Compression::Stored);
    let file_path = BoxPath::new("real_file.txt").unwrap();
    let content = b"content";
    let mut cursor = std::io::Cursor::new(content.to_vec());
    writer
        .insert(&config, file_path, &mut cursor, HashMap::new())
        .await
        .unwrap();

    // Try to create a symlink pointing to a non-existent index (index 999)
    // This should fail immediately, not at extraction time
    let fake_index = RecordIndex::new(999).unwrap();
    let link_path = BoxPath::new("bad_link.txt").unwrap();
    let result = writer.link(link_path, fake_index, HashMap::new());

    assert!(
        result.is_err(),
        "Creating symlink with invalid target should fail"
    );
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("999"),
        "Error should mention the bad index: {}",
        err
    );
}

/// Test that link() fails when target doesn't exist (different index values)
#[tokio::test]
async fn test_symlink_various_invalid_indices() {
    let (_temp_dir, archive_path) = create_test_archive().await;

    let mut writer = BoxFileWriter::create(&archive_path).await.unwrap();

    // Add one file so we have index 1
    let config = CompressionConfig::new(Compression::Stored);
    let file_path = BoxPath::new("file.txt").unwrap();
    let content = b"content";
    let mut cursor = std::io::Cursor::new(content.to_vec());
    writer
        .insert(&config, file_path, &mut cursor, HashMap::new())
        .await
        .unwrap();

    // Test various invalid indices
    for invalid_idx in [2, 10, 100, 9999] {
        let fake_index = RecordIndex::new(invalid_idx).unwrap();
        let result = writer.link(
            BoxPath::new(&format!("link{}.txt", invalid_idx)).unwrap(),
            fake_index,
            HashMap::new(),
        );
        assert!(
            result.is_err(),
            "Creating symlink with index {} should fail",
            invalid_idx
        );
    }

    // But index 1 (the file we added) should work
    let valid_index = RecordIndex::new(1).unwrap();
    let result = writer.link(
        BoxPath::new("valid_link.txt").unwrap(),
        valid_index,
        HashMap::new(),
    );
    assert!(
        result.is_ok(),
        "Creating symlink with valid index should succeed"
    );
}

/// Test that RecordIndex::new(0) fails (index must be non-zero)
#[tokio::test]
async fn test_record_index_zero_fails() {
    let result = RecordIndex::new(0);
    assert!(result.is_err(), "RecordIndex::new(0) should fail");
}

/// Test creating a symlink that points to itself (same path)
/// This should be allowed at creation but would be a broken symlink
#[tokio::test]
async fn test_symlink_self_reference() {
    let (temp_dir, archive_path) = create_test_archive().await;

    {
        let mut writer = BoxFileWriter::create(&archive_path).await.unwrap();

        // Add a file first
        let config = CompressionConfig::new(Compression::Stored);
        let file_path = BoxPath::new("file.txt").unwrap();
        let content = b"content";
        let mut cursor = std::io::Cursor::new(content.to_vec());
        writer
            .insert(&config, file_path, &mut cursor, HashMap::new())
            .await
            .unwrap();

        // Now create a symlink - get its own index after creation
        // This is a bit contrived but tests edge case
        let file_index = writer
            .metadata()
            .index(&BoxPath::new("file.txt").unwrap())
            .unwrap();

        // Create a symlink that points to the file (this is valid)
        let link_index = writer
            .link(
                BoxPath::new("link.txt").unwrap(),
                file_index,
                HashMap::new(),
            )
            .unwrap();

        // Now try to create another symlink pointing to the first symlink
        // (symlink chains are valid)
        writer
            .link(
                BoxPath::new("link2.txt").unwrap(),
                link_index,
                HashMap::new(),
            )
            .unwrap();

        writer.finish().await.unwrap();
    }

    // Extract and verify symlink chain works
    let extract_dir = temp_dir.path().join("extracted");
    fs::create_dir_all(&extract_dir).await.unwrap();

    let reader = box_format::BoxFileReader::open(&archive_path)
        .await
        .unwrap();
    reader.extract_all(&extract_dir).await.unwrap();

    // link2 -> link -> file
    let link2_target = fs::read_link(extract_dir.join("link2.txt")).await.unwrap();
    assert_eq!(link2_target.to_str().unwrap(), "link.txt");

    let link_target = fs::read_link(extract_dir.join("link.txt")).await.unwrap();
    assert_eq!(link_target.to_str().unwrap(), "file.txt");

    // Reading through the chain should work
    let content = fs::read_to_string(extract_dir.join("link2.txt"))
        .await
        .unwrap();
    assert_eq!(content, "content");
}

/// Test that path_for_index returns None for non-existent index
#[tokio::test]
async fn test_path_for_index_nonexistent() {
    let (_temp_dir, archive_path) = create_test_archive().await;

    {
        let mut writer = BoxFileWriter::create(&archive_path).await.unwrap();

        // Add a file
        let config = CompressionConfig::new(Compression::Stored);
        let file_path = BoxPath::new("file.txt").unwrap();
        let content = b"content";
        let mut cursor = std::io::Cursor::new(content.to_vec());
        writer
            .insert(&config, file_path, &mut cursor, HashMap::new())
            .await
            .unwrap();

        writer.finish().await.unwrap();
    }

    let reader = box_format::BoxFileReader::open(&archive_path)
        .await
        .unwrap();

    // Valid index should return Some
    let valid_index = RecordIndex::new(1).unwrap();
    assert!(reader.metadata().path_for_index(valid_index).is_some());

    // Invalid index should return None
    let invalid_index = RecordIndex::new(999).unwrap();
    assert!(reader.metadata().path_for_index(invalid_index).is_none());
}
