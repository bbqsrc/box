use super::*;

macro_rules! path {
    ($value:expr) => {
        BoxPath::new($value).unwrap()
    };
}

#[test]
// [spec:box:def:archive-state.root.writer/test]
fn test_new_writer() {
    let writer = ArchiveWriter::new(WriterOptions::default());
    assert_eq!(writer.version(), 1);
    assert_eq!(writer.alignment(), 0);
    assert!(!writer.allow_escapes());
    assert!(!writer.allow_external_symlinks());
}

#[test]
fn test_with_alignment() {
    let writer = ArchiveWriter::with_alignment(4096);
    assert_eq!(writer.alignment(), 4096);
}

#[test]
// [spec:box:sem:sans-io.root.alignment/test]
fn test_next_write_addr_no_alignment() {
    let mut writer = ArchiveWriter::new(WriterOptions::default());
    assert_eq!(writer.next_write_addr().get(), 32);

    writer.advance_position(100).unwrap();
    assert_eq!(writer.next_write_addr().get(), 132);
}

#[test]
// [spec:box:sem:sans-io.root.alignment/test]
fn test_next_write_addr_with_alignment() {
    let mut writer = ArchiveWriter::with_alignment(64);
    assert_eq!(writer.next_write_addr().get(), 64);

    writer.advance_position(10).unwrap();
    assert_eq!(writer.next_write_addr().get(), 128);
}

#[test]
fn test_mkdir() {
    let mut writer = ArchiveWriter::new(WriterOptions::default());
    let idx = writer.mkdir(path!("test"), HashMap::new()).unwrap();
    assert_eq!(idx.get(), 1);

    let record = writer.meta.record(idx).unwrap();
    assert!(record.as_directory().is_some());
    assert_eq!(record.name(), "test");
}

#[test]
// [spec:box:req:sans-io.root.hierarchy/test]
fn test_mkdir_all() {
    let mut writer = ArchiveWriter::new(WriterOptions::default());
    writer.mkdir_all(path!("a/b/c"), HashMap::new()).unwrap();

    assert!(writer.meta.index(&path!("a")).is_some());
    assert!(writer.meta.index(&path!("a/b")).is_some());
    assert!(writer.meta.index(&path!("a/b/c")).is_some());
}

#[test]
// [spec:box:req:sans-io.root.hierarchy/test]
fn test_mkdir_all_rejects_non_directory_parent() {
    let mut writer = ArchiveWriter::new(WriterOptions::default());
    writer
        .insert_file(
            path!("parent"),
            Compression::Stored,
            writer.next_write_addr(),
            0,
            0,
            HashMap::new(),
        )
        .unwrap();
    let record_count = writer.record_count();

    let error = writer
        .mkdir_all(path!("parent/child"), HashMap::new())
        .unwrap_err();

    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("not a directory"));
    assert_eq!(writer.record_count(), record_count);
}

#[test]
// [spec:box:req:sans-io.root.hierarchy/test]
fn test_mkdir_all_rejects_existing_non_directory() {
    let mut writer = ArchiveWriter::new(WriterOptions::default());
    let file_path = path!("existing");
    let file_index = writer
        .insert_file(
            file_path.clone(),
            Compression::Stored,
            writer.next_write_addr(),
            0,
            0,
            HashMap::new(),
        )
        .unwrap();
    let record_count = writer.record_count();

    let error = writer
        .mkdir_all(file_path.clone(), HashMap::new())
        .unwrap_err();

    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("not a directory"));
    assert_eq!(writer.record_count(), record_count);
    assert!(writer.meta.record(file_index).unwrap().as_file().is_some());
}

#[test]
fn test_encode_header() {
    let writer = ArchiveWriter::with_options(4096, true, true);
    let header = writer.encode_header();

    assert_eq!(&header[0..4], b"\xffBOX");
    assert_eq!(header[4], 1);
    assert_eq!(header[5], 0x03);
}

// [spec:box:sem:async-io.root.writer-lifecycle/test/unit]
// [spec:box:sem:sync-io.root.write/test/unit]
#[test]
fn existing_data_end_handles_chunked_overflow() {
    let mut meta = BoxMetadata::default();
    meta.records.push(Record::File(FileRecord {
        compression: Compression::Stored,
        length: 10,
        decompressed_length: 10,
        data: NonZeroU64::new(32).unwrap(),
        name: Cow::Borrowed("regular"),
        attrs: Default::default(),
    }));
    meta.records.push(Record::ChunkedFile(ChunkedFileRecord {
        compression: Compression::Stored,
        block_size: 8,
        length: 20,
        decompressed_length: 20,
        data: NonZeroU64::new(100).unwrap(),
        name: Cow::Borrowed("chunked"),
        attrs: Default::default(),
    }));
    assert_eq!(ArchiveWriter::existing_data_end(&meta).unwrap(), 120);

    meta.records.push(Record::File(FileRecord {
        compression: Compression::Stored,
        length: 1,
        decompressed_length: 1,
        data: NonZeroU64::new(u64::MAX).unwrap(),
        name: Cow::Borrowed("overflow"),
        attrs: Default::default(),
    }));
    assert_eq!(
        ArchiveWriter::existing_data_end(&meta).unwrap_err().kind(),
        std::io::ErrorKind::InvalidData
    );
}

// [spec:box:sem:sans-io.root.alignment/test/unit]
// [spec:box:sem:async-io.root.writer-lifecycle/test/unit]
// [spec:box:sem:sync-io.root.open/test/unit]
#[test]
fn writer_positions_reject_unalignable_or_overflowing_cursors() {
    let error =
        ArchiveWriter::from_existing(BoxHeader::default(), BoxMetadata::default(), u64::MAX)
            .err()
            .expect("u64::MAX cannot leave room for a trailer");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);

    let mut writer = ArchiveWriter::new(WriterOptions::default());
    assert_eq!(
        writer.set_position(u64::MAX).unwrap_err().kind(),
        std::io::ErrorKind::InvalidData
    );
    assert_eq!(writer.next_write_addr().get(), BoxHeader::SIZE as u64);
    assert_eq!(
        writer.advance_position(u64::MAX).unwrap_err().kind(),
        std::io::ErrorKind::InvalidData
    );
    assert_eq!(writer.next_write_addr().get(), BoxHeader::SIZE as u64);

    let mut aligned_header = BoxHeader::with_alignment(8);
    aligned_header.trailer = NonZeroU64::new(u64::MAX);
    let error = ArchiveWriter::from_existing(aligned_header, BoxMetadata::default(), u64::MAX - 3)
        .err()
        .expect("alignment must not wrap the cursor");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
}

// [spec:box:sem:sans-io.root.finalization/test/unit]
#[test]
fn finalization_rejects_bad_legacy_hierarchy() {
    let missing_index = RecordIndex::new(2).unwrap();
    let mut missing_meta = BoxMetadata::default();
    missing_meta
        .records
        .push(Record::Directory(DirectoryRecord {
            name: Cow::Borrowed("root"),
            entries: vec![missing_index],
            attrs: Default::default(),
        }));
    missing_meta.root.push(RecordIndex::new(1).unwrap());
    let error =
        ArchiveWriter::from_existing(BoxHeader::default(), missing_meta, BoxHeader::SIZE as u64)
            .err()
            .expect("missing hierarchy index must be rejected");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("missing record index 2"));

    let mut cyclic_meta = BoxMetadata::default();
    let root_index = cyclic_meta.insert_record(Record::Directory(DirectoryRecord {
        name: Cow::Borrowed("root"),
        entries: Vec::new(),
        attrs: Default::default(),
    }));
    cyclic_meta.root.push(root_index);
    cyclic_meta
        .record_mut(root_index)
        .unwrap()
        .as_directory_mut()
        .unwrap()
        .entries
        .push(root_index);
    let error =
        ArchiveWriter::from_existing(BoxHeader::default(), cyclic_meta, BoxHeader::SIZE as u64)
            .err()
            .expect("cyclic hierarchy must be rejected");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("appears more than once"));
}

// [spec:box:sem:sans-io.root.finalization/test/unit]
// [spec:box:syn:chunked-io.root.block-index-entry/test/unit]
#[test]
fn finalization_rejects_block_conflicts() {
    let mut meta = BoxMetadata::default();
    let record_index = meta.insert_record(Record::ChunkedFile(ChunkedFileRecord {
        compression: Compression::Stored,
        block_size: 4,
        length: 8,
        decompressed_length: 8,
        data: NonZeroU64::new(32).unwrap(),
        name: Cow::Borrowed("chunked"),
        attrs: Default::default(),
    }));
    meta.root.push(record_index);

    let mut key = [0u8; 16];
    key[..8].copy_from_slice(&record_index.get().to_be_bytes());
    let mut builder = box_fst::FstBuilder::new();
    builder.insert(&key, 32u64).unwrap();
    let mut second_key = key;
    second_key[8..].copy_from_slice(&4u64.to_be_bytes());
    builder.insert(&second_key, 36u64).unwrap();
    meta.block_fst = Some(
        box_fst::Fst::new(Cow::Owned(builder.finish().unwrap()))
            .expect("test block FST should be valid"),
    );

    let mut writer = ArchiveWriter::from_existing(BoxHeader::default(), meta, 40).unwrap();
    writer.block_entries.push((key, 33));

    let error = writer.finish().unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("block FST conflict"));
}
