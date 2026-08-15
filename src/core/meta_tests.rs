use super::*;

// [spec:box:sem:sans-io.root.lookup+1/test/unit]
#[test]
fn lookup_finds_pending_fst_descendants() {
    let mut metadata = BoxMetadata::default();
    let old = metadata.insert_record(DirectoryRecord::new("old".to_string()).into());
    let new = metadata.insert_record(DirectoryRecord::new("new".to_string()).into());
    let file = metadata.insert_record(DirectoryRecord::new("file".to_string()).into());

    metadata
        .record_mut(old)
        .unwrap()
        .as_directory_mut()
        .unwrap()
        .entries
        .push(new);
    metadata
        .record_mut(new)
        .unwrap()
        .as_directory_mut()
        .unwrap()
        .entries
        .push(file);

    let mut builder = box_fst::FstBuilder::<u64>::new();
    builder.insert(b"old", old.get()).unwrap();
    metadata.fst = Some(
        box_fst::Fst::new(Cow::Owned(builder.finish().unwrap()))
            .expect("test path FST should be well-formed"),
    );
    metadata.root.clear();

    assert_eq!(metadata.index(&BoxPath::new("old").unwrap()), Some(old));
    assert_eq!(metadata.index(&BoxPath::new("old/new").unwrap()), Some(new));
    assert_eq!(
        metadata.index(&BoxPath::new("old/new/file").unwrap()),
        Some(file)
    );
    assert!(
        metadata
            .index(&BoxPath::new("old/new/missing").unwrap())
            .is_none()
    );
}

// [spec:box:sem:archive-state.root.record-index/test/unit]
// [spec:box:req:records.root.references.resolution/test/unit]
#[test]
fn tree_lookup_skips_invalid_indices() {
    let invalid = RecordIndex::new(999).unwrap();
    let mut metadata = BoxMetadata::default();
    metadata.root.push(invalid);
    assert!(metadata.index(&BoxPath::new("missing").unwrap()).is_none());

    metadata.root.clear();
    let directory = metadata.insert_record(Record::Directory(DirectoryRecord {
        name: Cow::Borrowed("dir"),
        entries: vec![invalid],
        attrs: Default::default(),
    }));
    metadata.root.push(directory);

    assert!(
        metadata
            .index(&BoxPath::new("dir/missing").unwrap())
            .is_none()
    );
    assert!(metadata.record(invalid).is_none());
}

// [spec:box:req:attributes.root.integrity/test/unit]
#[test]
fn typed_vint_attributes_require_complete_in_range_encodings() {
    let metadata = BoxMetadata::default();
    let truncated = [0u8; 8];
    for attr_type in [
        AttrType::Vi32,
        AttrType::Vu32,
        AttrType::Vi64,
        AttrType::Vu64,
        AttrType::DateTime,
    ] {
        assert!(matches!(
            metadata.parse_attr_value(&truncated, attr_type),
            AttrValue::Bytes(_)
        ));
    }

    let too_large = fastvint::Vu64::new(u64::MAX).bytes().to_vec();
    assert!(matches!(
        metadata.parse_attr_value(&too_large, AttrType::Vu32),
        AttrValue::Bytes(_)
    ));
    assert!(matches!(
        metadata.parse_attr_value(&[0x80, 0x80], AttrType::Vu64),
        AttrValue::Bytes(_)
    ));

    let encoded = fastvint::encode_vi64(-42);
    assert!(matches!(
        metadata.parse_attr_value(encoded.bytes(), AttrType::DateTime),
        AttrValue::DateTime(-42)
    ));
}
