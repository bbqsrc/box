// SPDX-License-Identifier: GPL-2.0-only
//! End-to-end checks of the exported C-ABI surface against an archive written
//! by the box library itself, through the simulated block device.

use std::collections::BTreeMap;

use box_format::{aio, BoxPath, Compression, CompressionConfig, HashMap as WriterHashMap};

use crate::bindings::{DT_DIR, DT_LNK, DT_REG};
use crate::kernel_sim;

const CHUNK_BLOCK_SIZE: u32 = 65536;
const ALIGNMENT: u32 = 4096;

/// `unix.mode` is a FastVint-encoded Vu32; the library encoder keeps the
/// fixture from drifting from the format.
fn mode_attr(mode: u32) -> Vec<u8> {
    let mut buf = Vec::new();
    box_format::encode::encode_vu64(&mut buf, u64::from(mode));
    buf
}

fn attrs_with_mode(mode: u32) -> WriterHashMap<String, Vec<u8>> {
    let mut attrs = WriterHashMap::new();
    attrs.insert("unix.mode".to_string(), mode_attr(mode));
    attrs
}

/// Deterministic filler that still compresses, so chunked blocks differ.
fn filler(len: usize, seed: u64) -> Vec<u8> {
    let mut state = seed | 1;
    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let word = (state >> 33) as u32;
        out.extend_from_slice(format!("{word:08x} line {}\n", out.len()).as_bytes());
    }
    out.truncate(len);
    out
}

pub struct Fixture {
    pub image: Vec<u8>,
    pub files: BTreeMap<&'static str, (Vec<u8>, u32)>,
}

/// Writes an archive shaped like `box create --zstd-chunked --align 4096`:
/// stored and zstd files below one block, a chunked file above it, nested
/// directories, an internal and an external symlink, and an xattr.
pub fn build_fixture() -> Fixture {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let temp = tempfile::tempdir().expect("fixture directory");
    let archive_path = temp.path().join("fixture.box");

    let small = b"hello boxfs, this line is long enough to be worth compressing.".to_vec();
    let stored = b"short".to_vec();
    let empty: Vec<u8> = Vec::new();
    let mid = filler(200_003, 7);
    let chunked = filler(300_007, 11);
    let deep = filler(4097, 13);

    let mut files: BTreeMap<&'static str, (Vec<u8>, u32)> = BTreeMap::new();
    files.insert("small.txt", (small.clone(), 0o100644));
    files.insert("stored.bin", (stored.clone(), 0o100600));
    files.insert("empty.txt", (empty.clone(), 0o100644));
    files.insert("dir/mid.bin", (mid.clone(), 0o100755));
    files.insert("dir/chunked.bin", (chunked.clone(), 0o100644));
    files.insert("dir/inner/deep.txt", (deep.clone(), 0o100640));

    runtime.block_on(async {
        let mut writer =
            aio::BoxFileWriter::create_with_options(&archive_path, ALIGNMENT, false, true)
                .await
                .expect("create fixture archive");

        writer
            .mkdir(BoxPath::new("dir").unwrap(), attrs_with_mode(0o40755))
            .expect("mkdir dir");
        writer
            .mkdir(BoxPath::new("dir/inner").unwrap(), attrs_with_mode(0o40700))
            .expect("mkdir dir/inner");

        let mut small_attrs = attrs_with_mode(0o100644);
        small_attrs.insert(
            "linux.xattr.user.boxfs".to_string(),
            b"attribute-value".to_vec(),
        );
        writer
            .insert(
                &CompressionConfig::new(Compression::Zstd),
                BoxPath::new("small.txt").unwrap(),
                small.as_slice(),
                small_attrs,
            )
            .await
            .expect("insert small.txt");

        writer
            .insert(
                &CompressionConfig::new(Compression::Stored),
                BoxPath::new("stored.bin").unwrap(),
                stored.as_slice(),
                attrs_with_mode(0o100600),
            )
            .await
            .expect("insert stored.bin");

        writer
            .insert(
                &CompressionConfig::new(Compression::Stored),
                BoxPath::new("empty.txt").unwrap(),
                empty.as_slice(),
                attrs_with_mode(0o100644),
            )
            .await
            .expect("insert empty.txt");

        writer
            .insert(
                &CompressionConfig::new(Compression::Zstd),
                BoxPath::new("dir/mid.bin").unwrap(),
                mid.as_slice(),
                attrs_with_mode(0o100755),
            )
            .await
            .expect("insert dir/mid.bin");

        writer
            .insert_chunked(
                BoxPath::new("dir/chunked.bin").unwrap(),
                chunked.as_slice(),
                CHUNK_BLOCK_SIZE,
                Compression::Zstd,
                attrs_with_mode(0o100644),
            )
            .await
            .expect("insert dir/chunked.bin");

        writer
            .insert(
                &CompressionConfig::new(Compression::Zstd),
                BoxPath::new("dir/inner/deep.txt").unwrap(),
                deep.as_slice(),
                attrs_with_mode(0o100640),
            )
            .await
            .expect("insert dir/inner/deep.txt");

        let target = writer
            .metadata()
            .index(&BoxPath::new("small.txt").unwrap())
            .expect("small.txt record index");
        writer
            .link(
                BoxPath::new("link.txt").unwrap(),
                target,
                attrs_with_mode(0o120777),
            )
            .expect("internal symlink");

        writer
            .link(
                BoxPath::new("dir/inner/up-link").unwrap(),
                target,
                attrs_with_mode(0o120777),
            )
            .expect("nested internal symlink");

        writer
            .external_link(
                BoxPath::new("outside").unwrap(),
                "../../etc/hostname",
                attrs_with_mode(0o120777),
            )
            .expect("external symlink");

        writer.finish().await.expect("finish fixture archive");
    });

    let image = std::fs::read(&archive_path).expect("read fixture archive");
    Fixture { image, files }
}

fn mount(fixture: &Fixture) -> u64 {
    let ret = kernel_sim::mount(kernel_sim::pad_to_sector(fixture.image.clone()));
    assert_eq!(ret, 0, "fill_super failed with {ret}");
    kernel_sim::root_ino()
}

fn resolve(root: u64, path: &str) -> u64 {
    let mut ino = root;
    for component in path.split('/') {
        ino = kernel_sim::lookup(ino, component);
        assert_ne!(ino, 0, "lookup failed for {path} at {component}");
    }
    ino
}

// [spec:box:req:kernel-vfs.root.namespace/test/unit]
#[test]
fn namespace_matches_the_written_archive() {
    let _serial = kernel_sim::serialised();
    let fixture = build_fixture();
    let root = mount(&fixture);

    let mut root_names: Vec<_> = kernel_sim::readdir(root, 64)
        .into_iter()
        .map(|(name, _, dtype)| (name, dtype))
        .collect();
    root_names.sort();
    assert_eq!(
        root_names,
        vec![
            ("dir".to_string(), u32::from(DT_DIR)),
            ("empty.txt".to_string(), u32::from(DT_REG)),
            ("link.txt".to_string(), u32::from(DT_LNK)),
            ("outside".to_string(), u32::from(DT_LNK)),
            ("small.txt".to_string(), u32::from(DT_REG)),
            ("stored.bin".to_string(), u32::from(DT_REG)),
        ]
    );

    let dir = resolve(root, "dir");
    let mut dir_names: Vec<_> = kernel_sim::readdir(dir, 64)
        .into_iter()
        .map(|(name, _, _)| name)
        .collect();
    dir_names.sort();
    assert_eq!(dir_names, vec!["chunked.bin", "inner", "mid.bin"]);

    let inner = resolve(root, "dir/inner");
    let mut inner_names: Vec<_> = kernel_sim::readdir(inner, 64)
        .into_iter()
        .map(|(name, _, _)| name)
        .collect();
    inner_names.sort();
    assert_eq!(inner_names, vec!["deep.txt", "up-link"]);

    assert_eq!(kernel_sim::lookup(root, "missing"), 0);
    kernel_sim::detach();
}

// [spec:box:req:kernel-vfs.root.namespace/test/unit]
#[test]
fn iteration_resumes_after_full_buffer() {
    let _serial = kernel_sim::serialised();
    let fixture = build_fixture();
    let root = mount(&fixture);

    let batched = kernel_sim::readdir(root, 2);
    let single_pass = kernel_sim::readdir(root, 64);
    assert_eq!(batched.len(), 6);
    assert_eq!(batched, single_pass);

    kernel_sim::detach();
}

// [spec:box:req:kernel-vfs.root.data/test/unit]
#[test]
fn contents_round_trip_through_reads() {
    let _serial = kernel_sim::serialised();
    let fixture = build_fixture();
    let root = mount(&fixture);

    for (path, (contents, mode)) in &fixture.files {
        let ino = resolve(root, path);
        let (got_mode, size, blocks) = kernel_sim::getattr(ino).expect("getattr");
        assert_eq!(got_mode, *mode as u16, "mode for {path}");
        assert_eq!(size, contents.len() as u64, "size for {path}");
        assert_eq!(
            blocks,
            size.div_ceil(kernel_sim::SIM_BLOCK_SIZE),
            "block count for {path}"
        );

        for chunk in [4096usize, 65536, 131072] {
            let read = kernel_sim::read_all(ino, size, chunk).expect("read");
            assert_eq!(read, *contents, "contents for {path} at chunk {chunk}");
        }
    }

    kernel_sim::detach();
}

// [spec:box:req:kernel-vfs.root.data/test/unit]
#[test]
fn unaligned_reads_cross_chunk_boundaries() {
    let _serial = kernel_sim::serialised();
    let fixture = build_fixture();
    let root = mount(&fixture);

    let (contents, _) = &fixture.files["dir/chunked.bin"];
    let ino = resolve(root, "dir/chunked.bin");

    for offset in [
        0u64,
        1,
        4095,
        u64::from(CHUNK_BLOCK_SIZE) - 1,
        u64::from(CHUNK_BLOCK_SIZE),
        u64::from(CHUNK_BLOCK_SIZE) + 1,
        u64::from(CHUNK_BLOCK_SIZE) * 4 + 3,
        contents.len() as u64 - 1,
    ] {
        for len in [1usize, 7, 4096, 70000] {
            let mut buf = vec![0u8; len];
            let ret = crate::boxfs_rust_read(
                kernel_sim::sim_superblock(),
                ino,
                buf.as_mut_ptr() as *mut std::ffi::c_char,
                len,
                offset as i64,
            );
            assert!(ret >= 0, "read at {offset}+{len} failed with {ret}");
            let got = ret as usize;
            let expected_len = core::cmp::min(len, contents.len().saturating_sub(offset as usize));
            assert_eq!(got, expected_len, "short read at {offset}+{len}");
            assert_eq!(
                &buf[..got],
                &contents[offset as usize..offset as usize + got],
                "bytes at {offset}+{len}"
            );
        }
    }

    // Reads past EOF report zero rather than an error.
    let mut buf = [0u8; 16];
    let ret = crate::boxfs_rust_read(
        kernel_sim::sim_superblock(),
        ino,
        buf.as_mut_ptr() as *mut std::ffi::c_char,
        buf.len(),
        contents.len() as i64,
    );
    assert_eq!(ret, 0);

    kernel_sim::detach();
}

// [spec:box:req:kernel-vfs.root.links-and-xattrs/test/unit]
#[test]
fn links_and_xattrs_match_the_records() {
    let _serial = kernel_sim::serialised();
    let fixture = build_fixture();
    let root = mount(&fixture);

    // Internal links are stored as record indices; the mounted tree must show
    // the same relative text an extraction writes.
    let internal = resolve(root, "link.txt");
    assert_eq!(
        kernel_sim::readlink(internal).expect("readlink"),
        "small.txt"
    );

    let nested = resolve(root, "dir/inner/up-link");
    assert_eq!(
        kernel_sim::readlink(nested).expect("readlink"),
        "../../small.txt"
    );

    // st_size must be the target length so tools can size a readlink buffer.
    for (ino, expected) in [(internal, "small.txt"), (nested, "../../small.txt")] {
        let (mode, size, _) = kernel_sim::getattr(ino).expect("getattr");
        assert_eq!(mode, 0o120777);
        assert_eq!(size, expected.len() as u64);
    }

    let external = resolve(root, "outside");
    assert_eq!(
        kernel_sim::readlink(external).expect("readlink"),
        "../../etc/hostname"
    );
    assert_eq!(
        kernel_sim::getattr(external).expect("getattr").1,
        "../../etc/hostname".len() as u64
    );

    let small = resolve(root, "small.txt");
    assert_eq!(
        kernel_sim::listxattr(small).expect("listxattr"),
        vec!["user.boxfs".to_string()]
    );
    assert_eq!(
        kernel_sim::getxattr(small, "user.boxfs").expect("getxattr"),
        b"attribute-value".to_vec()
    );
    // ENODATA for an attribute the record does not carry.
    assert_eq!(kernel_sim::getxattr(small, "user.absent"), Err(-61));

    let stored = resolve(root, "stored.bin");
    assert_eq!(
        kernel_sim::listxattr(stored).expect("listxattr"),
        Vec::<String>::new()
    );

    kernel_sim::detach();
}

// [spec:box:syn:kernel-parser.root.trailer/test/unit]
#[test]
fn parsed_tree_matches_the_box_library() {
    let _serial = kernel_sim::serialised();
    let fixture = build_fixture();
    let temp = tempfile::tempdir().expect("reader directory");
    let archive_path = temp.path().join("fixture.box");
    std::fs::write(&archive_path, &fixture.image).expect("write archive");
    let reader = box_format::sync::BoxReader::open(&archive_path).expect("open with the library");

    let root = mount(&fixture);

    let mut seen = 0usize;
    for item in reader.metadata().iter() {
        let joined = item.path.to_string();
        let ino = resolve(root, &joined);
        let (mode, size, _) = kernel_sim::getattr(ino).expect("getattr");
        assert_eq!(
            u32::from(mode),
            reader.get_mode(item.record),
            "mode disagreement for {joined}"
        );
        match item.record {
            box_format::Record::File(file) => {
                assert_eq!(size, file.decompressed_length, "size for {joined}");
            }
            box_format::Record::ChunkedFile(file) => {
                assert_eq!(size, file.decompressed_length, "size for {joined}");
            }
            _ => {}
        }
        seen += 1;
    }
    assert_eq!(seen, 11, "every written record must be reachable");

    kernel_sim::detach();
}

// [spec:box:req:kernel-vfs.root.mount-lifecycle/test/unit]
#[test]
fn truncated_final_sector_is_refused() {
    let _serial = kernel_sim::serialised();
    let fixture = build_fixture();
    assert_ne!(
        fixture.image.len() as u64 % kernel_sim::SIM_BLOCK_SIZE,
        0,
        "fixture must not already be sector aligned"
    );

    // A loop device rounds the backing file down to whole sectors, which cuts
    // the tail off the trailer.
    let ret = kernel_sim::mount(fixture.image.clone());
    assert!(ret < 0, "a truncated trailer must not mount, got {ret}");
    kernel_sim::detach();
}
