// SPDX-License-Identifier: GPL-2.0-only
//! Userspace stand-ins for the kernel helpers `rust_helpers.c` provides.
//!
//! The module cannot be loaded from a test, so the exported C-ABI surface is
//! driven here against a file-backed fake block device that reproduces the
//! kernel's buffer-head behaviour, including refusing the trailing partial
//! block that `sb_bread` cannot reach.

use std::collections::HashMap;
use std::ffi::{c_char, c_int, c_void};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock};

use crate::bindings::{BlockDevice, BufferHead, DirContext, Folio, ReadaheadControl, SuperBlock};

/// Loop devices expose whole 512-byte sectors, so the simulation does too.
pub const SIM_BLOCK_SIZE: u64 = 512;

struct Device {
    image: Vec<u8>,
    metadata: usize,
    archive_size: u64,
    trailer_offset: u64,
    root_ino: u64,
}

static DEVICE: OnceLock<Mutex<Option<Device>>> = OnceLock::new();
static SERIAL: OnceLock<Mutex<()>> = OnceLock::new();
static META_DEPTH: AtomicI32 = AtomicI32::new(0);
static ALLOCATIONS: OnceLock<Mutex<HashMap<usize, usize>>> = OnceLock::new();

fn lock<T>(cell: &'static OnceLock<Mutex<T>>, init: impl FnOnce() -> T) -> MutexGuard<'static, T> {
    cell.get_or_init(|| Mutex::new(init()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn device() -> MutexGuard<'static, Option<Device>> {
    lock(&DEVICE, || None)
}

fn allocations() -> MutexGuard<'static, HashMap<usize, usize>> {
    lock(&ALLOCATIONS, HashMap::new)
}

/// A single global device stands in for the superblock, so tests take turns.
pub fn serialised() -> MutexGuard<'static, ()> {
    lock(&SERIAL, || ())
}

/// A fake superblock handle. The helpers ignore it and use the global device,
/// but the pointer must be non-null for the Rust core's null checks.
pub fn sim_superblock() -> *mut SuperBlock {
    static ANCHOR: u8 = 0;
    (&ANCHOR as *const u8) as *mut SuperBlock
}

/// Install an archive image as the backing device. Returns the padded image
/// length, which is what `boxfs_bdev_nr_bytes` will report.
pub fn attach(image: Vec<u8>) -> u64 {
    let len = image.len() as u64;
    *device() = Some(Device {
        image,
        metadata: 0,
        archive_size: 0,
        trailer_offset: 0,
        root_ino: 0,
    });
    len
}

/// Drop the archive image and any Rust-owned metadata still attached to it.
pub fn detach() {
    crate::boxfs_rust_put_super(sim_superblock());
    *device() = None;
    assert!(
        allocations().is_empty(),
        "simulated kernel allocations leaked"
    );
    assert_eq!(
        META_DEPTH.load(Ordering::SeqCst),
        0,
        "the metadata mutex was left held"
    );
}

pub fn root_ino() -> u64 {
    device().as_ref().expect("attached device").root_ino
}

/// Pad an archive to the sector granularity a loop device exposes.
pub fn pad_to_sector(mut image: Vec<u8>) -> Vec<u8> {
    let remainder = image.len() as u64 % SIM_BLOCK_SIZE;
    if remainder != 0 {
        let padding = (SIM_BLOCK_SIZE - remainder) as usize;
        image.resize(image.len() + padding, 0);
    }
    image
}

// ============================================================================
// DIRECTORY CONTEXT
// ============================================================================

/// Stand-in for `struct dir_context`, with a cap so the "buffer full" path is
/// reachable.
pub struct SimDirContext {
    pub pos: i64,
    pub entries: Vec<(String, u64, u32)>,
    pub capacity: usize,
}

impl SimDirContext {
    pub fn new(capacity: usize) -> Self {
        SimDirContext {
            pos: 0,
            entries: Vec::new(),
            capacity,
        }
    }

    pub fn as_ptr(&mut self) -> *mut DirContext {
        (self as *mut SimDirContext) as *mut DirContext
    }
}

// ============================================================================
// BUFFER HEADS
// ============================================================================

struct SimBufferHead {
    data: Vec<u8>,
}

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_sb_bread(_sb: *mut SuperBlock, block: u64) -> *mut BufferHead {
    let guard = device();
    let Some(dev) = guard.as_ref() else {
        return std::ptr::null_mut();
    };
    let max_block = dev.image.len() as u64 / SIM_BLOCK_SIZE;
    if block >= max_block {
        return std::ptr::null_mut();
    }
    let start = (block * SIM_BLOCK_SIZE) as usize;
    let end = start + SIM_BLOCK_SIZE as usize;
    let bh = Box::new(SimBufferHead {
        data: dev.image[start..end].to_vec(),
    });
    Box::into_raw(bh) as *mut BufferHead
}

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_sb_bread_unmovable(sb: *mut SuperBlock, block: u64) -> *mut BufferHead {
    boxfs_sb_bread(sb, block)
}

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_brelse(bh: *mut BufferHead) {
    if !bh.is_null() {
        drop(unsafe { Box::from_raw(bh as *mut SimBufferHead) });
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_put_bh(bh: *mut BufferHead) {
    boxfs_brelse(bh);
}

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_bh_data(bh: *mut BufferHead) -> *mut c_void {
    unsafe { (*(bh as *mut SimBufferHead)).data.as_mut_ptr() as *mut c_void }
}

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_bh_size(bh: *mut BufferHead) -> usize {
    unsafe { (*(bh as *mut SimBufferHead)).data.len() }
}

// ============================================================================
// SUPERBLOCK ACCESSORS
// ============================================================================

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_get_sb_info(_sb: *mut SuperBlock) -> *mut c_void {
    sim_superblock() as *mut c_void
}

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_set_metadata(_sb: *mut SuperBlock, metadata: *mut c_void) {
    device().as_mut().expect("attached device").metadata = metadata as usize;
}

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_get_metadata(_sb: *mut SuperBlock) -> *mut c_void {
    match device().as_ref() {
        Some(dev) => dev.metadata as *mut c_void,
        None => std::ptr::null_mut(),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_set_archive_size(_sb: *mut SuperBlock, size: u64) {
    device().as_mut().expect("attached device").archive_size = size;
}

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_set_trailer_offset(_sb: *mut SuperBlock, offset: u64) {
    device().as_mut().expect("attached device").trailer_offset = offset;
}

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_set_root_ino(_sb: *mut SuperBlock, ino: u64) {
    device().as_mut().expect("attached device").root_ino = ino;
}

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_get_root_ino(_sb: *mut SuperBlock) -> u64 {
    device().as_ref().expect("attached device").root_ino
}

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_meta_lock(_sb: *mut SuperBlock) {
    assert_eq!(
        META_DEPTH.fetch_add(1, Ordering::SeqCst),
        0,
        "the metadata mutex is not reentrant"
    );
}

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_meta_unlock(_sb: *mut SuperBlock) {
    assert_eq!(
        META_DEPTH.fetch_sub(1, Ordering::SeqCst),
        1,
        "metadata mutex unlocked without a matching lock"
    );
}

// ============================================================================
// BLOCK DEVICE
// ============================================================================

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_sb_bdev(_sb: *mut SuperBlock) -> *mut BlockDevice {
    if device().is_some() {
        sim_superblock() as *mut BlockDevice
    } else {
        std::ptr::null_mut()
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_sb_blocksize(_sb: *mut SuperBlock) -> u32 {
    SIM_BLOCK_SIZE as u32
}

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_sb_blocksize_bits(_sb: *mut SuperBlock) -> u8 {
    SIM_BLOCK_SIZE.trailing_zeros() as u8
}

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_bdev_nr_bytes(_bdev: *mut BlockDevice) -> i64 {
    let guard = device();
    let dev = guard.as_ref().expect("attached device");
    (dev.image.len() as u64 / SIM_BLOCK_SIZE * SIM_BLOCK_SIZE) as i64
}

// ============================================================================
// DIRECTORY EMISSION
// ============================================================================

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_dir_emit(
    ctx: *mut DirContext,
    name: *const u8,
    namelen: i32,
    ino: u64,
    dtype: u32,
) -> bool {
    let ctx = unsafe { &mut *(ctx as *mut SimDirContext) };
    if ctx.entries.len() >= ctx.capacity {
        return false;
    }
    let bytes = unsafe { std::slice::from_raw_parts(name, namelen as usize) };
    ctx.entries.push((
        String::from_utf8(bytes.to_vec()).expect("directory entry name is UTF-8"),
        ino,
        dtype,
    ));
    true
}

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_dir_emit_dot(_file: *mut c_void, _ctx: *mut DirContext) -> bool {
    true
}

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_dir_emit_dotdot(_file: *mut c_void, _ctx: *mut DirContext) -> bool {
    true
}

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_dir_ctx_pos(ctx: *mut DirContext) -> i64 {
    unsafe { (*(ctx as *mut SimDirContext)).pos }
}

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_dir_ctx_set_pos(ctx: *mut DirContext, pos: i64) {
    unsafe {
        (*(ctx as *mut SimDirContext)).pos = pos;
    }
}

// ============================================================================
// ALLOCATION
// ============================================================================

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_kmalloc(size: usize, _flags: u32) -> *mut c_void {
    let buffer = vec![0u8; size].into_boxed_slice();
    let ptr = Box::into_raw(buffer) as *mut u8;
    allocations().insert(ptr as usize, size);
    ptr as *mut c_void
}

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_kzalloc(size: usize, flags: u32) -> *mut c_void {
    boxfs_kmalloc(size, flags)
}

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_kfree(ptr: *mut c_void) {
    if ptr.is_null() {
        return;
    }
    let size = allocations()
        .remove(&(ptr as usize))
        .expect("free of an unknown simulated allocation");
    drop(unsafe {
        Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr as *mut u8, size)) as Box<[u8]>
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_kvmalloc(size: usize, flags: u32) -> *mut c_void {
    boxfs_kmalloc(size, flags)
}

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_kvfree(ptr: *mut c_void) {
    boxfs_kfree(ptr);
}

// ============================================================================
// LOGGING
// ============================================================================

fn c_str(msg: *const u8) -> String {
    let mut len = 0;
    unsafe {
        while *msg.add(len) != 0 {
            len += 1;
        }
        String::from_utf8_lossy(std::slice::from_raw_parts(msg, len)).into_owned()
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_pr_info(msg: *const u8) {
    eprintln!("boxfs: {}", c_str(msg));
}

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_pr_err(msg: *const u8) {
    eprintln!("boxfs: {}", c_str(msg));
}

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_pr_warn(msg: *const u8) {
    eprintln!("boxfs: {}", c_str(msg));
}

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_pr_debug(msg: *const u8) {
    eprintln!("boxfs: {}", c_str(msg));
}

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_panic(msg: *const u8) -> ! {
    panic!("boxfs: rust panic: {}", c_str(msg));
}

// ============================================================================
// DECOMPRESSION
// ============================================================================

fn decompress_into(
    src: *const c_void,
    src_len: usize,
    dst: *mut c_void,
    dst_len: usize,
    out_len: *mut usize,
    compression: box_format::Compression,
) -> c_int {
    let input = unsafe { std::slice::from_raw_parts(src as *const u8, src_len) };
    let Ok(output) = box_format::decompress_bytes_sync(input, compression, None) else {
        return -5;
    };
    if output.len() > dst_len {
        return -74;
    }
    unsafe {
        std::slice::from_raw_parts_mut(dst as *mut u8, dst_len)[..output.len()]
            .copy_from_slice(&output);
        *out_len = output.len();
    }
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_zstd_decompress(
    src: *const c_void,
    src_len: usize,
    dst: *mut c_void,
    dst_len: usize,
    out_len: *mut usize,
) -> c_int {
    decompress_into(
        src,
        src_len,
        dst,
        dst_len,
        out_len,
        box_format::Compression::Zstd,
    )
}

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_xz_decompress(
    src: *const c_void,
    src_len: usize,
    dst: *mut c_void,
    dst_len: usize,
    out_len: *mut usize,
) -> c_int {
    decompress_into(
        src,
        src_len,
        dst,
        dst_len,
        out_len,
        box_format::Compression::Xz,
    )
}

// ============================================================================
// READAHEAD AND FOLIOS
// ============================================================================

/// Readahead is folio-driven and has no userspace analogue; the simulation
/// reports an empty window so `boxfs_rust_readahead` is a no-op.
#[unsafe(no_mangle)]
pub extern "C" fn boxfs_readahead_folio(_ractl: *mut ReadaheadControl) -> *mut Folio {
    std::ptr::null_mut()
}

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_readahead_pos(_ractl: *mut ReadaheadControl) -> i64 {
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_readahead_length(_ractl: *mut ReadaheadControl) -> usize {
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_folio_pos(_folio: *mut Folio) -> i64 {
    unreachable!("no folios are handed out by the simulation")
}

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_folio_size(_folio: *mut Folio) -> usize {
    unreachable!("no folios are handed out by the simulation")
}

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_kmap_local_folio(_folio: *mut Folio, _offset: usize) -> *mut c_void {
    unreachable!("no folios are handed out by the simulation")
}

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_kunmap_local(_addr: *mut c_void) {
    unreachable!("no folios are handed out by the simulation")
}

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_folio_mark_uptodate(_folio: *mut Folio) {
    unreachable!("no folios are handed out by the simulation")
}

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_folio_unlock(_folio: *mut Folio) {
    unreachable!("no folios are handed out by the simulation")
}

#[unsafe(no_mangle)]
pub extern "C" fn boxfs_folio_zero_segment(_folio: *mut Folio, _start: usize, _end: usize) {
    unreachable!("no folios are handed out by the simulation")
}

// ============================================================================
// ENTRY-POINT WRAPPERS
// ============================================================================

pub fn mount(image: Vec<u8>) -> c_int {
    attach(image);
    crate::boxfs_rust_fill_super(sim_superblock(), std::ptr::null_mut(), 0)
}

pub fn lookup(dir_ino: u64, name: &str) -> u64 {
    crate::boxfs_rust_lookup(
        sim_superblock(),
        dir_ino,
        name.as_ptr() as *const c_char,
        name.len(),
    )
}

pub fn getattr(ino: u64) -> Result<(u16, u64, u64), c_int> {
    let mut mode = 0u16;
    let mut size = 0u64;
    let mut blocks = 0u64;
    let ret = crate::boxfs_rust_getattr(sim_superblock(), ino, &mut mode, &mut size, &mut blocks);
    if ret < 0 {
        return Err(ret);
    }
    Ok((mode, size, blocks))
}

pub fn readdir(ino: u64, capacity: usize) -> Vec<(String, u64, u32)> {
    let mut ctx = SimDirContext::new(capacity);
    let mut all = Vec::new();
    loop {
        ctx.entries.clear();
        let before = ctx.pos;
        let ptr = ctx.as_ptr();
        let ret = crate::boxfs_rust_iterate_dir(sim_superblock(), ino, ptr);
        assert_eq!(ret, 0, "iterate_dir failed");
        all.extend(ctx.entries.iter().cloned());
        if ctx.pos == before {
            break;
        }
    }
    all
}

pub fn read_all(ino: u64, size: u64, chunk: usize) -> Result<Vec<u8>, c_int> {
    let mut out = Vec::new();
    let mut offset = 0u64;
    while offset < size {
        let mut buf = vec![0u8; chunk];
        let ret = crate::boxfs_rust_read(
            sim_superblock(),
            ino,
            buf.as_mut_ptr() as *mut c_char,
            buf.len(),
            offset as i64,
        );
        if ret < 0 {
            return Err(ret as c_int);
        }
        if ret == 0 {
            break;
        }
        out.extend_from_slice(&buf[..ret as usize]);
        offset += ret as u64;
    }
    Ok(out)
}

pub fn readlink(ino: u64) -> Result<String, c_int> {
    let mut buf = vec![0u8; 4096];
    let ret = crate::boxfs_rust_readlink(
        sim_superblock(),
        ino,
        buf.as_mut_ptr() as *mut c_char,
        buf.len(),
    );
    if ret < 0 {
        return Err(ret);
    }
    let end = buf.iter().position(|b| *b == 0).unwrap_or(buf.len());
    Ok(String::from_utf8(buf[..end].to_vec()).expect("symlink target is UTF-8"))
}

pub fn listxattr(ino: u64) -> Result<Vec<String>, isize> {
    let needed = crate::boxfs_rust_listxattr(sim_superblock(), ino, std::ptr::null_mut(), 0);
    if needed < 0 {
        return Err(needed);
    }
    if needed == 0 {
        return Ok(Vec::new());
    }
    let mut buf = vec![0u8; needed as usize];
    let ret = crate::boxfs_rust_listxattr(
        sim_superblock(),
        ino,
        buf.as_mut_ptr() as *mut c_char,
        buf.len(),
    );
    if ret < 0 {
        return Err(ret);
    }
    Ok(buf[..ret as usize]
        .split(|b| *b == 0)
        .filter(|name| !name.is_empty())
        .map(|name| String::from_utf8(name.to_vec()).expect("xattr name is UTF-8"))
        .collect())
}

pub fn getxattr(ino: u64, name: &str) -> Result<Vec<u8>, isize> {
    let cname = std::ffi::CString::new(name).expect("xattr name has no interior NUL");
    let needed = crate::boxfs_rust_getxattr(
        sim_superblock(),
        ino,
        cname.as_ptr(),
        std::ptr::null_mut(),
        0,
    );
    if needed < 0 {
        return Err(needed);
    }
    let mut buf = vec![0u8; needed as usize];
    let ret = crate::boxfs_rust_getxattr(
        sim_superblock(),
        ino,
        cname.as_ptr(),
        buf.as_mut_ptr() as *mut c_void,
        buf.len(),
    );
    if ret < 0 {
        return Err(ret);
    }
    buf.truncate(ret as usize);
    Ok(buf)
}
