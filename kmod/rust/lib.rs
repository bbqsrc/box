// SPDX-License-Identifier: GPL-2.0-only
//! boxfs - Rust implementation for Box archive filesystem
//!
//! This module provides the Rust implementation called from the C shim layer.
//! It handles parsing the box archive format and providing file/directory data.

#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::ffi::{c_char, c_int, c_void};
use core::slice;

mod bindings;
mod error;
mod metadata;
mod parser;

#[cfg(all(test, feature = "std"))]
mod archive_tests;
#[cfg(all(test, feature = "std"))]
mod kernel_sim;

use bindings::*;
use error::KernelError;
use metadata::{BoxfsMetadata, DEFAULT_BLOCK_CACHE_BYTES, RecordData};

// ============================================================================
// INODE <-> COMPOSITE INDEX CONVERSION
// ============================================================================

/// Convert a FUSE/kernel u64 inode to our internal u128 composite index.
/// Inode 0 is invalid; inode 1 maps to the reserved synthetic root composite.
/// Uses 48-bit packing: (archive_id << 48) | local_index, stored as ino - 1.
// [spec:box:req:kernel-vfs.root.namespace]
fn ino_to_composite(ino: u64) -> Option<u128> {
    if ino == 0 {
        None
    } else {
        let packed = ino - 1;
        let archive_id = (packed >> 48) as u64;
        let local_index = packed & 0xFFFF_FFFF_FFFF;
        Some(BoxfsMetadata::pack_index(archive_id, local_index))
    }
}

/// Convert a u128 composite index to a FUSE/kernel u64 inode.
/// Uses 48-bit packing to fit in u64.
// [spec:box:req:kernel-vfs.root.namespace]
fn composite_to_ino(composite: u128) -> u64 {
    let (archive_id, local_index) = BoxfsMetadata::unpack_index(composite);
    // Pack into u64: (archive_id << 48) | local_index + 1 (to avoid ino 0)
    ((archive_id << 48) | (local_index & 0xFFFF_FFFF_FFFF)) + 1
}

// ============================================================================
// PREFETCH HELPER
// ============================================================================

/// Prefetch memory for read.
/// Uses architecture-specific prefetch instructions to hint to the CPU
/// that we'll need this memory soon.
#[inline(always)]
#[allow(unused_variables)]
fn prefetch_read(ptr: *const u8) {
    #[cfg(target_arch = "aarch64")]
    unsafe {
        core::arch::asm!("prfm pldl1keep, [{ptr}]", ptr = in(reg) ptr, options(nostack, preserves_flags));
    }
    #[cfg(all(target_arch = "x86_64", target_feature = "sse"))]
    unsafe {
        core::arch::x86_64::_mm_prefetch(ptr as *const i8, core::arch::x86_64::_MM_HINT_T0);
    }
}

// ============================================================================
// C-ABI EXPORTS
// ============================================================================

/// Initialize boxfs from the given superblock.
///
/// Called from C during mount. Reads the archive trailer, parses metadata,
/// and stores it in the superblock's fs_info.
///
/// Returns 0 on success, negative errno on failure.
#[unsafe(no_mangle)]
// [spec:box:req:kernel-vfs.root]
// [spec:box:req:kernel-abi.root]
pub extern "C" fn boxfs_rust_fill_super(
    sb: *mut SuperBlock,
    _data: *mut c_void,
    _silent: c_int,
) -> c_int {
    match unsafe { fill_super_impl(sb) } {
        Ok(()) => 0,
        Err(e) => e.to_errno(),
    }
}

/// Clean up Rust-allocated metadata.
///
/// Called from C during unmount.
#[unsafe(no_mangle)]
// [spec:box:req:kernel-vfs.root.mount-lifecycle]
// [spec:box:req:kernel-abi.root]
// [spec:box:req:kernel-abi.root.ownership-and-errors]
pub extern "C" fn boxfs_rust_put_super(sb: *mut SuperBlock) {
    unsafe {
        let metadata = boxfs_get_metadata(sb);
        if !metadata.is_null() {
            // Drop the Box to free the metadata
            let _ = Box::from_raw(metadata as *mut BoxfsMetadata);
            boxfs_set_metadata(sb, core::ptr::null_mut());
        }
    }
}

/// Get filesystem statistics.
///
/// Returns 0 on success, negative errno on failure.
#[unsafe(no_mangle)]
// [spec:box:req:kernel-abi.root]
pub extern "C" fn boxfs_rust_statfs(sb: *mut SuperBlock, buf: *mut KStatfs) -> c_int {
    match unsafe { statfs_impl(sb, buf) } {
        Ok(()) => 0,
        Err(e) => e.to_errno(),
    }
}

/// Look up a name in a directory.
///
/// Returns the inode number if found, 0 if not found.
#[unsafe(no_mangle)]
// [spec:box:req:kernel-abi.root]
// [spec:box:req:kernel-abi.root.ownership-and-errors]
pub extern "C" fn boxfs_rust_lookup(
    sb: *mut SuperBlock,
    dir_ino: u64,
    name: *const c_char,
    name_len: usize,
) -> u64 {
    let name_slice = unsafe { slice::from_raw_parts(name as *const u8, name_len) };
    let name_str = match core::str::from_utf8(name_slice) {
        Ok(s) => s,
        Err(_) => return 0,
    };

    unsafe { lookup_impl(sb, dir_ino, name_str).unwrap_or(0) }
}

/// Iterate directory entries.
///
/// Calls the dir_emit function for each entry in the directory.
/// Returns 0 on success, negative errno on failure.
#[unsafe(no_mangle)]
// [spec:box:req:kernel-abi.root]
pub extern "C" fn boxfs_rust_iterate_dir(
    sb: *mut SuperBlock,
    dir_ino: u64,
    ctx: *mut DirContext,
) -> c_int {
    match unsafe { iterate_dir_impl(sb, dir_ino, ctx) } {
        Ok(()) => 0,
        Err(e) => e.to_errno(),
    }
}

/// Read file data into buffer.
///
/// Returns bytes read on success, negative errno on failure.
#[unsafe(no_mangle)]
// [spec:box:req:kernel-abi.root]
pub extern "C" fn boxfs_rust_read(
    sb: *mut SuperBlock,
    ino: u64,
    buf: *mut c_char,
    len: usize,
    offset: i64,
) -> isize {
    let buf_slice = unsafe { slice::from_raw_parts_mut(buf as *mut u8, len) };
    match unsafe { read_impl(sb, ino, buf_slice, offset as u64) } {
        Ok(n) => n as isize,
        Err(e) => e.to_errno() as isize,
    }
}

/// Get inode attributes.
///
/// Returns 0 on success, negative errno on failure.
#[unsafe(no_mangle)]
// [spec:box:req:kernel-abi.root]
pub extern "C" fn boxfs_rust_getattr(
    sb: *mut SuperBlock,
    ino: u64,
    mode: *mut u16,
    size: *mut u64,
    blocks: *mut u64,
) -> c_int {
    match unsafe { getattr_impl(sb, ino) } {
        Ok((m, s, b)) => {
            unsafe {
                *mode = m;
                *size = s;
                *blocks = b;
            }
            0
        }
        Err(e) => e.to_errno(),
    }
}

/// Read symlink target.
///
/// Returns 0 on success, negative errno on failure.
#[unsafe(no_mangle)]
// [spec:box:req:kernel-abi.root]
pub extern "C" fn boxfs_rust_readlink(
    sb: *mut SuperBlock,
    ino: u64,
    buf: *mut c_char,
    buflen: usize,
) -> c_int {
    let buf_slice = unsafe { slice::from_raw_parts_mut(buf as *mut u8, buflen) };
    match unsafe { readlink_impl(sb, ino, buf_slice) } {
        Ok(()) => 0,
        Err(e) => e.to_errno(),
    }
}

/// Readahead for sequential read optimization.
///
/// Fills multiple folios from a single decompression when possible.
#[unsafe(no_mangle)]
// [spec:box:req:kernel-abi.root]
pub extern "C" fn boxfs_rust_readahead(
    sb: *mut SuperBlock,
    ino: u64,
    ractl: *mut ReadaheadControl,
) {
    // Best-effort - errors are silently ignored (kernel will retry with read_folio)
    let _ = unsafe { readahead_impl(sb, ino, ractl) };
}

// ============================================================================
// HELPER: METADATA MUTATION LOCK
// ============================================================================

/// Holds the superblock's metadata mutex. The block cache uses `RefCell`, which
/// has no cross-CPU serialisation of its own, and page faults on one archive
/// run concurrently.
struct MetaGuard(*mut SuperBlock);

impl MetaGuard {
    unsafe fn new(sb: *mut SuperBlock) -> Self {
        unsafe {
            boxfs_meta_lock(sb);
            MetaGuard(sb)
        }
    }
}

impl Drop for MetaGuard {
    fn drop(&mut self) {
        unsafe { boxfs_meta_unlock(self.0) }
    }
}

// ============================================================================
// HELPER: GET METADATA
// ============================================================================

unsafe fn get_metadata(sb: *mut SuperBlock) -> Result<&'static BoxfsMetadata, KernelError> {
    unsafe {
        let metadata = boxfs_get_metadata(sb);
        if metadata.is_null() {
            return Err(KernelError::NoDevice);
        }
        Ok(&*(metadata as *const BoxfsMetadata))
    }
}

// ============================================================================
// IMPLEMENTATION: FILL SUPER
// ============================================================================

// [spec:box:req:kernel-vfs.root.mount-lifecycle]
// [spec:box:req:kernel-abi.root.ownership-and-errors]
unsafe fn fill_super_impl(sb: *mut SuperBlock) -> Result<(), KernelError> {
    unsafe {
        // Get block device
        let bdev = boxfs_sb_bdev(sb);
        if bdev.is_null() {
            return Err(KernelError::NoDevice);
        }
        let device_size = boxfs_bdev_nr_bytes(bdev) as u64;
        let block_size = boxfs_sb_blocksize(sb) as u64;

        // Read the header (first 32 bytes)
        let (bh, block_data) = read_block(sb, 0).ok_or(KernelError::Io)?;

        if block_data.len() < parser::HEADER_SIZE {
            release_block(bh);
            return Err(KernelError::BadData);
        }

        let header = parser::parse_header(block_data);
        release_block(bh);
        let header = header?;

        // Read the trailer
        let trailer_offset = header.trailer_offset;
        let trailer_size = device_size
            .checked_sub(trailer_offset)
            .ok_or(KernelError::BadData)?;
        let trailer_capacity = usize::try_from(trailer_size).map_err(|_| KernelError::NoMemory)?;

        // Read all trailer blocks into a buffer
        let mut trailer_data = Vec::with_capacity(trailer_capacity);
        let mut offset = trailer_offset;

        while offset < device_size {
            let block_num = offset / block_size;
            let block_offset = (offset % block_size) as usize;

            let (bh, block_data) = read_block(sb, block_num).ok_or(KernelError::Io)?;

            let start = block_offset;
            let end = core::cmp::min(block_data.len(), start + (device_size - offset) as usize);
            trailer_data.extend_from_slice(&block_data[start..end]);

            release_block(bh);
            offset += (end - start) as u64;
        }

        // Parse the trailer into archive data
        let mut archive = parser::parse_trailer(&trailer_data)?;
        archive.archive_size = device_size;
        archive.data_offset_base = 0; // First archive starts at device offset 0

        // Create metadata and add the first archive
        let mut metadata = BoxfsMetadata::empty();
        // Set block cache capacity
        *metadata.block_cache.borrow_mut() = metadata::BlockCache::new(DEFAULT_BLOCK_CACHE_BYTES);
        let _archive_id = metadata.add_archive(archive);

        // Store metadata
        let meta_ptr = Box::into_raw(Box::new(metadata));
        boxfs_set_metadata(sb, meta_ptr as *mut c_void);
        boxfs_set_archive_size(sb, device_size);
        boxfs_set_trailer_offset(sb, trailer_offset);

        // Set root inode (use the root_index from metadata - now a composite index)
        let root_composite = (*(meta_ptr)).root_index();
        let root_ino = composite_to_ino(root_composite);
        boxfs_set_root_ino(sb, root_ino);

        Ok(())
    }
}

// ============================================================================
// IMPLEMENTATION: STATFS
// ============================================================================

unsafe fn statfs_impl(sb: *mut SuperBlock, buf: *mut KStatfs) -> Result<(), KernelError> {
    unsafe {
        let metadata = get_metadata(sb)?;

        // Calculate block counts (sum all archive sizes)
        let block_size = boxfs_sb_blocksize(sb) as u64;
        let total_size: u64 = metadata.archives.values().map(|a| a.archive_size).sum();
        let total_blocks = total_size / block_size;

        (*buf).f_type = BOXFS_MAGIC as i64;
        (*buf).f_bsize = block_size as i64;
        (*buf).f_blocks = total_blocks;
        (*buf).f_bfree = 0; // Read-only filesystem
        (*buf).f_bavail = 0;
        (*buf).f_files = metadata.record_count();
        (*buf).f_ffree = 0;
        (*buf).f_namelen = 255;

        Ok(())
    }
}

// ============================================================================
// IMPLEMENTATION: LOOKUP
// ============================================================================

// [spec:box:req:kernel-vfs.root.namespace]
unsafe fn lookup_impl(sb: *mut SuperBlock, dir_ino: u64, name: &str) -> Result<u64, KernelError> {
    unsafe {
        let metadata = get_metadata(sb)?;

        // Convert u64 inode to u128 composite for internal lookup
        let dir_composite = ino_to_composite(dir_ino).ok_or(KernelError::NotFound)?;
        let child_composite = metadata
            .find_child(dir_composite, name)
            .ok_or(KernelError::NotFound)?;
        // Convert back to u64 inode for return to kernel
        Ok(composite_to_ino(child_composite))
    }
}

// ============================================================================
// IMPLEMENTATION: ITERATE DIR
// ============================================================================

// [spec:box:req:kernel-vfs.root.namespace]
unsafe fn iterate_dir_impl(
    sb: *mut SuperBlock,
    dir_ino: u64,
    ctx: *mut DirContext,
) -> Result<(), KernelError> {
    unsafe {
        let metadata = get_metadata(sb)?;

        // Convert u64 inode to u128 composite
        let dir_composite = ino_to_composite(dir_ino).ok_or(KernelError::NotFound)?;

        // The C shim emits "." and ".." before calling in, so dir_context positions
        // 0 and 1 are already spent.
        const DOT_ENTRIES: i64 = 2;

        // Get current position
        let pos = boxfs_dir_ctx_pos(ctx);
        let start = usize::try_from(pos.saturating_sub(DOT_ENTRIES)).unwrap_or(0);

        // Get directory children
        let children = metadata.children(dir_composite);

        // Emit entries starting from pos
        for (i, (child_composite, record)) in children.iter().enumerate().skip(start) {
            let name_bytes = record.name.as_bytes();
            // Convert composite to u64 inode for dir_emit
            let child_ino = composite_to_ino(*child_composite);
            let emitted = boxfs_dir_emit(
                ctx,
                name_bytes.as_ptr(),
                name_bytes.len() as i32,
                child_ino,
                record.dtype() as u32,
            );

            if !emitted {
                // Buffer full, stop here
                break;
            }

            boxfs_dir_ctx_set_pos(ctx, i as i64 + 1 + DOT_ENTRIES);
        }

        Ok(())
    }
}

// ============================================================================
// HELPER: READ ARCHIVE RANGE
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CheckedRange {
    start: u64,
    end: u64,
    len: usize,
}

// [spec:box:req:kernel-vfs.root.data]
fn checked_subrange(
    envelope_start: u64,
    envelope_len: u64,
    start: u64,
    len: u64,
) -> Result<CheckedRange, KernelError> {
    let envelope_end = envelope_start
        .checked_add(envelope_len)
        .ok_or(KernelError::BadData)?;
    let end = start.checked_add(len).ok_or(KernelError::BadData)?;
    if start < envelope_start || end > envelope_end {
        return Err(KernelError::BadData);
    }
    Ok(CheckedRange {
        start,
        end,
        len: usize::try_from(len).map_err(|_| KernelError::BadData)?,
    })
}

fn checked_archive_range(
    archive_base: u64,
    archive_size: u64,
    relative_start: u64,
    len: u64,
) -> Result<CheckedRange, KernelError> {
    archive_base
        .checked_add(archive_size)
        .ok_or(KernelError::BadData)?;
    let relative = checked_subrange(0, archive_size, relative_start, len)?;
    Ok(CheckedRange {
        start: archive_base
            .checked_add(relative.start)
            .ok_or(KernelError::BadData)?,
        end: archive_base
            .checked_add(relative.end)
            .ok_or(KernelError::BadData)?,
        len: relative.len,
    })
}

fn checked_request_len(
    logical_size: u64,
    offset: u64,
    buffer_len: usize,
) -> Result<usize, KernelError> {
    if offset >= logical_size || buffer_len == 0 {
        return Ok(0);
    }
    let buffer_len = u64::try_from(buffer_len).map_err(|_| KernelError::BadData)?;
    let remaining = logical_size
        .checked_sub(offset)
        .ok_or(KernelError::BadData)?;
    usize::try_from(core::cmp::min(buffer_len, remaining)).map_err(|_| KernelError::BadData)
}

fn validate_stored_envelope(
    compressed_size: u64,
    decompressed_size: u64,
) -> Result<(), KernelError> {
    if compressed_size != decompressed_size {
        return Err(KernelError::BadData);
    }
    Ok(())
}

fn checked_output_range(
    offset: u64,
    len: usize,
    output_len: usize,
) -> Result<core::ops::Range<usize>, KernelError> {
    let start = usize::try_from(offset).map_err(|_| KernelError::BadData)?;
    let end = start.checked_add(len).ok_or(KernelError::BadData)?;
    if end > output_len {
        return Err(KernelError::BadData);
    }
    Ok(start..end)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BlockCopy {
    block_num: u64,
    start: usize,
    end: usize,
}

fn checked_block_copy(
    current_offset: u64,
    block_size: u64,
    block_data_len: usize,
    remaining: usize,
) -> Result<BlockCopy, KernelError> {
    if block_size == 0 || remaining == 0 {
        return Err(KernelError::BadData);
    }
    let block_num = current_offset / block_size;
    let block_offset =
        usize::try_from(current_offset % block_size).map_err(|_| KernelError::BadData)?;
    if block_offset >= block_data_len {
        return Err(KernelError::BadData);
    }
    let to_copy = core::cmp::min(block_data_len - block_offset, remaining);
    if to_copy == 0 {
        return Err(KernelError::BadData);
    }
    let end = block_offset
        .checked_add(to_copy)
        .filter(|end| *end <= block_data_len)
        .ok_or(KernelError::BadData)?;
    Ok(BlockCopy {
        block_num,
        start: block_offset,
        end,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CheckedChunkBlock {
    physical: CheckedRange,
    logical_end: u64,
    expected_output_len: usize,
    offset_in_block: usize,
    next: Option<(u64, u64)>,
}

fn checked_chunk_copy_len(
    block_len: usize,
    expected_block_len: usize,
    offset_in_block: usize,
    remaining: usize,
) -> Result<usize, KernelError> {
    if block_len != expected_block_len || offset_in_block >= block_len || remaining == 0 {
        return Err(KernelError::BadData);
    }
    let to_copy = core::cmp::min(block_len - offset_in_block, remaining);
    if to_copy == 0 {
        return Err(KernelError::BadData);
    }
    offset_in_block
        .checked_add(to_copy)
        .filter(|end| *end <= block_len)
        .ok_or(KernelError::BadData)?;
    Ok(to_copy)
}

fn checked_chunk_block(
    payload: CheckedRange,
    decompressed_size: u64,
    chunk_size: u64,
    current_offset: u64,
    block_logical: u64,
    block_physical: u64,
    next: Option<(u64, u64)>,
) -> Result<CheckedChunkBlock, KernelError> {
    if chunk_size == 0
        || current_offset >= decompressed_size
        || block_logical > current_offset
        || block_logical >= decompressed_size
    {
        return Err(KernelError::BadData);
    }

    let logical_end = block_logical
        .checked_add(chunk_size)
        .map_or(decompressed_size, |end| {
            core::cmp::min(end, decompressed_size)
        });
    if current_offset >= logical_end {
        return Err(KernelError::BadData);
    }

    let expected_output_len =
        usize::try_from(logical_end - block_logical).map_err(|_| KernelError::BadData)?;
    let offset_in_block =
        usize::try_from(current_offset - block_logical).map_err(|_| KernelError::BadData)?;
    if expected_output_len == 0 || offset_in_block >= expected_output_len {
        return Err(KernelError::BadData);
    }

    let physical_end = if logical_end < decompressed_size {
        let (next_logical, next_physical) = next.ok_or(KernelError::BadData)?;
        if next_logical != logical_end {
            return Err(KernelError::BadData);
        }
        next_physical
    } else {
        if next.is_some() {
            return Err(KernelError::BadData);
        }
        payload.end
    };
    if physical_end <= block_physical {
        return Err(KernelError::BadData);
    }
    let physical = checked_subrange(
        payload.start,
        payload.end - payload.start,
        block_physical,
        physical_end - block_physical,
    )?;

    Ok(CheckedChunkBlock {
        physical,
        logical_end,
        expected_output_len,
        offset_in_block,
        next,
    })
}

unsafe fn read_archive_into(
    sb: *mut SuperBlock,
    range: CheckedRange,
    output: &mut [u8],
    block_size: u64,
) -> Result<(), KernelError> {
    unsafe {
        if output.len() != range.len {
            return Err(KernelError::BadData);
        }
        let mut bytes_read = 0usize;
        while bytes_read < output.len() {
            let current_offset = range
                .start
                .checked_add(u64::try_from(bytes_read).map_err(|_| KernelError::BadData)?)
                .ok_or(KernelError::BadData)?;
            if current_offset >= range.end {
                return Err(KernelError::BadData);
            }
            if block_size == 0 {
                return Err(KernelError::BadData);
            }
            let block_num = current_offset / block_size;
            let (bh, block_data) = read_block(sb, block_num).ok_or(KernelError::Io)?;
            let copy = checked_block_copy(
                current_offset,
                block_size,
                block_data.len(),
                output.len() - bytes_read,
            );
            let copy = match copy {
                Ok(copy) => copy,
                Err(error) => {
                    release_block(bh);
                    return Err(error);
                }
            };
            if copy.block_num != block_num {
                release_block(bh);
                return Err(KernelError::BadData);
            }
            let copied = copy.end - copy.start;
            let output_end = match bytes_read.checked_add(copied) {
                Some(end) => end,
                None => {
                    release_block(bh);
                    return Err(KernelError::BadData);
                }
            };
            output[bytes_read..output_end].copy_from_slice(&block_data[copy.start..copy.end]);
            release_block(bh);
            bytes_read = output_end;
        }
        Ok(())
    }
}

/// Read a checked range of bytes from the archive into a Vec.
// [spec:box:req:kernel-abi.root.helpers-and-buffers]
unsafe fn read_archive_range(
    sb: *mut SuperBlock,
    range: CheckedRange,
    block_size: u64,
) -> Result<Vec<u8>, KernelError> {
    unsafe {
        let mut data = Vec::new();
        data.try_reserve_exact(range.len)
            .map_err(|_| KernelError::NoMemory)?;
        data.resize(range.len, 0);
        read_archive_into(sb, range, &mut data, block_size)?;
        Ok(data)
    }
}

// ============================================================================
// IMPLEMENTATION: READ
// ============================================================================

unsafe fn read_impl(
    sb: *mut SuperBlock,
    ino: u64,
    buf: &mut [u8],
    offset: u64,
) -> Result<usize, KernelError> {
    unsafe {
        let metadata = get_metadata(sb)?;
        let block_size = boxfs_sb_blocksize(sb) as u64;

        // Convert u64 inode to u128 composite
        let composite = ino_to_composite(ino).ok_or(KernelError::NotFound)?;
        let record = metadata.get(composite).ok_or(KernelError::NotFound)?;
        let archive = metadata
            .get_archive(composite)
            .ok_or(KernelError::BadData)?;

        match &record.data {
            RecordData::File {
                compression,
                data_offset,
                compressed_size,
                decompressed_size,
            } => read_file(
                sb,
                *compression,
                *data_offset,
                *compressed_size,
                *decompressed_size,
                buf,
                offset,
                block_size,
                archive.data_offset_base,
                archive.archive_size,
            ),
            RecordData::ChunkedFile {
                compression,
                block_size: chunk_size,
                data_offset,
                compressed_size,
                decompressed_size,
            } => read_chunked_file(
                sb,
                metadata,
                composite,
                *compression,
                *chunk_size,
                *data_offset,
                *compressed_size,
                *decompressed_size,
                buf,
                offset,
                block_size,
                archive.data_offset_base,
                archive.archive_size,
            ),
            _ => Err(KernelError::IsDir),
        }
    }
}

/// Read from a regular (non-chunked) file
// [spec:box:req:kernel-vfs.root.data]
// [spec:box:req:kernel-abi.root.helpers-and-buffers]
unsafe fn read_file(
    sb: *mut SuperBlock,
    compression: metadata::Compression,
    data_offset: u64,
    compressed_size: u64,
    decompressed_size: u64,
    buf: &mut [u8],
    offset: u64,
    block_size: u64,
    archive_base: u64,
    archive_size: u64,
) -> Result<usize, KernelError> {
    unsafe {
        let payload =
            checked_archive_range(archive_base, archive_size, data_offset, compressed_size)?;
        let to_read = checked_request_len(decompressed_size, offset, buf.len())?;

        match compression {
            metadata::Compression::Stored => {
                validate_stored_envelope(compressed_size, decompressed_size)?;
                if to_read == 0 {
                    return Ok(0);
                }
                let read_start = payload
                    .start
                    .checked_add(offset)
                    .ok_or(KernelError::BadData)?;
                let read_range = checked_subrange(
                    payload.start,
                    compressed_size,
                    read_start,
                    u64::try_from(to_read).map_err(|_| KernelError::BadData)?,
                )?;
                read_archive_into(sb, read_range, &mut buf[..to_read], block_size)?;
                Ok(to_read)
            }
            metadata::Compression::Zstd => decompress_and_read(
                sb,
                payload,
                decompressed_size,
                buf,
                offset,
                to_read,
                block_size,
                true,
            ),
            metadata::Compression::Xz => decompress_and_read(
                sb,
                payload,
                decompressed_size,
                buf,
                offset,
                to_read,
                block_size,
                false,
            ),
            metadata::Compression::Unknown(_) => Err(KernelError::Invalid),
        }
    }
}

/// Helper to decompress and read from a single compressed blob
// [spec:box:req:kernel-abi.root.helpers-and-buffers]
unsafe fn decompress_and_read(
    sb: *mut SuperBlock,
    payload: CheckedRange,
    decompressed_size: u64,
    buf: &mut [u8],
    offset: u64,
    to_read: usize,
    block_size: u64,
    use_zstd: bool,
) -> Result<usize, KernelError> {
    unsafe {
        if to_read == 0 {
            return Ok(0);
        }
        let decompressed_capacity =
            usize::try_from(decompressed_size).map_err(|_| KernelError::BadData)?;
        let output_range = checked_output_range(offset, to_read, decompressed_capacity)?;

        // Read compressed data from archive
        let compressed_data = read_archive_range(sb, payload, block_size)?;

        // Allocate decompression buffer
        let decomp_buf = boxfs_kvmalloc(decompressed_capacity, GFP_KERNEL);
        if decomp_buf.is_null() {
            return Err(KernelError::NoMemory);
        }

        // Decompress
        let mut out_len: usize = 0;
        let ret = if use_zstd {
            boxfs_zstd_decompress(
                compressed_data.as_ptr() as *const c_void,
                compressed_data.len(),
                decomp_buf,
                decompressed_capacity,
                &mut out_len,
            )
        } else {
            boxfs_xz_decompress(
                compressed_data.as_ptr() as *const c_void,
                compressed_data.len(),
                decomp_buf,
                decompressed_capacity,
                &mut out_len,
            )
        };

        if ret != 0 || out_len != decompressed_capacity || output_range.end > out_len {
            boxfs_kvfree(decomp_buf);
            return Err(if ret != 0 {
                KernelError::Io
            } else {
                KernelError::BadData
            });
        }

        // Copy requested range to output buffer
        let decomp_slice = core::slice::from_raw_parts(decomp_buf as *const u8, out_len);
        buf[..to_read].copy_from_slice(&decomp_slice[output_range]);

        boxfs_kvfree(decomp_buf);
        Ok(to_read)
    }
}

/// Read from a chunked file (multiple independently-compressed blocks)
// [spec:box:req:kernel-vfs.root.data]
// [spec:box:req:kernel-abi.root.helpers-and-buffers]
unsafe fn read_chunked_file(
    sb: *mut SuperBlock,
    metadata: &BoxfsMetadata,
    composite: u128,
    compression: metadata::Compression,
    chunk_size: u32,
    data_offset: u64,
    compressed_size: u64,
    decompressed_size: u64,
    buf: &mut [u8],
    offset: u64,
    dev_block_size: u64,
    archive_base: u64,
    archive_size: u64,
) -> Result<usize, KernelError> {
    unsafe {
        checked_archive_range(archive_base, archive_size, data_offset, compressed_size)?;
        let payload = checked_subrange(0, archive_size, data_offset, compressed_size)?;
        let to_read = checked_request_len(decompressed_size, offset, buf.len())?;
        if chunk_size == 0 {
            return Err(KernelError::BadData);
        }

        // For stored (uncompressed) chunked files, we can read directly
        if matches!(compression, metadata::Compression::Stored) {
            validate_stored_envelope(compressed_size, decompressed_size)?;
            if to_read == 0 {
                return Ok(0);
            }
            let relative_start = data_offset
                .checked_add(offset)
                .ok_or(KernelError::BadData)?;
            let relative_range = checked_subrange(
                payload.start,
                compressed_size,
                relative_start,
                u64::try_from(to_read).map_err(|_| KernelError::BadData)?,
            )?;
            let absolute_range = checked_archive_range(
                archive_base,
                archive_size,
                relative_range.start,
                relative_range.end - relative_range.start,
            )?;
            read_archive_into(sb, absolute_range, &mut buf[..to_read], dev_block_size)?;
            return Ok(to_read);
        }

        if to_read == 0 {
            return Ok(0);
        }
        if !matches!(
            compression,
            metadata::Compression::Zstd | metadata::Compression::Xz
        ) {
            return Err(KernelError::Invalid);
        }

        // For compressed chunked files, we need to find and decompress blocks
        let chunk_size = u64::from(chunk_size);
        let mut bytes_read = 0usize;
        let mut current_offset = offset;

        // Find the starting block
        let Some((mut block_physical, mut block_logical)) =
            metadata.find_block(composite, current_offset)
        else {
            return Err(KernelError::BadData);
        };

        while bytes_read < to_read {
            let next = metadata.next_block(composite, block_logical);
            let block = checked_chunk_block(
                payload,
                decompressed_size,
                chunk_size,
                current_offset,
                block_logical,
                block_physical,
                next,
            )?;
            let absolute_physical = checked_archive_range(
                archive_base,
                archive_size,
                block.physical.start,
                u64::try_from(block.physical.len).map_err(|_| KernelError::BadData)?,
            )?;

            // Check if this block is already in the cache
            let cached_copy = {
                let _guard = MetaGuard::new(sb);
                let mut cache = metadata.block_cache.borrow_mut();
                match cache.get(composite, block_logical) {
                    Some(cached_data) => {
                        let to_copy = checked_chunk_copy_len(
                            cached_data.len(),
                            block.expected_output_len,
                            block.offset_in_block,
                            to_read - bytes_read,
                        )?;
                        // Prefetch the source data into CPU cache before copying
                        prefetch_read(cached_data.as_ptr().wrapping_add(block.offset_in_block));

                        buf[bytes_read..bytes_read + to_copy].copy_from_slice(
                            &cached_data[block.offset_in_block..block.offset_in_block + to_copy],
                        );
                        Some(to_copy)
                    }
                    None => None,
                }
            };

            if let Some(to_copy) = cached_copy {
                bytes_read += to_copy;
                current_offset = current_offset
                    .checked_add(u64::try_from(to_copy).map_err(|_| KernelError::BadData)?)
                    .ok_or(KernelError::BadData)?;
            } else {
                // Read compressed block data
                let compressed_data = read_archive_range(sb, absolute_physical, dev_block_size)?;

                // Allocate decompression buffer for this block
                let decomp_buf = boxfs_kvmalloc(block.expected_output_len, GFP_KERNEL);
                if decomp_buf.is_null() {
                    return Err(KernelError::NoMemory);
                }

                // Decompress block
                let mut out_len: usize = 0;
                let ret = match compression {
                    metadata::Compression::Zstd => boxfs_zstd_decompress(
                        compressed_data.as_ptr() as *const c_void,
                        compressed_data.len(),
                        decomp_buf,
                        block.expected_output_len,
                        &mut out_len,
                    ),
                    metadata::Compression::Xz => boxfs_xz_decompress(
                        compressed_data.as_ptr() as *const c_void,
                        compressed_data.len(),
                        decomp_buf,
                        block.expected_output_len,
                        &mut out_len,
                    ),
                    _ => {
                        boxfs_kvfree(decomp_buf);
                        return Err(KernelError::Invalid);
                    }
                };

                if ret != 0 || out_len != block.expected_output_len {
                    boxfs_kvfree(decomp_buf);
                    return Err(if ret != 0 {
                        KernelError::Io
                    } else {
                        KernelError::BadData
                    });
                }

                // Copy decompressed data to a Box<[u8]> for caching
                let decomp_slice = core::slice::from_raw_parts(decomp_buf as *const u8, out_len);
                let block_data: Box<[u8]> = decomp_slice.into();
                boxfs_kvfree(decomp_buf);

                let to_copy = checked_chunk_copy_len(
                    block_data.len(),
                    block.expected_output_len,
                    block.offset_in_block,
                    to_read - bytes_read,
                )?;
                // Prefetch the source data into CPU cache before copying
                prefetch_read(block_data.as_ptr().wrapping_add(block.offset_in_block));

                buf[bytes_read..bytes_read + to_copy].copy_from_slice(
                    &block_data[block.offset_in_block..block.offset_in_block + to_copy],
                );

                bytes_read += to_copy;
                current_offset = current_offset
                    .checked_add(u64::try_from(to_copy).map_err(|_| KernelError::BadData)?)
                    .ok_or(KernelError::BadData)?;

                // Insert into cache
                let _guard = MetaGuard::new(sb);
                metadata
                    .block_cache
                    .borrow_mut()
                    .insert(composite, block_logical, block_data);
            }

            // Move to next block if needed
            if bytes_read < to_read {
                if current_offset != block.logical_end {
                    return Err(KernelError::BadData);
                }
                let (next_logical, next_physical) = block.next.ok_or(KernelError::BadData)?;
                block_logical = next_logical;
                block_physical = next_physical;
            }
        }

        if bytes_read != to_read {
            return Err(KernelError::BadData);
        }
        Ok(to_read)
    }
}

// ============================================================================
// IMPLEMENTATION: GETATTR
// ============================================================================

// [spec:box:req:kernel-vfs.root.namespace]
unsafe fn getattr_impl(sb: *mut SuperBlock, ino: u64) -> Result<(u16, u64, u64), KernelError> {
    unsafe {
        let metadata = get_metadata(sb)?;
        let block_size = boxfs_sb_blocksize(sb) as u64;

        // Convert u64 inode to u128 composite
        let composite = ino_to_composite(ino).ok_or(KernelError::NotFound)?;
        let record = metadata.get(composite).ok_or(KernelError::NotFound)?;

        // Tools that size their readlink buffer from st_size need the target
        // length, not the zero a link record carries as its data size.
        let size = match &record.data {
            RecordData::InternalLink { .. } | RecordData::ExternalLink { .. } => {
                link_target(metadata, composite, record)?.len() as u64
            }
            _ => record.size(),
        };
        let blocks = size.div_ceil(block_size);

        Ok((record.mode, size, blocks))
    }
}

// ============================================================================
// IMPLEMENTATION: READLINK
// ============================================================================

// [spec:box:req:kernel-vfs.root.links-and-xattrs]
fn link_target(
    metadata: &BoxfsMetadata,
    composite: u128,
    record: &metadata::Record,
) -> Result<alloc::string::String, KernelError> {
    match &record.data {
        RecordData::InternalLink { target_index } => {
            // For internal links, target_index is local to the same archive
            let (archive_id, _) = BoxfsMetadata::unpack_index(composite);
            let target_composite = BoxfsMetadata::pack_index(archive_id, *target_index);
            metadata
                .relative_link_target(composite, target_composite)
                .ok_or(KernelError::NotFound)
        }
        RecordData::ExternalLink { target } => Ok(target.clone()),
        _ => Err(KernelError::Invalid),
    }
}

// [spec:box:req:kernel-vfs.root.links-and-xattrs]
unsafe fn readlink_impl(sb: *mut SuperBlock, ino: u64, buf: &mut [u8]) -> Result<(), KernelError> {
    unsafe {
        let metadata = get_metadata(sb)?;

        // Convert u64 inode to u128 composite
        let composite = ino_to_composite(ino).ok_or(KernelError::NotFound)?;
        let record = metadata.get(composite).ok_or(KernelError::NotFound)?;

        let target = link_target(metadata, composite, record)?;

        let target_bytes = target.as_bytes();
        if target_bytes.len() >= buf.len() {
            return Err(KernelError::NameTooLong);
        }

        buf[..target_bytes.len()].copy_from_slice(target_bytes);
        buf[target_bytes.len()] = 0; // Null terminate

        Ok(())
    }
}

// ============================================================================
// IMPLEMENTATION: READAHEAD
// ============================================================================

/// Readahead implementation - fills multiple folios from cached/decompressed blocks.
// [spec:box:req:kernel-vfs.root.data]
// [spec:box:req:kernel-abi.root.helpers-and-buffers]
unsafe fn readahead_impl(
    sb: *mut SuperBlock,
    ino: u64,
    ractl: *mut ReadaheadControl,
) -> Result<(), KernelError> {
    unsafe {
        let metadata = get_metadata(sb)?;
        let dev_block_size = boxfs_sb_blocksize(sb) as u64;

        // Convert u64 inode to u128 composite
        let composite = ino_to_composite(ino).ok_or(KernelError::NotFound)?;
        let record = metadata.get(composite).ok_or(KernelError::NotFound)?;
        let archive = metadata
            .get_archive(composite)
            .ok_or(KernelError::BadData)?;

        // Get file info
        let (compression, chunk_size, data_offset, decompressed_size) = match &record.data {
            RecordData::File {
                compression,
                data_offset,
                decompressed_size,
                ..
            } => (*compression, 0u32, *data_offset, *decompressed_size),
            RecordData::ChunkedFile {
                compression,
                block_size,
                data_offset,
                decompressed_size,
                ..
            } => (*compression, *block_size, *data_offset, *decompressed_size),
            _ => return Err(KernelError::IsDir),
        };

        // Process each folio in the readahead window
        loop {
            let folio = boxfs_readahead_folio(ractl);
            if folio.is_null() {
                break;
            }

            let folio_offset = boxfs_folio_pos(folio) as u64;
            let folio_len = boxfs_folio_size(folio);

            // Map the folio for writing
            let buf_ptr = boxfs_kmap_local_folio(folio, 0);
            let buf = slice::from_raw_parts_mut(buf_ptr as *mut u8, folio_len);

            // Read data into the folio
            let result = if chunk_size > 0 {
                // Chunked file - use block cache
                let compressed_size = match &record.data {
                    RecordData::ChunkedFile {
                        compressed_size, ..
                    } => *compressed_size,
                    _ => 0,
                };
                read_chunked_file(
                    sb,
                    metadata,
                    composite,
                    compression,
                    chunk_size,
                    data_offset,
                    compressed_size,
                    decompressed_size,
                    buf,
                    folio_offset,
                    dev_block_size,
                    archive.data_offset_base,
                    archive.archive_size,
                )
            } else {
                // Regular file
                let compressed_size = match &record.data {
                    RecordData::File {
                        compressed_size, ..
                    } => *compressed_size,
                    _ => 0,
                };
                read_file(
                    sb,
                    compression,
                    data_offset,
                    compressed_size,
                    decompressed_size,
                    buf,
                    folio_offset,
                    dev_block_size,
                    archive.data_offset_base,
                    archive.archive_size,
                )
            };

            boxfs_kunmap_local(buf_ptr);

            match result {
                Ok(bytes_read) => {
                    // Zero any remaining part of the folio
                    if bytes_read < folio_len {
                        boxfs_folio_zero_segment(folio, bytes_read, folio_len);
                    }
                    boxfs_folio_mark_uptodate(folio);
                }
                Err(_) => {
                    // Error - just unlock, kernel will retry with read_folio
                }
            }

            boxfs_folio_unlock(folio);
        }

        Ok(())
    }
}

// ============================================================================
// IMPLEMENTATION: XATTR
// ============================================================================

/// Get extended attribute.
///
/// Returns the attribute size on success, negative errno on failure.
/// If buffer is provided (size > 0), copies the value into it.
/// If size is 0, just returns the attribute size.
#[unsafe(no_mangle)]
// [spec:box:req:kernel-abi.root]
pub extern "C" fn boxfs_rust_getxattr(
    sb: *mut SuperBlock,
    ino: u64,
    name: *const c_char,
    value: *mut c_void,
    size: usize,
) -> isize {
    // Convert name to &str
    let name_cstr = unsafe {
        // Find null terminator
        let mut len = 0;
        let mut ptr = name;
        while *ptr != 0 {
            len += 1;
            ptr = ptr.add(1);
        }
        core::slice::from_raw_parts(name as *const u8, len)
    };

    let name_str = match core::str::from_utf8(name_cstr) {
        Ok(s) => s,
        Err(_) => return KernelError::Invalid.to_ssize(),
    };

    match unsafe { getxattr_impl(sb, ino, name_str, value, size) } {
        Ok(n) => n as isize,
        Err(e) => e.to_ssize(),
    }
}

// [spec:box:req:kernel-vfs.root.links-and-xattrs]
unsafe fn getxattr_impl(
    sb: *mut SuperBlock,
    ino: u64,
    name: &str,
    value: *mut c_void,
    size: usize,
) -> Result<usize, KernelError> {
    unsafe {
        let metadata = get_metadata(sb)?;

        // Convert u64 inode to u128 composite
        let composite = ino_to_composite(ino).ok_or(KernelError::NotFound)?;
        let record = metadata.get(composite).ok_or(KernelError::NotFound)?;

        // Get the xattr value
        let xattr_value = metadata
            .get_xattr(composite, record, name)
            .ok_or(KernelError::NoData)?;

        let attr_size = xattr_value.len();

        // If size is 0, just return the size needed
        if size == 0 {
            return Ok(attr_size);
        }

        // If buffer is too small, return error
        if size < attr_size {
            return Err(KernelError::Range);
        }

        // Copy value to buffer
        let buf = core::slice::from_raw_parts_mut(value as *mut u8, size);
        buf[..attr_size].copy_from_slice(xattr_value);

        Ok(attr_size)
    }
}

/// List extended attributes.
///
/// Returns the total size of attribute names on success, negative errno on failure.
/// Names are null-separated (e.g., "user.foo\0user.bar\0").
/// If size is 0, just returns the total size needed.
#[unsafe(no_mangle)]
// [spec:box:req:kernel-abi.root]
pub extern "C" fn boxfs_rust_listxattr(
    sb: *mut SuperBlock,
    ino: u64,
    list: *mut c_char,
    size: usize,
) -> isize {
    match unsafe { listxattr_impl(sb, ino, list, size) } {
        Ok(n) => n as isize,
        Err(e) => e.to_ssize(),
    }
}

// [spec:box:req:kernel-vfs.root.links-and-xattrs]
unsafe fn listxattr_impl(
    sb: *mut SuperBlock,
    ino: u64,
    list: *mut c_char,
    size: usize,
) -> Result<usize, KernelError> {
    unsafe {
        let metadata = get_metadata(sb)?;

        // Convert u64 inode to u128 composite
        let composite = ino_to_composite(ino).ok_or(KernelError::NotFound)?;
        let record = metadata.get(composite).ok_or(KernelError::NotFound)?;

        // Calculate total size needed
        let mut total_size = 0;
        for name in metadata.list_xattrs(composite, record) {
            total_size += name.len() + 1; // +1 for null terminator
        }

        // If size is 0, just return the size needed
        if size == 0 {
            return Ok(total_size);
        }

        // If buffer is too small, return error
        if size < total_size {
            return Err(KernelError::Range);
        }

        // Copy names to buffer
        let buf = core::slice::from_raw_parts_mut(list as *mut u8, size);
        let mut pos = 0;

        for name in metadata.list_xattrs(composite, record) {
            let name_bytes = name.as_bytes();
            buf[pos..pos + name_bytes.len()].copy_from_slice(name_bytes);
            pos += name_bytes.len();
            buf[pos] = 0; // Null terminator
            pos += 1;
        }

        Ok(total_size)
    }
}

#[cfg(test)]
mod read_range_tests {
    use super::*;
    use crate::metadata::SYNTHETIC_ROOT_INDEX;

    // [spec:box:req:kernel-vfs.root.namespace/test/unit]
    #[test]
    fn inode_one_maps_to_synthetic_root() {
        assert_eq!(ino_to_composite(1), Some(SYNTHETIC_ROOT_INDEX));
        assert_eq!(composite_to_ino(SYNTHETIC_ROOT_INDEX), 1);
        assert_eq!(ino_to_composite(0), None);
    }

    // [spec:box:req:kernel-vfs.root.data/test/unit]
    #[test]
    fn archive_and_stored_ranges_reject_overflow_and_escape() {
        assert_eq!(
            checked_archive_range(100, 50, 10, 20).unwrap(),
            CheckedRange {
                start: 110,
                end: 130,
                len: 20,
            }
        );
        assert!(matches!(
            checked_archive_range(u64::MAX, 1, 0, 0),
            Err(KernelError::BadData)
        ));
        assert!(matches!(
            checked_subrange(10, 5, 14, 2),
            Err(KernelError::BadData)
        ));
        assert!(matches!(
            checked_subrange(0, u64::MAX, u64::MAX, 1),
            Err(KernelError::BadData)
        ));
        assert!(validate_stored_envelope(64, 64).is_ok());
        assert!(matches!(
            validate_stored_envelope(63, 64),
            Err(KernelError::BadData)
        ));
    }

    // [spec:box:req:kernel-vfs.root.data/test/unit]
    #[test]
    fn device_block_windows_reject_non_progress() {
        assert_eq!(
            checked_block_copy(4100, 4096, 4096, 10).unwrap(),
            BlockCopy {
                block_num: 1,
                start: 4,
                end: 14,
            }
        );
        assert!(matches!(
            checked_block_copy(0, 0, 4096, 1),
            Err(KernelError::BadData)
        ));
        assert!(matches!(
            checked_block_copy(4, 4096, 4, 1),
            Err(KernelError::BadData)
        ));
        assert!(matches!(
            checked_block_copy(0, 4096, 4096, 0),
            Err(KernelError::BadData)
        ));
    }

    // [spec:box:req:kernel-vfs.root.data/test/unit]
    #[test]
    fn chunk_ranges_reject_gaps_descents_and_escape() {
        let payload = CheckedRange {
            start: 100,
            end: 140,
            len: 40,
        };
        let valid = checked_chunk_block(payload, 16, 8, 3, 0, 100, Some((8, 120)))
            .expect("valid first chunk");
        assert_eq!(valid.logical_end, 8);
        assert_eq!(valid.offset_in_block, 3);
        assert_eq!(valid.expected_output_len, 8);
        assert_eq!(valid.physical.start, 100);
        assert_eq!(valid.physical.end, 120);

        for hostile in [
            checked_chunk_block(payload, 16, 0, 3, 0, 100, Some((8, 120))),
            checked_chunk_block(payload, 16, 8, 9, 0, 100, Some((8, 120))),
            checked_chunk_block(payload, 16, 8, 3, 4, 100, Some((12, 120))),
            checked_chunk_block(payload, 16, 8, 3, 0, 100, None),
            checked_chunk_block(payload, 16, 8, 3, 0, 100, Some((9, 120))),
            checked_chunk_block(payload, 16, 8, 3, 0, 100, Some((8, 100))),
            checked_chunk_block(payload, 16, 8, 3, 0, 99, Some((8, 120))),
            checked_chunk_block(payload, 16, 8, 3, 0, 100, Some((8, 141))),
            checked_chunk_block(payload, 16, 8, 9, 8, 120, Some((16, 130))),
        ] {
            assert!(matches!(hostile, Err(KernelError::BadData)));
        }
    }

    // [spec:box:req:kernel-vfs.root.data/test/unit]
    #[test]
    fn decoder_cache_lengths_match_logical_span() {
        assert_eq!(checked_output_range(4, 4, 8).unwrap(), 4..8);
        assert!(matches!(
            checked_output_range(4, 5, 8),
            Err(KernelError::BadData)
        ));
        assert_eq!(checked_chunk_copy_len(8, 8, 3, 8).unwrap(), 5);
        assert!(matches!(
            checked_chunk_copy_len(7, 8, 3, 8),
            Err(KernelError::BadData)
        ));
        assert!(matches!(
            checked_chunk_copy_len(8, 8, 8, 1),
            Err(KernelError::BadData)
        ));
        assert!(matches!(
            checked_chunk_copy_len(8, 8, 3, 0),
            Err(KernelError::BadData)
        ));
    }
}

// ============================================================================
// PANIC HANDLER (required for no_std in kernel)
// ============================================================================

/// Fixed-size sink for rendering panic text without allocating.
#[cfg(all(not(test), not(feature = "std")))]
struct PanicMessage {
    bytes: [u8; 240],
    len: usize,
}

#[cfg(all(not(test), not(feature = "std")))]
impl core::fmt::Write for PanicMessage {
    fn write_str(&mut self, s: &str) -> core::fmt::Result {
        let room = self.bytes.len() - 1 - self.len;
        let take = core::cmp::min(room, s.len());
        self.bytes[self.len..self.len + take].copy_from_slice(&s.as_bytes()[..take]);
        self.len += take;
        Ok(())
    }
}

// Only define panic handler when building for kernel (no std available)
#[cfg(all(not(test), not(feature = "std")))]
#[panic_handler]
fn panic(info: &core::panic::PanicInfo) -> ! {
    use core::fmt::Write;

    let mut message = PanicMessage {
        bytes: [0; 240],
        len: 0,
    };
    let _ = write!(message, "{}", info);
    unsafe { boxfs_panic(message.bytes.as_ptr()) }
}

// ============================================================================
// ALLOCATOR (required for alloc crate in kernel)
// ============================================================================

// Only define allocator when building for kernel
#[cfg(all(not(test), not(feature = "std")))]
#[global_allocator]
static ALLOCATOR: KernelAllocator = KernelAllocator;

#[cfg(all(not(test), not(feature = "std")))]
struct KernelAllocator;

/// kmalloc only guarantees natural alignment for power-of-two requests, and
/// kvmalloc falls back to page-aligned vmalloc above that.
#[cfg(all(not(test), not(feature = "std")))]
fn kernel_alloc_size(layout: core::alloc::Layout) -> usize {
    let layout = layout.pad_to_align();
    if layout.align() > 8 {
        layout.size().next_power_of_two()
    } else {
        layout.size()
    }
}

#[cfg(all(not(test), not(feature = "std")))]
unsafe impl core::alloc::GlobalAlloc for KernelAllocator {
    unsafe fn alloc(&self, layout: core::alloc::Layout) -> *mut u8 {
        unsafe { boxfs_kvmalloc(kernel_alloc_size(layout), GFP_KERNEL) as *mut u8 }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, _layout: core::alloc::Layout) {
        unsafe { boxfs_kvfree(ptr as *mut c_void) };
    }
}
