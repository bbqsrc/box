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

use bindings::*;
use error::KernelError;
use metadata::{BoxfsMetadata, RecordData, DEFAULT_BLOCK_CACHE_BYTES};

// ============================================================================
// INODE <-> COMPOSITE INDEX CONVERSION
// ============================================================================

/// Convert a FUSE/kernel u64 inode to our internal u128 composite index.
/// Inodes 0 and 1 are special (root), others are packed composites.
/// Uses 48-bit packing: (archive_id << 48) | local_index, stored as ino - 1.
fn ino_to_composite(ino: u64) -> Option<u128> {
    if ino <= 1 {
        None // Special inodes, handle separately
    } else {
        let packed = ino - 1;
        let archive_id = (packed >> 48) as u64;
        let local_index = packed & 0xFFFF_FFFF_FFFF;
        Some(BoxfsMetadata::pack_index(archive_id, local_index))
    }
}

/// Convert a u128 composite index to a FUSE/kernel u64 inode.
/// Uses 48-bit packing to fit in u64.
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
#[no_mangle]
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
#[no_mangle]
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
#[no_mangle]
pub extern "C" fn boxfs_rust_statfs(
    sb: *mut SuperBlock,
    buf: *mut KStatfs,
) -> c_int {
    match unsafe { statfs_impl(sb, buf) } {
        Ok(()) => 0,
        Err(e) => e.to_errno(),
    }
}

/// Look up a name in a directory.
///
/// Returns the inode number if found, 0 if not found.
#[no_mangle]
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
#[no_mangle]
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
#[no_mangle]
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
#[no_mangle]
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
#[no_mangle]
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
#[no_mangle]
pub extern "C" fn boxfs_rust_readahead(
    sb: *mut SuperBlock,
    ino: u64,
    ractl: *mut ReadaheadControl,
) {
    // Best-effort - errors are silently ignored (kernel will retry with read_folio)
    let _ = unsafe { readahead_impl(sb, ino, ractl) };
}

// ============================================================================
// HELPER: GET METADATA
// ============================================================================

unsafe fn get_metadata(sb: *mut SuperBlock) -> Result<&'static BoxfsMetadata, KernelError> {
    let metadata = boxfs_get_metadata(sb);
    if metadata.is_null() {
        return Err(KernelError::NoDevice);
    }
    Ok(&*(metadata as *const BoxfsMetadata))
}

// ============================================================================
// IMPLEMENTATION: FILL SUPER
// ============================================================================

unsafe fn fill_super_impl(sb: *mut SuperBlock) -> Result<(), KernelError> {
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

    let header = parser::parse_header(block_data)?;
    release_block(bh);

    // Read the trailer
    let trailer_offset = header.trailer_offset;
    let trailer_size = device_size - trailer_offset;

    // Read all trailer blocks into a buffer
    let mut trailer_data = Vec::with_capacity(trailer_size as usize);
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

// ============================================================================
// IMPLEMENTATION: STATFS
// ============================================================================

unsafe fn statfs_impl(sb: *mut SuperBlock, buf: *mut KStatfs) -> Result<(), KernelError> {
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

// ============================================================================
// IMPLEMENTATION: LOOKUP
// ============================================================================

unsafe fn lookup_impl(
    sb: *mut SuperBlock,
    dir_ino: u64,
    name: &str,
) -> Result<u64, KernelError> {
    let metadata = get_metadata(sb)?;

    // Convert u64 inode to u128 composite for internal lookup
    let dir_composite = ino_to_composite(dir_ino).ok_or(KernelError::NotFound)?;
    let child_composite = metadata.find_child(dir_composite, name).ok_or(KernelError::NotFound)?;
    // Convert back to u64 inode for return to kernel
    Ok(composite_to_ino(child_composite))
}

// ============================================================================
// IMPLEMENTATION: ITERATE DIR
// ============================================================================

unsafe fn iterate_dir_impl(
    sb: *mut SuperBlock,
    dir_ino: u64,
    ctx: *mut DirContext,
) -> Result<(), KernelError> {
    let metadata = get_metadata(sb)?;

    // Convert u64 inode to u128 composite
    let dir_composite = ino_to_composite(dir_ino).ok_or(KernelError::NotFound)?;

    // Get current position
    let pos = boxfs_dir_ctx_pos(ctx);

    // Get directory children
    let children = metadata.children(dir_composite);

    // Emit entries starting from pos
    for (i, (child_composite, record)) in children.iter().enumerate() {
        if (i as i64) < pos {
            continue;
        }

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

        boxfs_dir_ctx_set_pos(ctx, (i + 1) as i64);
    }

    Ok(())
}

// ============================================================================
// HELPER: READ ARCHIVE RANGE
// ============================================================================

/// Read a range of bytes from the archive into a Vec.
unsafe fn read_archive_range(
    sb: *mut SuperBlock,
    offset: u64,
    size: usize,
    block_size: u64,
) -> Result<Vec<u8>, KernelError> {
    let mut data = Vec::with_capacity(size);
    let mut bytes_read = 0;

    while bytes_read < size {
        let current_offset = offset + bytes_read as u64;
        let block_num = current_offset / block_size;
        let block_offset = (current_offset % block_size) as usize;

        let (bh, block_data) = read_block(sb, block_num).ok_or(KernelError::Io)?;

        let available = block_data.len() - block_offset;
        let to_copy = core::cmp::min(available, size - bytes_read);

        data.extend_from_slice(&block_data[block_offset..block_offset + to_copy]);

        release_block(bh);
        bytes_read += to_copy;
    }

    Ok(data)
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
    let metadata = get_metadata(sb)?;
    let block_size = boxfs_sb_blocksize(sb) as u64;

    // Convert u64 inode to u128 composite
    let composite = ino_to_composite(ino).ok_or(KernelError::NotFound)?;
    let record = metadata.get(composite).ok_or(KernelError::NotFound)?;

    match &record.data {
        RecordData::File { compression, data_offset, compressed_size, decompressed_size } => {
            read_file(sb, *compression, *data_offset, *compressed_size, *decompressed_size, buf, offset, block_size)
        }
        RecordData::ChunkedFile { compression, block_size: chunk_size, data_offset, compressed_size, decompressed_size } => {
            read_chunked_file(sb, metadata, composite, *compression, *chunk_size, *data_offset, *compressed_size, *decompressed_size, buf, offset, block_size)
        }
        _ => Err(KernelError::IsDir),
    }
}

/// Read from a regular (non-chunked) file
unsafe fn read_file(
    sb: *mut SuperBlock,
    compression: metadata::Compression,
    data_offset: u64,
    compressed_size: u64,
    decompressed_size: u64,
    buf: &mut [u8],
    offset: u64,
    block_size: u64,
) -> Result<usize, KernelError> {
    // Check if offset is past end of file
    if offset >= decompressed_size {
        return Ok(0);
    }

    // Calculate how much to read
    let remaining = decompressed_size - offset;
    let to_read = core::cmp::min(buf.len() as u64, remaining) as usize;

    match compression {
        metadata::Compression::Stored => {
            // Direct read from archive
            let read_offset = data_offset + offset;
            let mut bytes_read = 0;

            while bytes_read < to_read {
                let current_offset = read_offset + bytes_read as u64;
                let block_num = current_offset / block_size;
                let block_offset = (current_offset % block_size) as usize;

                let (bh, block_data) = read_block(sb, block_num).ok_or(KernelError::Io)?;

                let available = block_data.len() - block_offset;
                let to_copy = core::cmp::min(available, to_read - bytes_read);

                buf[bytes_read..bytes_read + to_copy]
                    .copy_from_slice(&block_data[block_offset..block_offset + to_copy]);

                release_block(bh);
                bytes_read += to_copy;
            }

            Ok(bytes_read)
        }
        metadata::Compression::Zstd => {
            decompress_and_read(sb, data_offset, compressed_size, decompressed_size, buf, offset, to_read, block_size, true)
        }
        metadata::Compression::Xz => {
            decompress_and_read(sb, data_offset, compressed_size, decompressed_size, buf, offset, to_read, block_size, false)
        }
        metadata::Compression::Unknown(_) => {
            Err(KernelError::Invalid)
        }
    }
}

/// Helper to decompress and read from a single compressed blob
unsafe fn decompress_and_read(
    sb: *mut SuperBlock,
    data_offset: u64,
    compressed_size: u64,
    decompressed_size: u64,
    buf: &mut [u8],
    offset: u64,
    to_read: usize,
    block_size: u64,
    use_zstd: bool,
) -> Result<usize, KernelError> {
    // Read compressed data from archive
    let compressed_data = read_archive_range(sb, data_offset, compressed_size as usize, block_size)?;

    // Allocate decompression buffer
    let decomp_buf = boxfs_kmalloc(decompressed_size as usize, GFP_KERNEL);
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
            decompressed_size as usize,
            &mut out_len,
        )
    } else {
        boxfs_xz_decompress(
            compressed_data.as_ptr() as *const c_void,
            compressed_data.len(),
            decomp_buf,
            decompressed_size as usize,
            &mut out_len,
        )
    };

    if ret != 0 {
        boxfs_kfree(decomp_buf);
        return Err(KernelError::Io);
    }

    // Copy requested range to output buffer
    let decomp_slice = core::slice::from_raw_parts(
        decomp_buf as *const u8,
        out_len,
    );
    buf[..to_read].copy_from_slice(&decomp_slice[offset as usize..offset as usize + to_read]);

    boxfs_kfree(decomp_buf);
    Ok(to_read)
}

/// Read from a chunked file (multiple independently-compressed blocks)
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
) -> Result<usize, KernelError> {
    // Check if offset is past end of file
    if offset >= decompressed_size {
        return Ok(0);
    }

    // Calculate how much to read
    let remaining = decompressed_size - offset;
    let to_read = core::cmp::min(buf.len() as u64, remaining) as usize;

    // For stored (uncompressed) chunked files, we can read directly
    if matches!(compression, metadata::Compression::Stored) {
        let read_offset = data_offset + offset;
        let mut bytes_read = 0;

        while bytes_read < to_read {
            let current_offset = read_offset + bytes_read as u64;
            let block_num = current_offset / dev_block_size;
            let block_offset = (current_offset % dev_block_size) as usize;

            let (bh, block_data) = read_block(sb, block_num).ok_or(KernelError::Io)?;

            let available = block_data.len() - block_offset;
            let to_copy = core::cmp::min(available, to_read - bytes_read);

            buf[bytes_read..bytes_read + to_copy]
                .copy_from_slice(&block_data[block_offset..block_offset + to_copy]);

            release_block(bh);
            bytes_read += to_copy;
        }

        return Ok(bytes_read);
    }

    // For compressed chunked files, we need to find and decompress blocks
    let chunk_size = chunk_size as u64;
    let mut bytes_read = 0;
    let mut current_offset = offset;

    // Find the starting block
    let Some((mut block_physical, mut block_logical)) = metadata.find_block(composite, current_offset) else {
        return Err(KernelError::Io);
    };

    while bytes_read < to_read {
        // Calculate what portion of this block we need
        let offset_in_block = (current_offset - block_logical) as usize;

        // Check if this block is already in the cache
        if let Some(cached_data) = metadata.block_cache.borrow_mut().get(composite, block_logical) {
            // Prefetch the source data into CPU cache before copying
            prefetch_read(cached_data.as_ptr().wrapping_add(offset_in_block));

            let available_in_block = cached_data.len() - offset_in_block;
            let to_copy = core::cmp::min(available_in_block, to_read - bytes_read);

            buf[bytes_read..bytes_read + to_copy]
                .copy_from_slice(&cached_data[offset_in_block..offset_in_block + to_copy]);

            bytes_read += to_copy;
            current_offset += to_copy as u64;
        } else {
            // Cache miss - need to decompress

            // Calculate compressed block size from next block's offset (or end of data)
            let compressed_end = if let Some((_, next_physical)) = metadata.next_block(composite, block_logical) {
                // Next block's physical offset tells us where this block ends
                next_physical
            } else {
                // Last block - ends at data_offset + compressed_size
                data_offset + compressed_size
            };

            let block_compressed_size = (compressed_end - block_physical) as usize;

            // Calculate decompressed block size (last block may be smaller)
            let block_decompressed_size = if block_logical + chunk_size > decompressed_size {
                (decompressed_size - block_logical) as usize
            } else {
                chunk_size as usize
            };

            // Read compressed block data
            let compressed_data = read_archive_range(sb, block_physical, block_compressed_size, dev_block_size)?;

            // Allocate decompression buffer for this block
            let decomp_buf = boxfs_kmalloc(block_decompressed_size, GFP_KERNEL);
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
                    block_decompressed_size,
                    &mut out_len,
                ),
                metadata::Compression::Xz => boxfs_xz_decompress(
                    compressed_data.as_ptr() as *const c_void,
                    compressed_data.len(),
                    decomp_buf,
                    block_decompressed_size,
                    &mut out_len,
                ),
                _ => {
                    boxfs_kfree(decomp_buf);
                    return Err(KernelError::Invalid);
                }
            };

            if ret != 0 {
                boxfs_kfree(decomp_buf);
                return Err(KernelError::Io);
            }

            // Copy decompressed data to a Box<[u8]> for caching
            let decomp_slice = core::slice::from_raw_parts(decomp_buf as *const u8, out_len);
            let block_data: Box<[u8]> = decomp_slice.into();
            boxfs_kfree(decomp_buf);

            // Prefetch the source data into CPU cache before copying
            prefetch_read(block_data.as_ptr().wrapping_add(offset_in_block));

            // Copy requested portion to output buffer
            let available_in_block = block_data.len() - offset_in_block;
            let to_copy = core::cmp::min(available_in_block, to_read - bytes_read);

            buf[bytes_read..bytes_read + to_copy]
                .copy_from_slice(&block_data[offset_in_block..offset_in_block + to_copy]);

            bytes_read += to_copy;
            current_offset += to_copy as u64;

            // Insert into cache
            metadata.block_cache.borrow_mut().insert(composite, block_logical, block_data);
        }

        // Move to next block if needed
        if bytes_read < to_read {
            if let Some((next_logical, next_physical)) = metadata.next_block(composite, block_logical) {
                block_logical = next_logical;
                block_physical = next_physical;
            } else {
                // No more blocks
                break;
            }
        }
    }

    Ok(bytes_read)
}

// ============================================================================
// IMPLEMENTATION: GETATTR
// ============================================================================

unsafe fn getattr_impl(
    sb: *mut SuperBlock,
    ino: u64,
) -> Result<(u16, u64, u64), KernelError> {
    let metadata = get_metadata(sb)?;
    let block_size = boxfs_sb_blocksize(sb) as u64;

    // Convert u64 inode to u128 composite
    let composite = ino_to_composite(ino).ok_or(KernelError::NotFound)?;
    let record = metadata.get(composite).ok_or(KernelError::NotFound)?;

    let size = record.size();
    let blocks = (size + block_size - 1) / block_size;

    Ok((record.mode, size, blocks))
}

// ============================================================================
// IMPLEMENTATION: READLINK
// ============================================================================

unsafe fn readlink_impl(
    sb: *mut SuperBlock,
    ino: u64,
    buf: &mut [u8],
) -> Result<(), KernelError> {
    let metadata = get_metadata(sb)?;

    // Convert u64 inode to u128 composite
    let composite = ino_to_composite(ino).ok_or(KernelError::NotFound)?;
    let record = metadata.get(composite).ok_or(KernelError::NotFound)?;

    let target = match &record.data {
        RecordData::InternalLink { target_index } => {
            // Resolve internal link to a path
            // For internal links, target_index is local to the same archive
            let (archive_id, _) = BoxfsMetadata::unpack_index(composite);
            let target_composite = BoxfsMetadata::pack_index(archive_id, *target_index);
            metadata.path_string_for_index(target_composite)
                .ok_or(KernelError::NotFound)?
        }
        RecordData::ExternalLink { target } => {
            target.clone()
        }
        _ => return Err(KernelError::Invalid),
    };

    let target_bytes = target.as_bytes();
    if target_bytes.len() >= buf.len() {
        return Err(KernelError::NameTooLong);
    }

    buf[..target_bytes.len()].copy_from_slice(target_bytes);
    buf[target_bytes.len()] = 0; // Null terminate

    Ok(())
}

// ============================================================================
// IMPLEMENTATION: READAHEAD
// ============================================================================

/// Readahead implementation - fills multiple folios from cached/decompressed blocks.
unsafe fn readahead_impl(
    sb: *mut SuperBlock,
    ino: u64,
    ractl: *mut ReadaheadControl,
) -> Result<(), KernelError> {
    let metadata = get_metadata(sb)?;
    let dev_block_size = boxfs_sb_blocksize(sb) as u64;

    // Convert u64 inode to u128 composite
    let composite = ino_to_composite(ino).ok_or(KernelError::NotFound)?;
    let record = metadata.get(composite).ok_or(KernelError::NotFound)?;

    // Get file info
    let (compression, chunk_size, data_offset, decompressed_size) = match &record.data {
        RecordData::File { compression, data_offset, decompressed_size, .. } => {
            (*compression, 0u32, *data_offset, *decompressed_size)
        }
        RecordData::ChunkedFile { compression, block_size, data_offset, decompressed_size, .. } => {
            (*compression, *block_size, *data_offset, *decompressed_size)
        }
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
                RecordData::ChunkedFile { compressed_size, .. } => *compressed_size,
                _ => 0,
            };
            read_chunked_file(
                sb, metadata, composite, compression, chunk_size,
                data_offset, compressed_size, decompressed_size,
                buf, folio_offset, dev_block_size,
            )
        } else {
            // Regular file
            let compressed_size = match &record.data {
                RecordData::File { compressed_size, .. } => *compressed_size,
                _ => 0,
            };
            read_file(
                sb, compression, data_offset, compressed_size,
                decompressed_size, buf, folio_offset, dev_block_size,
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

// ============================================================================
// IMPLEMENTATION: XATTR
// ============================================================================

/// Get extended attribute.
///
/// Returns the attribute size on success, negative errno on failure.
/// If buffer is provided (size > 0), copies the value into it.
/// If size is 0, just returns the attribute size.
#[no_mangle]
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
        Err(_) => return -(KernelError::Invalid.to_errno() as isize),
    };

    match unsafe { getxattr_impl(sb, ino, name_str, value, size) } {
        Ok(n) => n as isize,
        Err(e) => -(e.to_errno() as isize),
    }
}

unsafe fn getxattr_impl(
    sb: *mut SuperBlock,
    ino: u64,
    name: &str,
    value: *mut c_void,
    size: usize,
) -> Result<usize, KernelError> {
    let metadata = get_metadata(sb)?;

    // Convert u64 inode to u128 composite
    let composite = ino_to_composite(ino).ok_or(KernelError::NotFound)?;
    let record = metadata.get(composite).ok_or(KernelError::NotFound)?;

    // Get the xattr value
    let xattr_value = metadata.get_xattr(composite, record, name)
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

/// List extended attributes.
///
/// Returns the total size of attribute names on success, negative errno on failure.
/// Names are null-separated (e.g., "user.foo\0user.bar\0").
/// If size is 0, just returns the total size needed.
#[no_mangle]
pub extern "C" fn boxfs_rust_listxattr(
    sb: *mut SuperBlock,
    ino: u64,
    list: *mut c_char,
    size: usize,
) -> isize {
    match unsafe { listxattr_impl(sb, ino, list, size) } {
        Ok(n) => n as isize,
        Err(e) => -(e.to_errno() as isize),
    }
}

unsafe fn listxattr_impl(
    sb: *mut SuperBlock,
    ino: u64,
    list: *mut c_char,
    size: usize,
) -> Result<usize, KernelError> {
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

// ============================================================================
// PANIC HANDLER (required for no_std in kernel)
// ============================================================================

// Only define panic handler when building for kernel (no std available)
#[cfg(all(not(test), not(feature = "std")))]
#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    // In kernel context, we should never panic
    // If we do, just loop forever
    loop {
        core::hint::spin_loop();
    }
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

#[cfg(all(not(test), not(feature = "std")))]
unsafe impl core::alloc::GlobalAlloc for KernelAllocator {
    unsafe fn alloc(&self, layout: core::alloc::Layout) -> *mut u8 {
        boxfs_kmalloc(layout.size(), GFP_KERNEL) as *mut u8
    }

    unsafe fn dealloc(&self, ptr: *mut u8, _layout: core::alloc::Layout) {
        boxfs_kfree(ptr as *mut c_void);
    }
}
