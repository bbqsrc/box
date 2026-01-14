//! ProjFS callback implementations.
//!
//! These callbacks are invoked by Windows when the filesystem is accessed.
//! Each callback receives a `PRJ_CALLBACK_DATA` containing context and request info.

use box_format::{Record, attrs};
use fastvint::ReadVintExt;
use windows::Win32::Foundation::{
    ERROR_FILE_NOT_FOUND, ERROR_INSUFFICIENT_BUFFER, ERROR_NOT_ENOUGH_MEMORY,
};
use windows::Win32::Storage::ProjectedFileSystem::*;
use windows::core::{HRESULT, PCWSTR};

use crate::enumeration::{EnumerationEntry, file_attributes, matches_search_expression};
use crate::error::hresult;
use crate::path::{pcwstr_to_string, to_wide_string};
use crate::provider::BoxProvider;
use crate::time::{box_to_filetime, now_as_filetime};

/// Convert a GUID to u128 for use as a map key.
fn guid_to_u128(guid: &windows::core::GUID) -> u128 {
    let mut bytes = [0u8; 16];
    bytes[0..4].copy_from_slice(&guid.data1.to_le_bytes());
    bytes[4..6].copy_from_slice(&guid.data2.to_le_bytes());
    bytes[6..8].copy_from_slice(&guid.data3.to_le_bytes());
    bytes[8..16].copy_from_slice(&guid.data4);
    u128::from_le_bytes(bytes)
}

/// Get provider from callback data.
///
/// # Safety
/// Assumes InstanceContext is a valid pointer to BoxProvider.
unsafe fn get_provider<'a>(callback_data: &PRJ_CALLBACK_DATA) -> &'a BoxProvider {
    // SAFETY: Caller guarantees InstanceContext is valid BoxProvider pointer
    unsafe { &*(callback_data.InstanceContext as *const BoxProvider) }
}

/// Extract timestamp from a record's attribute.
fn get_record_timestamp(provider: &BoxProvider, record: &Record<'_>, attr: &str) -> i64 {
    record
        .attr(provider.reader.metadata(), attr)
        .and_then(|bytes| {
            let mut cursor = std::io::Cursor::new(bytes);
            cursor.read_vi64().ok()
        })
        .map(box_to_filetime)
        .unwrap_or_else(now_as_filetime)
}

/// Build EnumerationEntry from a record.
fn record_to_entry(
    provider: &BoxProvider,
    name: String,
    index: box_format::RecordIndex,
    record: &Record<'_>,
) -> EnumerationEntry {
    let (is_directory, file_size) = match record {
        Record::File(f) => (false, f.decompressed_length),
        Record::Directory(_) => (true, 0),
        Record::Link(_) => (false, 0), // Treat links as files for now
    };

    let creation_time = get_record_timestamp(provider, record, attrs::CREATED);
    let last_write_time =
        get_record_timestamp(provider, record, attrs::MODIFIED).max(creation_time);

    let file_attributes = if is_directory {
        file_attributes::FILE_ATTRIBUTE_DIRECTORY
    } else {
        file_attributes::FILE_ATTRIBUTE_ARCHIVE | file_attributes::FILE_ATTRIBUTE_READONLY
    };

    EnumerationEntry {
        name,
        index,
        is_directory,
        file_size,
        creation_time,
        last_write_time,
        file_attributes,
    }
}

/// PRJ_START_DIRECTORY_ENUMERATION_CB implementation.
///
/// Called when a directory enumeration is starting.
pub unsafe extern "system" fn start_directory_enumeration(
    callback_data: *const PRJ_CALLBACK_DATA,
    enumeration_id: *const windows::core::GUID,
) -> HRESULT {
    // SAFETY: ProjFS guarantees callback_data and enumeration_id are valid
    unsafe {
        let data = &*callback_data;
        let provider = get_provider(data);

        let path = pcwstr_to_string(data.FilePathName);
        tracing::debug!("start_directory_enumeration: {}", path);

        // Find directory record (empty path = root)
        let dir_index = if path.is_empty() {
            None
        } else {
            match provider.resolve_path(&path) {
                Some((idx, Record::Directory(_))) => Some(idx),
                Some(_) => {
                    tracing::warn!("Path is not a directory: {}", path);
                    return hresult::from_win32(ERROR_FILE_NOT_FOUND.0);
                }
                None => {
                    tracing::warn!("Directory not found: {}", path);
                    return hresult::from_win32(ERROR_FILE_NOT_FOUND.0);
                }
            }
        };

        let guid = guid_to_u128(&*enumeration_id);
        provider.create_enumeration(guid, dir_index);

        hresult::S_OK
    }
}

/// PRJ_GET_DIRECTORY_ENUMERATION_CB implementation.
///
/// Called to get directory entries. May be called multiple times if buffer fills.
pub unsafe extern "system" fn get_directory_enumeration(
    callback_data: *const PRJ_CALLBACK_DATA,
    enumeration_id: *const windows::core::GUID,
    search_expression: PCWSTR,
    dir_entry_buffer_handle: PRJ_DIR_ENTRY_BUFFER_HANDLE,
) -> HRESULT {
    // SAFETY: ProjFS guarantees all pointers are valid
    unsafe {
        let data = &*callback_data;
        let provider = get_provider(data);
        let guid = guid_to_u128(&*enumeration_id);

        let search = if search_expression.is_null() {
            None
        } else {
            Some(pcwstr_to_string(search_expression))
        };

        tracing::debug!(
            "get_directory_enumeration: guid={:x}, search={:?}, flags={:?}",
            guid,
            search,
            data.Flags
        );

        // Check for restart scan flag (PRJ_CB_DATA_FLAG_ENUM_RESTART_SCAN = 0x1)
        let restart = (data.Flags.0 & 0x1) != 0;

        let result = provider.get_enumeration_mut(guid, |session| {
            // Restart if requested
            if restart {
                session.restart();
            }

            // Populate entries if not done yet or on restart
            if !session.has_populated() || restart {
                session.set_search_expression(search.clone());

                // Get entries from archive
                let records = match session.dir_index {
                    Some(idx) => provider.dir_records(idx),
                    None => provider.root_records(),
                };

                let search_pattern = session.search_expression();
                let entries: Vec<EnumerationEntry> = records
                    .into_iter()
                    .map(|(idx, record)| {
                        let name = record.name().to_string();
                        record_to_entry(provider, name, idx, record)
                    })
                    .filter(|entry| {
                        search_pattern
                            .map(|s| matches_search_expression(&entry.name, s))
                            .unwrap_or(true)
                    })
                    .collect();

                session.populate(entries);
            }

            // Fill buffer with entries
            while let Some(entry) = session.current_entry() {
                let name_wide = to_wide_string(&entry.name);

                let file_info = PRJ_FILE_BASIC_INFO {
                    IsDirectory: entry.is_directory.into(),
                    FileSize: entry.file_size as i64,
                    CreationTime: entry.creation_time,
                    LastAccessTime: entry.creation_time,
                    LastWriteTime: entry.last_write_time,
                    ChangeTime: entry.last_write_time,
                    FileAttributes: entry.file_attributes,
                };

                let result = PrjFillDirEntryBuffer(
                    PCWSTR(name_wide.as_ptr()),
                    Some(&file_info),
                    dir_entry_buffer_handle,
                );

                match result {
                    Ok(()) => {
                        session.advance();
                    }
                    Err(e) => {
                        // Check for buffer full error
                        if e.code().0 == hresult::from_win32(ERROR_INSUFFICIENT_BUFFER.0).0 {
                            // Buffer full, will be called again
                            tracing::debug!("Buffer full, cursor at {}", session.entry_count());
                            return hresult::S_OK;
                        }
                        tracing::error!("PrjFillDirEntryBuffer failed: {:?}", e);
                        return HRESULT(e.code().0);
                    }
                }
            }

            hresult::S_OK
        });

        result.unwrap_or_else(|| {
            tracing::warn!("Enumeration session not found: {:x}", guid);
            hresult::from_win32(ERROR_FILE_NOT_FOUND.0)
        })
    }
}

/// PRJ_END_DIRECTORY_ENUMERATION_CB implementation.
///
/// Called when directory enumeration is complete.
pub unsafe extern "system" fn end_directory_enumeration(
    callback_data: *const PRJ_CALLBACK_DATA,
    enumeration_id: *const windows::core::GUID,
) -> HRESULT {
    // SAFETY: ProjFS guarantees pointers are valid
    unsafe {
        let data = &*callback_data;
        let provider = get_provider(data);
        let guid = guid_to_u128(&*enumeration_id);

        tracing::debug!("end_directory_enumeration: {:x}", guid);

        provider.remove_enumeration(guid);
        hresult::S_OK
    }
}

/// PRJ_GET_PLACEHOLDER_INFO_CB implementation.
///
/// Called to get file/directory metadata for placeholder creation.
pub unsafe extern "system" fn get_placeholder_info(
    callback_data: *const PRJ_CALLBACK_DATA,
) -> HRESULT {
    // SAFETY: ProjFS guarantees callback_data is valid
    unsafe {
        let data = &*callback_data;
        let provider = get_provider(data);

        let path = pcwstr_to_string(data.FilePathName);
        tracing::debug!("get_placeholder_info: {}", path);

        let (index, record) = match provider.resolve_path(&path) {
            Some(r) => r,
            None => {
                tracing::debug!("Path not found: {}", path);
                return hresult::from_win32(ERROR_FILE_NOT_FOUND.0);
            }
        };

        let entry = record_to_entry(provider, record.name().to_string(), index, record);

        let placeholder_info = PRJ_PLACEHOLDER_INFO {
            FileBasicInfo: PRJ_FILE_BASIC_INFO {
                IsDirectory: entry.is_directory.into(),
                FileSize: entry.file_size as i64,
                CreationTime: entry.creation_time,
                LastAccessTime: entry.creation_time,
                LastWriteTime: entry.last_write_time,
                ChangeTime: entry.last_write_time,
                FileAttributes: entry.file_attributes,
            },
            EaInformation: Default::default(),
            SecurityInformation: Default::default(),
            StreamsInformation: Default::default(),
            VersionInfo: Default::default(),
            VariableData: [0],
        };

        let path_wide = to_wide_string(&path);

        match PrjWritePlaceholderInfo(
            data.NamespaceVirtualizationContext,
            PCWSTR(path_wide.as_ptr()),
            &placeholder_info,
            std::mem::size_of::<PRJ_PLACEHOLDER_INFO>() as u32,
        ) {
            Ok(()) => hresult::S_OK,
            Err(e) => {
                tracing::error!("PrjWritePlaceholderInfo failed for {}: {:?}", path, e);
                HRESULT(e.code().0)
            }
        }
    }
}

/// PRJ_GET_FILE_DATA_CB implementation.
///
/// Called to get file contents when a file is read.
pub unsafe extern "system" fn get_file_data(
    callback_data: *const PRJ_CALLBACK_DATA,
    byte_offset: u64,
    length: u32,
) -> HRESULT {
    // SAFETY: ProjFS guarantees callback_data is valid
    unsafe {
        let data = &*callback_data;
        let provider = get_provider(data);

        let path = pcwstr_to_string(data.FilePathName);
        tracing::debug!(
            "get_file_data: {} offset={} length={}",
            path,
            byte_offset,
            length
        );

        let (index, record) = match provider.resolve_path(&path) {
            Some(r) => r,
            None => {
                tracing::warn!("File not found: {}", path);
                return hresult::from_win32(ERROR_FILE_NOT_FOUND.0);
            }
        };

        // Verify it's a file
        if record.as_file().is_none() {
            tracing::warn!("Not a file: {}", path);
            return hresult::from_win32(ERROR_FILE_NOT_FOUND.0);
        }

        // Get decompressed data
        let file_data = match provider.get_or_decompress(index) {
            Ok(d) => d,
            Err(e) => {
                tracing::error!("Decompression failed for {}: {}", path, e);
                return crate::error::io_error_to_hresult(&e);
            }
        };

        // Allocate aligned buffer
        let aligned_buffer =
            PrjAllocateAlignedBuffer(data.NamespaceVirtualizationContext, length as usize);

        if aligned_buffer.is_null() {
            tracing::error!("Failed to allocate aligned buffer");
            return hresult::from_win32(ERROR_NOT_ENOUGH_MEMORY.0);
        }

        // Copy requested range to aligned buffer
        let start = byte_offset as usize;
        let end = (start + length as usize).min(file_data.len());
        let copy_len = end.saturating_sub(start);

        if copy_len > 0 {
            std::ptr::copy_nonoverlapping(
                file_data[start..].as_ptr(),
                aligned_buffer as *mut u8,
                copy_len,
            );
        }

        // Write to ProjFS
        let result = PrjWriteFileData(
            data.NamespaceVirtualizationContext,
            &data.DataStreamId,
            aligned_buffer,
            byte_offset,
            copy_len as u32,
        );

        PrjFreeAlignedBuffer(aligned_buffer);

        match result {
            Ok(()) => hresult::S_OK,
            Err(e) => {
                tracing::error!("PrjWriteFileData failed for {}: {:?}", path, e);
                HRESULT(e.code().0)
            }
        }
    }
}

/// PRJ_QUERY_FILE_NAME_CB implementation.
///
/// Called to check if a file exists in the provider's namespace.
/// This is optional but improves performance.
pub unsafe extern "system" fn query_file_name(callback_data: *const PRJ_CALLBACK_DATA) -> HRESULT {
    // SAFETY: ProjFS guarantees callback_data is valid
    unsafe {
        let data = &*callback_data;
        let provider = get_provider(data);

        let path = pcwstr_to_string(data.FilePathName);
        tracing::debug!("query_file_name: {}", path);

        // Empty path = root, always exists
        if path.is_empty() {
            return hresult::S_OK;
        }

        match provider.resolve_path(&path) {
            Some(_) => hresult::S_OK,
            None => hresult::from_win32(ERROR_FILE_NOT_FOUND.0),
        }
    }
}
