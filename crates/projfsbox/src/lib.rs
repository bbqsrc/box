//! projfsbox - Windows ProjFS driver for mounting .box archives as virtual filesystems.
//!
//! This crate provides a read-only filesystem provider using Windows Projected File System
//! (ProjFS) to expose `.box` archives as virtual directories.
//!
//! # Requirements
//!
//! - Windows 10 version 1809 or later
//! - ProjFS feature must be enabled:
//!   ```powershell
//!   Enable-WindowsOptionalFeature -Online -FeatureName Client-ProjFS -NoRestart
//!   ```

#![cfg(windows)]

use std::path::Path;

pub mod callbacks;
pub mod enumeration;
pub mod error;
pub mod path;
pub mod provider;
pub mod time;

pub use provider::BoxProvider;

/// Mark a directory as a ProjFS virtualization root.
///
/// This must be called once on an empty directory before starting virtualization.
/// The directory must exist.
pub fn mark_as_virtualization_root<P: AsRef<Path>>(path: P) -> Result<(), windows::core::Error> {
    use windows::Win32::Storage::ProjectedFileSystem::PrjMarkDirectoryAsPlaceholder;
    use windows::core::PCWSTR;

    let path_wide = crate::path::to_wide_string(&path.as_ref().to_string_lossy());

    unsafe {
        match PrjMarkDirectoryAsPlaceholder(
            PCWSTR(path_wide.as_ptr()),
            None,
            None,
            std::ptr::null(),
        ) {
            Ok(()) => Ok(()),
            Err(e) => {
                // Ignore ERROR_ALREADY_EXISTS (0x800700B7 = -2147024713)
                if e.code().0 == -2147024713 {
                    Ok(())
                } else {
                    Err(e)
                }
            }
        }
    }
}
