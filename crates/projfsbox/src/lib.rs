//! projfsbox - Windows ProjFS driver for mounting .box archives as virtual filesystems.
//!
//! This crate provides a read-only filesystem provider using Windows Projected File System
//! (ProjFS) to expose `.box` archives as virtual directories.
//!
//! # Requirements
//!
//! - Windows 10 version 2004 or later (for projected symbolic links)
//! - ProjFS feature must be enabled:
//!   ```powershell
//!   Enable-WindowsOptionalFeature -Online -FeatureName Client-ProjFS -NoRestart
//!   ```

#![cfg(any(windows, test))]

#[cfg(windows)]
use std::path::Path;

#[cfg(windows)]
pub mod callbacks;
#[cfg(windows)]
pub mod enumeration;
#[cfg(windows)]
pub mod error;
mod file_data;
mod lifecycle;
pub mod path;
#[cfg(windows)]
pub mod provider;
pub mod time;

#[cfg(windows)]
pub use provider::BoxProvider;

/// Mark a directory as a ProjFS virtualization root.
///
/// This must be called once on an empty directory before starting virtualization.
/// The directory must exist.
#[cfg(windows)]
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
                if lifecycle::is_already_exists_hresult(e.code().0) {
                    Ok(())
                } else {
                    Err(e)
                }
            }
        }
    }
}
