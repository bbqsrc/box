//! Filesystem utilities for extracting metadata as archive attributes.

use std::collections::HashMap;

use fastvlq::Vi64;

/// Box epoch: 2020-01-01 00:00:00 UTC (seconds since Unix epoch)
pub const BOX_EPOCH_UNIX: i64 = 1577836800;

/// Default mode for files (0o644 = rw-r--r--)
pub const DEFAULT_FILE_MODE: u32 = 0o100644;
/// Default mode for directories (0o755 = rwxr-xr-x)
pub const DEFAULT_DIR_MODE: u32 = 0o40755;

#[cfg(unix)]
pub fn metadata_to_attrs(
    meta: &std::fs::Metadata,
    timestamps: bool,
    ownership: bool,
) -> HashMap<String, Vec<u8>> {
    use std::os::unix::fs::MetadataExt;

    let mut attrs = HashMap::new();

    if timestamps {
        let created_minutes = (meta.ctime() - BOX_EPOCH_UNIX) / 60;
        let modified_minutes = (meta.mtime() - BOX_EPOCH_UNIX) / 60;
        let accessed_minutes = (meta.atime() - BOX_EPOCH_UNIX) / 60;
        attrs.insert(
            "created".into(),
            Vi64::new(created_minutes).as_slice().to_vec(),
        );
        attrs.insert(
            "modified".into(),
            Vi64::new(modified_minutes).as_slice().to_vec(),
        );
        attrs.insert(
            "accessed".into(),
            Vi64::new(accessed_minutes).as_slice().to_vec(),
        );
    }

    // Only store mode if non-default
    let mode = meta.mode();
    let default_mode = if meta.is_dir() {
        DEFAULT_DIR_MODE
    } else {
        DEFAULT_FILE_MODE
    };
    if mode != default_mode {
        attrs.insert(
            "unix.mode".into(),
            fastvlq::Vu32::new(mode).as_slice().to_vec(),
        );
    }

    if ownership {
        attrs.insert(
            "unix.uid".into(),
            fastvlq::Vu32::new(meta.uid()).as_slice().to_vec(),
        );
        attrs.insert(
            "unix.gid".into(),
            fastvlq::Vu32::new(meta.gid()).as_slice().to_vec(),
        );
    }

    attrs
}

#[cfg(not(unix))]
pub fn metadata_to_attrs(
    meta: &std::fs::Metadata,
    timestamps: bool,
    _ownership: bool,
) -> HashMap<String, Vec<u8>> {
    let mut attrs = HashMap::new();

    if timestamps {
        if let Ok(created) = meta.created() {
            if let Ok(duration) = created.duration_since(std::time::SystemTime::UNIX_EPOCH) {
                let minutes = (duration.as_secs() as i64 - BOX_EPOCH_UNIX) / 60;
                attrs.insert("created".into(), Vi64::new(minutes).as_slice().to_vec());
            }
        }

        if let Ok(modified) = meta.modified() {
            if let Ok(duration) = modified.duration_since(std::time::SystemTime::UNIX_EPOCH) {
                let minutes = (duration.as_secs() as i64 - BOX_EPOCH_UNIX) / 60;
                attrs.insert("modified".into(), Vi64::new(minutes).as_slice().to_vec());
            }
        }

        if let Ok(accessed) = meta.accessed() {
            if let Ok(duration) = accessed.duration_since(std::time::SystemTime::UNIX_EPOCH) {
                let minutes = (duration.as_secs() as i64 - BOX_EPOCH_UNIX) / 60;
                attrs.insert("accessed".into(), Vi64::new(minutes).as_slice().to_vec());
            }
        }
    }

    // ownership is not applicable on Windows
    attrs
}

/// Check if a path is hidden.
///
/// On Unix: starts with '.'
/// On Windows: has the hidden file attribute
#[cfg(unix)]
pub fn is_hidden(path: &std::path::Path) -> bool {
    path.file_name()
        .and_then(|n| n.to_str())
        .map(|s| s.starts_with('.'))
        .unwrap_or(false)
}

#[cfg(windows)]
pub fn is_hidden(path: &std::path::Path) -> bool {
    use std::os::windows::fs::MetadataExt;
    const FILE_ATTRIBUTE_HIDDEN: u32 = 2;

    std::fs::metadata(path)
        .map(|m| (m.file_attributes() & FILE_ATTRIBUTE_HIDDEN) != 0)
        .unwrap_or(false)
}
