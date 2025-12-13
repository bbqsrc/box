//! Filesystem utilities for extracting metadata as archive attributes.

use std::collections::HashMap;

/// Extract platform-specific metadata as archive attributes.
///
/// On Unix: ctime, mtime, atime, mode, uid, gid
/// On Windows: created, modified, accessed
#[cfg(unix)]
pub fn metadata_to_attrs(meta: &std::fs::Metadata) -> HashMap<String, Vec<u8>> {
    use std::os::unix::fs::MetadataExt;

    let mut attrs = HashMap::new();

    attrs.insert("created".into(), meta.ctime().to_le_bytes().to_vec());
    attrs.insert("modified".into(), meta.mtime().to_le_bytes().to_vec());
    attrs.insert("accessed".into(), meta.atime().to_le_bytes().to_vec());
    attrs.insert("unix.mode".into(), meta.mode().to_le_bytes().to_vec());
    attrs.insert("unix.uid".into(), meta.uid().to_le_bytes().to_vec());
    attrs.insert("unix.gid".into(), meta.gid().to_le_bytes().to_vec());

    attrs
}

#[cfg(not(unix))]
pub fn metadata_to_attrs(meta: &std::fs::Metadata) -> HashMap<String, Vec<u8>> {
    let mut attrs = HashMap::new();

    if let Ok(created) = meta.created() {
        if let Ok(duration) = created.duration_since(std::time::SystemTime::UNIX_EPOCH) {
            attrs.insert("created".into(), duration.as_secs().to_le_bytes().to_vec());
        }
    }

    if let Ok(modified) = meta.modified() {
        if let Ok(duration) = modified.duration_since(std::time::SystemTime::UNIX_EPOCH) {
            attrs.insert("modified".into(), duration.as_secs().to_le_bytes().to_vec());
        }
    }

    if let Ok(accessed) = meta.accessed() {
        if let Ok(duration) = accessed.duration_since(std::time::SystemTime::UNIX_EPOCH) {
            attrs.insert("accessed".into(), duration.as_secs().to_le_bytes().to_vec());
        }
    }

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
