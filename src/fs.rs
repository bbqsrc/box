//! Filesystem utilities for extracting metadata as archive attributes.

use crate::compat::HashMap;

use fastvint::Vi64;

use crate::BOX_EPOCH_UNIX;

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
            Vi64::new(created_minutes).bytes().to_vec(),
        );
        attrs.insert(
            "modified".into(),
            Vi64::new(modified_minutes).bytes().to_vec(),
        );
        attrs.insert(
            "accessed".into(),
            Vi64::new(accessed_minutes).bytes().to_vec(),
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
            fastvint::Vu32::new(mode).bytes().to_vec(),
        );
    }

    if ownership {
        attrs.insert(
            "unix.uid".into(),
            fastvint::Vu32::new(meta.uid()).bytes().to_vec(),
        );
        attrs.insert(
            "unix.gid".into(),
            fastvint::Vu32::new(meta.gid()).bytes().to_vec(),
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
        if let Ok(created) = meta.created()
            && let Ok(duration) = created.duration_since(std::time::SystemTime::UNIX_EPOCH)
        {
            let minutes = (duration.as_secs() as i64 - BOX_EPOCH_UNIX) / 60;
            attrs.insert("created".into(), Vi64::new(minutes).bytes().to_vec());
        }

        if let Ok(modified) = meta.modified()
            && let Ok(duration) = modified.duration_since(std::time::SystemTime::UNIX_EPOCH)
        {
            let minutes = (duration.as_secs() as i64 - BOX_EPOCH_UNIX) / 60;
            attrs.insert("modified".into(), Vi64::new(minutes).bytes().to_vec());
        }

        if let Ok(accessed) = meta.accessed()
            && let Ok(duration) = accessed.duration_since(std::time::SystemTime::UNIX_EPOCH)
        {
            let minutes = (duration.as_secs() as i64 - BOX_EPOCH_UNIX) / 60;
            attrs.insert("accessed".into(), Vi64::new(minutes).bytes().to_vec());
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

/// Read all extended attributes from a path.
/// Returns HashMap where keys are "linux.xattr.<name>" (e.g., "linux.xattr.user.myattr").
#[cfg(all(feature = "xattr", target_os = "linux"))]
pub fn read_xattrs(path: &std::path::Path) -> std::io::Result<HashMap<String, Vec<u8>>> {
    let mut attrs = HashMap::new();
    for name in xattr::list(path)? {
        if let Some(name_str) = name.to_str() {
            if let Some(value) = xattr::get(path, &name)? {
                attrs.insert(format!("linux.xattr.{}", name_str), value);
            }
        }
    }
    Ok(attrs)
}

#[cfg(not(all(feature = "xattr", target_os = "linux")))]
pub fn read_xattrs(_path: &std::path::Path) -> std::io::Result<HashMap<String, Vec<u8>>> {
    Ok(HashMap::new())
}

/// Write extended attributes to a path.
/// Expects keys in "linux.xattr.<name>" format.
#[cfg(all(feature = "xattr", target_os = "linux"))]
pub fn write_xattrs<'a>(path: &std::path::Path, attrs: impl Iterator<Item = (&'a str, &'a [u8])>) {
    for (key, value) in attrs {
        if let Some(name) = key.strip_prefix("linux.xattr.") {
            if let Err(e) = xattr::set(path, name, value) {
                tracing::warn!("Failed to set xattr {} on {}: {}", name, path.display(), e);
            }
        }
    }
}

#[cfg(not(all(feature = "xattr", target_os = "linux")))]
pub fn write_xattrs<'a>(
    _path: &std::path::Path,
    _attrs: impl Iterator<Item = (&'a str, &'a [u8])>,
) {
}
