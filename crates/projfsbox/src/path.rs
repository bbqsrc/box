//! Path conversion utilities between BoxPath and Windows paths.

use std::path::PathBuf;

use box_format::path::BoxPath;

/// Convert a Windows relative path (from ProjFS callback) to BoxPath.
///
/// Windows paths use `\` separators. Each callback component is validated
/// before constructing the archive path.
/// Empty paths return None (represents root directory).
pub fn windows_to_box_path(windows_path: &str) -> Option<BoxPath<'static>> {
    if windows_path.is_empty() {
        return None;
    }

    let mut native_path = PathBuf::new();
    for component in windows_path.split(['\\', '/']) {
        if component.is_empty() || component == "." || component == ".." {
            return None;
        }
        native_path.push(component);
    }

    BoxPath::new(native_path).ok()
}

/// Convert a BoxPath to Windows relative path string.
///
/// BoxPath uses `\x1f` separator, Windows uses `\`.
pub fn box_path_to_windows(box_path: &BoxPath<'_>) -> String {
    box_path.iter().collect::<Vec<_>>().join("\\")
}

/// Convert a filename to wide string (null-terminated UTF-16).
pub fn to_wide_string(s: &str) -> Vec<u16> {
    s.encode_utf16().chain(std::iter::once(0)).collect()
}

#[cfg(windows)]
use windows::core::PCWSTR;

/// Convert a PCWSTR to Rust String.
///
/// # Safety
/// The pointer must be valid and null-terminated.
#[cfg(windows)]
pub unsafe fn pcwstr_to_string(s: PCWSTR) -> String {
    if s.is_null() {
        return String::new();
    }
    // SAFETY: We're in an unsafe fn and caller guarantees validity
    unsafe {
        let ptr = s.0;
        let len = (0..).find(|&i| *ptr.add(i) == 0).unwrap_or(0);
        String::from_utf16_lossy(std::slice::from_raw_parts(ptr, len))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_windows_to_box_path_simple() {
        let result = windows_to_box_path("foo\\bar\\baz.txt");
        assert!(result.is_some());
        let bp = result.unwrap();
        assert_eq!(bp.iter().collect::<Vec<_>>(), ["foo", "bar", "baz.txt"]);
        assert_eq!(bp.filename(), "baz.txt");
    }

    #[test]
    fn windows_to_box_path_rejects_non_relative_parts() {
        assert!(windows_to_box_path("foo\\..\\bar").is_none());
        assert!(windows_to_box_path("foo\\\\bar").is_none());
        assert!(windows_to_box_path("\\foo").is_none());
    }

    #[test]
    fn test_windows_to_box_path_empty() {
        let result = windows_to_box_path("");
        assert!(result.is_none());
    }

    #[test]
    fn test_windows_to_box_path_single() {
        let result = windows_to_box_path("readme.txt");
        assert!(result.is_some());
        let bp = result.unwrap();
        assert_eq!(bp.filename(), "readme.txt");
    }

    #[test]
    fn test_box_path_to_windows() {
        let bp = BoxPath::new("foo/bar/baz.txt").unwrap();
        let win = box_path_to_windows(&bp);
        assert_eq!(win, "foo\\bar\\baz.txt");
    }

    #[test]
    fn test_to_wide_string() {
        let wide = to_wide_string("hello");
        assert_eq!(wide.len(), 6); // 5 chars + null
        assert_eq!(wide[5], 0);
    }
}
