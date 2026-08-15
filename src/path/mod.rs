use crate::compat::Cow;
use core::fmt;
#[cfg(feature = "std")]
use std::path::{Path, PathBuf};

#[cfg(feature = "std")]
use unic_ucd::GeneralCategory;

mod error;

pub use self::error::IntoBoxPathError;

#[cfg(not(windows))]
/// The platform-specific separator as a string, used for splitting
/// platform-supplied paths and printing `BoxPath`s in the platform-preferred
/// manner.
pub const PATH_PLATFORM_SEP: &str = "/";

#[cfg(windows)]
/// The platform-specific separator as a string, used for splitting
/// platform-supplied paths and printing `BoxPath`s in the platform-preferred
/// manner.
pub const PATH_PLATFORM_SEP: &str = "\\";

/// The separator used in `BoxPath` type paths, used primarily in
/// `FileRecord` and `DirectoryRecord` fields.
pub const PATH_BOX_SEP: &str = "\x1f";

// [spec:box:def:paths.root.encoding]
#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct BoxPath<'a>(pub(crate) Cow<'a, str>);

// [spec:box:req:paths.root.normalization]
#[cfg(feature = "std")]
pub fn sanitize<P: AsRef<Path>>(path: P) -> Option<Vec<String>> {
    use std::path::Component;
    use unic_normal::StrNormalForm;

    let mut out = vec![];

    for component in path.as_ref().components() {
        match component {
            Component::CurDir | Component::RootDir | Component::Prefix(_) => {}
            Component::ParentDir => {
                out.pop();
            }
            Component::Normal(os_str) => out.push(
                os_str
                    .to_str()
                    .map(|x| x.trim())
                    .filter(|x| !x.is_empty())
                    .filter(|x| {
                        !x.chars().any(|c| {
                            let cat = GeneralCategory::of(c);
                            c == '\\'
                                || cat == GeneralCategory::Control
                                || (cat.is_separator() && c != ' ')
                        })
                    })
                    .map(|x| x.nfc().collect::<String>())?,
            ),
        }
    }

    Some(out)
}

/// Check if a character is allowed in a filename (used for both direct chars and decoded escapes).
#[cfg(feature = "std")]
fn is_char_allowed(c: char) -> bool {
    let cat = GeneralCategory::of(c);
    // Reject: backslash, control chars, separators (except space)
    !(c == '\\' || c == '/' || cat == GeneralCategory::Control || (cat.is_separator() && c != ' '))
}

/// Validate a path component that may contain `\xNN` escape sequences.
/// Returns true if the component is valid (all escapes decode to allowed chars,
/// and non-escape chars are also allowed).
#[cfg(feature = "std")]
fn validate_component_with_escapes(s: &str) -> bool {
    let bytes = s.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        if bytes[i] == b'\\' {
            // Check for \xNN pattern
            if i + 3 < bytes.len() && bytes[i + 1] == b'x' {
                let hex_str = &s[i + 2..i + 4];
                if let Ok(byte_val) = u8::from_str_radix(hex_str, 16) {
                    // Validate the decoded character is allowed
                    if byte_val < 0x80 {
                        // ASCII range - check directly
                        if !is_char_allowed(byte_val as char) {
                            return false;
                        }
                    }
                    // Non-ASCII escapes are allowed (they'd form part of UTF-8 sequences)
                    i += 4;
                    continue;
                }
            }
            // Invalid escape or bare backslash - reject
            return false;
        } else {
            // Regular character - validate it
            let c = s[i..].chars().next().unwrap();
            if !is_char_allowed(c) {
                return false;
            }
            i += c.len_utf8();
        }
    }

    true
}

/// Sanitize a path, allowing `\xNN` escape sequences.
/// Used when the archive has the allow_escapes flag set.
// [spec:box:req:paths.root.normalization]
#[cfg(feature = "std")]
pub fn sanitize_with_escapes<P: AsRef<Path>>(path: P) -> Option<Vec<String>> {
    use std::path::Component;
    use unic_normal::StrNormalForm;

    let mut out = vec![];

    for component in path.as_ref().components() {
        match component {
            Component::CurDir | Component::RootDir | Component::Prefix(_) => {}
            Component::ParentDir => {
                out.pop();
            }
            Component::Normal(os_str) => out.push(
                os_str
                    .to_str()
                    .map(|x| x.trim())
                    .filter(|x| !x.is_empty())
                    .filter(|x| validate_component_with_escapes(x))
                    .map(|x| x.nfc().collect::<String>())?,
            ),
        }
    }

    Some(out)
}

impl AsRef<[u8]> for BoxPath<'_> {
    #[inline(always)]
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

#[cfg(feature = "std")]
impl BoxPath<'static> {
    pub fn new<P: AsRef<Path>>(path: P) -> std::result::Result<BoxPath<'static>, IntoBoxPathError> {
        let out = sanitize(&path).ok_or(IntoBoxPathError::UnrepresentableStr)?;

        if out.is_empty() {
            return Err(IntoBoxPathError::EmptyPath);
        }

        Ok(BoxPath(Cow::Owned(out.join(PATH_BOX_SEP))))
    }

    /// Create a BoxPath allowing `\xNN` escape sequences.
    /// Use this when the archive has the allow_escapes flag set.
    pub fn new_with_escapes<P: AsRef<Path>>(
        path: P,
    ) -> std::result::Result<BoxPath<'static>, IntoBoxPathError> {
        let out = sanitize_with_escapes(&path).ok_or(IntoBoxPathError::UnrepresentableStr)?;

        if out.is_empty() {
            return Err(IntoBoxPathError::EmptyPath);
        }

        Ok(BoxPath(Cow::Owned(out.join(PATH_BOX_SEP))))
    }
}

impl<'a> BoxPath<'a> {
    /// Basic validation of a deserialized BoxPath (no_std compatible).
    /// Checks that:
    /// - Path is not empty
    /// - No empty components (consecutive separators, leading/trailing separator)
    /// - No `.` or `..` components
    /// - No `/` or `\` characters within components
    ///
    /// For full validation including control character checks, use `validate()` (std only).
    // [spec:box:req:paths.root]
    pub fn validate_basic(&self) -> Result<(), &'static str> {
        if self.0.is_empty() {
            return Err("empty path");
        }

        // Check for leading/trailing separator
        if self.0.starts_with(PATH_BOX_SEP) || self.0.ends_with(PATH_BOX_SEP) {
            return Err("path has empty component (leading/trailing separator)");
        }

        for component in self.0.split(PATH_BOX_SEP) {
            // Empty component (consecutive separators)
            if component.is_empty() {
                return Err("path has empty component");
            }

            // Dot and dot-dot components
            if component == "." || component == ".." {
                return Err("path contains . or .. component");
            }

            // Check for slash/backslash
            for c in component.chars() {
                if c == '/' || c == '\\' {
                    return Err("path component contains slash or backslash");
                }
            }
        }

        Ok(())
    }

    /// Validate a deserialized BoxPath.
    /// Checks that:
    /// - Path is not empty
    /// - No empty components (consecutive separators, leading/trailing separator)
    /// - No `.` or `..` components
    /// - No `/` or `\` characters within components
    /// - No control characters within components (except separators)
    // [spec:box:req:paths.root]
    #[cfg(feature = "std")]
    pub fn validate(&self) -> std::io::Result<()> {
        self.validate_for_extraction(false)
    }

    /// Validate an untrusted archive path without normalizing it before
    /// filesystem materialization. Escapes are accepted only as complete
    /// `\xNN` spellings whose decoded byte is valid in a filename.
    // [spec:box:req:paths.root.extraction-gates+1]
    #[cfg(feature = "std")]
    pub(crate) fn validate_for_extraction(&self, allow_escapes: bool) -> std::io::Result<()> {
        if self.0.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "empty path",
            ));
        }

        // Check for leading/trailing separator
        if self.0.starts_with(PATH_BOX_SEP) || self.0.ends_with(PATH_BOX_SEP) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "path has empty component (leading/trailing separator)",
            ));
        }

        for component in self.0.split(PATH_BOX_SEP) {
            // Empty component (consecutive separators)
            if component.is_empty() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "path has empty component",
                ));
            }

            // Dot and dot-dot components
            if component == "." || component == ".." {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("path contains '{}' component", component),
                ));
            }

            if allow_escapes {
                if !validate_component_with_escapes(component) {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "path component contains an invalid escape or filename character",
                    ));
                }
            } else if component.chars().any(|c| !is_char_allowed(c)) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "path component contains slash, backslash, control, or separator character",
                ));
            }
        }

        if self
            .to_path_buf()
            .components()
            .any(|component| !matches!(component, std::path::Component::Normal(_)))
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "archive path does not remain relative normal components on this platform",
            ));
        }

        Ok(())
    }

    /// Validate both an indexed path and the record name it claims to locate.
    /// Record names are single components; the path's final component must be
    /// byte-for-byte identical to that name.
    #[cfg(feature = "std")]
    pub(crate) fn validate_indexed_for_extraction(
        &self,
        record_name: &str,
        allow_escapes: bool,
    ) -> std::io::Result<()> {
        self.validate_for_extraction(allow_escapes)?;

        let record_component = BoxPath(Cow::Borrowed(record_name));
        record_component.validate_for_extraction(allow_escapes)?;
        if record_component.depth() != 0 || self.filename() != record_name {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "indexed path final component {:?} does not match record name {:?}",
                    self.filename(),
                    record_name
                ),
            ));
        }

        Ok(())
    }

    #[cfg(feature = "std")]
    pub fn to_path_buf(&self) -> PathBuf {
        PathBuf::from(self.to_string())
    }

    pub fn parent(&self) -> Option<BoxPath<'_>> {
        self.0
            .rfind(PATH_BOX_SEP)
            .map(|pos| BoxPath(Cow::Borrowed(&self.0[..pos])))
    }

    pub fn filename(&self) -> &str {
        self.iter().last().unwrap()
    }

    pub fn depth(&self) -> usize {
        self.0.chars().filter(|c| c == &'\x1f').count()
    }

    pub fn starts_with(&self, other: &BoxPath) -> bool {
        !self
            .0
            .split(PATH_BOX_SEP)
            .zip(other.0.split(PATH_BOX_SEP))
            .any(|(a, b)| a != b)
    }

    #[cfg(feature = "std")]
    pub fn join<P: AsRef<Path>>(
        &self,
        tail: P,
    ) -> std::result::Result<BoxPath<'static>, IntoBoxPathError> {
        BoxPath::new(self.to_path_buf().join(tail))
    }

    pub(crate) fn join_unchecked(&self, tail: &str) -> BoxPath<'static> {
        use crate::compat::String;
        let mut s = String::with_capacity(self.0.len() + 1 + tail.len());
        s.push_str(&self.0);
        s.push_str(PATH_BOX_SEP);
        s.push_str(tail);
        BoxPath(Cow::Owned(s))
    }

    pub fn iter(&self) -> core::str::Split<'_, &str> {
        self.0.split(PATH_BOX_SEP)
    }

    pub fn into_owned(self) -> BoxPath<'static> {
        BoxPath(Cow::Owned(self.0.into_owned()))
    }
}

// [spec:box:def:paths.root.encoding]
impl fmt::Display for BoxPath<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut iter = self.0.split(PATH_BOX_SEP);
        if let Some(v) = iter.next() {
            f.write_str(v)?;
        }
        for v in iter {
            f.write_str(PATH_PLATFORM_SEP)?;
            f.write_str(v)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // [spec:box:def:paths.root.encoding/test]
    // [spec:box:req:paths.root.normalization/test]
    #[test]
    fn sanitisation() {
        let box_path = BoxPath::new("/something/../somethingelse/./foo.txt").unwrap();
        assert_eq!(box_path.0, "somethingelse\x1ffoo.txt");
        let box_path = BoxPath::new("../something/../somethingelse/./foo.txt/.").unwrap();
        assert_eq!(box_path.0, "somethingelse\x1ffoo.txt");

        // This one will do different things on Windows and Unix, because Unix loves a good backslash
        let box_path = BoxPath::new(r"..\something\..\somethingelse\.\foo.txt\.");

        #[cfg(not(windows))]
        assert!(box_path.is_err());
        #[cfg(windows)]
        assert_eq!(box_path.unwrap().0, "somethingelse\x1ffoo.txt");

        let box_path = BoxPath::new(r"..\something/..\somethingelse\./foodir\");
        #[cfg(not(windows))]
        assert!(box_path.is_err());
        #[cfg(windows)]
        assert_eq!(box_path.unwrap().0, "somethingelse\x1ffoodir");
    }

    // [spec:box:req:paths.root/test]
    #[test]
    fn sanitisation2() {
        // Null is a sassy fellow
        let box_path = BoxPath::new("\0");
        assert!(box_path.is_err());
    }

    #[test]
    fn sanitisation3() {
        // Blank string is a sassy fellow if you can find him
        let box_path = BoxPath::new("");
        assert!(box_path.is_err());
    }

    #[test]
    fn sanitisation4() {
        // Blank string is a sassy fellow if you can find him
        let box_path = BoxPath::new("/cant/hate//the/path");
        println!("{:?}", box_path);
        assert_eq!(box_path.unwrap().0, "cant\x1fhate\x1fthe\x1fpath");
    }

    #[test]
    fn sanitisation_bidi() {
        // Blank string is a sassy fellow if you can find him
        let box_path = BoxPath::new("this is now العَرَبِيَّة.txt");
        println!("{:?}", box_path);
        assert_eq!(box_path.unwrap().0, "this is now العَرَبِيَّة.txt");
    }

    #[test]
    fn sanitisation_basmala() {
        // Blank string is a sassy fellow if you can find him
        let box_path = BoxPath::new("this is now ﷽.txt");
        println!("{:?}", box_path);
        assert_eq!(box_path.unwrap().0, "this is now ﷽.txt");
    }

    #[test]
    fn sanitisation_icecube_emoji() {
        let box_path = BoxPath::new("///🧊/🧊");
        println!("{:?}", box_path);
        assert_eq!(box_path.unwrap().0, "🧊\x1f🧊");
    }

    #[test]
    fn sanitisation_simple_self() {
        let box_path = BoxPath::new("./self");
        println!("{:?}", box_path);
        assert_eq!(box_path.unwrap().0, "self");
    }

    #[test]
    fn sanitisation_slash() {
        let box_path = BoxPath::new("/");
        println!("{:?}", box_path);
        assert!(box_path.is_err());
    }

    // Escape sequence tests

    // [spec:box:req:paths.root.normalization/test]
    #[test]
    fn escape_dash_allowed() {
        // \x2d is dash, which is allowed
        let box_path = BoxPath::new_with_escapes(r"foo\x2dbar");
        assert!(box_path.is_ok());
        let box_path = box_path.unwrap();
        assert_eq!(box_path.0, r"foo\x2dbar");
        assert!(box_path.validate_for_extraction(true).is_ok());
        assert!(box_path.validate_for_extraction(false).is_err());
    }

    // [spec:box:req:paths.root.normalization/test]
    #[test]
    fn escape_slash_rejected() {
        // \x2f is forward slash, not allowed in filenames
        let box_path = BoxPath::new_with_escapes(r"foo\x2fbar");
        assert!(box_path.is_err());
    }

    #[test]
    fn escape_null_rejected() {
        // \x00 is null, not allowed
        let box_path = BoxPath::new_with_escapes(r"foo\x00bar");
        assert!(box_path.is_err());
    }

    #[test]
    fn escape_backslash_rejected() {
        // \x5c is backslash, not allowed
        let box_path = BoxPath::new_with_escapes(r"foo\x5cbar");
        assert!(box_path.is_err());
    }

    #[test]
    fn bare_backslash_rejected() {
        // Bare backslash without valid escape is rejected
        let box_path = BoxPath::new_with_escapes(r"foo\bar");
        assert!(box_path.is_err());
    }

    #[test]
    fn invalid_hex_rejected() {
        // \xZZ is not valid hex
        let box_path = BoxPath::new_with_escapes(r"foo\xZZbar");
        assert!(box_path.is_err());
    }

    #[test]
    fn escape_without_flag_rejected() {
        // Standard BoxPath::new should reject escapes
        let box_path = BoxPath::new(r"foo\x2dbar");
        assert!(box_path.is_err());
    }

    #[test]
    fn systemd_style_path() {
        // Typical systemd escaped path
        let box_path = BoxPath::new_with_escapes(r"dev\x2ddisk\x2dby\x2duuid\x2d1234.device");
        assert!(box_path.is_ok());
    }
}
