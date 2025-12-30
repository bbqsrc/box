//! Box archive path types.

use core::fmt;

#[cfg(feature = "alloc")]
use alloc::borrow::Cow;
#[cfg(feature = "alloc")]
use alloc::string::String;

/// The separator used in BoxPath type paths.
/// Uses UNIT SEPARATOR (U+001F) to avoid conflicts with platform separators.
pub const PATH_BOX_SEP: &str = "\x1f";

/// The separator as a byte.
pub const PATH_BOX_SEP_BYTE: u8 = 0x1f;

/// Error type for path conversion failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntoBoxPathError {
    /// The path contained characters that cannot be represented.
    UnrepresentableStr,
    /// The path was not in canonical form.
    NonCanonical,
    /// An empty path was provided.
    EmptyPath,
}

impl fmt::Display for IntoBoxPathError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl IntoBoxPathError {
    /// Get a string description of the error.
    pub const fn as_str(&self) -> &'static str {
        match self {
            IntoBoxPathError::NonCanonical => "non-canonical path received as input",
            IntoBoxPathError::UnrepresentableStr => "unrepresentable string found in path",
            IntoBoxPathError::EmptyPath => "no path provided",
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for IntoBoxPathError {}

/// A path within a Box archive.
///
/// Paths are always relative (no leading separator), delimited by
/// UNIT SEPARATOR (U+001F), and may not contain `.` or `..` path chunks.
#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq, Hash)]
#[repr(transparent)]
#[cfg(feature = "alloc")]
pub struct BoxPath<'a>(pub(crate) Cow<'a, str>);

#[cfg(feature = "alloc")]
impl<'a> BoxPath<'a> {
    /// Create a new BoxPath from a pre-sanitized string.
    ///
    /// The string should already be in canonical form (using PATH_BOX_SEP as delimiter).
    /// For path sanitization, use the `std` feature and platform-specific methods.
    pub fn from_canonical(s: impl Into<Cow<'a, str>>) -> Result<Self, IntoBoxPathError> {
        let s = s.into();
        if s.is_empty() {
            return Err(IntoBoxPathError::EmptyPath);
        }
        // Basic validation: no . or .. components, no empty components
        for component in s.split(PATH_BOX_SEP) {
            if component.is_empty() || component == "." || component == ".." {
                return Err(IntoBoxPathError::NonCanonical);
            }
        }
        Ok(BoxPath(s))
    }

    /// Create a BoxPath without validation.
    ///
    /// # Safety
    /// The caller must ensure the path is in valid canonical form.
    pub fn from_canonical_unchecked(s: impl Into<Cow<'a, str>>) -> Self {
        BoxPath(s.into())
    }

    /// Get the parent path, if any.
    pub fn parent(&self) -> Option<BoxPath<'_>> {
        self.0
            .rfind(PATH_BOX_SEP)
            .map(|pos| BoxPath(Cow::Borrowed(&self.0[..pos])))
    }

    /// Get the filename (last component) of the path.
    pub fn filename(&self) -> &str {
        self.iter().last().unwrap_or(&self.0)
    }

    /// Get the depth of the path (number of separators).
    pub fn depth(&self) -> usize {
        self.0.bytes().filter(|&c| c == PATH_BOX_SEP_BYTE).count()
    }

    /// Check if this path starts with another path.
    pub fn starts_with(&self, other: &BoxPath) -> bool {
        !self
            .0
            .split(PATH_BOX_SEP)
            .zip(other.0.split(PATH_BOX_SEP))
            .any(|(a, b)| a != b)
    }

    /// Join this path with a component (unchecked).
    pub fn join_unchecked(&self, tail: &str) -> BoxPath<'static> {
        let mut s = String::with_capacity(self.0.len() + 1 + tail.len());
        s.push_str(&self.0);
        s.push_str(PATH_BOX_SEP);
        s.push_str(tail);
        BoxPath(Cow::Owned(s))
    }

    /// Iterate over path components.
    pub fn iter(&self) -> core::str::Split<'_, &str> {
        self.0.split(PATH_BOX_SEP)
    }

    /// Convert to an owned BoxPath with 'static lifetime.
    pub fn into_owned(self) -> BoxPath<'static> {
        BoxPath(Cow::Owned(self.0.into_owned()))
    }

    /// Get the path as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Get the path as bytes.
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

#[cfg(feature = "alloc")]
impl AsRef<[u8]> for BoxPath<'_> {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

#[cfg(feature = "alloc")]
impl AsRef<str> for BoxPath<'_> {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[cfg(feature = "alloc")]
impl fmt::Display for BoxPath<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Display using forward slash as a universal separator
        let mut iter = self.0.split(PATH_BOX_SEP);
        if let Some(v) = iter.next() {
            f.write_str(v)?;
        }
        for v in iter {
            f.write_str("/")?;
            f.write_str(v)?;
        }
        Ok(())
    }
}
