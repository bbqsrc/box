//! Directory enumeration session management for ProjFS.

use box_format::RecordIndex;

/// A single directory entry prepared for enumeration.
#[derive(Debug, Clone)]
pub struct EnumerationEntry {
    /// The file/directory name (just the final component, not full path).
    pub name: String,
    /// Record index in the archive.
    pub index: RecordIndex,
    /// Whether this entry is a directory.
    pub is_directory: bool,
    /// File size in bytes (0 for directories).
    pub file_size: u64,
    /// Creation time as Windows FILETIME.
    pub creation_time: i64,
    /// Last write time as Windows FILETIME.
    pub last_write_time: i64,
    /// Windows file attributes.
    pub file_attributes: u32,
}

/// Windows file attribute constants.
pub mod file_attributes {
    pub const FILE_ATTRIBUTE_READONLY: u32 = 0x00000001;
    pub const FILE_ATTRIBUTE_DIRECTORY: u32 = 0x00000010;
    pub const FILE_ATTRIBUTE_ARCHIVE: u32 = 0x00000020;
    pub const FILE_ATTRIBUTE_NORMAL: u32 = 0x00000080;
}

/// State for a directory enumeration session.
///
/// ProjFS may call the enumeration callbacks multiple times for a single
/// directory listing (e.g., if the buffer fills up). This struct tracks
/// the state across calls.
#[derive(Debug)]
pub struct EnumerationSession {
    /// GUID identifying this session (stored as u128 for easy comparison).
    pub id: u128,
    /// Record index of the directory being enumerated (None = root).
    pub dir_index: Option<RecordIndex>,
    /// Cached and sorted list of entries.
    entries: Vec<EnumerationEntry>,
    /// Current position in enumeration.
    cursor: usize,
    /// Captured search expression for filtering.
    search_expression: Option<String>,
    /// Whether enumeration has been populated at least once.
    has_populated: bool,
}

impl EnumerationSession {
    /// Create a new enumeration session.
    pub fn new(id: u128, dir_index: Option<RecordIndex>) -> Self {
        Self {
            id,
            dir_index,
            entries: Vec::new(),
            cursor: 0,
            search_expression: None,
            has_populated: false,
        }
    }

    /// Reset cursor for restart scan.
    pub fn restart(&mut self) {
        self.cursor = 0;
    }

    /// Check if we've reached the end of entries.
    pub fn is_done(&self) -> bool {
        self.cursor >= self.entries.len()
    }

    /// Get the current entry without advancing.
    pub fn current_entry(&self) -> Option<&EnumerationEntry> {
        self.entries.get(self.cursor)
    }

    /// Advance to the next entry.
    pub fn advance(&mut self) {
        if self.cursor < self.entries.len() {
            self.cursor += 1;
        }
    }

    /// Check if entries have been populated.
    pub fn has_populated(&self) -> bool {
        self.has_populated
    }

    /// Set the search expression for filtering.
    pub fn set_search_expression(&mut self, search: Option<String>) {
        self.search_expression = search;
    }

    /// Get the search expression.
    pub fn search_expression(&self) -> Option<&str> {
        self.search_expression.as_deref()
    }

    /// Populate entries from the provider.
    ///
    /// This should be called when the session is first created or when
    /// a restart scan is requested.
    pub fn populate(&mut self, entries: Vec<EnumerationEntry>) {
        self.entries = entries;
        // Sort entries using case-insensitive comparison (Windows convention)
        self.entries
            .sort_by(|a, b| a.name.to_lowercase().cmp(&b.name.to_lowercase()));
        self.cursor = 0;
        self.has_populated = true;
    }

    /// Get the number of entries.
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }
}

/// Check if a filename matches a search expression with wildcards.
///
/// Supports `*` (match any characters) and `?` (match single character).
/// Matching is case-insensitive per Windows convention.
pub fn matches_search_expression(name: &str, search: &str) -> bool {
    // Empty search or "*" matches everything
    if search.is_empty() || search == "*" {
        return true;
    }

    let name_lower = name.to_lowercase();
    let search_lower = search.to_lowercase();

    // Simple glob matching
    let mut name_chars = name_lower.chars().peekable();
    let mut search_chars = search_lower.chars().peekable();

    fn match_recursive(
        name: &mut std::iter::Peekable<std::str::Chars>,
        search: &mut std::iter::Peekable<std::str::Chars>,
    ) -> bool {
        loop {
            match (search.peek().copied(), name.peek().copied()) {
                (None, None) => return true,
                (None, Some(_)) => return false,
                (Some('*'), _) => {
                    search.next();
                    // '*' at end matches everything
                    if search.peek().is_none() {
                        return true;
                    }
                    // Try matching '*' with 0 or more characters
                    loop {
                        let mut name_clone = name.clone();
                        let mut search_clone = search.clone();
                        if match_recursive(&mut name_clone, &mut search_clone) {
                            return true;
                        }
                        if name.next().is_none() {
                            return false;
                        }
                    }
                }
                (Some('?'), Some(_)) => {
                    search.next();
                    name.next();
                }
                (Some('?'), None) => return false,
                (Some(s), Some(n)) if s == n => {
                    search.next();
                    name.next();
                }
                (Some(_), _) => return false,
            }
        }
    }

    match_recursive(&mut name_chars, &mut search_chars)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_matches_star() {
        assert!(matches_search_expression("test.txt", "*"));
        assert!(matches_search_expression("test.txt", "*.txt"));
        assert!(matches_search_expression("test.txt", "test.*"));
        assert!(matches_search_expression("test.txt", "*.*"));
        assert!(!matches_search_expression("test.txt", "*.doc"));
    }

    #[test]
    fn test_matches_question() {
        assert!(matches_search_expression("test.txt", "????.txt"));
        assert!(!matches_search_expression("test.txt", "???.txt"));
        assert!(matches_search_expression("a.txt", "?.txt"));
    }

    #[test]
    fn test_matches_case_insensitive() {
        assert!(matches_search_expression("TEST.TXT", "test.txt"));
        assert!(matches_search_expression("test.txt", "TEST.TXT"));
        assert!(matches_search_expression("Test.Txt", "*.txt"));
    }

    #[test]
    fn test_matches_empty() {
        assert!(matches_search_expression("anything", ""));
        assert!(matches_search_expression("", ""));
    }
}
