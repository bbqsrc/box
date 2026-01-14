//! Well-known attribute names for box archives.
//!
//! These constants define the standard attribute names used throughout the box format
//! for storing file metadata, timestamps, permissions, and checksums.

// Timestamp attributes (stored as minutes since BOX_EPOCH_UNIX)
/// File/directory creation time (minutes since BOX_EPOCH)
pub const CREATED: &str = "created";
/// File/directory modification time (minutes since BOX_EPOCH)
pub const MODIFIED: &str = "modified";
/// File/directory access time (minutes since BOX_EPOCH)
pub const ACCESSED: &str = "accessed";

// High-precision timestamp attributes (for sub-minute precision)
/// Creation time seconds component
pub const CREATED_SECONDS: &str = "created.seconds";
/// Creation time nanoseconds component
pub const CREATED_NANOSECONDS: &str = "created.nanoseconds";
/// Modification time seconds component
pub const MODIFIED_SECONDS: &str = "modified.seconds";
/// Modification time nanoseconds component
pub const MODIFIED_NANOSECONDS: &str = "modified.nanoseconds";
/// Access time seconds component
pub const ACCESSED_SECONDS: &str = "accessed.seconds";
/// Access time nanoseconds component
pub const ACCESSED_NANOSECONDS: &str = "accessed.nanoseconds";

// Unix permission attributes
/// Unix file mode (permissions + file type bits)
pub const UNIX_MODE: &str = "unix.mode";
/// Unix user ID
pub const UNIX_UID: &str = "unix.uid";
/// Unix group ID
pub const UNIX_GID: &str = "unix.gid";

// Checksum attributes
/// Blake3 hash of file contents
pub const BLAKE3: &str = "blake3";

// Extended attribute prefix
/// Prefix for Linux extended attributes (e.g., "linux.xattr.user.myattr")
pub const LINUX_XATTR_PREFIX: &str = "linux.xattr.";
