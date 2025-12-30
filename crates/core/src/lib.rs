//! Core types for the Box archive format.
//!
//! This crate provides `no_std` compatible data structures for the Box format,
//! enabling code sharing between userspace and kernel implementations.
//!
//! # Features
//!
//! - `std` (default): Enables standard library support
//! - `alloc`: Enables types that require heap allocation (Record, BoxPath, etc.)
//!
//! With no features enabled, only basic types like `Compression`, `BoxHeader`,
//! and `RecordIndex` are available.

#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(feature = "alloc")]
extern crate alloc;

pub mod compression;
pub mod header;
pub mod path;
pub mod record;

// Core types available without alloc
pub use compression::{ByteCount, Compression};
pub use header::{BoxHeader, MAGIC_BYTES, VERSION};
pub use path::{IntoBoxPathError, PATH_BOX_SEP, PATH_BOX_SEP_BYTE};
pub use record::{RecordIndex, RecordIndexError};

// Types requiring alloc
#[cfg(feature = "alloc")]
pub use path::BoxPath;
#[cfg(feature = "alloc")]
pub use record::{DirectoryRecord, FileRecord, LinkRecord, Record};

#[cfg(feature = "alloc")]
pub use alloc::collections::BTreeMap;

/// Attribute map type: maps interned key indices to byte values.
#[cfg(feature = "alloc")]
pub type AttrMap = BTreeMap<usize, alloc::vec::Vec<u8>>;
