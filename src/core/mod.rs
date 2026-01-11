//! Sans-IO core state machines for box archive reading and writing.
//!
//! This module provides pure state machines that work with byte buffers
//! without performing any I/O. They can be used by different frontends:
//! - Async (tokio)
//! - Sync (std)
//! - Kernel implementations
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────┐
//! │  Frontends                              │
//! │  - BoxFileReader (async/tokio)          │
//! │  - BoxReader (std/sync)                 │
//! │  - Future: kernel impl                  │
//! ├─────────────────────────────────────────┤
//! │  Sans-IO Core (this module)             │
//! │  - ArchiveReader                        │
//! │  - ArchiveWriter                        │
//! └─────────────────────────────────────────┘
//! ```

pub mod meta;
#[cfg(feature = "std")]
mod reader;
#[cfg(feature = "std")]
mod writer;

pub use meta::{
    AttrKey, AttrMap, AttrType, AttrValue, BoxMetadata, MetadataIter, RecordIndex, Records,
    RecordsItem,
};
#[cfg(feature = "std")]
pub use reader::ArchiveReader;
#[cfg(feature = "std")]
pub use writer::{ArchiveWriter, WriterOptions};
