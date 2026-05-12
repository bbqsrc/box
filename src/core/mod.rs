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
#[cfg(all(feature = "std", feature = "reader"))]
mod reader;
#[cfg(all(feature = "std", feature = "writer"))]
mod writer;

#[cfg(all(feature = "std", feature = "reader"))]
pub use reader::ArchiveReader;

#[cfg(all(feature = "std", feature = "writer"))]
pub use writer::{ArchiveWriter, WriterOptions};

pub use meta::{
    AttrKey, AttrMap, AttrType, AttrValue, BoxMetadata, MetadataIter, RecordIndex, Records,
    RecordsItem,
};
