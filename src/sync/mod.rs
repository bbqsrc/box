//! Sync (std) frontends for reading and writing Box archives.

#[cfg(feature = "reader")]
mod reader;
#[cfg(test)]
mod tests;
#[cfg(feature = "writer")]
mod writer;

#[cfg(feature = "reader")]
pub use reader::BoxReader;
#[cfg(feature = "writer")]
pub use writer::BoxWriter;
