//! Async (tokio) frontends for reading and writing Box archives.

#[cfg(feature = "reader")]
mod reader;
#[cfg(test)]
mod tests;
#[cfg(feature = "writer")]
mod writer;

#[cfg(feature = "reader")]
pub use reader::{
    BoxFileReader, ExtractError, ExtractOptions, ExtractProgress, ExtractStats, ExtractTiming,
    OpenError, ValidateProgress, ValidateStats,
};
#[cfg(feature = "writer")]
pub use writer::{
    AddOptions, AddStats, BoxFileWriter, CompressedData, CompressedFile, FileJob, ParallelProgress,
    calculate_memory_threshold, compress_file,
};
