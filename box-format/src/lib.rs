mod compression;
mod de;
mod file;
mod header;
pub mod path;
mod record;
mod ser;

#[cfg(feature = "ffi")]
pub mod ffi;

pub use compression::Compression;
pub use file::{AttrMap, BoxFileReader, BoxFileWriter, BoxMetadata};
use header::BoxHeader;
pub use path::BoxPath;
pub use record::{DirectoryRecord, FileRecord, Record};

#[doc(hidden)]
pub use comde;
