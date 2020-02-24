mod compression;
#[cfg(feature = "reader")]
mod de;
mod file;
mod header;
pub mod path;
mod record;
#[cfg(feature = "writer")]
mod ser;

#[cfg(feature = "ffi")]
pub mod ffi;

pub use self::file::Inode;
pub use compression::Compression;
#[cfg(feature = "reader")]
pub use file::reader::BoxFileReader;
#[cfg(feature = "writer")]
pub use file::writer::BoxFileWriter;
pub use file::{AttrMap, BoxMetadata};
use header::BoxHeader;
pub use path::BoxPath;
pub use record::{DirectoryRecord, FileRecord, LinkRecord, Record};

#[doc(hidden)]
pub use comde;
