use std::path::PathBuf;

use box_format::BoxPath;
use box_format::path::IntoBoxPathError;
use miette::Diagnostic;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error, Diagnostic)]
pub enum Error {
    #[error("Cannot handle path `{}`", .path.display())]
    InvalidPath {
        path: PathBuf,
        #[source]
        source: IntoBoxPathError,
    },

    #[error("Cannot open archive `{}`", .path.display())]
    #[diagnostic(help("Is this a valid .box file?"))]
    OpenArchive {
        path: PathBuf,
        #[source]
        source: box_format::OpenError,
    },

    #[error("Cannot open file `{}`", path.display())]
    OpenFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("Cannot create directory `{path}`")]
    CreateDirectory {
        path: BoxPath,
        #[source]
        source: std::io::Error,
    },

    #[error("Cannot create link `{path}`")]
    CreateLink {
        path: BoxPath,
        #[source]
        source: std::io::Error,
    },

    #[error("Cannot add file to archive `{}`", path.display())]
    AddFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("Cannot finish archive `{}`", path.display())]
    FinishArchive {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("Cannot set attribute `{key}`")]
    SetAttribute {
        key: String,
        #[source]
        source: std::io::Error,
    },

    #[error("Cannot create archive `{}`", path.display())]
    CreateArchive {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("Cannot process directory entry")]
    ProcessDirEntry {
        #[source]
        source: std::io::Error,
    },

    #[error("Cannot process file `{}`", .path.display())]
    ProcessFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("Cannot canonicalize path `{}`", .path.display())]
    CanonicalizePath {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("Cannot read link `{}`", .path.display())]
    ReadLink {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("Cannot extract files")]
    Extract {
        #[source]
        source: box_format::ExtractError,
    },

    #[error("Archive already exists: `{}`", path.display())]
    #[diagnostic(help("Use -f/--force to overwrite"))]
    ArchiveExists { path: PathBuf },

    #[error("No files specified to add to archive")]
    #[diagnostic(help("Specify one or more files or directories to archive"))]
    NoFilesSpecified,

    #[error("Failed to add files in parallel")]
    AddFilesParallel {
        #[source]
        source: std::io::Error,
    },
}
