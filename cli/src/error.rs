#![allow(unused_assignments)] // miette bug workaround

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

    #[diagnostic(help("{}", source.diagnostic_help()))]
    #[error("Cannot open archive `{}`", .path.display())]
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
        path: BoxPath<'static>,
        #[source]
        source: std::io::Error,
    },

    #[error("Cannot create link `{path}`")]
    CreateLink {
        path: BoxPath<'static>,
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

    #[diagnostic(help("Use -f/--force to overwrite"))]
    #[error("Archive already exists: `{}`", path.display())]
    ArchiveExists { path: PathBuf },

    #[error("No files specified to add to archive")]
    #[diagnostic(help("Specify one or more files or directories to archive"))]
    NoFilesSpecified,

    #[error("Failed to add files in parallel")]
    AddFilesParallel {
        #[source]
        source: std::io::Error,
    },

    #[error("File not found in archive: `{}`", path.display())]
    FileNotFound { path: PathBuf },

    #[error("Archive contains escaped paths")]
    #[diagnostic(help("Use --allow-escapes to extract archives with escaped paths"))]
    AllowEscapesRequired,

    #[error("Archive contains external symlinks")]
    #[diagnostic(help("Use --allow-external-symlinks to extract archives with external symlinks"))]
    ExternalSymlinksRequired,

    #[error("External symlink detected: `{}` -> `{target}`", link_path.display())]
    #[diagnostic(help(
        "Use --allow-external-symlinks to include external symlinks in the archive"
    ))]
    ExternalSymlinkDetected { link_path: PathBuf, target: String },
}

#[cfg(test)]
mod tests {
    use miette::Diagnostic;

    use super::Error;

    fn help_for(source: box_format::OpenError) -> String {
        let error = Error::OpenArchive {
            path: "archive.box".into(),
            source,
        };
        error.help().unwrap().to_string()
    }

    #[test]
    fn trailer_failure_help_acknowledges_valid_header() {
        let help = help_for(box_format::OpenError::InvalidTrailer(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "truncated",
        )));

        assert!(help.contains("Box header is valid"));
        assert!(help.contains("record, field, and byte offsets"));
        assert!(!help.contains("Is this a valid"));
    }

    #[test]
    fn header_failure_help_still_questions_the_input_format() {
        let help = help_for(box_format::OpenError::MissingHeader(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid magic",
        )));

        assert!(help.contains("No valid Box header was found"));
        assert!(help.contains("32-byte header"));
    }
}
