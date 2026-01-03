use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    name = "box",
    about = "Create, modify and extract box archives.",
    version
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    #[command(visible_alias = "c", about = "Create a new archive")]
    Create(CreateArgs),

    #[command(visible_alias = "x", about = "Extract files from an archive")]
    Extract(ExtractArgs),

    #[command(visible_aliases = ["l", "ls"], about = "List files in an archive")]
    List(ListArgs),

    #[command(about = "Show archive metadata and statistics")]
    Info(InfoArgs),

    #[command(visible_aliases = ["t", "test"], about = "Validate archive checksums")]
    Validate(ValidateArgs),
}

#[derive(Debug, clap::Args)]
#[command(after_help = "\
\x1b[1m\x1b[4mCompression Flags:\x1b[0m
  \x1b[1m--zstd\x1b[0m      Zstandard compression (default)
  \x1b[1m--stored\x1b[0m    No compression
  \x1b[1m--brotli\x1b[0m    Brotli compression
  \x1b[1m--deflate\x1b[0m   Deflate compression
  \x1b[1m--xz\x1b[0m        XZ/LZMA compression
  \x1b[1m--snappy\x1b[0m    Snappy compression

Compression flags are position-sensitive and apply to all subsequent paths.
Use \x1b[1m-O key=value\x1b[0m to set compression options for the current algorithm.

\x1b[1m\x1b[4mCompression Options:\x1b[0m
  \x1b[1m-O level=N\x1b[0m         Compression level (zstd: 1-22, brotli: 0-11, deflate/xz: 0-9)
  \x1b[1m-O window_log=N\x1b[0m    Window size as power of 2 (zstd: 10-31)
  \x1b[1m-O window_size=N\x1b[0m   Window size as power of 2 (brotli: 0-24)
  \x1b[1m-O checksum=BOOL\x1b[0m   Emit checksum in frame (zstd only)
  \x1b[1m-O text_mode=BOOL\x1b[0m  Optimize for text (brotli only)

\x1b[1m\x1b[4mExamples:\x1b[0m
  box create archive.box src/
  box create archive.box --zstd -O level=19 src/
  box create archive.box --brotli -O level=6 assets/ --zstd src/
  box create --exclude '*.log' archive.box .")]
pub struct CreateArgs {
    /// Output archive path
    pub archive: PathBuf,

    /// Disable checksum computation
    #[arg(long)]
    pub no_checksum: bool,

    /// Store file timestamps (created, modified, accessed)
    #[arg(long)]
    pub timestamps: bool,

    /// Store file ownership (uid, gid)
    #[arg(long)]
    pub ownership: bool,

    /// Preserve all file metadata (implies --timestamps --ownership)
    #[arg(short = 'A', long)]
    pub archive_metadata: bool,

    /// Include hidden files
    #[arg(short = 'a', long = "all")]
    pub include_hidden: bool,

    /// Don't recurse into directories
    #[arg(long = "no-recursive")]
    pub no_recursive: bool,

    /// Byte alignment for records
    #[arg(long = "align", default_value_t = 8)]
    pub alignment: u32,

    /// Exclude files matching pattern
    #[arg(long = "exclude", value_name = "PATTERN")]
    pub exclude: Vec<String>,

    /// Suppress output
    #[arg(short = 'q', long)]
    pub quiet: bool,

    /// Overwrite existing archive
    #[arg(short = 'f', long)]
    pub force: bool,

    /// Process files sequentially (disable parallel compression)
    #[arg(long)]
    pub serial: bool,

    /// Number of parallel compression tasks (default: CPU count)
    #[arg(short = 'j', long = "jobs")]
    pub jobs: Option<usize>,

    /// Allow \xNN escape sequences in paths (for systemd-style filenames)
    #[arg(long)]
    pub allow_escapes: bool,

    /// Allow symlinks pointing outside the archive
    #[arg(long)]
    pub allow_external_symlinks: bool,

    /// Files and directories to archive
    #[arg(
        trailing_var_arg = true,
        allow_hyphen_values = true,
        value_name = "PATH"
    )]
    pub paths_and_compression: Vec<String>,
}

#[derive(Debug, clap::Args)]
pub struct ExtractArgs {
    /// Path to the .box archive to extract
    pub archive: PathBuf,

    /// Output directory (defaults to current directory)
    #[arg(short = 'o', long = "output")]
    pub output: Option<PathBuf>,

    /// Skip checksum verification
    #[arg(long)]
    pub no_checksum: bool,

    /// Suppress output (quiet mode)
    #[arg(short = 'q', long)]
    pub quiet: bool,

    /// Process files sequentially (disable parallel extraction)
    #[arg(long)]
    pub serial: bool,

    /// Number of parallel extraction tasks (default: CPU count)
    #[arg(short = 'j', long = "jobs")]
    pub jobs: Option<usize>,

    /// Show timing breakdown
    #[arg(long)]
    pub timings: bool,

    /// Allow \xNN escape sequences in paths (required for archives with escaped paths)
    #[arg(long)]
    pub allow_escapes: bool,

    /// Allow extracting archives with external symlinks (pointing outside the archive)
    #[arg(long)]
    pub allow_external_symlinks: bool,

    /// Specific files to extract (extracts all if none specified)
    pub files: Vec<PathBuf>,
}

#[derive(Debug, clap::Args)]
pub struct ListArgs {
    /// Path to the .box archive
    pub archive: PathBuf,

    /// Show detailed information (timestamps, permissions, checksums)
    #[arg(short = 'l', long)]
    pub long: bool,

    /// Output in JSON format
    #[arg(short = 'j', long)]
    pub json: bool,
}

#[derive(Debug, clap::Args)]
pub struct InfoArgs {
    /// Path to the .box archive
    pub archive: PathBuf,

    /// Optional file path within the archive to show info for
    pub file: Option<String>,
}

#[derive(Debug, clap::Args)]
pub struct ValidateArgs {
    /// Path to the .box archive
    pub archive: PathBuf,

    /// Suppress output (quiet mode)
    #[arg(short = 'q', long)]
    pub quiet: bool,

    /// Process files sequentially (disable parallel validation)
    #[arg(long)]
    pub serial: bool,

    /// Number of parallel validation tasks (default: CPU count)
    #[arg(short = 'j', long = "jobs")]
    pub jobs: Option<usize>,
}
