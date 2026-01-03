use std::path::Path;

use box_format::{BOX_EPOCH_UNIX, BoxPath, Compression, CompressionConfig};
use indicatif::{ProgressBar, ProgressStyle};

/// A path with its associated compression configuration
#[derive(Debug, Clone)]
pub struct PathWithCompression {
    pub path: String,
    pub config: CompressionConfig,
    pub is_glob: bool,
}

/// Parse the trailing arguments into paths with compression settings.
/// Compression flags are position-sensitive: they apply to all subsequent paths.
/// Use -O key=value to set compression options.
pub fn parse_paths_with_compression(args: &[String]) -> Vec<PathWithCompression> {
    let mut result = Vec::new();
    let mut current_config = CompressionConfig::new(Compression::Zstd); // Default
    let mut iter = args.iter().peekable();

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--zstd" | "--zstandard" => current_config = CompressionConfig::new(Compression::Zstd),
            "--stored" => current_config = CompressionConfig::new(Compression::Stored),
            "--brotli" => current_config = CompressionConfig::new(Compression::Brotli),
            "--deflate" => current_config = CompressionConfig::new(Compression::Deflate),
            "--xz" => current_config = CompressionConfig::new(Compression::Xz),
            "--snappy" => current_config = CompressionConfig::new(Compression::Snappy),
            "-O" => {
                if let Some(opt) = iter.next()
                    && let Some((key, value)) = opt.split_once('=')
                {
                    current_config.set_option(key, value);
                }
            }
            path => {
                let is_glob = path.contains('*') || path.contains('?') || path.contains('[');
                result.push(PathWithCompression {
                    path: path.to_string(),
                    config: current_config.clone(),
                    is_glob,
                });
            }
        }
    }

    result
}

/// Check if a path matches any of the exclude patterns
pub fn matches_exclude(path: &Path, excludes: &[glob::Pattern]) -> bool {
    let path_str = path.to_string_lossy();
    excludes.iter().any(|pattern| pattern.matches(&path_str))
}

/// Create a progress bar for file operations
pub fn create_progress_bar(total: u64, message: &str) -> ProgressBar {
    let pb = ProgressBar::new(total);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{msg} [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
            .unwrap()
            .progress_chars("=> "),
    );
    pb.set_message(message.to_string());
    pb
}

/// Create a spinner for indeterminate operations
pub fn create_spinner(message: &str) -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {msg}")
            .unwrap(),
    );
    pb.set_message(message.to_string());
    pb.enable_steady_tick(std::time::Duration::from_millis(100));
    pb
}

/// Format a timestamp from bytes (Vi64 minutes since Box epoch)
pub fn format_time(attr: Option<&[u8]>) -> String {
    attr.and_then(|x| {
        let (minutes, len) = fastvint::decode_vi64_slice(x);
        if len > 0 { Some(minutes) } else { None }
    })
    .map(|minutes| {
        let unix_seconds = minutes * 60 + BOX_EPOCH_UNIX;
        std::time::UNIX_EPOCH + std::time::Duration::new(unix_seconds as u64, 0)
    })
    .map(|x| {
        let datetime: chrono::DateTime<chrono::Utc> = x.into();
        datetime.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
    })
    .unwrap_or_else(|| "-".into())
}

/// Format Unix ACL from bytes (fastvint Vu32 encoded mode)
pub fn format_acl(attr: Option<&[u8]>) -> String {
    attr.map(|x| {
        let (mode, len) = fastvint::decode_vu32_slice(x);
        let mode = if len > 0 { mode } else { 0o644 };
        // Extract just the permission bits (lower 9 bits)
        let acl = (mode & 0o777) as u16;
        let mut s = String::with_capacity(9);

        macro_rules! add {
            ($bit:expr, $char:expr) => {
                if (acl & $bit) > 0 {
                    s.push($char);
                } else {
                    s.push('-');
                }
            };
        }

        add!(0b1_0000_0000, 'r');
        add!(0b0_1000_0000, 'w');
        add!(0b0_0100_0000, 'x');
        add!(0b0_0010_0000, 'r');
        add!(0b0_0001_0000, 'w');
        add!(0b0_0000_1000, 'x');
        add!(0b0_0000_0100, 'r');
        add!(0b0_0000_0010, 'w');
        add!(0b0_0000_0001, 'x');

        s
    })
    .unwrap_or_else(|| "-".into())
}

/// Format a BoxPath for display
pub fn format_path(box_path: &BoxPath, is_dir: bool) -> String {
    use box_format::path::PATH_PLATFORM_SEP;

    let mut path: String = "[".into();
    path.push_str(&box_path.to_string());
    if is_dir {
        path.push_str(PATH_PLATFORM_SEP);
    }
    path.push(']');
    path
}

/// Format file size in human-readable form
pub fn format_size(bytes: u64) -> String {
    use humansize::{BINARY, FormatSize};
    bytes.format_size(BINARY)
}
