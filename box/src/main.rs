// Copyright (c) 2019-2020  Brendan Molloy <brendan@bbqsrc.net>
// Licensed under the EUPL 1.2 or later. See LICENSE file.

use std::collections::{HashMap, HashSet};
use std::io::{BufReader, BufWriter, Read, Write};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use box_format::{
    path::PATH_PLATFORM_SEP, BoxFileReader, BoxFileWriter, BoxPath, Compression, Record,
};
use byteorder::{LittleEndian, ReadBytesExt};
use jwalk::{ClientState, DirEntry};
use structopt::{clap::AppSettings::*, StructOpt};

type Result<T> = std::result::Result<T, Error>;

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

#[cfg(windows)]
use std::os::windows::fs::MetadataExt;

#[cfg(windows)]
mod winapi {
    pub const FILE_ATTRIBUTE_HIDDEN: u32 = 2;
}

const SELF_EXTRACTOR_BIN: &'static [u8] = include_bytes!(env!("SELFEXTRACT_PATH"));
const DIVIDER_UUID: u128 = 0xaae8ea9c35484ee4bf28f1a25a6b3c6c;

fn parse_compression(src: &str) -> std::result::Result<Compression, Error> {
    let compression = match src {
        "stored" => Compression::Stored,
        "brotli" => Compression::Brotli,
        "deflate" => Compression::Deflate,
        "zstd" | "zstandard" => Compression::Zstd,
        "xz" => Compression::Xz,
        "snappy" => Compression::Snappy,
        _ => {
            return Err(Error::UnknownCompressionFormat {
                name: src.to_string(),
            })
        }
    };

    Ok(compression)
}

#[inline(always)]
#[allow(dead_code)] // used in Commands
fn stored() -> Compression {
    Compression::Stored
}

#[derive(Debug, StructOpt)]
enum Commands {
    #[structopt(
        name = "l",
        alias = "list",
        about = "List files of an archive [aliases: list]"
    )]
    List {
        #[structopt(
            name = "boxfile",
            parse(from_os_str),
            help = "Path to the .box archive"
        )]
        path: PathBuf,
    },

    #[structopt(
        name = "cs",
        alias = "create-selfextracting",
        about = "Create a new create-selfextracting archive [aliases: create-selfextracting]"
    )]
    CreateSelfExtracting {
        #[structopt(
            short = "c",
            help = "Shell exec string to run upon self-extraction (ad-hoc installers, etc)"
        )]
        exec_cmd: Option<String>,

        #[structopt(short, long, help = "Recursively handle provided paths")]
        recursive: bool,

        #[structopt(short = "H", long = "hidden", help = "Allow adding hidden files")]
        allow_hidden: bool,

        #[structopt(
            name = "boxfile",
            parse(from_os_str),
            help = "Path to the .box archive"
        )]
        path: PathBuf,
    },

    #[structopt(
        name = "c",
        alias = "create",
        about = "Create a new archive [aliases: create]"
    )]
    Create {
        #[structopt(
            short = "A",
            long,
            help = "Align inserted records by specified bytes [unsigned 64-bit int, default: none]"
        )]
        alignment: Option<NonZeroU64>,

        #[structopt(
            short = "C",
            long,
            parse(try_from_str = parse_compression),
            hide_default_value = true,
            default_value = "stored",
            possible_values = Compression::available_variants(),
            help = "Compression to be used for a file [default: stored]"
        )]
        compression: Compression,

        #[structopt(short, long, help = "Recursively handle provided paths")]
        recursive: bool,

        #[structopt(short = "H", long = "hidden", help = "Allow adding hidden files")]
        allow_hidden: bool,

        #[structopt(
            name = "boxfile",
            parse(from_os_str),
            help = "Path to the .box archive"
        )]
        path: PathBuf,
    },

    #[structopt(
        name = "x",
        alias = "extract",
        about = "Extract files from an archive [aliases: extract]"
    )]
    Extract {
        #[structopt(
            short = "o",
            long = "output",
            name = "output",
            parse(from_os_str),
            help = "Output directory"
        )]
        output_path: Option<PathBuf>,

        #[structopt(
            name = "boxfile",
            parse(from_os_str),
            help = "Path to the .box archive"
        )]
        path: PathBuf,
    },

    #[structopt(
        name = "t",
        alias = "test",
        about = "Test and verify integrity of archive [aliases: test]"
    )]
    Test {
        #[structopt(
            name = "boxfile",
            parse(from_os_str),
            help = "Path to the .box archive"
        )]
        path: PathBuf,
    },
}

#[derive(Debug, StructOpt)]
#[structopt(
    name = "box",
    about = "Brendan Molloy <https://github.com/bbqsrc/box>\nCreate, modify and extract box archives.",
    settings = &[SubcommandRequiredElseHelp, DisableHelpSubcommand, VersionlessSubcommands],
    usage = "box (c|l|t|x) [FLAGS|OPTIONS] <boxfile> [files]..."
)]
struct CliOpts {
    #[structopt(short, long, help = "Show verbose output", global = true)]
    verbose: bool,

    #[structopt(subcommand)]
    cmd: Commands,

    #[structopt(
        name = "files",
        parse(from_os_str),
        help = "Selected files/directories to extract, list or add to an archive",
        global = true
    )]
    selected_files: Vec<PathBuf>,
}

macro_rules! add {
    ($ident:ident, $value:tt => $s:ident) => {
        if $ident {
            $s.push($value);
        } else {
            $s.push('-');
        }
    };
}

#[inline(always)]
fn format_path(box_path: &BoxPath, is_dir: bool) -> String {
    let mut path: String = "[".into();
    path.push_str(&box_path.to_string());
    if is_dir {
        path.push_str(PATH_PLATFORM_SEP);
    }
    path.push_str("]");
    path
}

#[inline(always)]
fn from_acl_u16(acl: u16) -> String {
    let or = (acl & 0b1_0000_0000) > 0;
    let ow = (acl & 0b0_1000_0000) > 0;
    let ox = (acl & 0b0_0100_0000) > 0;
    let gr = (acl & 0b0_0010_0000) > 0;
    let gw = (acl & 0b0_0001_0000) > 0;
    let gx = (acl & 0b0_0000_1000) > 0;
    let ar = (acl & 0b0_0000_0100) > 0;
    let aw = (acl & 0b0_0000_0010) > 0;
    let ax = (acl & 0b0_0000_0001) > 0;

    let mut s = String::new();
    add!(or, 'r' => s);
    add!(ow, 'w' => s);
    add!(ox, 'x' => s);
    add!(gr, 'r' => s);
    add!(gw, 'w' => s);
    add!(gx, 'x' => s);
    add!(ar, 'r' => s);
    add!(aw, 'w' => s);
    add!(ax, 'x' => s);

    s
}

#[inline(always)]
fn time(attr: Option<&[u8]>) -> String {
    attr.and_then(|mut x| x.read_u64::<LittleEndian>().ok())
        .map(|x| std::time::UNIX_EPOCH + std::time::Duration::new(x, 0))
        .map(|x| {
            let datetime: chrono::DateTime<chrono::Utc> = x.into();
            datetime.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        })
        .unwrap_or_else(|| "-".into())
}

#[inline(always)]
fn unix_acl(attr: Option<&[u8]>) -> String {
    attr.map(|x| from_acl_u16(u16::from_le_bytes([x[0], x[1]])))
        .unwrap_or_else(|| "-".into())
}

fn list(path: &Path, _selected_files: Vec<PathBuf>, verbose: bool) -> Result<()> {
    use humansize::{file_size_opts as options, FileSize};

    let bf = BoxFileReader::open(path).map_err(|source| Error::CannotOpenArchive {
        path: path.to_path_buf(),
        source,
    })?;
    let metadata = bf.metadata();

    if verbose {
        println!("{:#?}", metadata);
        return Ok(());
    }

    let alignment = match bf.alignment() {
        0 => "None".into(),
        v => format!("{} bytes", v),
    };
    println!("Box archive: {} (alignment: {})", path.display(), alignment);
    println!("-------------  -------------  -------------  ---------------------  ----------  ---------  --------");
    println!(" Method         Compressed     Length         Created                Attrs       CRC32      Path");
    println!("-------------  -------------  -------------  ---------------------  ----------  ---------  --------");
    for result in bf.metadata().iter() {
        let record = result.record;

        let acl = unix_acl(record.attr(bf.metadata(), "unix.mode"));
        let time = time(record.attr(bf.metadata(), "created"));
        let path = format_path(&result.path, record.as_directory().is_some());

        match record {
            Record::Directory(_) => {
                println!(
                    " {:12}  {:>12}   {:>12}   {:<20}   {:<9}   {:>8}   {}",
                    "<directory>", "-", "-", time, acl, "-", path,
                );
            }
            Record::Link(link_record) => {
                let target = format_path(
                    &link_record.target,
                    bf.resolve_link(&link_record)
                        .map(|x| x.record.as_directory().is_some())
                        .unwrap_or(false),
                );

                println!(
                    " {:12}  {:>12}   {:>12}   {:<20}   {:<9}   {:>8}   {} -> {}",
                    "<link>", "-", "-", time, acl, "-", path, target,
                );
            }
            Record::File(record) => {
                let length = record.length.file_size(options::BINARY).unwrap();
                let decompressed_length = record
                    .decompressed_length
                    .file_size(options::BINARY)
                    .unwrap();
                let crc32 = record
                    .attr(bf.metadata(), "crc32")
                    .map(|x| Some(u32::from_le_bytes([x[0], x[1], x[2], x[3]])))
                    .unwrap_or(None)
                    .map(|x| format!("{:x}", x))
                    .unwrap_or_else(|| "-".to_string());

                println!(
                    " {:12}  {:>12}   {:>12}   {:<20}   {:<9}   {:>8}   {}",
                    format!("{:?}", record.compression),
                    length,
                    decompressed_length,
                    time,
                    acl,
                    crc32,
                    path,
                );
            }
        }
    }

    Ok(())
}

fn extract(
    path: &Path,
    output_path: &Path,
    _selected_files: Vec<PathBuf>,
    _verbose: bool,
) -> Result<()> {
    println!("{} {}", path.display(), output_path.display());
    let bf = BoxFileReader::open(path).map_err(|source| Error::CannotOpenArchive {
        path: path.to_path_buf(),
        source,
    })?;
    bf.extract_all(output_path)
        .map_err(|source| Error::CannotExtractFiles { source })
}

type ParentDirs = (BoxPath, HashMap<String, Vec<u8>>);

fn collect_parent_directories<P: AsRef<Path>>(path: P) -> Result<Vec<ParentDirs>> {
    let box_path = BoxPath::new(&path).map_err(|source| Error::CannotHandlePath {
        path: path.as_ref().to_path_buf(),
        source,
    })?;
    let levels = box_path.depth();

    let path = match path.as_ref().parent() {
        Some(v) => v,
        None => return Ok(vec![]),
    };

    let mut v = path
        .ancestors()
        .take(levels)
        .map(|path: &Path| {
            Ok((
                BoxPath::new(path).map_err(|source| Error::CannotHandlePath {
                    path: path.to_path_buf(),
                    source,
                })?,
                metadata(
                    &path
                        .metadata()
                        .map_err(|source| Error::CannotReadFileMetadata {
                            path: path.to_path_buf(),
                            source,
                        })?,
                ),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    v.reverse();
    Ok(v)
}

#[cfg(unix)]
#[inline(always)]
fn metadata(meta: &std::fs::Metadata) -> HashMap<String, Vec<u8>> {
    let mut attrs = HashMap::new();

    macro_rules! attr {
        ($map:ident, $name:expr, $data:expr) => {
            $map.insert($name.into(), $data.to_le_bytes().to_vec())
        };
    }

    attr!(attrs, "created", meta.ctime());
    attr!(attrs, "modified", meta.mtime());
    attr!(attrs, "accessed", meta.atime());
    attr!(attrs, "unix.mode", meta.mode());
    attr!(attrs, "unix.uid", meta.uid());
    attr!(attrs, "unix.gid", meta.gid());

    attrs
}

#[cfg(not(unix))]
#[inline(always)]
fn metadata(meta: &std::fs::Metadata) -> HashMap<String, Vec<u8>> {
    let mut attrs = HashMap::new();

    macro_rules! attr_systime {
        ($map:ident, $name:expr, $data:expr) => {
            if let Ok(value) = $data {
                let bytes = value
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    .to_le_bytes()
                    .to_vec();

                $map.insert($name.into(), bytes);
            }
        };
    }

    attr_systime!(attrs, "created", meta.created());
    attr_systime!(attrs, "modified", meta.modified());
    attr_systime!(attrs, "accessed", meta.accessed());

    attrs
}

#[inline(always)]
#[cfg(not(windows))]
fn is_hidden<C: ClientState>(entry: &DirEntry<C>) -> bool {
    entry
        .file_name
        .to_str()
        .map(|s| s.starts_with('.'))
        .unwrap_or(false)
}

#[inline(always)]
#[cfg(windows)]
fn is_hidden<C: ClientState>(entry: &DirEntry<C>) -> bool {
    match entry.metadata().ok() {
        Some(m) => (m.file_attributes() & winapi::FILE_ATTRIBUTE_HIDDEN) != 0,
        None => false,
    }
}

struct Crc32Reader<R: Read> {
    inner: R,
    hasher: crc32fast::Hasher,
}

impl<R: Read> Crc32Reader<R> {
    pub fn new(inner: R) -> Crc32Reader<R> {
        Crc32Reader {
            inner,
            hasher: crc32fast::Hasher::new(),
        }
    }

    pub fn finalize(self) -> u32 {
        self.hasher.finalize()
    }
}

impl<R: Read> Read for Crc32Reader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.hasher.update(&buf);
        self.inner.read(buf)
    }
}

#[allow(clippy::too_many_arguments)]
#[inline(always)]
fn process_files<I: Iterator<Item = PathBuf>>(
    iter: I,
    recursive: bool,
    allow_hidden: bool,
    verbose: bool,
    compression: Compression,
    mut bf: BoxFileWriter,
    mut known_dirs: HashSet<BoxPath>,
    mut known_files: HashSet<BoxPath>,
) -> Result<()> {
    let iter = iter.flat_map(|path| {
        let mut walker = jwalk::WalkDir::new(&path).sort(true);
        if !recursive {
            walker = walker.parallelism(jwalk::Parallelism::Serial).max_depth(0);
        }
        if !allow_hidden {
            walker = walker.process_read_dir(|_, e| {
                e.retain(|entry| match entry {
                    Ok(v) => !is_hidden(&v),
                    _ => true,
                });
            });
        }
        walker.into_iter()
    });

    for entry in iter {
        let entry = entry.map_err(|source| Error::CannotProcessDirEntry { source })?;
        let file_type = entry.file_type;
        let file_path = entry.parent_path.join(&entry.file_name);
        let meta = entry
            .metadata()
            .map_err(|source| Error::CannotProcessFile {
                path: file_path.to_path_buf(),
                source,
            })?;
        tracing::debug!("File Path: {:?}", &file_path);
        let canonical_dir = std::env::current_dir()
            .map_err(|source| Error::CannotGetCurrentDir { source })
            .and_then(|p| {
                p.join(&entry.parent_path).canonicalize().map_err(|source| {
                    Error::CannotCanonicalizePath {
                        path: file_path.to_path_buf(),
                        source,
                    }
                })
            })?;
        let canonical_path = canonical_dir.join(&entry.file_name);
        tracing::debug!("Canonical Path: {:?}", &canonical_path);

        if bf.path() == canonical_path {
            continue;
        }

        let parents = collect_parent_directories(&*file_path)
            .map_err(Box::new)
            .map_err(|source| Error::CannotProcessParents {
                path: file_path.clone(),
                source,
            })?;
        let box_path = BoxPath::new(&file_path).map_err(|source| Error::CannotHandlePath {
            path: file_path.to_path_buf(),
            source,
        })?;

        for (parent, meta) in parents.into_iter() {
            if !known_dirs.contains(&parent) {
                bf.mkdir(parent.clone(), meta)
                    .map_err(|source| Error::CannotCreateDirectory {
                        path: parent.clone(),
                        source,
                    })?;
                known_dirs.insert(parent);
            }
        }

        if file_type.is_symlink() {
            let target_path =
                std::fs::read_link(&file_path).map_err(|source| Error::CannotReadLink {
                    path: file_path.to_path_buf(),
                    source,
                })?;
            tracing::trace!("XXX {:?}", &target_path);

            // Get relative path from current ref
            let target_path = entry.parent_path.join(&target_path);
            tracing::trace!("{:?}", &target_path);

            // Ensure it's not also a symlink
            let _ = std::fs::canonicalize(&target_path).map_err(|source| {
                Error::CannotCanonicalizePath {
                    path: file_path.to_path_buf(),
                    source,
                }
            })?;
            tracing::trace!("{:?} {:?}", &target_path, &canonical_path);

            let target_path =
                BoxPath::new(&target_path).map_err(|source| Error::CannotHandlePath {
                    path: target_path.to_path_buf(),
                    source,
                })?;

            tracing::trace!("XXX {:?}", &target_path);

            if file_type.is_dir() {
                if !known_dirs.contains(&box_path) {
                    if verbose {
                        println!("{} -> {} (link)", &file_path.display(), &target_path);
                    }
                    bf.link(box_path.clone(), target_path, metadata(&meta))
                        .map_err(|source| Error::CannotCreateLink {
                            path: box_path.clone(),
                            source,
                        })?;
                    known_dirs.insert(box_path);
                }
            } else if !known_files.contains(&box_path) {
                if verbose {
                    println!("{} -> {} (link)", &file_path.display(), &target_path);
                }
                bf.link(box_path.clone(), target_path, metadata(&meta))
                    .map_err(|source| Error::CannotCreateLink {
                        path: box_path.clone(),
                        source,
                    })?;
                known_files.insert(box_path);
            }
        } else if file_type.is_dir() {
            if !known_dirs.contains(&box_path) {
                if verbose {
                    println!("{} (directory)", &file_path.display());
                }
                let mut dir_meta = metadata(&meta);
                #[cfg(unix)]
                {
                    let mode = std::fs::metadata(file_path).unwrap().mode();
                    dir_meta.insert("unix.mode".to_string(), mode.to_le_bytes().to_vec());
                }
                bf.mkdir(box_path.clone(), dir_meta)
                    .map_err(|source| Error::CannotCreateDirectory {
                        path: box_path.clone(),
                        source,
                    })?;
                known_dirs.insert(box_path);
            }
        } else if !known_files.contains(&box_path) {
            let file = std::fs::File::open(&file_path).map_err(|source| Error::CannotOpenFile {
                path: file_path.to_path_buf(),
                source,
            })?;
            let mut file_meta = metadata(&meta);
            
            #[cfg(unix)]
            {
                let mode = file.metadata().unwrap().mode();
                file_meta.insert("unix.mode".to_string(), mode.to_le_bytes().to_vec());
            }

            let mut file = BufReader::new(Crc32Reader::new(file));
            let record = bf
                .insert(compression, box_path.clone(), &mut file, file_meta)
                .map_err(|source| Error::CannotAddFile {
                    path: file_path.to_path_buf(),
                    source,
                })?;
            if verbose {
                let len = if record.decompressed_length == 0 {
                    100.0f64
                } else {
                    100.0 - (record.length as f64 / record.decompressed_length as f64 * 100.0)
                };
                println!("{} (compressed {:.*}%)", &file_path.display(), 2, len);
            }

            let hash = file.into_inner().finalize().to_le_bytes().to_vec();
            bf.set_attr(&box_path, "crc32", hash)
                .map_err(|source| Error::CannotAddChecksum {
                    path: file_path,
                    source,
                })?;

            known_files.insert(box_path);
        }
    }

    let path = bf.path().to_path_buf();
    bf.finish()
        .map_err(|source| Error::CannotCreateFile { path, source })
        .map(|_| {})
}

fn create(
    mut path: PathBuf,
    selected_files: Vec<PathBuf>,
    compression: Compression,
    recursive: bool,
    allow_hidden: bool,
    verbose: bool,
    alignment: Option<NonZeroU64>,
    is_self_extracting: bool,
    exec_cmd: Option<String>,
) -> Result<()> {
    let original_path = path.clone();

    // if is_self_extracting {
    path.set_file_name(format!(
        "{}.tmp",
        Path::new(path.file_name().unwrap()).display()
    ));
    // }

    let mut bf = match alignment {
        None => BoxFileWriter::create(&path),
        Some(alignment) => BoxFileWriter::create_with_alignment(&path, alignment.get()),
    }
    .map_err(|source| Error::CannotCreateArchive {
        path: path.to_path_buf(),
        source,
    })?;

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .to_le_bytes();

    bf.set_file_attr("created", now.to_vec())
        .map_err(|source| Error::CannotSetAttribute {
            key: "created".to_string(),
            value: now.to_vec(),
            source,
        })?;

    if let Some(exec_str) = exec_cmd {
        bf.set_file_attr("box.exec", exec_str.as_bytes().to_vec()).unwrap();
    }

    process_files(
        selected_files.into_iter(),
        recursive,
        allow_hidden,
        verbose,
        compression,
        bf,
        HashSet::new(),
        HashSet::new(),
    )
    .map_err(Box::new)
    .map_err(|source| Error::CannotAddFiles {
        path: path.to_path_buf(),
        source,
    })?;

    if is_self_extracting {
        let tmp_path = path;
        let stem = tmp_path.file_stem().unwrap();
        let mut path = tmp_path.to_path_buf();

        #[cfg(unix)]
        path.set_file_name(stem);
        #[cfg(windows)]
        path.set_file_name(format!("{}.exe", Path::new(stem).display()));

        let mut file = std::fs::OpenOptions::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            file.mode(0o755);
        }

        let file = file.create(true).write(true).open(&path).unwrap();
        let mut writer = BufWriter::new(file);

        let tmp_file = std::fs::File::open(&tmp_path).unwrap();
        let mut reader = BufReader::new(tmp_file);

        writer.write_all(&SELF_EXTRACTOR_BIN).unwrap();
        writer.write_all(&DIVIDER_UUID.to_le_bytes()).unwrap();
        std::io::copy(&mut reader, &mut writer).unwrap();

        drop(reader);
        drop(writer);
        std::fs::remove_file(tmp_path).unwrap();
    } else {
        std::fs::rename(path, original_path).unwrap();
    }

    Ok(())
}

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let opts = CliOpts::from_iter(wild::args_os());

    match opts.cmd {
        Commands::List { path } => list(&path, opts.selected_files, opts.verbose),
        Commands::Extract { path, output_path } => extract(
            &path,
            &output_path.unwrap_or_else(|| std::env::current_dir().expect("no pwd")),
            opts.selected_files,
            opts.verbose,
        ),
        Commands::Create {
            path,
            alignment,
            compression,
            recursive,
            allow_hidden,
        } => create(
            path,
            opts.selected_files,
            compression,
            recursive,
            allow_hidden,
            opts.verbose,
            alignment,
            false,
            None,
        ),
        Commands::CreateSelfExtracting {
            path,
            recursive,
            allow_hidden,
            exec_cmd,
        } => create(
            path,
            opts.selected_files,
            Compression::Zstd,
            recursive,
            allow_hidden,
            opts.verbose,
            None,
            true,
            exec_cmd,
        ),
        Commands::Test { .. } => unimplemented!(),
    }
}

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("Unknown compression method `{name}`")]
    UnknownCompressionFormat { name: String },

    #[error("Cannot handle path `{}`", .path.display())]
    CannotHandlePath {
        path: PathBuf,
        #[source]
        source: box_format::path::IntoBoxPathError,
    },

    #[error("Cannot open archive `{}`", .path.display())]
    CannotOpenArchive {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("Cannot open file `{}`", path.display())]
    CannotOpenFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("Cannot create directory `{}`", path)]
    CannotCreateDirectory {
        path: BoxPath,
        #[source]
        source: std::io::Error,
    },

    #[error("Cannot create link `{}`", path)]
    CannotCreateLink {
        path: BoxPath,
        #[source]
        source: std::io::Error,
    },

    #[error("Cannot read metadata of file `{}`", path.display())]
    CannotReadFileMetadata {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("Cannot add file to archive `{}`", path.display())]
    CannotAddFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("Cannot create file `{}`", path.display())]
    CannotCreateFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("Cannot add checksum for file `{}`", path.display())]
    CannotAddChecksum {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("Cannot create archive `{}`", path.display())]
    CannotCreateArchive {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("Cannot process parents `{}`", path.display())]
    CannotProcessParents {
        path: PathBuf,
        #[source]
        source: Box<Error>,
    },

    #[error("Cannot add files to archive `{}`", .path.display())]
    CannotAddFiles {
        path: PathBuf,
        #[source]
        source: Box<Error>,
    },

    #[error("Cannot directory entry")]
    CannotProcessDirEntry {
        #[source]
        source: jwalk::Error,
    },

    #[error("Cannot process file `{}`", .path.display())]
    CannotProcessFile {
        path: PathBuf,
        #[source]
        source: jwalk::Error,
    },

    #[error("Cannot canonicalize path `{}`", .path.display())]
    CannotCanonicalizePath {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("Cannot read link  `{}`", .path.display())]
    CannotReadLink {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("Cannot extract files")]
    CannotExtractFiles {
        #[source]
        source: std::io::Error,
    },

    #[error("Cannot set attribute `{key}` to value `{value:x?}`")]
    CannotSetAttribute {
        key: String,
        value: Vec<u8>,
        #[source]
        source: std::io::Error,
    },

    #[error("Cannot get current directory`")]
    CannotGetCurrentDir {
        #[source]
        source: std::io::Error,
    },
}
