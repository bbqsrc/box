// Copyright (c) 2019  Brendan Molloy <brendan@bbqsrc.net>
// Licensed under the EUPL 1.2 or later. See LICENSE file.

use std::collections::{HashMap, HashSet};
use std::io::{BufReader, Read};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};

use box_format::{
    path::PATH_PLATFORM_SEP, BoxFileReader, BoxFileWriter, BoxPath, Compression, Record,
};
use byteorder::{LittleEndian, ReadBytesExt};
use jwalk::DirEntry;
use snafu::ResultExt;
use structopt::StructOpt;

type Result<T> = std::result::Result<T, Error>;

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(windows)]
use std::os::windows::fs::MetadataExt;

#[cfg(windows)]
mod winapi {
    pub const FILE_ATTRIBUTE_HIDDEN: u32 = 2;
}

fn parse_compression(src: &str) -> std::result::Result<Compression, Error> {
    let compression = match src {
        "stored" => Compression::Stored,
        "brotli" => Compression::Brotli,
        "deflate" => Compression::Deflate,
        "zstd" | "zstandard" => Compression::Zstd,
        "xz" => Compression::Xz,
        "snappy" => Compression::Snappy,
        _ => {
            return UnknownCompressionFormat {
                name: src.to_string(),
            }
            .fail()
        }
    };

    Ok(compression)
}

use structopt::clap::AppSettings::*;

#[inline(always)]
#[allow(dead_code)] // used in Commands
fn stored() -> Compression {
    Compression::Stored
}

#[derive(Debug, StructOpt)]
enum Commands {
    #[structopt(
        name = "a",
        visible_alias = "append",
        about = "Append files to an existing archive"
    )]
    Append {
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

    #[structopt(name = "l", visible_alias = "list", about = "List files of an archive")]
    List {
        #[structopt(
            name = "boxfile",
            parse(from_os_str),
            help = "Path to the .box archive"
        )]
        path: PathBuf,
    },

    #[structopt(name = "c", visible_alias = "create", about = "Create a new archive")]
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
        visible_alias = "extract",
        about = "Extract files from an archive"
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
        visible_alias = "test",
        about = "Test and verify integrity of archive"
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
    usage = "box (a|c|l|t|x) [FLAGS|OPTIONS] <boxfile> [files]..."
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

fn append(
    path: PathBuf,
    selected_files: Vec<PathBuf>,
    compression: Compression,
    recursive: bool,
    allow_hidden: bool,
    verbose: bool,
) -> Result<()> {
    // let bf = BoxFileWriter::open(&path).context(CannotOpenArchive { path: &path })?;

    // let (known_dirs, known_files) = {
    //     (
    //         bf.metadata()
    //             .records()
    //             .iter()
    //             .filter_map(|x| x.as_directory())
    //             .map(|r| r.path.clone())
    //             .collect::<std::collections::HashSet<_>>(),
    //         bf.metadata()
    //             .records()
    //             .iter()
    //             .filter_map(|x| x.as_file())
    //             .map(|r| r.path.clone())
    //             .collect::<std::collections::HashSet<_>>(),
    //     )
    // };

    // process_files(
    //     selected_files.into_iter(),
    //     recursive,
    //     allow_hidden,
    //     verbose,
    //     compression,
    //     bf,
    //     known_dirs,
    //     known_files,
    // )
    // .map_err(Box::new)
    // .context(CannotAddFiles { path: &path })?;

    // Ok(())
    todo!()
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
fn time(attr: Option<&Vec<u8>>) -> String {
    attr.and_then(|x| x.as_slice().read_u64::<LittleEndian>().ok())
        .map(|x| std::time::UNIX_EPOCH + std::time::Duration::new(x, 0))
        .map(|x| {
            let datetime: chrono::DateTime<chrono::Utc> = x.into();
            datetime.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        })
        .unwrap_or_else(|| "-".into())
}

#[inline(always)]
fn unix_acl(attr: Option<&Vec<u8>>) -> String {
    attr.map(|x| from_acl_u16(u16::from_le_bytes([x[0], x[1]])))
        .unwrap_or_else(|| "-".into())
}

fn list(path: &Path, _selected_files: Vec<PathBuf>, verbose: bool) -> Result<()> {
    use humansize::{file_size_opts as options, FileSize};

    let bf = BoxFileReader::open(path).context(CannotOpenArchive { path })?;
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
    for result in bf.iter() {
        let record = result.record;

        let acl = unix_acl(record.attr(&bf, "unix.mode"));
        let time = time(record.attr(&bf, "created"));
        let path = format_path(&result.path, record.as_directory().is_some());

        match record {
            Record::Directory(_) => {
                println!(
                    " {:12}  {:>12}   {:>12}   {:<20}   {:<9}   {:>8}   {}",
                    "<directory>", "-", "-", time, acl, "-", path,
                );
            }
            Record::Link(link_record) => {
                // let target = format_path(
                //     &link_record.target,
                //     bf.resolve_link(&link_record)
                //         .map(|x| x.as_directory().is_some())
                //         .unwrap_or(false),
                // );

                // println!(
                //     " {:12}  {:>12}   {:>12}   {:<20}   {:<9}   {:>8}   {} -> {}",
                //     "<link>", "-", "-", time, acl, "-", path, target,
                // );
            }
            Record::File(record) => {
                let length = record.length.file_size(options::BINARY).unwrap();
                let decompressed_length = record
                    .decompressed_length
                    .file_size(options::BINARY)
                    .unwrap();
                let crc32 = record
                    .attr(&bf, "crc32")
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
    verbose: bool,
) -> Result<()> {
    println!("{} {}", path.display(), output_path.display());
    let bf = BoxFileReader::open(path).context(CannotOpenArchive { path })?;
    Ok(bf.extract_all(output_path).unwrap())
}

type ParentDirs = (BoxPath, HashMap<String, Vec<u8>>);

fn collect_parent_directories<P: AsRef<Path>>(path: P) -> Result<Vec<ParentDirs>> {
    let box_path = BoxPath::new(&path).context(CannotHandlePath {
        path: path.as_ref(),
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
                BoxPath::new(path).context(CannotHandlePath { path: &path })?,
                metadata(
                    &path
                        .metadata()
                        .context(CannotReadFileMetadata { path: &path })?,
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
fn is_hidden(entry: &DirEntry) -> bool {
    entry
        .file_name
        .to_str()
        .map(|s| s.starts_with('.'))
        .unwrap_or(false)
}

#[cfg(windows)]
fn is_hidden(entry: &DirEntry) -> bool {
    match entry.metadata.as_ref() {
        Some(m) => m
            .as_ref()
            .map(|m| (m.file_attributes() & winapi::FILE_ATTRIBUTE_HIDDEN) != 0)
            .unwrap_or(false),
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
        let mut walker = jwalk::WalkDir::new(&path).sort(true).preload_metadata(true);
        if !recursive {
            walker = walker.num_threads(1).max_depth(0);
        }
        if !allow_hidden {
            walker = walker.process_entries(|e| {
                e.retain(|entry| match entry {
                    Ok(v) => !is_hidden(&v),
                    _ => true,
                });
            });
        }
        walker.into_iter()
    });

    for entry in iter {
        let entry = entry.context(CannotProcessFile)?;
        let file_type = entry.file_type.context(CannotProcessFile)?;
        let meta = entry
            .metadata
            .expect("read file metadata")
            .context(CannotProcessFile)?;
        println!("Path: {:?}", entry.parent_spec.path);
        let file_path = entry.parent_spec.path.join(&entry.file_name);
        println!("File Path: {:?}", &file_path);
        let canonical_dir = std::env::current_dir()
            .unwrap()
            .join(&entry.parent_spec.path)
            .canonicalize()
            .context(CannotProcessFile)?;
        let canonical_path = canonical_dir.join(&entry.file_name);
        println!("File Path: {:?}", &file_path);

        if bf.path() == canonical_path {
            continue;
        }

        let parents = collect_parent_directories(&*file_path)
            .map_err(Box::new)
            .with_context(|| CannotProcessParents {
                path: file_path.clone(),
            })?;
        let box_path = BoxPath::new(&file_path).context(CannotHandlePath { path: &file_path })?;

        for (parent, meta) in parents.into_iter() {
            if !known_dirs.contains(&parent) {
                bf.mkdir(parent.clone(), meta)
                    .with_context(|| CannotCreateDirectory {
                        path: parent.clone(),
                    })?;
                known_dirs.insert(parent);
            }
        }

        if file_type.is_symlink() {
            let target_path = std::fs::read_link(&file_path).context(CannotProcessFile)?;
            println!("XXX {:?}", &target_path);

            // Get relative path from current ref
            let target_path = entry.parent_spec.path.join(&target_path);
            println!("{:?}", &target_path);

            // Ensure it's not also a symlink
            let _ = std::fs::canonicalize(&target_path).unwrap();
            println!("{:?} {:?}", &target_path, &canonical_path);

            // let target_path = pathdiff::diff_paths(&target_path, &canonical_path).unwrap();
            // println!("YYY {:?}", &target_path);
            // println!("PPP {:?}", entry.parent_spec.path);

            let target_path =
                BoxPath::new(&target_path).context(CannotHandlePath { path: &target_path })?;
            // println!("ZZZ {:?}", &archive_base_path);
            // let target_path = pathdiff::diff_paths(canonical_path.join(&target_path), &archive_base_path).unwrap();

            println!("XXX {:?}", &target_path);
            // let target_box_path = match BoxPath::new(&target_path) {
            //     Ok(v) => v,
            //     Err(e) => { eprintln!("{:?}", e); panic!(); }
            // };

            if file_type.is_dir() {
                if !known_dirs.contains(&box_path) {
                    if verbose {
                        println!("{} -> {} (link)", &file_path.display(), &target_path);
                    }
                    bf.link(box_path.clone(), target_path, metadata(&meta))
                        .with_context(|| CannotCreateLink {
                            path: box_path.clone(),
                        })?;
                    known_dirs.insert(box_path);
                }
            } else {
                if !known_files.contains(&box_path) {
                    if verbose {
                        println!("{} -> {} (link)", &file_path.display(), &target_path);
                    }
                    bf.link(box_path.clone(), target_path, metadata(&meta))
                        .with_context(|| CannotCreateLink {
                            path: box_path.clone(),
                        })?;
                    known_files.insert(box_path);
                }
            }
        } else if file_type.is_dir() {
            if !known_dirs.contains(&box_path) {
                if verbose {
                    println!("{} (directory)", &file_path.display());
                }
                bf.mkdir(box_path.clone(), metadata(&meta))
                    .with_context(|| CannotCreateDirectory {
                        path: box_path.clone(),
                    })?;
                known_dirs.insert(box_path);
            }
        } else if !known_files.contains(&box_path) {
            let file =
                std::fs::File::open(&file_path).context(CannotOpenFile { path: &file_path })?;
            let mut file = BufReader::new(Crc32Reader::new(file));
            let record = bf
                .insert(compression, box_path.clone(), &mut file, metadata(&meta))
                .context(CannotAddFile { path: &file_path })?;
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
                .context(CannotAddChecksum { path: &file_path })?;

            known_files.insert(box_path);
        }
    }

    bf.finish().unwrap();

    Ok(())
}

fn create(
    path: PathBuf,
    selected_files: Vec<PathBuf>,
    compression: Compression,
    recursive: bool,
    allow_hidden: bool,
    verbose: bool,
    alignment: Option<NonZeroU64>,
) -> Result<()> {
    let bf = match alignment {
        None => BoxFileWriter::create(&path),
        Some(alignment) => BoxFileWriter::create_with_alignment(&path, alignment.get()),
    }
    .context(CannotCreateArchive { path: &path })?;

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
    .context(CannotAddFiles { path: &path })?;

    Ok(())
}

fn main() -> Result<()> {
    let opts = CliOpts::from_args();

    match opts.cmd {
        Commands::Append {
            path,
            compression,
            recursive,
            allow_hidden,
        } => append(
            path,
            opts.selected_files,
            compression,
            recursive,
            allow_hidden,
            opts.verbose,
        ),
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
        ),
        Commands::Test { .. } => unimplemented!(),
    }
}

#[derive(snafu::Snafu, snafu_cli_debug::SnafuCliDebug)]
enum Error {
    #[snafu(display("Unknown compression method `{}`", name))]
    UnknownCompressionFormat {
        name: String,
        backtrace: snafu::Backtrace,
    },
    #[snafu(display("Cannot handle path `{}`", path.display()))]
    CannotHandlePath {
        path: PathBuf,
        source: box_format::path::IntoBoxPathError,
        backtrace: snafu::Backtrace,
    },
    #[snafu(display("Cannot open archive `{}`", path.display()))]
    CannotOpenArchive {
        path: PathBuf,
        source: std::io::Error,
        backtrace: snafu::Backtrace,
    },
    #[snafu(display("Cannot open file `{}`", path.display()))]
    CannotOpenFile {
        path: PathBuf,
        source: std::io::Error,
        backtrace: snafu::Backtrace,
    },
    #[snafu(display("Cannot create directory `{}`", path))]
    CannotCreateDirectory {
        path: BoxPath,
        source: std::io::Error,
        backtrace: snafu::Backtrace,
    },
    #[snafu(display("Cannot create link `{}`", path))]
    CannotCreateLink {
        path: BoxPath,
        source: std::io::Error,
        backtrace: snafu::Backtrace,
    },
    #[snafu(display("Cannot read metadata of file `{}`", path.display()))]
    CannotReadFileMetadata {
        path: PathBuf,
        source: std::io::Error,
        backtrace: snafu::Backtrace,
    },
    #[snafu(display("Cannot add file to archive `{}`", path.display()))]
    CannotAddFile {
        path: PathBuf,
        source: std::io::Error,
        backtrace: snafu::Backtrace,
    },
    #[snafu(display("Cannot create file `{}`", path.display()))]
    CannotCreateFile {
        path: PathBuf,
        source: std::io::Error,
        backtrace: snafu::Backtrace,
    },
    #[snafu(display("Cannot add checksum for file `{}`", path.display()))]
    CannotAddChecksum {
        path: PathBuf,
        source: std::io::Error,
        backtrace: snafu::Backtrace,
    },
    #[snafu(display("Cannot decompress file `{}` to `{}`", archive_path, target_path.display()))]
    CannotDecompressFile {
        archive_path: BoxPath,
        target_path: PathBuf,
        source: std::io::Error,
        backtrace: snafu::Backtrace,
    },
    #[snafu(display("Cannot create archive `{}`", path.display()))]
    CannotCreateArchive {
        path: PathBuf,
        source: std::io::Error,
        backtrace: snafu::Backtrace,
    },
    #[snafu(display("Cannot process parents `{}`", path.display()))]
    CannotProcessParents {
        path: PathBuf,
        source: Box<Error>,
        backtrace: snafu::Backtrace,
    },
    #[snafu(display("Cannot add files to archive `{}`", path.display()))]
    CannotAddFiles {
        path: PathBuf,
        source: Box<Error>,
        backtrace: snafu::Backtrace,
    },
    #[snafu(display("Cannot process file"))]
    CannotProcessFile {
        source: std::io::Error,
        backtrace: snafu::Backtrace,
    },
    #[snafu(display("Cowardly refusing to recursively archive self. `{}` is part of selected files", path.display()))]
    WillNotArchiveSelf {
        path: PathBuf,
        backtrace: snafu::Backtrace,
    },
}
mod pathdiff {
    use std::path::{Component, Path, PathBuf};

    // Copyright 2012-2015 The Rust Project Developers. See the COPYRIGHT
    // file at the top-level directory of this distribution and at
    // http://rust-lang.org/COPYRIGHT.
    //
    // Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
    // http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
    // <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
    // option. This file may not be copied, modified, or distributed
    // except according to those terms.

    // Adapted from rustc's path_relative_from
    // https://github.com/rust-lang/rust/blob/e1d0de82cc40b666b88d4a6d2c9dcbc81d7ed27f/src/librustc_back/rpath.rs#L116-L158

    /// Construct a relative path from a provided base directory path to the provided path.
    ///
    /// ```rust
    /// use pathdiff::diff_paths;
    /// use std::path::*;
    ///
    /// let baz = "/foo/bar/baz";
    /// let bar = "/foo/bar";
    /// let quux = "/foo/bar/quux";
    /// assert_eq!(diff_paths(bar, baz), Some("../".into()));
    /// assert_eq!(diff_paths(baz, bar), Some("baz".into()));
    /// assert_eq!(diff_paths(quux, baz), Some("../quux".into()));
    /// assert_eq!(diff_paths(baz, quux), Some("../baz".into()));
    /// assert_eq!(diff_paths(bar, quux), Some("../".into()));
    ///
    /// assert_eq!(diff_paths(&baz, &bar.to_string()), Some("baz".into()));
    /// assert_eq!(diff_paths(Path::new(baz), Path::new(bar).to_path_buf()), Some("baz".into()));
    /// ```
    pub fn diff_paths<P, B>(path: P, base: B) -> Option<PathBuf>
    where
        P: AsRef<Path>,
        B: AsRef<Path>,
    {
        let path = path.as_ref();
        let base = base.as_ref();

        if path.is_absolute() != base.is_absolute() {
            if path.is_absolute() {
                Some(PathBuf::from(path))
            } else {
                None
            }
        } else {
            let mut ita = path.components();
            let mut itb = base.components();
            let mut comps: Vec<Component> = vec![];
            loop {
                match (ita.next(), itb.next()) {
                    (None, None) => break,
                    (Some(a), None) => {
                        comps.push(a);
                        comps.extend(ita.by_ref());
                        break;
                    }
                    (None, _) => comps.push(Component::ParentDir),
                    (Some(a), Some(b)) if comps.is_empty() && a == b => (),
                    (Some(a), Some(b)) if b == Component::CurDir => comps.push(a),
                    (Some(_), Some(b)) if b == Component::ParentDir => return None,
                    (Some(a), Some(_)) => {
                        comps.push(Component::ParentDir);
                        for _ in itb {
                            comps.push(Component::ParentDir);
                        }
                        comps.push(a);
                        comps.extend(ita.by_ref());
                        break;
                    }
                }
            }
            Some(comps.iter().map(|c| c.as_os_str()).collect())
        }
    }
}
