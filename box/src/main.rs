use std::collections::HashMap;
use std::collections::HashSet;
use std::io::Result;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};

use box_format::{AttrMap, BoxFile, BoxPath, Compression, Record, PATH_BOX_SEP, PATH_PLATFORM_SEP};
use byteorder::{LittleEndian, ReadBytesExt};
use crc32fast::Hasher as Crc32Hasher;
use structopt::StructOpt;
use walkdir::{DirEntry, WalkDir};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

#[derive(Debug)]
struct ParseCompressionError(String);

impl std::error::Error for ParseCompressionError {}

impl std::fmt::Display for ParseCompressionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Unknown compression method: {}", self.0)
    }
}

fn parse_compression(src: &str) -> std::result::Result<Compression, ParseCompressionError> {
    let compression = match src {
        "stored" => Compression::Stored,
        "deflate" => Compression::Deflate,
        "zstd" | "zstandard" => Compression::Zstd,
        "xz" => Compression::Xz,
        "snappy" => Compression::Snappy,
        _ => return Err(ParseCompressionError(src.to_string())),
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
            default_value = "Compression::Stored",
            help = "Compression to be used for a file [default: stored]"
        )]
        compression: Compression,

        #[structopt(short, long, help = "Recursively handle provided paths")]
        recursive: bool,

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
            help = "Compression to be used for a file [default: stored]"
        )]
        compression: Compression,

        #[structopt(short, long, help = "Recursively handle provided paths")]
        recursive: bool,

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
    verbose: bool,
) -> Result<()> {
    if selected_files.contains(&path) {
        eprintln!("Cowardly refusing to recursively archive self; aborting.");
        std::process::exit(1);
    }

    let mut bf = BoxFile::open(path)?;

    let (mut known_dirs, known_files) = {
        (
            bf.metadata()
                .records()
                .iter()
                .filter_map(|x| x.as_directory())
                .map(|r| r.path.clone())
                .collect::<std::collections::HashSet<_>>(),
            bf.metadata()
                .records()
                .iter()
                .filter_map(|x| x.as_file())
                .map(|r| r.path.clone())
                .collect::<std::collections::HashSet<_>>(),
        )
    };

    let duplicate = selected_files
        .iter()
        .map(|x| BoxPath::new(&x).unwrap())
        .find(|x| known_files.contains(x));

    if let Some(duplicate) = duplicate {
        eprintln!(
            "Archive already contains file for path: {}; aborting.",
            duplicate.to_string()
        );
        std::process::exit(1);
    }

    // Iterate to capture all known directories
    for file_path in selected_files.into_iter() {
        let parents = collect_parent_directories(&file_path);
        let box_path = BoxPath::new(&file_path).unwrap();

        for (parent, meta) in parents.into_iter() {
            if known_dirs.get(&parent).is_none() {
                bf.mkdir(parent.clone(), meta)?;
                known_dirs.insert(parent);
            }
        }

        if file_path.is_dir() {
            bf.mkdir(box_path.clone(), metadata(&file_path))?;
            known_dirs.insert(box_path);
        } else {
            let file = std::fs::File::open(&file_path)?;
            bf.insert(compression, box_path, file, metadata(&file_path))?;
        }
    }
    Ok(())
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
fn format_path(record: &Record) -> String {
    let mut path = record.path().to_string();
    if record.as_directory().is_some() {
        path.push_str(PATH_PLATFORM_SEP);
    }
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

fn list(path: PathBuf, selected_files: Vec<PathBuf>, verbose: bool) -> Result<()> {
    use humansize::{file_size_opts as options, FileSize};

    let bf = BoxFile::open(&path)?;
    let metadata = bf.metadata();

    let alignment = match bf.header().alignment() {
        Some(v) => format!("{} bytes", v.get()),
        None => "None".into(),
    };
    println!("Box archive: {} (alignment: {})", path.display(), alignment);
    println!("-------------  -------------  -------------  ---------------------  ----------  ---------  --------");
    println!(" Method         Compressed     Length         Created                Unix ACL    CRC32      Path");
    println!("-------------  -------------  -------------  ---------------------  ----------  ---------  --------");
    for record in metadata.records().iter() {
        let acl = unix_acl(record.attr(&bf, "unix.acl"));
        let time = time(record.attr(&bf, "created"));
        let path = format_path(record);

        match record {
            Record::Directory(_) => {
                println!(
                    " {:12}  {:>12}   {:>12}   {:<20}   {:<9}   {:>8}   {}",
                    "<directory>", "-", "-", time, acl, "-", path,
                );
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

fn extract(path: PathBuf, selected_files: Vec<PathBuf>, verbose: bool) -> Result<()> {
    let bf = BoxFile::open(path)?;
    let metadata = bf.metadata();

    for record in metadata.records().iter() {
        let path = format_path(record);
        if verbose {
            println!("{}", path);
        }

        match record {
            Record::File(file) => {
                let out_file = std::fs::File::create(&path)?;
                bf.decompress(file, out_file)?;
            }
            Record::Directory(dir) => {
                std::fs::create_dir_all(&dir.path.to_path_buf())?;
            }
        }
    }

    Ok(())
}

fn collect_parent_directories(path: &Path) -> Vec<(BoxPath, HashMap<String, Vec<u8>>)> {
    let mut out = vec![];
    let path = match path.parent() {
        Some(v) => v,
        None => return vec![],
    };
    for ancestor in path.ancestors() {
        out.push((BoxPath::new(ancestor).unwrap(), metadata(ancestor)));
    }
    out.pop();
    out.reverse();
    out
}

fn metadata(path: &Path) -> HashMap<String, Vec<u8>> {
    use std::time::SystemTime;

    let mut attrs = HashMap::new();
    let meta = match path.metadata() {
        Ok(v) => v,
        Err(_) => return attrs,
    };

    if let Ok(created) = meta.created() {
        let bytes = created
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .to_le_bytes();
        attrs.insert("created".into(), bytes.to_vec());
    }

    #[cfg(unix)]
    attrs.insert(
        "unix.acl".into(),
        meta.permissions().mode().to_le_bytes().to_vec(),
    );

    attrs
}

#[inline(always)]
fn is_hidden(entry: &DirEntry) -> bool {
    entry
        .file_name()
        .to_str()
        .map(|s| s.starts_with("."))
        .unwrap_or(false)
}

#[inline(always)]
fn add_crc32(bf: &mut BoxFile, box_path: &BoxPath) -> std::io::Result<()> {
    let mut hasher = Crc32Hasher::new();
    let record = bf.metadata().records().last().unwrap().as_file().unwrap();
    hasher.update(&*bf.data(record).unwrap());
    let hash = hasher.finalize().to_le_bytes().to_vec();
    bf.set_attr(box_path, "crc32", hash)
}

#[inline(always)]
fn process_files<I: Iterator<Item = PathBuf>>(
    iter: I,
    recursive: bool,
    verbose: bool,
    compression: Compression,
    bf: &mut BoxFile,
    known_dirs: &mut HashSet<BoxPath>,
) -> std::io::Result<()> {
    for file_path in iter {
        let parents = collect_parent_directories(&file_path);
        let box_path = BoxPath::new(&file_path).unwrap();

        for (parent, meta) in parents.into_iter() {
            if known_dirs.contains(&parent) {
                bf.mkdir(parent.clone(), meta)?;
                known_dirs.insert(parent);
            }
        }

        if file_path.is_dir() {
            if !known_dirs.contains(&box_path) {
                if verbose {
                    println!("{} (directory)", &file_path.display());
                }
                bf.mkdir(box_path.clone(), metadata(&file_path))?;
                known_dirs.insert(box_path);
            }
        } else {
            let file = std::fs::File::open(&file_path)?;
            let record = bf.insert(compression, box_path.clone(), file, metadata(&file_path))?;
            if verbose {
                println!(
                    "{} (compressed {:.*}%)",
                    &file_path.display(),
                    2,
                    100.0 - (record.length as f64 / record.decompressed_length as f64 * 100.0)
                );
            }
            add_crc32(bf, &box_path)?;
        }
    }

    Ok(())
}

fn create(
    path: PathBuf,
    selected_files: Vec<PathBuf>,
    compression: Compression,
    recursive: bool,
    verbose: bool,
    alignment: Option<NonZeroU64>,
) -> Result<()> {
    // TODO: silently ignore self-archiving unless it's the only thing in the list.
    if selected_files.contains(&path) {
        eprintln!("Cowardly refusing to recursively archive self; aborting.");
        std::process::exit(1);
    }

    let mut bf = match alignment {
        None => BoxFile::create(path),
        Some(alignment) => BoxFile::create_with_alignment(path, alignment),
    }?;

    let mut known_dirs = std::collections::HashSet::new();

    process_files(
        selected_files.into_iter(),
        recursive,
        verbose,
        compression,
        &mut bf,
        &mut known_dirs,
    )?;

    Ok(())
}

fn main() {
    let opts = CliOpts::from_args();

    let result = match opts.cmd {
        Commands::Append {
            path,
            compression,
            recursive,
        } => append(path, opts.selected_files, compression, opts.verbose),
        Commands::List { path } => list(path, opts.selected_files, opts.verbose),
        Commands::Extract { path } => extract(path, opts.selected_files, opts.verbose),
        Commands::Create {
            path,
            alignment,
            compression,
            recursive,
        } => create(
            path,
            opts.selected_files,
            compression,
            recursive,
            opts.verbose,
            alignment,
        ),
        Commands::Test { path } => unimplemented!(),
    };

    if let Err(e) = result {
        eprintln!("Error: {:?}", e);
        std::process::exit(1);
    }
}
