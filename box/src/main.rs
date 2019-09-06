use std::collections::HashMap;
use std::io::Result;
use std::path::{Path, PathBuf};

use box_format::{BoxFile, Compression, Record};
use byteorder::{LittleEndian, ReadBytesExt};
use structopt::StructOpt;

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
        _ => return Err(ParseCompressionError(src.to_string())),
    };

    Ok(compression)
}

use structopt::clap::{ArgGroup, AppSettings::*};


#[derive(Debug, StructOpt)]
#[structopt(
    name = "box",
    about = "Create, modify and extract box archives.",
    settings = &[ArgRequiredElseHelp, DontCollapseArgsInUsage],
    group = ArgGroup::with_name("verb").required(true),
    usage = "box <-a|-c|-l|-x> [FLAGS|OPTIONS] <boxfile> [files]..."
)]
struct CliOpts {
    #[structopt(short = "a", long, group = "verb", conflicts_with_all = &["create", "list", "extract"], help = "Append files to an existing archive")]
    append: bool,

    #[structopt(short = "l", long, group = "verb", conflicts_with_all = &["append", "create", "extract"], help = "List files of an archive")]
    list: bool,

    #[structopt(short = "c", long, group = "verb", conflicts_with_all = &["append", "list", "extract"], help = "Create a new archive")]
    create: bool,

    #[structopt(short = "x", long, group = "verb", conflicts_with_all = &["append", "create", "list"], help = "Extract files from an archive")]
    extract: bool,

    #[structopt(
        short = "A",
        long,
        help = "Byte alignment to be used for an archive (create only, default none)"
    )]
    alignment: Option<u64>,

    #[structopt(short = "C", long, parse(try_from_str = parse_compression), help = "Compression to be used for a file (create/append only, default stored)")]
    compression: Option<Compression>,

    #[structopt(short, long, help = "Recursively handle provided paths")]
    recursive: bool,

    #[structopt(short, long, help = "Show verbose output")]
    verbose: bool,

    #[structopt(parse(from_os_str), help = "Path to the .box archive")]
    path: PathBuf,

    #[structopt(
        parse(from_os_str),
        help = "Selected files/directories to extract, list or add to an archive"
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
                .map(|r| r.path.to_string())
                .collect::<std::collections::HashSet<_>>(),
            bf.metadata()
                .records()
                .iter()
                .filter_map(|x| x.as_file())
                .map(|r| r.path.to_string())
                .collect::<std::collections::HashSet<_>>(),
        )
    };

    let duplicate = selected_files
        .iter()
        .map(|x| convert_to_box_path(&x).unwrap())
        .find(|x| known_files.contains(x));

    if let Some(duplicate) = duplicate {
        eprintln!(
            "Archive already contains file for path: {}; aborting.",
            duplicate.split("\x1f").collect::<Vec<_>>().join(SEP)
        );
        std::process::exit(1);
    }

    // Iterate to capture all known directories
    for file_path in selected_files.into_iter() {
        let parents = collect_parent_directories(&file_path);
        let box_path = convert_to_box_path(&file_path).unwrap();

        for (parent, meta) in parents.into_iter() {
            match known_dirs.get(&parent) {
                None => {
                    bf.mkdir(&parent, meta)?;
                    known_dirs.insert(parent);
                }
                _ => {}
            }
        }

        if file_path.is_dir() {
            bf.mkdir(&box_path, metadata(&file_path))?;
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

#[cfg(not(windows))]
static SEP: &'static str = "/";
#[cfg(windows)]
static SEP: &'static str = "\\";

const STD_SEP: &'static str = "\x1f";

#[inline(always)]
fn format_path(record: &Record) -> String {
    let mut path = record.path().split("\x1f").collect::<Vec<_>>();
    if record.as_directory().is_some() {
        path.push("");
    }
    path.join(SEP)
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
fn time(attrs: &HashMap<String, Vec<u8>>) -> String {
    attrs
        .get("created".into())
        .and_then(|x| x.as_slice().read_u64::<LittleEndian>().ok())
        .map(|x| std::time::UNIX_EPOCH + std::time::Duration::new(x, 0))
        .map(|x| {
            let datetime: chrono::DateTime<chrono::Utc> = x.into();
            datetime.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        })
        .unwrap_or_else(|| "-".into())
}

#[inline(always)]
fn unix_acl(attrs: &HashMap<String, Vec<u8>>) -> String {
    attrs
        .get("unix.acl".into())
        .map(|x| from_acl_u16(u16::from_le_bytes([x[0], x[1]])))
        .unwrap_or_else(|| "-".into())
}

fn list(path: PathBuf, selected_files: Vec<PathBuf>, verbose: bool) -> Result<()> {
    use humansize::{file_size_opts as options, FileSize};

    let bf = BoxFile::open(path)?;
    let metadata = bf.metadata();

    println!("Method    Compressed     Length         Created                Unix ACL    Path");
    println!("--------  -------------  -------------  ---------------------  ----------  --------");
    for record in metadata.records().iter() {
        let acl = unix_acl(record.attrs());
        let time = time(record.attrs());
        let path = format_path(record);

        match record {
            Record::Directory(record) => {
                println!(
                    "{:8}  {:>12}  {:>12}    {:<20}   {:<9}   {}",
                    "", "", "", time, acl, path,
                );
            }
            Record::File(record) => {
                let length = record.length.file_size(options::BINARY).unwrap();
                let decompressed_length = record
                    .decompressed_length
                    .file_size(options::BINARY)
                    .unwrap();

                println!(
                    "{:<8}  {:>12}   {:>12}   {:<20}   {:<9}   {}",
                    format!("{:?}", record.compression),
                    length,
                    decompressed_length,
                    time,
                    acl,
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
                std::fs::create_dir_all(&dir.path)?;
            }
        }
    }

    Ok(())
}

fn convert_to_box_path(path: &Path) -> std::result::Result<String, String> {
    // TODO do this right
    match path
        .to_str()
        .map(|x| x.split(SEP).collect::<Vec<_>>().join(STD_SEP))
    {
        Some(v) => Ok(v),
        None => Err("Invalid path".into()),
    }
}

fn collect_parent_directories(path: &Path) -> Vec<(String, HashMap<String, Vec<u8>>)> {
    let mut out = vec![];
    let path = match path.parent() {
        Some(v) => v,
        None => return vec![],
    };
    for ancestor in path.ancestors() {
        out.push((convert_to_box_path(ancestor).unwrap(), metadata(ancestor)));
    }
    out.pop();
    out.reverse();
    out
}

fn metadata(path: &Path) -> HashMap<String, Vec<u8>> {
    use std::os::unix::fs::PermissionsExt;
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

    attrs.insert(
        "unix.acl".into(),
        meta.permissions().mode().to_le_bytes().to_vec(),
    );

    attrs
}

fn create(
    path: PathBuf,
    selected_files: Vec<PathBuf>,
    compression: Compression,
    recursive: bool,
    verbose: bool,
    alignment: Option<u64>,
) -> Result<()> {
    if selected_files.contains(&path) {
        eprintln!("Cowardly refusing to recursively archive self; aborting.");
        std::process::exit(1);
    }

    let mut bf = BoxFile::create(path)?;
    let mut known_dirs = std::collections::HashSet::new();

    for file_path in selected_files.into_iter() {
        let parents = collect_parent_directories(&file_path);
        let box_path = convert_to_box_path(&file_path).unwrap();

        for (parent, meta) in parents.into_iter() {
            match known_dirs.get(&parent) {
                None => {
                    bf.mkdir(&parent, meta)?;
                    known_dirs.insert(parent);
                }
                _ => {}
            }
        }

        if file_path.is_dir() {
            bf.mkdir(&box_path, metadata(&file_path))?;
            known_dirs.insert(box_path);
        } else {
            let file = std::fs::File::open(&file_path)?;
            bf.insert(compression, box_path, file, metadata(&file_path))?;
        }
    }

    Ok(())
}

fn main() {
    let opts = CliOpts::from_args();

    let actions_count = [opts.append, opts.list, opts.create, opts.extract]
        .into_iter()
        .filter(|x| **x)
        .count();

    if actions_count > 1 {
        eprintln!("Multiple actions selected; aborting.");
    } else if actions_count == 0 {
        eprintln!("No actions selected; aborting.");
    }

    let compression = opts.compression.unwrap_or(Compression::Stored);

    let result = if opts.append {
        append(opts.path, opts.selected_files, compression, opts.verbose)
    } else if opts.list {
        list(opts.path, opts.selected_files, opts.verbose)
    } else if opts.create {
        create(
            opts.path,
            opts.selected_files,
            compression,
            opts.recursive,
            opts.verbose,
            opts.alignment,
        )
    } else if opts.extract {
        extract(opts.path, opts.selected_files, opts.verbose)
    } else {
        CliOpts::clap().print_help().unwrap();
        println!();
        std::process::exit(1);
    };

    match result {
        Err(e) => {
            eprintln!("{:?}", e);
            std::process::exit(1);
        }
        _ => {}
    }
}