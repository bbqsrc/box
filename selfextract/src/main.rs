#![windows_subsystem = "windows"]

use std::process::Stdio;

use mmap_io::MemoryMappedFile;
use box_format::BoxFileReader;
use gumdrop::Options;
use tokio::process::Command;

const DIVIDER_UUID: u128 = 0xaae8ea9c35484ee4bf28f1a25a6b3c6c;

#[derive(Options)]
struct Args {
    #[options(help = "print help message")]
    help: bool,

    #[options(help = "verbose output")]
    verbose: bool,

    #[options(no_short, help = "will not run exec script if found")]
    no_exec: bool,

    #[options(help = "override output directory")]
    output: Option<std::path::PathBuf>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse_args_default_or_exit();
    std::process::exit(run(args).await);
}

async fn open_box_segment() -> Result<BoxFileReader, i32> {
    let path = match std::env::current_exe() {
        Ok(path) => path,
        Err(e) => {
            eprintln!("ERROR: Could not access self-extractor for opening!");
            eprintln!("{:?}", e);
            return Err(1);
        }
    };

    let mmap = match MemoryMappedFile::open_ro(&path) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("ERROR: Could not access self-extractor for opening!");
            eprintln!("{:?}", e);
            return Err(3);
        }
    };

    let mmap_slice = match mmap.as_slice(0, mmap.len()) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("ERROR: Could not read self-extractor!");
            eprintln!("{:?}", e);
            return Err(3);
        }
    };

    let boundary = twoway::find_bytes(mmap_slice, &DIVIDER_UUID.to_le_bytes());
    let offset = match boundary {
        Some(v) => v + std::mem::size_of::<u128>(),
        None => {
            eprintln!("ERROR: Could not find embedded .box file data to extract.");
            return Err(4);
        }
    };

    let bf = match box_format::BoxFileReader::open_at_offset(path, offset as u64).await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("ERROR: Could not read .box data!");
            eprintln!("{:?}", e);
            return Err(5);
        }
    };

    Ok(bf)
}

async fn process(bf: &BoxFileReader, path: Option<&std::path::Path>, _is_verbose: bool) -> i32 {
    let path = match path {
        Some(v) => v.to_path_buf(),
        None => match std::env::current_dir() {
            Ok(path) => path,
            Err(e) => {
                eprintln!("ERROR: Could not access current directory!");
                eprintln!("{:?}", e);
                return 10;
            }
        },
    };

    match tokio::fs::create_dir_all(&path).await {
        Ok(_) => {}
        Err(e) => {
            eprintln!("ERROR: Could not create output directory!");
            eprintln!("{:?}", e);
            return 12;
        }
    }

    match bf.extract_all(&path).await {
        Ok(_) => {}
        Err(e) => {
            eprintln!(
                "ERROR: Could not extract files to path '{}'!",
                path.display()
            );
            eprintln!("{:?}", e);
            return 11;
        }
    }

    0
}

async fn process_exec(bf: &BoxFileReader, exec: &str, args: &[&str], is_verbose: bool) -> i32 {
    let tempdir = match tempfile::tempdir() {
        Ok(v) => v,
        Err(e) => {
            eprintln!("ERROR: Could not create temporary directory!");
            eprintln!("{:?}", e);
            return 12;
        }
    };

    match bf.extract_all(tempdir.path()).await {
        Ok(_) => {}
        Err(e) => {
            eprintln!(
                "ERROR: Could not extract files to path '{}'!",
                tempdir.path().display()
            );
            eprintln!("{:?}", e);
            return 11;
        }
    }

    run_shell_exec(exec, args, tempdir.path(), is_verbose).await
}

async fn run_shell_exec(input: &str, args: &[&str], cwd: &std::path::Path, is_verbose: bool) -> i32 {
    if is_verbose {
        println!(
            "TRACE: Running `{} {}` in '{}'...",
            input,
            args.iter().cloned().collect::<Vec<_>>().join(" "),
            cwd.display()
        );
    }

    let exec = match tokio::fs::canonicalize(cwd.join(input)).await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("ERROR: Running exec script failed!");
            eprintln!("{:?}", e);
            return 101;
        }
    };

    let status = match Command::new(exec)
        .args(args)
        .current_dir(cwd)
        .stdout(if is_verbose {
            Stdio::inherit()
        } else {
            Stdio::null()
        })
        .stderr(if is_verbose {
            Stdio::inherit()
        } else {
            Stdio::null()
        })
        .status()
        .await
    {
        Ok(v) => v,
        Err(e) => {
            eprintln!("ERROR: Running exec script failed!");
            eprintln!("{:?}", e);
            return 101;
        }
    };

    status.code().unwrap_or(0)
}

#[inline(always)]
async fn run(args: Args) -> i32 {
    let bf = match open_box_segment().await {
        Ok(v) => v,
        Err(e) => return e,
    };

    let exec_attr = bf.metadata().file_attr("box.exec");
    let args_attr = bf.metadata().file_attr("box.args");
    let args_attr: Vec<String> = match args_attr {
        Some(value) => {
            let args_str = match std::str::from_utf8(value) {
                Ok(v) => v,
                Err(_) => {
                    eprintln!("ERROR: Could not read args string, invalid UTF-8!");
                    return 7;
                }
            };

            match shell_words::split(&args_str) {
                Ok(args) => args,
                Err(_) => {
                    eprintln!("ERROR: Could not read args string, invalid UTF-8!");
                    return 7;
                }
            }
        }
        None => vec![],
    };

    match exec_attr {
        Some(value) if !args.no_exec => {
            let exec_str = match std::str::from_utf8(value) {
                Ok(v) => v,
                Err(_) => {
                    eprintln!("ERROR: Could not read exec string, invalid UTF-8!");
                    return 6;
                }
            };

            process_exec(&bf, exec_str, &args_attr.iter().map(|s| &**s).collect::<Vec<_>>(), args.verbose).await
        }
        _ => process(&bf, args.output.as_ref().map(|x| &**x), args.verbose).await,
    }
}
