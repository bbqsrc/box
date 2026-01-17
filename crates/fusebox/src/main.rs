use std::path::PathBuf;

use clap::Parser;
use fuser::MountOption;

use box_format::sync::BoxReader;
use fusebox::{BoxFs, LruCache, MultiArchive};

#[derive(Debug, Parser)]
#[command(
    name = "fusebox",
    about = "Mount one or more .box archives as a FUSE filesystem"
)]
struct Options {
    /// Path(s) to the .box archive file(s).
    /// When multiple archives are provided, they are merged into a unified hierarchy.
    /// Later archives take precedence for conflicting files (directories merge).
    #[arg(required = true)]
    archives: Vec<PathBuf>,

    /// Mount point directory
    #[arg(short, long)]
    mount: PathBuf,

    /// Maximum cache size in MB (default: 256)
    #[arg(long, default_value = "256")]
    cache_size: usize,
}

fn main() {
    tracing_subscriber::fmt::init();
    let opts = Options::parse();

    let mountpoint = std::fs::canonicalize(&opts.mount).unwrap();

    // Create multi-archive container
    let mut archives = MultiArchive::new();

    // Add each archive
    let mut fs_name_parts = Vec::new();
    for box_path in &opts.archives {
        let boxfile = std::fs::canonicalize(box_path).unwrap();
        tracing::info!("Adding archive {:?}", boxfile);

        let reader = BoxReader::open(&boxfile).unwrap();
        fs_name_parts.push(reader.path().to_string_lossy().to_string());
        archives.add_archive(reader);
    }

    tracing::info!(
        "Mounting {} archive(s) at {:?}",
        opts.archives.len(),
        mountpoint
    );

    // Create filesystem name from archive count (avoid special chars that confuse FUSE)
    let fs_name = if fs_name_parts.len() == 1 {
        fs_name_parts[0].clone()
    } else {
        format!("fusebox:{}_archives", fs_name_parts.len())
    };

    let mount_opts = &[MountOption::RO, MountOption::FSName(fs_name)];

    let boxfs = BoxFs::new(archives, LruCache::new(opts.cache_size));

    fuser::mount2(boxfs, &mountpoint, mount_opts).unwrap();
}
