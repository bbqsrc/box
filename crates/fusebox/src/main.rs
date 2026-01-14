use std::path::PathBuf;

use clap::Parser;
use fuser::MountOption;

use box_format::sync::BoxReader;
use fusebox::{BoxFs, LruCache};

#[derive(Debug, Parser)]
#[command(name = "fusebox", about = "Mount a .box archive as a FUSE filesystem")]
struct Options {
    /// Path to the .box archive file
    box_file: PathBuf,

    /// Mount point directory
    mountpoint: PathBuf,

    /// Maximum cache size in MB (default: 256)
    #[arg(long, default_value = "256")]
    cache_size: usize,
}

fn main() {
    tracing_subscriber::fmt::init();
    let opts = Options::parse();

    let mountpoint = std::fs::canonicalize(&opts.mountpoint).unwrap();
    let boxfile = std::fs::canonicalize(&opts.box_file).unwrap();
    tracing::info!("Mounting box file {:?} at {:?}", boxfile, mountpoint);

    let bf = BoxReader::open(boxfile).unwrap();
    tracing::trace!("{:?}", &bf);

    let mount_opts = &[
        MountOption::RO,
        MountOption::FSName(bf.path().to_string_lossy().to_string()),
    ];

    let boxfs = BoxFs::new(bf, LruCache::new(opts.cache_size));

    fuser::mount2(boxfs, &opts.mountpoint, mount_opts).unwrap();
}
