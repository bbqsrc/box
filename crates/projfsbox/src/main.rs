//! projfsbox CLI - Mount .box archives as virtual filesystems using Windows ProjFS.

#![cfg(windows)]

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;

#[derive(Parser)]
#[command(name = "projfsbox")]
#[command(about = "Mount .box archives as virtual filesystems using Windows ProjFS")]
#[command(version)]
struct Args {
    /// Path to the .box archive file
    #[arg(value_name = "BOX_FILE")]
    box_file: PathBuf,

    /// Virtualization root directory (must exist and be empty)
    #[arg(value_name = "MOUNT_POINT")]
    mountpoint: PathBuf,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    let args = Args::parse();

    // Verify box file exists
    if !args.box_file.exists() {
        eprintln!("Error: Box file not found: {}", args.box_file.display());
        std::process::exit(1);
    }

    // Verify mountpoint exists and is a directory
    if !args.mountpoint.exists() {
        eprintln!(
            "Error: Mount point does not exist: {}",
            args.mountpoint.display()
        );
        eprintln!(
            "Create the directory first with: mkdir {:?}",
            args.mountpoint
        );
        std::process::exit(1);
    }

    if !args.mountpoint.is_dir() {
        eprintln!(
            "Error: Mount point is not a directory: {}",
            args.mountpoint.display()
        );
        std::process::exit(1);
    }

    // Mark directory as virtualization root
    tracing::info!(
        "Marking {} as virtualization root",
        args.mountpoint.display()
    );
    projfsbox::mark_as_virtualization_root(&args.mountpoint)?;

    // Create tokio runtime
    let runtime = tokio::runtime::Runtime::new()?;

    // Open archive
    tracing::info!("Opening archive: {}", args.box_file.display());
    let reader = runtime.block_on(box_format::BoxFileReader::open(&args.box_file))?;

    tracing::info!("Archive opened: {}", args.box_file.display());

    // Create provider
    let provider = Arc::new(projfsbox::BoxProvider::new(
        reader,
        runtime,
        args.mountpoint.clone(),
    ));

    // Start virtualization
    tracing::info!("Starting ProjFS virtualization...");
    provider.clone().start()?;

    println!(
        "Mounted {} at {}",
        args.box_file.display(),
        args.mountpoint.display()
    );
    println!("Press Ctrl+C to unmount...");

    // Set up Ctrl+C handler
    let provider_for_handler = provider.clone();
    ctrlc_handler(move || {
        tracing::info!("Received shutdown signal");
        provider_for_handler.stop();
        std::process::exit(0);
    })?;

    // Keep running
    loop {
        std::thread::park();
    }
}

fn ctrlc_handler<F: Fn() + Send + Sync + 'static>(
    handler: F,
) -> Result<(), Box<dyn std::error::Error>> {
    use std::sync::OnceLock;
    use windows::Win32::System::Console::SetConsoleCtrlHandler;
    use windows::core::BOOL;

    // CTRL_C_EVENT = 0, CTRL_BREAK_EVENT = 1
    const CTRL_C_EVENT: u32 = 0;
    const CTRL_BREAK_EVENT: u32 = 1;

    static HANDLER: OnceLock<Box<dyn Fn() + Send + Sync>> = OnceLock::new();
    HANDLER.get_or_init(|| Box::new(handler));

    unsafe extern "system" fn ctrl_handler(ctrl_type: u32) -> BOOL {
        if ctrl_type == CTRL_C_EVENT || ctrl_type == CTRL_BREAK_EVENT {
            if let Some(handler) = HANDLER.get() {
                handler();
            }
            BOOL(1)
        } else {
            BOOL(0)
        }
    }

    // SAFETY: ctrl_handler is a valid extern "system" fn
    unsafe {
        SetConsoleCtrlHandler(Some(ctrl_handler), true)?;
    }

    Ok(())
}
