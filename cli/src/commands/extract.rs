use std::collections::HashMap;
use std::io::IsTerminal;
use std::sync::{Arc, Mutex};

use box_format::{BoxFileReader, BoxPath, ExtractOptions, ExtractProgress, ExtractStats};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

use crate::cli::ExtractArgs;
use crate::error::{Error, Result};

pub async fn run(args: ExtractArgs) -> Result<()> {
    let bf = BoxFileReader::open(&args.archive)
        .await
        .map_err(|source| Error::OpenArchive {
            path: args.archive.clone(),
            source,
        })?;

    // Check if archive has escaped paths but CLI flag not provided
    if bf.allow_escapes() && !args.allow_escapes {
        return Err(Error::AllowEscapesRequired);
    }

    // Check if archive has external symlinks but CLI flag not provided
    if bf.allow_external_symlinks() && !args.allow_external_symlinks {
        return Err(Error::ExternalSymlinksRequired);
    }

    let output_path = args
        .output
        .unwrap_or_else(|| std::env::current_dir().expect("no current directory"));

    // Create output directory if it doesn't exist
    tokio::fs::create_dir_all(&output_path)
        .await
        .map_err(|source| Error::ProcessFile {
            path: output_path.clone(),
            source,
        })?;

    let total_files = bf.metadata().iter().count() as u64;
    let show_progress = !args.quiet && std::io::stdout().is_terminal();

    let options = ExtractOptions {
        verify_checksums: !args.no_checksum,
        allow_escapes: args.allow_escapes,
        allow_external_symlinks: args.allow_external_symlinks,
        xattrs: args.xattrs,
    };

    let stats: ExtractStats = if args.files.is_empty() {
        // Extract all files
        if args.serial {
            // Sequential extraction (simple progress bar)
            let progress = if !show_progress {
                None
            } else {
                let style = ProgressStyle::default_bar()
                    .template("  {bar:40.cyan/blue} {pos:>7}/{len:7} files")
                    .unwrap()
                    .progress_chars("━━─");
                let pb = ProgressBar::new(total_files);
                pb.set_style(style);
                Some(pb)
            };

            let stats = bf
                .extract_all_with_options(&output_path, options)
                .await
                .map_err(|source| Error::Extract { source })?;

            if let Some(pb) = progress {
                pb.finish_and_clear();
            }
            stats
        } else {
            // Parallel extraction (default) with multi-progress display
            let concurrency = args.jobs.unwrap_or_else(num_cpus::get);

            // Set up progress channel
            let (progress_tx, mut progress_rx) =
                tokio::sync::mpsc::unbounded_channel::<ExtractProgress>();

            // Set up multi-progress display
            let multi = MultiProgress::new();
            let main_style = ProgressStyle::default_bar()
                .template("  {bar:40.cyan/blue} {pos:>7}/{len:7} files")
                .unwrap()
                .progress_chars("━━─");
            let main_bar = multi.add(ProgressBar::new(total_files));
            main_bar.set_style(main_style);

            let file_style = ProgressStyle::default_spinner()
                .template("    {spinner:.green} {msg}")
                .unwrap();

            // Track active file spinners
            let active_bars: Arc<Mutex<HashMap<String, ProgressBar>>> =
                Arc::new(Mutex::new(HashMap::new()));

            // Spawn progress handler task
            let progress_task = if !show_progress {
                tokio::spawn(async move { while progress_rx.recv().await.is_some() {} })
            } else {
                let active_bars = active_bars.clone();
                let multi = multi.clone();
                let file_style = file_style.clone();
                let main_bar = main_bar.clone();

                tokio::spawn(async move {
                    while let Some(update) = progress_rx.recv().await {
                        match update {
                            ExtractProgress::Started { total_files, .. } => {
                                main_bar.set_length(total_files);
                            }
                            ExtractProgress::Extracting { ref path } => {
                                let pb = multi.add(ProgressBar::new_spinner());
                                pb.set_style(file_style.clone());
                                pb.set_message(path.to_string());
                                pb.enable_steady_tick(std::time::Duration::from_millis(80));
                                active_bars.lock().unwrap().insert(path.to_string(), pb);
                            }
                            ExtractProgress::Extracted { ref path, .. } => {
                                let path_str = path.to_string();
                                if let Some(pb) = active_bars.lock().unwrap().remove(&path_str) {
                                    pb.finish_and_clear();
                                    multi.remove(&pb);
                                }
                                main_bar.inc(1);
                            }
                            ExtractProgress::Finished => {
                                main_bar.finish_and_clear();
                            }
                            _ => {}
                        }
                    }
                })
            };

            let stats = bf
                .extract_all_parallel_with_progress(
                    &output_path,
                    options,
                    concurrency,
                    Some(progress_tx),
                )
                .await
                .map_err(|source| Error::Extract { source })?;

            // Wait for progress task to finish
            let _ = progress_task.await;

            stats
        }
    } else {
        // Extract specific files (always sequential for now)
        let progress = if !show_progress {
            None
        } else {
            let style = ProgressStyle::default_bar()
                .template("  {bar:40.cyan/blue} {pos:>7}/{len:7} files")
                .unwrap()
                .progress_chars("━━─");
            let pb = ProgressBar::new(args.files.len() as u64);
            pb.set_style(style);
            Some(pb)
        };

        let mut total_stats = ExtractStats::default();
        for file_path in &args.files {
            let box_path = BoxPath::new(file_path).map_err(|source| Error::InvalidPath {
                path: file_path.clone(),
                source,
            })?;

            let stats = bf
                .extract_recursive_with_options(&box_path, &output_path, options.clone())
                .await
                .map_err(|source| Error::Extract { source })?;

            total_stats += stats;

            if let Some(ref pb) = progress {
                pb.inc(1);
            }
        }

        if let Some(pb) = progress {
            pb.finish_and_clear();
        }
        total_stats
    };

    if !args.quiet {
        println!(
            "Extracted {} files to {}",
            stats.files_extracted,
            output_path.display()
        );

        if stats.checksum_failures > 0 {
            eprintln!(
                "WARNING: {} files failed checksum verification",
                stats.checksum_failures
            );
        }

        if args.timings {
            let timing = &stats.timing;
            let total = timing.collect + timing.directories + timing.decompress + timing.symlinks;
            println!("\nTiming:");
            println!("  Collect:     {:>8.2?}", timing.collect);
            println!("  Directories: {:>8.2?}", timing.directories);
            println!("  Decompress:  {:>8.2?}", timing.decompress);
            println!("  Symlinks:    {:>8.2?}", timing.symlinks);
            println!("  Total:       {:>8.2?}", total);
        }
    }

    if stats.checksum_failures > 0 {
        std::process::exit(1);
    }

    Ok(())
}
