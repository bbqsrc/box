use box_format::{BoxFileReader, BoxPath, ExtractOptions, ExtractProgress, ExtractStats};
use indicatif::ProgressBar;

use crate::cli::ExtractArgs;
use crate::error::{Error, Result};
use crate::util::create_progress_bar;

pub async fn run(args: ExtractArgs) -> Result<()> {
    let bf = BoxFileReader::open(&args.archive)
        .await
        .map_err(|source| Error::OpenArchive {
            path: args.archive.clone(),
            source,
        })?;

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
    let progress: Option<ProgressBar> = if args.quiet {
        None
    } else {
        Some(create_progress_bar(total_files, "Extracting"))
    };

    let options = ExtractOptions {
        verify_checksums: !args.no_checksum,
    };

    let stats: ExtractStats = if args.files.is_empty() {
        // Extract all files
        if args.serial {
            // Sequential extraction
            let stats = bf
                .extract_all_with_options(&output_path, options)
                .await
                .map_err(|source| Error::Extract { source })?;

            if let Some(pb) = progress {
                pb.finish_with_message("Done");
            }
            stats
        } else {
            // Parallel extraction (default)
            let concurrency = args.jobs.unwrap_or_else(num_cpus::get);

            // Set up progress channel
            let (progress_tx, mut progress_rx) =
                tokio::sync::mpsc::unbounded_channel::<ExtractProgress>();

            // Spawn progress handler task
            let progress_clone = progress.clone();
            let progress_task = tokio::spawn(async move {
                while let Some(update) = progress_rx.recv().await {
                    if let Some(ref pb) = progress_clone {
                        match update {
                            ExtractProgress::Started { total_files, .. } => {
                                pb.set_length(total_files);
                                pb.set_message("Starting...");
                            }
                            ExtractProgress::Extracting { ref path } => {
                                pb.set_message(format!("Extracting: {}", path));
                            }
                            ExtractProgress::Extracted {
                                files_extracted, ..
                            } => {
                                pb.set_position(files_extracted);
                            }
                            ExtractProgress::Finished => {
                                pb.finish_with_message("Done");
                            }
                            _ => {}
                        }
                    }
                }
            });

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
            pb.finish_with_message("Done");
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
    }

    if stats.checksum_failures > 0 {
        std::process::exit(1);
    }

    Ok(())
}
