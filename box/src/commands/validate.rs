use box_format::{BoxFileReader, ValidateProgress, ValidateStats};

use crate::cli::ValidateArgs;
use crate::error::{Error, Result};
use crate::util::{create_progress_bar, create_spinner};

pub async fn run(args: ValidateArgs) -> Result<()> {
    let bf = BoxFileReader::open(&args.archive)
        .await
        .map_err(|source| Error::OpenArchive {
            path: args.archive.clone(),
            source,
        })?;

    let stats: ValidateStats = if args.serial {
        // Sequential validation - use spinner since we process one at a time
        let spinner = if args.quiet {
            None
        } else {
            Some(create_spinner("Validating..."))
        };

        let stats = bf
            .validate_all()
            .await
            .map_err(|source| Error::Extract { source })?;

        if let Some(pb) = spinner {
            pb.finish_with_message("Done");
        }
        stats
    } else {
        // Parallel validation (default)
        let concurrency = args.jobs.unwrap_or_else(num_cpus::get);

        // Set up progress channel
        let (progress_tx, mut progress_rx) =
            tokio::sync::mpsc::unbounded_channel::<ValidateProgress>();

        // Spawn progress handler task - starts with spinner, switches to progress bar
        let quiet = args.quiet;
        let progress_task = tokio::spawn(async move {
            let mut spinner: Option<indicatif::ProgressBar> = if quiet {
                None
            } else {
                Some(create_spinner("Scanning archive..."))
            };
            let mut progress_bar: Option<indicatif::ProgressBar> = None;

            while let Some(update) = progress_rx.recv().await {
                match update {
                    ValidateProgress::Started { total_files } => {
                        // Switch from spinner to progress bar
                        if let Some(s) = spinner.take() {
                            s.finish_and_clear();
                        }
                        if !quiet {
                            progress_bar = Some(create_progress_bar(total_files, "Validating"));
                        }
                    }
                    ValidateProgress::Validating { ref path } => {
                        if let Some(ref pb) = progress_bar {
                            pb.set_message(format!("{}", path));
                        }
                    }
                    ValidateProgress::Validated {
                        files_checked,
                        success,
                        ..
                    } => {
                        if let Some(ref pb) = progress_bar {
                            pb.set_position(files_checked);
                            if !success {
                                pb.set_message("Checksum failure!");
                            }
                        }
                    }
                    ValidateProgress::Finished => {
                        if let Some(pb) = progress_bar.take() {
                            pb.finish_with_message("Done");
                        }
                    }
                }
            }
        });

        let stats = bf
            .validate_all_parallel_with_progress(concurrency, Some(progress_tx))
            .await
            .map_err(|source| Error::Extract { source })?;

        // Wait for progress task to finish
        let _ = progress_task.await;

        stats
    };

    if !args.quiet {
        println!(
            "Validated {} files ({} without checksum, {} failures)",
            stats.files_checked, stats.files_without_checksum, stats.checksum_failures
        );
    }

    if stats.checksum_failures > 0 {
        std::process::exit(1);
    }

    Ok(())
}
