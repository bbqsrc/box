use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime};

use async_walkdir::WalkDir;
use box_format::{
    AttrValue, BOX_EPOCH_UNIX, BoxFileWriter, BoxPath, CompressionConfig, FileJob,
    ParallelProgress, fs,
};
use futures::StreamExt;

use crate::cli::CreateArgs;
use crate::error::{Error, Result};
use crate::util::{create_spinner, matches_exclude, parse_paths_with_compression};

/// Timing stats for archive creation phases.
#[derive(Default)]
struct TimingStats {
    collect: Duration,
    directories: Duration,
    symlinks: Duration,
    compress: Duration,
    write: Duration,
    finalize: Duration,
}

struct CollectedEntry {
    fs_path: PathBuf,
    box_path: BoxPath<'static>,
    meta: std::fs::Metadata,
    config: CompressionConfig,
}

enum EntryKind {
    Directory(CollectedEntry),
    Symlink { entry: CollectedEntry, is_dir: bool },
    File(CollectedEntry),
}

pub async fn run(args: CreateArgs) -> Result<()> {
    let archive_path = &args.archive;

    // Check if archive exists
    if archive_path.exists() && !args.force {
        return Err(Error::ArchiveExists {
            path: archive_path.clone(),
        });
    }

    // Parse exclude patterns
    let exclude_patterns: Vec<glob::Pattern> = args
        .exclude
        .iter()
        .filter_map(|p| glob::Pattern::new(p).ok())
        .collect();

    // Parse paths with compression
    let paths_with_compression = parse_paths_with_compression(&args.paths_and_compression);

    if paths_with_compression.is_empty() {
        return Err(Error::NoFilesSpecified);
    }

    // Remove existing archive if force is set
    if archive_path.exists() && args.force {
        tokio::fs::remove_file(archive_path)
            .await
            .map_err(|source| Error::CreateArchive {
                path: archive_path.clone(),
                source,
            })?;
    }

    // Create the archive
    let mut bf = BoxFileWriter::create_with_options(
        archive_path,
        args.alignment,
        args.allow_escapes,
        args.allow_external_symlinks,
    )
    .await
    .map_err(|source| Error::CreateArchive {
        path: archive_path.clone(),
        source,
    })?;

    // Calculate effective metadata flags (-A implies both --timestamps and --ownership)
    let timestamps = args.timestamps || args.archive_metadata;
    let ownership = args.ownership || args.archive_metadata;

    // Set creation timestamp as DateTime (minutes since Box epoch)
    let unix_secs = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let minutes_since_epoch = (unix_secs - BOX_EPOCH_UNIX) / 60;

    bf.set_file_attr("created", AttrValue::DateTime(minutes_since_epoch))
        .map_err(|source| Error::SetAttribute {
            key: "created".to_string(),
            source,
        })?;

    let progress = if args.quiet {
        None
    } else {
        Some(create_spinner("Collecting files..."))
    };

    let mut timing = TimingStats::default();

    // First pass: collect all entries
    let collect_start = Instant::now();
    let mut entries = Vec::new();
    let mut known_dirs: HashSet<BoxPath> = HashSet::new();
    let mut known_files: HashSet<BoxPath> = HashSet::new();
    let archive_canonical =
        tokio::fs::canonicalize(bf.path())
            .await
            .map_err(|source| Error::CanonicalizePath {
                path: bf.path().to_path_buf(),
                source,
            })?;
    let archive_filename = archive_canonical.file_name();

    for pwc in &paths_with_compression {
        if pwc.is_glob {
            let _pattern = glob::Pattern::new(&pwc.path).map_err(|_| Error::InvalidPath {
                path: PathBuf::from(&pwc.path),
                source: box_format::path::IntoBoxPathError::EmptyPath,
            })?;

            for path in glob::glob(&pwc.path)
                .map_err(|_| Error::InvalidPath {
                    path: PathBuf::from(&pwc.path),
                    source: box_format::path::IntoBoxPathError::EmptyPath,
                })?
                .flatten()
            {
                collect_path(
                    &path,
                    &pwc.config,
                    &exclude_patterns,
                    args.include_hidden,
                    !args.no_recursive,
                    &archive_canonical,
                    archive_filename,
                    &mut entries,
                    &mut known_dirs,
                    &mut known_files,
                )
                .await?;
            }
        } else {
            let path = PathBuf::from(&pwc.path);
            collect_path(
                &path,
                &pwc.config,
                &exclude_patterns,
                args.include_hidden,
                !args.no_recursive,
                &archive_canonical,
                archive_filename,
                &mut entries,
                &mut known_dirs,
                &mut known_files,
            )
            .await?;
        }
    }

    timing.collect = collect_start.elapsed();

    if let Some(ref pb) = progress {
        pb.set_message("Creating directories...");
    }

    // Separate entries by type
    let dirs_start = Instant::now();
    let mut directories = Vec::new();
    let mut symlinks = Vec::new();
    let mut files = Vec::new();

    for entry in entries {
        match entry {
            EntryKind::Directory(e) => directories.push(e),
            EntryKind::Symlink { entry, is_dir } => symlinks.push((entry, is_dir)),
            EntryKind::File(e) => files.push(e),
        }
    }

    // Create directories (must be sequential)
    let mut created_dirs: HashSet<BoxPath<'static>> = HashSet::new();
    for dir in directories {
        if !created_dirs.contains(&dir.box_path) && bf.metadata().index(&dir.box_path).is_none() {
            // Ensure parent exists
            if let Some(parent) = dir.box_path.parent()
                && !created_dirs.contains(&parent)
                && bf.metadata().index(&parent).is_none()
            {
                let parent_owned = parent.into_owned();
                bf.mkdir_all(parent_owned.clone(), Default::default())
                    .map_err(|source| Error::CreateDirectory {
                        path: parent_owned,
                        source,
                    })?;
            }

            let mut dir_meta = fs::metadata_to_attrs(&dir.meta, timestamps, ownership);
            if args.xattrs {
                if let Ok(xattrs) = fs::read_xattrs(&dir.fs_path) {
                    dir_meta.extend(xattrs);
                }
            }
            bf.mkdir(dir.box_path.clone(), dir_meta)
                .map_err(|source| Error::CreateDirectory {
                    path: dir.box_path.clone(),
                    source,
                })?;
            created_dirs.insert(dir.box_path);
        }
    }

    timing.directories = dirs_start.elapsed();

    // Process symlinks (must be sequential)
    let symlinks_start = Instant::now();
    for (link, _is_dir) in symlinks {
        let raw_target = tokio::fs::read_link(&link.fs_path)
            .await
            .map_err(|source| Error::ReadLink {
                path: link.fs_path.clone(),
                source,
            })?;

        let parent_path = link.fs_path.parent().unwrap_or(Path::new(""));
        let resolved_target = parent_path.join(&raw_target);

        // Ensure parent directory exists
        if let Some(parent) = link.box_path.parent()
            && !created_dirs.contains(&parent)
            && bf.metadata().index(&parent).is_none()
        {
            let parent_owned = parent.into_owned();
            bf.mkdir_all(parent_owned.clone(), Default::default())
                .map_err(|source| Error::CreateDirectory {
                    path: parent_owned.clone(),
                    source,
                })?;
            created_dirs.insert(parent_owned);
        }

        let mut link_meta = fs::metadata_to_attrs(&link.meta, timestamps, ownership);
        if args.xattrs {
            if let Ok(xattrs) = fs::read_xattrs(&link.fs_path) {
                link_meta.extend(xattrs);
            }
        }

        // Try to resolve as internal link first
        let target_box_path = BoxPath::new(&resolved_target).ok();
        let target_index = target_box_path
            .as_ref()
            .and_then(|p| bf.metadata().index(p));

        if let Some(target_index) = target_index {
            // Internal symlink - target exists in archive
            bf.link(link.box_path.clone(), target_index, link_meta)
                .map_err(|source| Error::CreateLink {
                    path: link.box_path,
                    source,
                })?;
        } else {
            // External symlink - target not in archive
            if !args.allow_external_symlinks {
                return Err(Error::ExternalSymlinkDetected {
                    link_path: link.box_path.to_path_buf(),
                    target: raw_target.to_string_lossy().to_string(),
                });
            }

            // Use the raw target path (preserving relative components like ../)
            let target_str = raw_target.to_string_lossy();
            // Normalize path separators to forward slashes
            let normalized_target = target_str.replace('\\', "/");

            bf.external_link(link.box_path.clone(), &normalized_target, link_meta)
                .map_err(|source| Error::CreateLink {
                    path: link.box_path,
                    source,
                })?;
        }
    }

    timing.symlinks = symlinks_start.elapsed();

    // Process files
    let files_start = Instant::now();
    let (files_added, bytes_original, bytes_compressed) = if args.serial {
        // Sequential processing
        if let Some(ref pb) = progress {
            pb.set_message("Adding files...");
        }

        let mut files_added = 0u64;
        let mut bytes_original = 0u64;
        let mut bytes_compressed = 0u64;

        for file in files {
            // Ensure parent directory exists
            if let Some(parent) = file.box_path.parent()
                && !created_dirs.contains(&parent)
                && bf.metadata().index(&parent).is_none()
            {
                let parent_owned = parent.into_owned();
                bf.mkdir_all(parent_owned.clone(), Default::default())
                    .map_err(|source| Error::CreateDirectory {
                        path: parent_owned.clone(),
                        source,
                    })?;
                created_dirs.insert(parent_owned);
            }

            let f = tokio::fs::File::open(&file.fs_path)
                .await
                .map_err(|source| Error::OpenFile {
                    path: file.fs_path.clone(),
                    source,
                })?;

            let mut file_meta = fs::metadata_to_attrs(&file.meta, timestamps, ownership);
            if args.xattrs {
                if let Ok(xattrs) = fs::read_xattrs(&file.fs_path) {
                    file_meta.extend(xattrs);
                }
            }

            let record = if !args.no_checksum {
                bf.insert_streaming::<_, blake3::Hasher>(
                    &file.config,
                    file.box_path.clone(),
                    f,
                    file_meta,
                )
                .await
                .map_err(|source| Error::AddFile {
                    path: file.fs_path.clone(),
                    source,
                })?
            } else {
                let buf_reader = tokio::io::BufReader::new(f);
                bf.insert(&file.config, file.box_path.clone(), buf_reader, file_meta)
                    .await
                    .map_err(|source| Error::AddFile {
                        path: file.fs_path.clone(),
                        source,
                    })?
            };

            files_added += 1;
            bytes_original += record.decompressed_length;
            bytes_compressed += record.length;

            if let Some(ref pb) = progress {
                pb.set_message(format!("Adding: {}", file.box_path));
                pb.tick();
            }
        }

        (files_added, bytes_original, bytes_compressed)
    } else {
        // Parallel processing (default)
        let concurrency = args.jobs.unwrap_or_else(num_cpus::get);

        // Ensure all parent directories exist before parallel processing
        for file in &files {
            if let Some(parent) = file.box_path.parent()
                && !created_dirs.contains(&parent)
                && bf.metadata().index(&parent).is_none()
            {
                let parent_owned = parent.into_owned();
                bf.mkdir_all(parent_owned.clone(), Default::default())
                    .map_err(|source| Error::CreateDirectory {
                        path: parent_owned.clone(),
                        source,
                    })?;
                created_dirs.insert(parent_owned);
            }
        }

        // Convert to FileJob items
        let file_jobs: Vec<FileJob> = files
            .into_iter()
            .map(|f| FileJob {
                fs_path: f.fs_path,
                box_path: f.box_path,
                config: f.config,
            })
            .collect();

        // Set up progress channel
        let (progress_tx, mut progress_rx) =
            tokio::sync::mpsc::unbounded_channel::<ParallelProgress>();

        // Spawn progress handler task
        let progress_clone = progress.clone();
        let progress_task = tokio::spawn(async move {
            let mut total = 0u64;
            let mut compressed_count = 0u64;
            let mut compress_end: Option<Instant> = None;
            while let Some(update) = progress_rx.recv().await {
                if let Some(ref pb) = progress_clone {
                    match update {
                        ParallelProgress::Started { total_files } => {
                            total = total_files;
                            pb.set_message(format!(
                                "Compressing 0/{} ({} parallel)...",
                                total_files, concurrency
                            ));
                        }
                        ParallelProgress::Compressed { .. } => {
                            compressed_count += 1;
                            pb.set_message(format!(
                                "Compressing {}/{}...",
                                compressed_count, total
                            ));
                            if compressed_count == total {
                                compress_end = Some(Instant::now());
                            }
                        }
                        ParallelProgress::Written {
                            files_written,
                            total_files,
                            ..
                        } => {
                            pb.set_message(format!("Writing {}/{}...", files_written, total_files));
                        }
                        ParallelProgress::Finished => {
                            pb.set_message("Finalizing...");
                        }
                        _ => {}
                    }
                }
            }
            compress_end
        });

        let stats = bf
            .add_paths_parallel_with_progress(
                file_jobs,
                !args.no_checksum,
                timestamps,
                ownership,
                concurrency,
                Some(progress_tx),
            )
            .await
            .map_err(|source| Error::AddFilesParallel { source })?;

        // Wait for progress task to finish and get compress end time
        let compress_end = progress_task.await.ok().flatten();
        let files_end = Instant::now();

        // Calculate timing: compress is from files_start to compress_end, write is from compress_end to now
        if let Some(ce) = compress_end {
            timing.compress = ce.duration_since(files_start);
            timing.write = files_end.duration_since(ce);
        } else {
            // Fallback if we didn't get the compress end time
            timing.compress = files_end.duration_since(files_start);
        }

        (
            stats.files_added,
            stats.bytes_original,
            stats.bytes_compressed,
        )
    };

    // Finish the archive
    let finalize_start = Instant::now();
    let archive_path = bf.path().to_path_buf();
    let final_size = bf.finish().await.map_err(|source| Error::FinishArchive {
        path: archive_path.clone(),
        source,
    })?;
    timing.finalize = finalize_start.elapsed();

    if let Some(pb) = progress {
        pb.finish_and_clear();
    }

    if !args.quiet {
        let ratio = if bytes_original == 0 {
            0.0
        } else {
            100.0 - (bytes_compressed as f64 / bytes_original as f64 * 100.0)
        };

        println!(
            "Created {} ({} files, {} -> {}, {:.1}% compression)",
            archive_path.display(),
            files_added,
            crate::util::format_size(bytes_original),
            crate::util::format_size(final_size),
            ratio
        );

        // Print timing breakdown
        let total = timing.collect
            + timing.directories
            + timing.symlinks
            + timing.compress
            + timing.write
            + timing.finalize;
        println!("\nTiming:");
        println!("  Collect:     {:>8.2?}", timing.collect);
        println!("  Directories: {:>8.2?}", timing.directories);
        println!("  Symlinks:    {:>8.2?}", timing.symlinks);
        println!("  Compress:    {:>8.2?}", timing.compress);
        println!("  Write:       {:>8.2?}", timing.write);
        println!("  Finalize:    {:>8.2?}", timing.finalize);
        println!("  Total:       {:>8.2?}", total);
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn collect_path(
    path: &Path,
    config: &CompressionConfig,
    exclude_patterns: &[glob::Pattern],
    include_hidden: bool,
    recursive: bool,
    archive_canonical: &Path,
    archive_filename: Option<&std::ffi::OsStr>,
    entries: &mut Vec<EntryKind>,
    known_dirs: &mut HashSet<BoxPath<'static>>,
    known_files: &mut HashSet<BoxPath<'static>>,
) -> Result<()> {
    let path_meta = tokio::fs::metadata(path)
        .await
        .map_err(|source| Error::ProcessFile {
            path: path.to_path_buf(),
            source,
        })?;

    if path_meta.is_file() {
        let box_path = BoxPath::new(path).map_err(|source| Error::InvalidPath {
            path: path.to_path_buf(),
            source,
        })?;

        if !known_files.contains(&box_path) {
            known_files.insert(box_path.clone());
            entries.push(EntryKind::File(CollectedEntry {
                fs_path: path.to_path_buf(),
                box_path,
                meta: path_meta,
                config: config.clone(),
            }));
        }
        return Ok(());
    }

    // Directory - walk it
    let mut walker = WalkDir::new(path);

    while let Some(entry) = walker.next().await {
        let entry = entry.map_err(|e| Error::ProcessDirEntry { source: e.into() })?;

        // Skip hidden files if not allowed
        if !include_hidden && fs::is_hidden(&entry.path()) {
            continue;
        }

        let file_path = entry.path();

        // Check excludes
        if matches_exclude(&file_path, exclude_patterns) {
            continue;
        }

        let file_type = entry
            .file_type()
            .await
            .map_err(|source| Error::ProcessFile {
                path: file_path.to_path_buf(),
                source,
            })?;

        let meta = entry
            .metadata()
            .await
            .map_err(|source| Error::ProcessFile {
                path: file_path.to_path_buf(),
                source,
            })?;

        // Skip non-recursive if not at top level
        if !recursive && file_path != path && file_type.is_dir() {
            continue;
        }

        // Quick filename check before expensive canonicalize
        if file_path.file_name() == archive_filename {
            let canonical_path = tokio::fs::canonicalize(&file_path)
                .await
                .map_err(|source| Error::CanonicalizePath {
                    path: file_path.to_path_buf(),
                    source,
                })?;
            if archive_canonical == canonical_path {
                continue;
            }
        }

        let box_path = BoxPath::new(&file_path).map_err(|source| Error::InvalidPath {
            path: file_path.to_path_buf(),
            source,
        })?;

        if file_type.is_symlink() {
            if !known_files.contains(&box_path) && !known_dirs.contains(&box_path) {
                let is_dir = file_type.is_dir();
                if is_dir {
                    known_dirs.insert(box_path.clone());
                } else {
                    known_files.insert(box_path.clone());
                }
                entries.push(EntryKind::Symlink {
                    entry: CollectedEntry {
                        fs_path: file_path.to_path_buf(),
                        box_path,
                        meta,
                        config: config.clone(),
                    },
                    is_dir,
                });
            }
        } else if file_type.is_dir() {
            if !known_dirs.contains(&box_path) {
                known_dirs.insert(box_path.clone());
                entries.push(EntryKind::Directory(CollectedEntry {
                    fs_path: file_path.to_path_buf(),
                    box_path,
                    meta,
                    config: config.clone(),
                }));
            }
        } else if !known_files.contains(&box_path) {
            known_files.insert(box_path.clone());
            entries.push(EntryKind::File(CollectedEntry {
                fs_path: file_path.to_path_buf(),
                box_path,
                meta,
                config: config.clone(),
            }));
        }
    }

    Ok(())
}
