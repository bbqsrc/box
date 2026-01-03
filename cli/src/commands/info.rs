use std::collections::BTreeMap;

use box_format::{AttrMap, AttrValue, BOX_EPOCH_UNIX, BoxFileReader, BoxMetadata, BoxPath, Record};

use crate::cli::InfoArgs;
use crate::error::{Error, Result};
use crate::util::format_size;

/// Resolve a record's attrs map to key names and AttrValue.
fn resolve_attrs<'a>(
    attrs: &'a AttrMap,
    metadata: &'a BoxMetadata<'_>,
) -> BTreeMap<&'a str, AttrValue<'a>> {
    let mut map = BTreeMap::new();
    for (key_idx, value) in attrs {
        if let Some(key) = metadata.attr_key_name(*key_idx)
            && let Some(attr_type) = metadata.attr_key_type(*key_idx)
        {
            map.insert(key, metadata.parse_attr_value(value, attr_type));
        }
    }
    map
}

/// Format an attribute with its type annotation and value.
fn format_attr(key: &str, value: &AttrValue<'_>) -> String {
    match value {
        AttrValue::Bytes([]) => format!("{}[bytes]: (empty)", key),
        AttrValue::Bytes(bytes) => {
            let hex: String = bytes
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<Vec<_>>()
                .join(" ");
            format!("{}[{} bytes]: {}", key, bytes.len(), hex)
        }
        AttrValue::String("") => format!("{}[str]: (empty)", key),
        AttrValue::String(s) => format!("{}[str]: {}", key, s),
        AttrValue::Json(json) => {
            let v = serde_json::to_string(json).unwrap_or_else(|_| format!("{:?}", json));
            format!("{}[json]: {}", key, v)
        }
        AttrValue::U8(v) => format!("{}[u8]: {}", key, v),
        AttrValue::Vi32(v) => format!("{}[vi32]: {}", key, v),
        AttrValue::Vu32(v) => {
            if key == "unix.mode" {
                format!("{}[vu32 oct]: {:o}", key, v)
            } else {
                format!("{}[vu32]: {}", key, v)
            }
        }
        AttrValue::Vi64(v) => format!("{}[vi64]: {}", key, v),
        AttrValue::Vu64(v) => format!("{}[vu64]: {}", key, v),
        AttrValue::U128(bytes) => {
            let hex: String = bytes.iter().map(|b| format!("{:02x}", b)).collect();
            format!("{}[u128]: {}", key, hex)
        }
        AttrValue::U256(bytes) => {
            let hex: String = bytes.iter().map(|b| format!("{:02x}", b)).collect();
            format!("{}[u256]: {}", key, hex)
        }
        AttrValue::DateTime(minutes) => {
            let unix_secs = minutes * 60 + BOX_EPOCH_UNIX;
            let time = std::time::UNIX_EPOCH + std::time::Duration::new(unix_secs as u64, 0);
            let datetime: chrono::DateTime<chrono::Utc> = time.into();
            let formatted = datetime.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
            format!("{}[datetime]: {}", key, formatted)
        }
    }
}

pub async fn run(args: InfoArgs) -> Result<()> {
    let bf = BoxFileReader::open(&args.archive)
        .await
        .map_err(|source| Error::OpenArchive {
            path: args.archive.clone(),
            source,
        })?;

    // If a file path is provided, show info for that file
    if let Some(file_path) = &args.file {
        return show_file_info(&bf, file_path);
    }

    // Otherwise show archive info
    show_archive_info(&bf, &args)
}

fn show_file_info(bf: &BoxFileReader, file_path: &str) -> Result<()> {
    let metadata = bf.metadata();

    let box_path = BoxPath::new(file_path).map_err(|source| Error::InvalidPath {
        path: file_path.into(),
        source,
    })?;

    let index = metadata
        .index(&box_path)
        .ok_or_else(|| Error::FileNotFound {
            path: file_path.into(),
        })?;

    let record = metadata.record(index).ok_or_else(|| Error::FileNotFound {
        path: file_path.into(),
    })?;

    println!("Path:  {}", file_path);

    match record {
        Record::File(f) => {
            println!("Type:  file");
            println!("Compression: {}", f.compression);
            println!(
                "Size:  {} (compressed: {})",
                format_size(f.decompressed_length),
                format_size(f.length)
            );

            let ratio = if f.decompressed_length == 0 {
                0.0
            } else {
                100.0 - (f.length as f64 / f.decompressed_length as f64 * 100.0)
            };
            println!("Ratio: {:.1}%", ratio);

            println!();
            println!("Record:");
            println!("  Index:  {}", index.get());
            println!("  Offset: {:#x}", f.data.get());

            // Show attributes
            let attrs = resolve_attrs(&f.attrs, metadata);
            if !attrs.is_empty() {
                println!();
                println!("Attributes:");
                for (key, value) in attrs {
                    println!("  {}", format_attr(key, &value));
                }
            }
        }
        Record::Directory(d) => {
            println!("Type:  directory");
            println!("Entries: {}", d.entries.len());

            println!();
            println!("Record:");
            println!("  Index: {}", index.get());

            // Show attributes
            let attrs = resolve_attrs(&d.attrs, metadata);
            if !attrs.is_empty() {
                println!();
                println!("Attributes:");
                for (key, value) in attrs {
                    println!("  {}", format_attr(key, &value));
                }
            }
        }
        Record::Link(l) => {
            println!("Type:   symlink");
            let target_path = metadata
                .path_for_index(l.target)
                .map(|p| p.to_string())
                .unwrap_or_else(|| format!("<invalid:{}>", l.target.get()));
            println!("Target: {}", target_path);

            println!();
            println!("Record:");
            println!("  Index: {}", index.get());

            // Show attributes
            let attrs = resolve_attrs(&l.attrs, metadata);
            if !attrs.is_empty() {
                println!();
                println!("Attributes:");
                for (key, value) in attrs {
                    println!("  {}", format_attr(key, &value));
                }
            }
        }
    }

    Ok(())
}

fn show_archive_info(bf: &BoxFileReader, args: &InfoArgs) -> Result<()> {
    let metadata = bf.metadata();

    // Count files, directories, and links
    let mut file_count = 0u64;
    let mut dir_count = 0u64;
    let mut link_count = 0u64;
    let mut total_size = 0u64;
    let mut total_compressed = 0u64;

    for result in metadata.iter() {
        match result.record {
            Record::File(f) => {
                file_count += 1;
                total_size += f.decompressed_length;
                total_compressed += f.length;
            }
            Record::Directory(_) => dir_count += 1,
            Record::Link(_) => link_count += 1,
        }
    }

    println!("Archive:     {}", args.archive.display());
    println!("Version:     {}", bf.version());

    let alignment = match bf.alignment() {
        0 => "None".to_string(),
        v => format!("{} bytes", v),
    };
    println!("Alignment:   {}", alignment);

    println!();
    println!("Contents:");
    println!("  Files:       {}", file_count);
    println!("  Directories: {}", dir_count);
    if link_count > 0 {
        println!("  Links:       {}", link_count);
    }

    println!();
    println!("Size:");
    println!("  Original:    {}", format_size(total_size));
    println!("  Compressed:  {}", format_size(total_compressed));
    println!("  Trailer:     {}", format_size(bf.trailer_size()));

    let ratio = if total_size == 0 {
        0.0
    } else {
        100.0 - (total_compressed as f64 / total_size as f64 * 100.0)
    };
    println!("  Ratio:       {:.1}%", ratio);

    // Show all attribute keys used in the archive (with indices)
    let all_keys = metadata.attr_keys_with_indices();
    if !all_keys.is_empty() {
        println!();
        println!("Attribute keys:");
        for (idx, key) in all_keys {
            println!("  {}: {}", idx, key);
        }
    }

    // Show file-level (archive) attributes if any
    let attrs = metadata.file_attrs();
    if !attrs.is_empty() {
        println!();
        println!("Archive attributes:");
        for (key, value) in &attrs {
            println!("  {}", format_attr(key, value));
        }
    }

    Ok(())
}
