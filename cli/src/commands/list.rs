use box_format::{BoxFileReader, Record};
use serde::Serialize;

use crate::cli::ListArgs;
use crate::error::{Error, Result};
use crate::util::{format_acl, format_path, format_size, format_time};

#[derive(Serialize)]
struct JsonEntry {
    path: String,
    #[serde(rename = "type")]
    entry_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    compressed_size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    compression: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    created: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    checksum: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    target: Option<String>,
}

#[derive(Serialize)]
struct JsonArchive {
    archive: String,
    alignment: u32,
    attributes: serde_json::Map<String, serde_json::Value>,
    entries: Vec<JsonEntry>,
}

// [spec:box:req:cli-commands.root.inspect]
pub async fn run(args: ListArgs) -> Result<()> {
    let mut json_archives = Vec::new();

    for (i, path) in args.archives.iter().enumerate() {
        let bf = BoxFileReader::open(path)
            .await
            .map_err(|source| Error::OpenArchive {
                path: path.clone(),
                source,
            })?;

        if args.json {
            json_archives.push(json_archive(&args.archives[i], &bf));
        } else {
            if i > 0 {
                println!();
            }
            if args.long {
                list_long(&bf)?;
            } else {
                if args.archives.len() > 1 {
                    println!("Archive: {}", bf.path().display());
                }
                list_compact(&bf)?;
            }
        }
    }

    if args.json {
        println!("{}", serde_json::to_string_pretty(&json_archives).unwrap());
    }

    Ok(())
}

fn json_archive(path: &std::path::Path, bf: &BoxFileReader) -> JsonArchive {
    let attributes = bf
        .metadata()
        .file_attrs()
        .iter()
        .map(|(key, value)| (key.to_string(), attr_json(value)))
        .collect();

    JsonArchive {
        archive: path.display().to_string(),
        alignment: bf.alignment(),
        attributes,
        entries: collect_json(bf),
    }
}

/// Attribute values as data, not display: typed values keep their type,
/// binary values are hex, and timestamps render as UTC RFC 3339.
fn attr_json(value: &box_format::AttrValue) -> serde_json::Value {
    use box_format::AttrValue;

    match value {
        AttrValue::String(s) => (*s).into(),
        AttrValue::Json(v) => v.clone(),
        AttrValue::U8(n) => (*n).into(),
        AttrValue::Vi32(n) => (*n).into(),
        AttrValue::Vu32(n) => (*n).into(),
        AttrValue::Vi64(n) => (*n).into(),
        AttrValue::Vu64(n) => (*n).into(),
        AttrValue::U128(b) => hex(b.as_slice()).into(),
        AttrValue::U256(b) => hex(b.as_slice()).into(),
        AttrValue::Bytes(b) => hex(b).into(),
        AttrValue::DateTime(minutes) => {
            let unix_seconds = minutes * 60 + box_format::BOX_EPOCH_UNIX;
            let time = std::time::UNIX_EPOCH + std::time::Duration::new(unix_seconds as u64, 0);
            let datetime: chrono::DateTime<chrono::Utc> = time.into();
            datetime.format("%Y-%m-%dT%H:%M:%SZ").to_string().into()
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Entry checksums as data: the full digest, algorithm-prefixed, never the
/// table view's truncation.
fn json_checksum(blake3: Option<&[u8]>, crc32: Option<&[u8]>) -> Option<String> {
    if let Some(bytes) = blake3 {
        return Some(format!("blake3:{}", hex(bytes)));
    }
    let bytes = crc32?;
    if bytes.len() >= 4 {
        let value = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        Some(format!("crc32:{:08x}", value))
    } else {
        None
    }
}

fn list_compact(bf: &BoxFileReader) -> Result<()> {
    println!("{:>12}  {:>12}  {:>6}  Path", "Compressed", "Size", "Ratio");
    println!("{}", "-".repeat(60));

    let mut total_compressed = 0u64;
    let mut total_size = 0u64;

    for result in bf.metadata().iter() {
        let record = result.record;
        let path = format_path(&result.path, record.as_directory().is_some());

        match record {
            Record::Directory(_) => {
                println!("{:>12}  {:>12}  {:>6}  {}", "-", "-", "-", path);
            }
            Record::Link(link) => {
                let target = bf
                    .resolve_link(link)
                    .map(|item| format_path(&item.path, item.record.as_directory().is_some()))
                    .unwrap_or_else(|_| format!("<invalid:{}>", link.target.get()));
                println!(
                    "{:>12}  {:>12}  {:>6}  {} -> {}",
                    "-", "-", "-", path, target
                );
            }
            Record::ExternalLink(link) => {
                println!(
                    "{:>12}  {:>12}  {:>6}  {} -> {} (external)",
                    "-", "-", "-", path, link.target
                );
            }
            Record::File(file) => {
                let ratio = if file.decompressed_length == 0 {
                    0.0
                } else {
                    100.0 - (file.length as f64 / file.decompressed_length as f64 * 100.0)
                };

                println!(
                    "{:>12}  {:>12}  {:>5.1}%  {}",
                    format_size(file.length),
                    format_size(file.decompressed_length),
                    ratio,
                    path
                );

                total_compressed += file.length;
                total_size += file.decompressed_length;
            }
            Record::ChunkedFile(file) => {
                let ratio = if file.decompressed_length == 0 {
                    0.0
                } else {
                    100.0 - (file.length as f64 / file.decompressed_length as f64 * 100.0)
                };

                println!(
                    "{:>12}  {:>12}  {:>5.1}%  {}",
                    format_size(file.length),
                    format_size(file.decompressed_length),
                    ratio,
                    path
                );

                total_compressed += file.length;
                total_size += file.decompressed_length;
            }
        }
    }

    println!("{}", "-".repeat(60));
    let total_ratio = if total_size == 0 {
        0.0
    } else {
        100.0 - (total_compressed as f64 / total_size as f64 * 100.0)
    };
    println!(
        "{:>12}  {:>12}  {:>5.1}%  Total",
        format_size(total_compressed),
        format_size(total_size),
        total_ratio
    );

    Ok(())
}

fn list_long(bf: &BoxFileReader) -> Result<()> {
    let alignment = match bf.alignment() {
        0 => "None".into(),
        v => format!("{} bytes", v),
    };

    println!(
        "Archive: {} (alignment: {})",
        bf.path().display(),
        alignment
    );

    if let Some(v) = bf.metadata().file_attr("created") {
        println!("Created: {}", format_time(Some(v)));
    }

    println!();
    println!(
        "{:8}  {:>12}  {:>12}  {:20}  {:9}  {:>16}  Path",
        "Method", "Compressed", "Size", "Created", "Perms", "Checksum"
    );
    println!("{}", "-".repeat(100));

    let mut any_chunked = false;
    for result in bf.metadata().iter() {
        let record = result.record;
        let path = format_path(&result.path, record.as_directory().is_some());
        let acl = format_acl(record.attr(bf.metadata(), "unix.mode"));
        let time = format_time(record.attr(bf.metadata(), "created"));

        match record {
            Record::Directory(_) => {
                println!(
                    "{:8}  {:>12}  {:>12}  {:20}  {:9}  {:>16}  {}",
                    "<dir>", "-", "-", time, acl, "-", path
                );
            }
            Record::Link(link) => {
                let target = bf
                    .resolve_link(link)
                    .map(|item| format_path(&item.path, item.record.as_directory().is_some()))
                    .unwrap_or_else(|_| format!("<invalid:{}>", link.target.get()));
                println!(
                    "{:8}  {:>12}  {:>12}  {:20}  {:9}  {:>16}  {} -> {}",
                    "<link>", "-", "-", time, acl, "-", path, target
                );
            }
            Record::ExternalLink(link) => {
                println!(
                    "{:8}  {:>12}  {:>12}  {:20}  {:9}  {:>16}  {} -> {} (external)",
                    "<xlink>", "-", "-", time, acl, "-", path, link.target
                );
            }
            Record::File(file) => {
                let checksum = format_checksum_file(file, bf.metadata());
                println!(
                    "{:8}  {:>12}  {:>12}  {:20}  {:9}  {:>16}  {}",
                    format!("{}", file.compression),
                    format_size(file.length),
                    format_size(file.decompressed_length),
                    time,
                    acl,
                    checksum,
                    path
                );
            }
            Record::ChunkedFile(file) => {
                let checksum = format_checksum_chunked(file, bf.metadata());
                any_chunked = true;
                println!(
                    "{:8}  {:>12}  {:>12}  {:20}  {:9}  {:>16}  {}",
                    format!("{}*", file.compression),
                    format_size(file.length),
                    format_size(file.decompressed_length),
                    time,
                    acl,
                    checksum,
                    path
                );
            }
        }
    }

    if any_chunked {
        println!();
        println!("* chunked");
    }

    // Print file attributes if any
    let mut attrs = bf.metadata().file_attrs();
    attrs.remove("created");

    if !attrs.is_empty() {
        println!();
        println!("Archive attributes:");
        for (key, value) in attrs {
            if let box_format::AttrValue::Json(json) = value {
                let v = serde_json::to_string_pretty(&json).unwrap();
                println!("  {} = {}", key, textwrap::indent(&v, "  ").trim_start());
            } else {
                println!("  {} = {}", key, value);
            }
        }
    }

    Ok(())
}

fn collect_json(bf: &BoxFileReader) -> Vec<JsonEntry> {
    let mut entries = Vec::new();

    for result in bf.metadata().iter() {
        let record = result.record;
        let path = result.path.to_string();

        let entry = match record {
            Record::Directory(_) => JsonEntry {
                path,
                entry_type: "directory".to_string(),
                size: None,
                compressed_size: None,
                compression: None,
                created: record
                    .attr(bf.metadata(), "created")
                    .map(|v| format_time(Some(v)))
                    .filter(|s| s != "-"),
                checksum: None,
                target: None,
            },
            Record::Link(link) => JsonEntry {
                path,
                entry_type: "link".to_string(),
                size: None,
                compressed_size: None,
                compression: None,
                created: record
                    .attr(bf.metadata(), "created")
                    .map(|v| format_time(Some(v)))
                    .filter(|s| s != "-"),
                checksum: None,
                target: bf
                    .metadata()
                    .path_for_index(link.target)
                    .map(|p| p.to_string()),
            },
            Record::ExternalLink(link) => JsonEntry {
                path,
                entry_type: "external_link".to_string(),
                size: None,
                compressed_size: None,
                compression: None,
                created: record
                    .attr(bf.metadata(), "created")
                    .map(|v| format_time(Some(v)))
                    .filter(|s| s != "-"),
                checksum: None,
                target: Some(link.target.to_string()),
            },
            Record::File(file) => JsonEntry {
                path,
                entry_type: "file".to_string(),
                size: Some(file.decompressed_length),
                compressed_size: Some(file.length),
                compression: Some(format!("{}", file.compression)),
                created: record
                    .attr(bf.metadata(), "created")
                    .map(|v| format_time(Some(v)))
                    .filter(|s| s != "-"),
                checksum: json_checksum(
                    file.attr(bf.metadata(), "blake3"),
                    file.attr(bf.metadata(), "crc32"),
                ),
                target: None,
            },
            Record::ChunkedFile(file) => JsonEntry {
                path,
                entry_type: "chunked_file".to_string(),
                size: Some(file.decompressed_length),
                compressed_size: Some(file.length),
                compression: Some(format!("{}", file.compression)),
                created: record
                    .attr(bf.metadata(), "created")
                    .map(|v| format_time(Some(v)))
                    .filter(|s| s != "-"),
                checksum: json_checksum(
                    file.attr(bf.metadata(), "blake3"),
                    file.attr(bf.metadata(), "crc32"),
                ),
                target: None,
            },
        };

        entries.push(entry);
    }

    entries
}

fn format_checksum_file(file: &box_format::FileRecord, meta: &box_format::BoxMetadata) -> String {
    if let Some(blake3_bytes) = file.attr(meta, "blake3") {
        blake3_bytes
            .iter()
            .take(8)
            .map(|b| format!("{:02x}", b))
            .collect::<String>()
    } else if let Some(crc32_bytes) = file.attr(meta, "crc32") {
        if crc32_bytes.len() >= 4 {
            format!(
                "{:08x}",
                u32::from_le_bytes([
                    crc32_bytes[0],
                    crc32_bytes[1],
                    crc32_bytes[2],
                    crc32_bytes[3]
                ])
            )
        } else {
            "-".to_string()
        }
    } else {
        "-".to_string()
    }
}

fn format_checksum_chunked(
    file: &box_format::ChunkedFileRecord,
    meta: &box_format::BoxMetadata,
) -> String {
    if let Some(blake3_bytes) = file.attr(meta, "blake3") {
        blake3_bytes
            .iter()
            .take(8)
            .map(|b| format!("{:02x}", b))
            .collect::<String>()
    } else if let Some(crc32_bytes) = file.attr(meta, "crc32") {
        if crc32_bytes.len() >= 4 {
            format!(
                "{:08x}",
                u32::from_le_bytes([
                    crc32_bytes[0],
                    crc32_bytes[1],
                    crc32_bytes[2],
                    crc32_bytes[3]
                ])
            )
        } else {
            "-".to_string()
        }
    } else {
        "-".to_string()
    }
}
