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

pub async fn run(args: ListArgs) -> Result<()> {
    let bf = BoxFileReader::open(&args.archive)
        .await
        .map_err(|source| Error::OpenArchive {
            path: args.archive.clone(),
            source,
        })?;

    if args.json {
        list_json(&bf)
    } else if args.long {
        list_long(&bf)
    } else {
        list_compact(&bf)
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
            Record::File(file) => {
                let checksum = format_checksum(file, bf.metadata());
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
        }
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

fn list_json(bf: &BoxFileReader) -> Result<()> {
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
                checksum: Some(format_checksum(file, bf.metadata())).filter(|s| s != "-"),
                target: None,
            },
        };

        entries.push(entry);
    }

    println!("{}", serde_json::to_string_pretty(&entries).unwrap());
    Ok(())
}

fn format_checksum(file: &box_format::FileRecord, meta: &box_format::BoxMetadata) -> String {
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
