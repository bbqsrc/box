use box_format::{BoxFileReader, Record};

use crate::cli::InfoArgs;
use crate::error::{Error, Result};
use crate::util::{format_size, format_time};

pub async fn run(args: InfoArgs) -> Result<()> {
    let bf = BoxFileReader::open(&args.archive)
        .await
        .map_err(|source| Error::OpenArchive {
            path: args.archive.clone(),
            source,
        })?;

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

    if let Some(created) = metadata.file_attr("created") {
        println!("Created:     {}", format_time(Some(created)));
    }

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

    let ratio = if total_size == 0 {
        0.0
    } else {
        100.0 - (total_compressed as f64 / total_size as f64 * 100.0)
    };
    println!("  Ratio:       {:.1}%", ratio);

    // Show file attributes if any (excluding created which we already showed)
    let mut attrs = metadata.file_attrs();
    attrs.remove("created");

    if !attrs.is_empty() {
        println!();
        println!("Attributes:");
        for (key, value) in attrs {
            if let box_format::AttrValue::Json(json) = value {
                let v = serde_json::to_string(&json).unwrap();
                println!("  {}: {}", key, v);
            } else {
                println!("  {}: {}", key, value);
            }
        }
    }

    Ok(())
}
