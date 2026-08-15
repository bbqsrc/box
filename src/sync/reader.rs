use std::collections::HashSet;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use mmap_io::MemoryMappedFile;
use mmap_io::segment::Segment;

#[cfg(feature = "xz")]
use crate::compression::xz::XzDecompressor;
#[cfg(feature = "zstd")]
use crate::compression::zstd::ZstdDecompressor;
use crate::core::{ArchiveReader, AttrValue, BoxMetadata, RecordIndex};
use crate::path::BoxPath;
use crate::record::{ChunkedFileRecord, FileRecord, Record};
use crate::{
    compression::{Compression, constants::DEFAULT_BLOCK_SIZE},
    de::deserialize_metadata_borrowed,
    header::BoxHeader,
};

use crate::aio::{ExtractError, ExtractOptions, ExtractStats, OpenError, ValidateStats};

/// Sync reader for Box archives.
///
/// This is a frontend that wraps the sans-IO [`ArchiveReader`] core,
/// providing sync I/O operations for reading archives.
// [spec:box:sem:sync-io.root]
pub struct BoxReader {
    /// The sans-IO core that manages archive state
    pub(crate) core: ArchiveReader<'static>,
    /// Path to the archive file
    pub(crate) path: PathBuf,
    /// Holds the mmapped trailer data. The Arc inside keeps the data alive.
    /// This must not be dropped before `core.meta` is dropped.
    #[allow(dead_code)]
    pub(crate) trailer_segment: Segment,
}

impl std::fmt::Debug for BoxReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoxReader")
            .field("path", &self.path)
            .field("header", &self.core.header)
            .field("meta", &self.core.meta)
            .field("offset", &self.core.offset)
            .finish_non_exhaustive()
    }
}

fn read_header_sync<R: Read + Seek>(file: &mut R, offset: u64) -> std::io::Result<BoxHeader> {
    file.seek(SeekFrom::Start(offset))?;

    let mut buf = [0u8; 32];
    file.read_exact(&mut buf)?;
    let (header_data, _) = crate::parse::parse_header(&buf)?;

    Ok(BoxHeader {
        version: header_data.version,
        allow_external_symlinks: header_data.allow_external_symlinks,
        allow_escapes: header_data.allow_escapes,
        alignment: header_data.alignment,
        trailer: std::num::NonZeroU64::new(header_data.trailer_offset),
    })
}

fn invalid_chunked_metadata(message: impl Into<String>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message.into())
}

fn chunked_data_end(record: &ChunkedFileRecord<'_>) -> std::io::Result<u64> {
    record
        .data
        .get()
        .checked_add(record.length)
        .ok_or_else(|| invalid_chunked_metadata("chunked file data range overflows u64"))
}

fn checked_chunked_block<'a>(
    record: &ChunkedFileRecord<'_>,
    all_data: &'a [u8],
    physical_offset: u64,
    compressed_end: u64,
) -> std::io::Result<&'a [u8]> {
    let data_start = record.data.get();
    let data_end = chunked_data_end(record)?;

    if physical_offset < data_start || physical_offset >= data_end {
        return Err(invalid_chunked_metadata(format!(
            "chunked block offset {physical_offset} is outside data range [{data_start}, {data_end})"
        )));
    }
    if compressed_end <= physical_offset || compressed_end > data_end {
        return Err(invalid_chunked_metadata(format!(
            "chunked block end {compressed_end} does not follow offset {physical_offset} within data range [{data_start}, {data_end})"
        )));
    }

    let block_start = usize::try_from(physical_offset - data_start)
        .map_err(|_| invalid_chunked_metadata("chunked block offset does not fit in usize"))?;
    let block_end = usize::try_from(compressed_end - data_start)
        .map_err(|_| invalid_chunked_metadata("chunked block end does not fit in usize"))?;

    all_data.get(block_start..block_end).ok_or_else(|| {
        invalid_chunked_metadata(format!(
            "chunked block byte range [{block_start}, {block_end}) exceeds mapped data length {}",
            all_data.len()
        ))
    })
}

impl BoxReader {
    /// This will open an existing `.box` file for reading and error if the file is not valid.
    // [spec:box:sem:sync-io.root.open]
    // [spec:box:req:wire.root.bounds]
    pub fn open_at_offset<P: AsRef<Path>>(path: P, offset: u64) -> Result<BoxReader, OpenError> {
        let path = path.as_ref().to_path_buf();
        let path = std::fs::canonicalize(&path)
            .map_err(|e| OpenError::InvalidPath(e, path.to_path_buf()))?;

        let file =
            std::fs::File::open(&path).map_err(|e| OpenError::ReadFailed(e, path.clone()))?;

        // Read the header to get the trailer pointer
        let header = {
            let mut reader = std::io::BufReader::new(file);
            read_header_sync(&mut reader, offset).map_err(OpenError::MissingHeader)?
        };

        let trailer_ptr = header.trailer.ok_or(OpenError::MissingTrailer)?;

        // Memory-map the file and use zero-copy deserialization for the trailer
        let mmap = MemoryMappedFile::builder(&path)
            .huge_pages(true)
            .open()
            .map_err(|e| OpenError::ReadFailed(std::io::Error::other(e), path.clone()))?;

        // Get file size to calculate trailer segment bounds
        let file_size = std::fs::metadata(&path)
            .map_err(|e| OpenError::ReadFailed(e, path.clone()))?
            .len();

        let trailer_offset = offset.checked_add(trailer_ptr.get()).ok_or_else(|| {
            OpenError::invalid_trailer_pointer(
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "archive base plus trailer offset overflows a 64-bit file position",
                ),
                header.version,
                offset,
                trailer_ptr.get(),
                file_size,
            )
        })?;
        if trailer_offset > file_size {
            return Err(OpenError::invalid_trailer_at(
                std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!(
                        "metadata trailer starts {} bytes beyond the end of the archive",
                        trailer_offset - file_size
                    ),
                ),
                header.version,
                trailer_offset,
                file_size,
                None,
            ));
        }
        let trailer_len = file_size - trailer_offset;

        let trailer_segment =
            Segment::new(mmap.into(), trailer_offset, trailer_len).map_err(|e| {
                OpenError::invalid_trailer_at(
                    std::io::Error::other(e),
                    header.version,
                    trailer_offset,
                    file_size,
                    None,
                )
            })?;

        let trailer_data = trailer_segment.as_slice().map_err(|e| {
            OpenError::invalid_trailer_at(
                std::io::Error::other(e),
                header.version,
                trailer_offset,
                file_size,
                None,
            )
        })?;

        // Deserialize with borrowed data from the mmap
        let mut pos = 0;
        let meta = deserialize_metadata_borrowed(trailer_data, &mut pos, header.version).map_err(
            |source| {
                OpenError::invalid_trailer_at(
                    source,
                    header.version,
                    trailer_offset,
                    file_size,
                    Some(pos),
                )
            },
        )?;

        // Safety: The trailer_segment holds an Arc<MemoryMappedFile> which keeps the
        // underlying memory alive. As long as BoxReader exists, the segment exists,
        // and the borrowed references in meta remain valid. We transmute to 'static
        // to express this in the type system.
        let meta: BoxMetadata<'static> = unsafe { std::mem::transmute(meta) };

        // Create the sans-IO core reader
        let core = ArchiveReader::new(header, meta, offset);

        Ok(BoxReader {
            core,
            path,
            trailer_segment,
        })
    }

    /// This will open an existing `.box` file for reading and error if the file is not valid.
    #[inline]
    pub fn open<P: AsRef<Path>>(path: P) -> Result<BoxReader, OpenError> {
        Self::open_at_offset(path, 0)
    }

    #[inline(always)]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[inline(always)]
    pub fn alignment(&self) -> u32 {
        self.core.alignment()
    }

    #[inline(always)]
    pub fn version(&self) -> u8 {
        self.core.version()
    }

    /// Returns true if this archive allows `\xNN` escape sequences in paths.
    #[inline(always)]
    pub fn allow_escapes(&self) -> bool {
        self.core.allow_escapes()
    }

    /// Returns true if this archive contains external symlinks (pointing outside the archive).
    #[inline(always)]
    pub fn allow_external_symlinks(&self) -> bool {
        self.core.allow_external_symlinks()
    }

    #[inline(always)]
    pub fn metadata(&self) -> &BoxMetadata<'static> {
        self.core.metadata()
    }

    /// Get file-level attributes with type-aware parsing.
    pub fn file_attrs(&self) -> std::collections::BTreeMap<&str, AttrValue<'_>> {
        self.core.file_attrs()
    }

    #[inline(always)]
    pub fn trailer_size(&self) -> u64 {
        self.trailer_segment.len()
    }

    /// Get an attribute value with fallback to archive-level attributes.
    ///
    /// Checks: record attr -> archive attr -> None
    pub fn get_attr<'a>(&'a self, record: &'a Record<'_>, key: &str) -> Option<&'a [u8]> {
        if let Some(value) = record.attr(self.core.metadata(), key) {
            return Some(value);
        }
        self.core.metadata().file_attr(key)
    }

    /// Get the unix mode for a record, with fallback to defaults.
    #[cfg(unix)]
    pub fn get_mode(&self, record: &Record<'_>) -> u32 {
        self.core.get_mode(record)
    }

    // [spec:box:sem:sync-io.root.read]
    pub fn decompress<W: Write>(
        &self,
        record: &FileRecord<'_>,
        mut dest: W,
    ) -> std::io::Result<()> {
        let segment = self.memory_map(record)?;
        let data = segment.as_slice().map_err(std::io::Error::other)?;
        let mut cursor = std::io::Cursor::new(data);

        match record.compression {
            Compression::Stored => {
                std::io::copy(&mut cursor, &mut dest)?;
            }
            #[cfg(feature = "zstd")]
            Compression::Zstd => {
                let dict = self.core.dictionary();
                let mut decompressor = match dict {
                    Some(d) => ZstdDecompressor::with_dictionary(d)?,
                    None => ZstdDecompressor::new()?,
                };
                let mut read_buf = vec![0u8; DEFAULT_BLOCK_SIZE as usize];
                let mut out_buf = vec![0u8; DEFAULT_BLOCK_SIZE as usize];

                loop {
                    let n = cursor.read(&mut read_buf)?;
                    if n == 0 {
                        break;
                    }

                    let mut in_pos = 0;
                    while in_pos < n {
                        let status = decompressor.decompress(&read_buf[in_pos..n], &mut out_buf)?;
                        let consumed = status.bytes_consumed();
                        let produced = status.bytes_produced();
                        if produced > 0 {
                            dest.write_all(&out_buf[..produced])?;
                        }
                        in_pos += consumed;
                        if status.is_done() {
                            break;
                        }
                    }
                }
            }
            #[cfg(feature = "xz")]
            Compression::Xz => {
                let mut decompressor = XzDecompressor::new()?;
                let mut read_buf = vec![0u8; DEFAULT_BLOCK_SIZE as usize];
                let mut out_buf = vec![0u8; DEFAULT_BLOCK_SIZE as usize];

                loop {
                    let n = cursor.read(&mut read_buf)?;
                    if n == 0 {
                        break;
                    }

                    let mut in_pos = 0;
                    while in_pos < n {
                        let status = decompressor.decompress(&read_buf[in_pos..n], &mut out_buf)?;
                        let consumed = status.bytes_consumed();
                        let produced = status.bytes_produced();
                        if produced > 0 {
                            dest.write_all(&out_buf[..produced])?;
                        }
                        in_pos += consumed;
                        if status.is_done() {
                            break;
                        }
                    }
                }
            }
            Compression::Unknown(id) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Unknown compression ID: {}", id),
                ));
            }
        }
        dest.flush()?;
        Ok(())
    }

    /// Decompress a chunked file by decompressing each block separately.
    // [spec:box:sem:chunked-io.root.block-decompression]
    pub fn decompress_chunked<W: Write>(
        &self,
        record: &ChunkedFileRecord<'_>,
        record_index: RecordIndex,
        mut dest: W,
    ) -> std::io::Result<()> {
        let blocks = self.core.blocks_for_record(record_index);

        if blocks.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "chunked file has no block FST entries",
            ));
        }

        let segment = self.memory_map_chunked(record)?;
        let all_data = segment.as_slice().map_err(std::io::Error::other)?;
        let data_end = chunked_data_end(record)?;

        for i in 0..blocks.len() {
            let (_logical_offset, physical_offset) = blocks[i];

            let compressed_end = if i + 1 < blocks.len() {
                blocks[i + 1].1
            } else {
                data_end
            };
            let block_data =
                checked_chunked_block(record, all_data, physical_offset, compressed_end)?;

            let block_output = self.core.decompress_chunked_block(record, block_data)?;
            dest.write_all(&block_output)?;
        }

        dest.flush()?;
        Ok(())
    }

    /// Decompress only the blocks covering a byte range [start_byte, end_byte).
    ///
    /// This is optimized for random access patterns - instead of decompressing
    /// the entire file, only the blocks that contain the requested range are
    /// decompressed. Returns exactly the bytes from start_byte to end_byte.
    // [spec:box:sem:chunked-io.root.sync-range]
    pub fn decompress_chunked_range(
        &self,
        record: &ChunkedFileRecord<'_>,
        record_index: RecordIndex,
        start_byte: u64,
        end_byte: u64,
    ) -> std::io::Result<Vec<u8>> {
        let file_size = record.decompressed_length;

        // Clamp range to file size
        let start_byte = start_byte.min(file_size);
        let end_byte = end_byte.min(file_size);

        if start_byte >= end_byte {
            return Ok(Vec::new());
        }

        // Find the first block containing start_byte
        let (first_physical_offset, first_logical_offset) = self
            .core
            .find_block(record_index, start_byte)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "could not find block for start offset",
                )
            })?;

        // Get all blocks we need to decompress
        let mut blocks_needed = vec![(first_logical_offset, first_physical_offset)];
        let mut current_logical = first_logical_offset;

        // Keep getting next blocks until we've covered end_byte
        while let Some((next_logical, next_physical)) =
            self.core.next_block(record_index, current_logical)
        {
            if next_logical >= end_byte {
                break;
            }

            current_logical = next_logical;
            blocks_needed.push((next_logical, next_physical));
        }

        // Memory map the chunked file data
        let segment = self.memory_map_chunked(record)?;
        let all_data = segment.as_slice().map_err(std::io::Error::other)?;
        let data_end = chunked_data_end(record)?;

        let first_block_logical = blocks_needed[0].0;
        let total_blocks = blocks_needed.len();
        let requested_len = usize::try_from(end_byte - start_byte).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "requested chunked range does not fit in usize",
            )
        })?;
        // The logical length is archive metadata, so do not use it to drive an
        // eager allocation before the compressed block envelopes are checked.
        let mut output = Vec::new();

        for (i, &(logical_offset, physical_offset)) in blocks_needed.iter().enumerate() {
            // Calculate compressed size for this block
            let compressed_end = if i + 1 < total_blocks {
                blocks_needed[i + 1].1
            } else {
                // For the last block we need, find the next block's physical offset
                // or use the end of data
                self.core
                    .next_block(record_index, logical_offset)
                    .map(|(_, phys)| phys)
                    .unwrap_or(data_end)
            };

            let block_data =
                checked_chunked_block(record, all_data, physical_offset, compressed_end)?;

            let block_output = self.core.decompress_chunked_block(record, block_data)?;
            output.extend_from_slice(&block_output);
        }

        // Slice to exact requested range
        let offset_in_output = start_byte
            .checked_sub(first_block_logical)
            .and_then(|offset| usize::try_from(offset).ok())
            .ok_or_else(|| {
                invalid_chunked_metadata(
                    "block FST logical offset lies after the requested range start",
                )
            })?;
        let end_in_output = offset_in_output
            .checked_add(requested_len)
            .ok_or_else(|| invalid_chunked_metadata("requested chunked range overflows usize"))?;
        let requested = output.get(offset_in_output..end_in_output).ok_or_else(|| {
            invalid_chunked_metadata(format!(
                "decompressed blocks produced {} bytes, too few for requested range [{offset_in_output}, {end_in_output})",
                output.len()
            ))
        })?;

        Ok(requested.to_vec())
    }

    /// Decompress a single block from a chunked file by block index.
    ///
    /// Returns the decompressed block data. Block indices are 0-based.
    /// This is useful for caching individual blocks.
    // [spec:box:sem:chunked-io.root.sync-range]
    pub fn decompress_chunked_block(
        &self,
        record: &ChunkedFileRecord<'_>,
        record_index: RecordIndex,
        block_index: u64,
    ) -> std::io::Result<Vec<u8>> {
        let block_size = record.block_size as u64;
        let logical_offset = block_index.checked_mul(block_size).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "chunked block index overflows the logical offset range",
            )
        })?;

        let (physical_offset, block_logical) = self
            .core
            .find_block(record_index, logical_offset)
            .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("block {} not found", block_index),
            )
        })?;

        // Verify we found the right block
        if block_logical != logical_offset {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "block mismatch: requested {} but found block at {}",
                    logical_offset, block_logical
                ),
            ));
        }

        // Find the end of this block's compressed data
        let compressed_end = self
            .core
            .next_block(record_index, logical_offset)
            .map(|(_, phys)| phys)
            .map(Ok)
            .unwrap_or_else(|| chunked_data_end(record))?;

        // Memory map and decompress
        let segment = self.memory_map_chunked(record)?;
        let all_data = segment.as_slice().map_err(std::io::Error::other)?;
        let block_data = checked_chunked_block(record, all_data, physical_offset, compressed_end)?;

        self.core.decompress_chunked_block(record, block_data)
    }

    pub fn find(&self, path: &BoxPath<'_>) -> Result<&Record<'static>, ExtractError> {
        self.core
            .find(path)
            .ok_or_else(|| ExtractError::NotFoundInArchive(path.to_path_buf()))
    }

    fn validate_materialization_path(
        &self,
        path: &BoxPath<'_>,
        index: RecordIndex,
    ) -> Result<(), ExtractError> {
        self.core
            .validate_extraction_path(path, index)
            .map_err(|error| ExtractError::InvalidArchiveHierarchy(error, path.to_path_buf()))
    }

    // [spec:box:req:paths.root.extraction-gates+1]
    pub fn extract<P: AsRef<Path>>(
        &self,
        path: &BoxPath<'_>,
        output_path: P,
    ) -> Result<(), ExtractError> {
        if self.core.allow_escapes() {
            return Err(ExtractError::AllowEscapesRequired);
        }
        if self.core.allow_external_symlinks() {
            return Err(ExtractError::ExternalSymlinksRequired);
        }
        let output_path = output_path.as_ref();
        let record_index = self
            .core
            .metadata()
            .index(path)
            .ok_or_else(|| ExtractError::NotFoundInArchive(path.to_path_buf()))?;
        let record = self
            .core
            .record(record_index)
            .ok_or_else(|| ExtractError::NotFoundInArchive(path.to_path_buf()))?;
        self.extract_inner(path, record, record_index, output_path)
    }

    pub fn extract_recursive<P: AsRef<Path>>(
        &self,
        path: &BoxPath<'_>,
        output_path: P,
    ) -> Result<(), ExtractError> {
        self.extract_recursive_with_options(path, output_path, ExtractOptions::default())
            .map(|_| ())
    }

    pub fn extract_all<P: AsRef<Path>>(&self, output_path: P) -> Result<(), ExtractError> {
        self.extract_all_with_options(output_path, ExtractOptions::default())
            .map(|_| ())
    }

    /// Extract all files with options, returning extraction statistics.
    // [spec:box:req:paths.root.extraction-gates+1]
    // [spec:box:sem:sync-io.root.extraction-validation+2]
    pub fn extract_all_with_options<P: AsRef<Path>>(
        &self,
        output_path: P,
        options: ExtractOptions,
    ) -> Result<ExtractStats, ExtractError> {
        if self.core.allow_escapes() && !options.allow_escapes {
            return Err(ExtractError::AllowEscapesRequired);
        }
        if self.core.allow_external_symlinks() && !options.allow_external_symlinks {
            return Err(ExtractError::ExternalSymlinksRequired);
        }
        let output_path = output_path.as_ref();
        let mut stats = ExtractStats::default();
        let start = Instant::now();
        for item in self.core.iter() {
            self.extract_inner_with_options(
                &item.path,
                item.record,
                item.index,
                output_path,
                &options,
                &mut stats,
            )?;
        }
        stats.timing.decompress = start.elapsed();
        Ok(stats)
    }

    /// Extract a path and all children with options, returning extraction statistics.
    // [spec:box:req:paths.root.extraction-gates+1]
    pub fn extract_recursive_with_options<P: AsRef<Path>>(
        &self,
        path: &BoxPath<'_>,
        output_path: P,
        options: ExtractOptions,
    ) -> Result<ExtractStats, ExtractError> {
        if self.core.allow_escapes() && !options.allow_escapes {
            return Err(ExtractError::AllowEscapesRequired);
        }
        if self.core.allow_external_symlinks() && !options.allow_external_symlinks {
            return Err(ExtractError::ExternalSymlinksRequired);
        }
        let output_path = output_path.as_ref();
        let mut stats = ExtractStats::default();

        let index = self
            .core
            .metadata()
            .index(path)
            .ok_or_else(|| ExtractError::NotFoundInArchive(path.to_path_buf()))?;

        let mut pending = vec![(path.clone().into_owned(), index)];
        let mut visited = HashSet::new();
        while let Some((item_path, item_index)) = pending.pop() {
            if !visited.insert(item_index) {
                return Err(ExtractError::InvalidArchiveHierarchy(
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "record index {} appears more than once in the recursive hierarchy",
                            item_index.get()
                        ),
                    ),
                    item_path.to_path_buf(),
                ));
            }
            let record = self
                .core
                .record(item_index)
                .ok_or_else(|| ExtractError::NotFoundInArchive(item_path.to_path_buf()))?;
            let children = if matches!(record, Record::Directory(_)) {
                self.core
                    .extraction_children_by_index(item_index, &item_path)
                    .map_err(|error| {
                        ExtractError::InvalidArchiveHierarchy(error, item_path.to_path_buf())
                    })?
                    .into_iter()
                    .rev()
                    .collect()
            } else {
                Vec::new()
            };

            self.extract_inner_with_options(
                &item_path,
                record,
                item_index,
                output_path,
                &options,
                &mut stats,
            )?;
            pending.extend(children);
        }
        Ok(stats)
    }

    /// Validate all files by checking their checksums.
    // [spec:box:req:checksums.root.verification]
    // [spec:box:sem:checksums.root.verification.checksum-less]
    // [spec:box:sem:sync-io.root.extraction-validation+2]
    pub fn validate_all(&self) -> std::io::Result<ValidateStats> {
        let mut stats = ValidateStats::default();
        let mut out_buf = Vec::new();

        for item in self.core.iter() {
            match item.record {
                Record::File(f) => {
                    let expected_hash: Option<[u8; 32]> = match item
                        .record
                        .attr_value(self.metadata(), crate::attrs::BLAKE3)
                    {
                        Some(AttrValue::U256(h)) => Some(*h),
                        _ => None,
                    };

                    if let Some(expected) = expected_hash {
                        out_buf.clear();
                        self.decompress(f, &mut out_buf)?;
                        let actual = blake3::hash(&out_buf);
                        if actual.as_bytes() != &expected {
                            stats.checksum_failures += 1;
                        }
                    } else {
                        stats.files_without_checksum += 1;
                    }
                    stats.files_checked += 1;
                }
                Record::ChunkedFile(f) => {
                    let expected_hash: Option<[u8; 32]> = match item
                        .record
                        .attr_value(self.metadata(), crate::attrs::BLAKE3)
                    {
                        Some(AttrValue::U256(h)) => Some(*h),
                        _ => None,
                    };

                    if let Some(expected) = expected_hash {
                        out_buf.clear();
                        self.decompress_chunked(f, item.index, &mut out_buf)?;
                        let actual = blake3::hash(&out_buf);
                        if actual.as_bytes() != &expected {
                            stats.checksum_failures += 1;
                        }
                    } else {
                        stats.files_without_checksum += 1;
                    }
                    stats.files_checked += 1;
                }
                _ => {}
            }
        }
        Ok(stats)
    }

    /// Memory-map the file and return a segment for the record's data.
    // [spec:box:sem:sync-io.root.read]
    pub fn memory_map(&self, record: &FileRecord<'_>) -> std::io::Result<Segment> {
        let mmap = MemoryMappedFile::builder(&self.path)
            .huge_pages(true)
            .open()
            .map_err(std::io::Error::other)?;
        let offset = self
            .core
            .offset
            .checked_add(record.data.get())
            .ok_or_else(|| invalid_chunked_metadata("archive and record data offsets overflow"))?;
        Segment::new(mmap.into(), offset, record.length).map_err(std::io::Error::other)
    }

    /// Memory-map the file and return a segment for a chunked file record's data.
    pub fn memory_map_chunked(&self, record: &ChunkedFileRecord<'_>) -> std::io::Result<Segment> {
        let mmap = MemoryMappedFile::builder(&self.path)
            .huge_pages(true)
            .open()
            .map_err(std::io::Error::other)?;
        let offset = self
            .core
            .offset
            .checked_add(record.data.get())
            .ok_or_else(|| invalid_chunked_metadata("archive and chunked data offsets overflow"))?;
        Segment::new(mmap.into(), offset, record.length).map_err(std::io::Error::other)
    }

    fn extract_inner(
        &self,
        path: &BoxPath<'_>,
        record: &Record<'_>,
        record_index: RecordIndex,
        output_path: &Path,
    ) -> Result<(), ExtractError> {
        let mut stats = ExtractStats::default();
        self.extract_inner_with_options(
            path,
            record,
            record_index,
            output_path,
            &ExtractOptions::default(),
            &mut stats,
        )
    }

    // [spec:box:req:checksums.root.verification]
    // [spec:box:sem:checksums.root.verification.extraction-statistics]
    // [spec:box:req:records.root.references.resolution]
    // [spec:box:sem:sync-io.root.extraction-validation+2]
    // [spec:box:sem:chunked-io.root.slice-extraction]
    fn extract_inner_with_options(
        &self,
        path: &BoxPath<'_>,
        record: &Record<'_>,
        record_index: RecordIndex,
        output_path: &Path,
        options: &ExtractOptions,
        stats: &mut ExtractStats,
    ) -> Result<(), ExtractError> {
        self.validate_materialization_path(path, record_index)?;
        match record {
            Record::Directory(_) => {
                std::fs::create_dir_all(output_path)
                    .map_err(|e| ExtractError::CreateDirFailed(e, output_path.to_path_buf()))?;
                let new_dir = output_path.join(path.to_path_buf());
                std::fs::create_dir_all(&new_dir)
                    .map_err(|e| ExtractError::CreateDirFailed(e, new_dir.clone()))?;

                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let mode = self.get_mode(record);
                    std::fs::set_permissions(&new_dir, std::fs::Permissions::from_mode(mode))
                        .map_err(|e| ExtractError::CreateDirFailed(e, new_dir.clone()))?;
                }

                stats.dirs_created += 1;
            }
            Record::File(f) => {
                std::fs::create_dir_all(output_path)
                    .map_err(|e| ExtractError::CreateDirFailed(e, output_path.to_path_buf()))?;
                let new_file = output_path.join(path.to_path_buf());
                if let Some(parent) = new_file.parent() {
                    std::fs::create_dir_all(parent)
                        .map_err(|e| ExtractError::CreateDirFailed(e, parent.to_path_buf()))?;
                }
                let file = std::fs::File::create(&new_file)
                    .map_err(|e| ExtractError::CreateFileFailed(e, new_file.clone()))?;
                let mut writer = std::io::BufWriter::new(file);
                self.decompress(f, &mut writer)
                    .map_err(|e| ExtractError::DecompressionFailed(e, new_file.clone()))?;

                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let mode = self.get_mode(record);
                    std::fs::set_permissions(&new_file, std::fs::Permissions::from_mode(mode))
                        .map_err(|e| ExtractError::CreateFileFailed(e, new_file.clone()))?;
                }

                // Set xattrs if enabled
                #[cfg(all(target_os = "linux", feature = "xattr"))]
                if options.xattrs {
                    for (key, value) in record.attrs_iter(self.metadata()) {
                        if let Some(xattr_name) = key.strip_prefix(crate::attrs::LINUX_XATTR_PREFIX)
                        {
                            let _ = xattr::set(&new_file, xattr_name, value);
                        }
                    }
                }

                // Verify checksum if enabled
                if options.verify_checksums {
                    if let Some(AttrValue::U256(expected)) =
                        record.attr_value(self.metadata(), crate::attrs::BLAKE3)
                    {
                        let contents = std::fs::read(&new_file)
                            .map_err(|e| ExtractError::VerificationFailed(e, new_file.clone()))?;
                        let actual = blake3::hash(&contents);
                        if actual.as_bytes() != expected {
                            stats.checksum_failures += 1;
                        }
                    }
                }

                stats.files_extracted += 1;
                stats.bytes_written += f.decompressed_length;
            }
            Record::ChunkedFile(f) => {
                std::fs::create_dir_all(output_path)
                    .map_err(|e| ExtractError::CreateDirFailed(e, output_path.to_path_buf()))?;
                let new_file = output_path.join(path.to_path_buf());
                if let Some(parent) = new_file.parent() {
                    std::fs::create_dir_all(parent)
                        .map_err(|e| ExtractError::CreateDirFailed(e, parent.to_path_buf()))?;
                }
                let file = std::fs::File::create(&new_file)
                    .map_err(|e| ExtractError::CreateFileFailed(e, new_file.clone()))?;
                let mut writer = std::io::BufWriter::new(file);
                self.decompress_chunked(f, record_index, &mut writer)
                    .map_err(|e| ExtractError::DecompressionFailed(e, new_file.clone()))?;

                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let mode = self.get_mode(record);
                    std::fs::set_permissions(&new_file, std::fs::Permissions::from_mode(mode))
                        .map_err(|e| ExtractError::CreateFileFailed(e, new_file.clone()))?;
                }

                #[cfg(all(target_os = "linux", feature = "xattr"))]
                if options.xattrs {
                    for (key, value) in record.attrs_iter(self.metadata()) {
                        if let Some(xattr_name) = key.strip_prefix(crate::attrs::LINUX_XATTR_PREFIX)
                        {
                            let _ = xattr::set(&new_file, xattr_name, value);
                        }
                    }
                }

                if options.verify_checksums {
                    if let Some(AttrValue::U256(expected)) =
                        record.attr_value(self.metadata(), crate::attrs::BLAKE3)
                    {
                        let contents = std::fs::read(&new_file)
                            .map_err(|e| ExtractError::VerificationFailed(e, new_file.clone()))?;
                        let actual = blake3::hash(&contents);
                        if actual.as_bytes() != expected {
                            stats.checksum_failures += 1;
                        }
                    }
                }

                stats.files_extracted += 1;
                stats.bytes_written += f.decompressed_length;
            }
            Record::Link(link) => {
                let target_idx = link.target;
                self.core.record(target_idx).ok_or_else(|| {
                    ExtractError::ResolveLinkFailed(
                        std::io::Error::new(std::io::ErrorKind::NotFound, "link target not found"),
                        link.clone().into_owned(),
                    )
                })?;
                let target_path =
                    self.core
                        .extraction_path_for_index(target_idx)
                        .map_err(|error| {
                            ExtractError::ResolveLinkFailed(error, link.clone().into_owned())
                        })?;
                let relative_target = self
                    .core
                    .compute_relative_symlink_target(path, &target_path);
                let new_file = output_path.join(path.to_path_buf());
                if let Some(parent) = new_file.parent() {
                    std::fs::create_dir_all(parent)
                        .map_err(|e| ExtractError::CreateDirFailed(e, parent.to_path_buf()))?;
                }
                #[cfg(unix)]
                {
                    std::os::unix::fs::symlink(&relative_target, &new_file).map_err(|e| {
                        ExtractError::CreateLinkFailed(e, new_file, relative_target)
                    })?;
                }
                #[cfg(not(unix))]
                {
                    return Err(ExtractError::CreateLinkFailed(
                        std::io::Error::new(
                            std::io::ErrorKind::Unsupported,
                            "symlinks not supported",
                        ),
                        new_file,
                        relative_target,
                    ));
                }
                #[cfg(unix)]
                {
                    stats.links_created += 1;
                }
            }
            Record::ExternalLink(link) => {
                let new_file = output_path.join(path.to_path_buf());
                if let Some(parent) = new_file.parent() {
                    std::fs::create_dir_all(parent)
                        .map_err(|e| ExtractError::CreateDirFailed(e, parent.to_path_buf()))?;
                }
                let target_path = std::path::PathBuf::from(link.target.as_ref());
                #[cfg(unix)]
                {
                    std::os::unix::fs::symlink(&target_path, &new_file)
                        .map_err(|e| ExtractError::CreateLinkFailed(e, new_file, target_path))?;
                }
                #[cfg(not(unix))]
                {
                    return Err(ExtractError::CreateLinkFailed(
                        std::io::Error::new(
                            std::io::ErrorKind::Unsupported,
                            "symlinks not supported",
                        ),
                        new_file,
                        target_path,
                    ));
                }
                #[cfg(unix)]
                {
                    stats.links_created += 1;
                }
            }
        }
        Ok(())
    }
}
