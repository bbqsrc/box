use std::io::SeekFrom;
use std::ops::Deref;
use std::pin::Pin;
use std::task::{Context, Poll};

use lru::LruCache;
use mmap_io::segment::Segment;

use crate::core::RecordIndex;
use crate::record::ChunkedFileRecord;

use super::reader::{
    BoxFileReader, chunked_block_data_range, chunked_data_end, invalid_chunked_data,
    logical_file_buffer,
};

// ============================================================================
// BLOCK CACHE
// ============================================================================

/// LRU cache for decompressed blocks.
///
/// Caches decompressed block data keyed by (record_index, block_logical_offset).
/// This significantly speeds up sequential reads and repeated access patterns.
// [spec:box:sem:chunked-io.root.block-cache]
pub struct BlockCache {
    cache: LruCache<(u64, u64), Box<[u8]>>,
}

impl BlockCache {
    /// Create a new block cache with the specified capacity.
    ///
    /// Capacity is the number of blocks to cache, not bytes.
    /// With 2MB blocks, 8 blocks = 16MB cache.
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: LruCache::new(
                std::num::NonZeroUsize::new(capacity).expect("capacity must be > 0"),
            ),
        }
    }

    /// Get a cached block if present.
    pub fn get(&mut self, record_index: u64, block_offset: u64) -> Option<&[u8]> {
        self.cache.get(&(record_index, block_offset)).map(|b| &**b)
    }

    /// Insert a decompressed block into the cache.
    pub fn insert(&mut self, record_index: u64, block_offset: u64, data: Box<[u8]>) {
        self.cache.put((record_index, block_offset), data);
    }

    /// Check if a block is in the cache without updating LRU order.
    #[allow(dead_code)]
    pub fn contains(&self, record_index: u64, block_offset: u64) -> bool {
        self.cache.contains(&(record_index, block_offset))
    }

    /// Clear all cached blocks.
    #[allow(dead_code)]
    pub fn clear(&mut self) {
        self.cache.clear();
    }

    /// Number of blocks currently cached.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Check if cache is empty.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }
}

impl Default for BlockCache {
    fn default() -> Self {
        // Default: 8 blocks (16MB with 2MB blocks)
        Self::new(8)
    }
}

// ============================================================================
// CHUNKED READER (AsyncRead + AsyncSeek)
// ============================================================================

/// Currently loaded block for the chunked reader.
struct CurrentBlock {
    /// Logical offset where this block starts
    logical_offset: u64,
    /// Decompressed block data
    data: Vec<u8>,
}

/// Async reader for chunked files with seek support.
///
/// Implements `AsyncRead` and `AsyncSeek` for random access to chunked file contents.
/// Includes an LRU block cache for efficient sequential and repeated access patterns.
///
/// Uses synchronous decompression (sans-IO) internally, making it suitable for
/// contexts where async runtimes are not available or blocking is acceptable.
///
/// # Example
/// ```ignore
/// let mut reader = bf.chunked_reader(&record, record_index)?;
/// let mut buf = vec![0u8; 1024];
/// reader.read(&mut buf).await?;
/// reader.seek(SeekFrom::Start(1000)).await?;
/// ```
// [spec:box:sem:chunked-io.root.seek-reader]
pub struct ChunkedReader<'a> {
    reader: &'a BoxFileReader,
    record: &'a ChunkedFileRecord<'a>,
    record_index: RecordIndex,
    position: u64,
    cache: BlockCache,
    segment: Segment,
    blocks: Vec<(u64, u64)>,
    current_block: Option<CurrentBlock>,
}

impl<'a> ChunkedReader<'a> {
    /// Create a new chunked file reader.
    pub fn new(
        reader: &'a BoxFileReader,
        record: &'a ChunkedFileRecord<'a>,
        record_index: RecordIndex,
    ) -> std::io::Result<Self> {
        let segment = reader.memory_map_chunked(record)?;
        let blocks = reader.core.blocks_for_record(record_index);

        if blocks.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "chunked file has no block FST entries",
            ));
        }

        let mapped_len = segment.as_slice().map_err(std::io::Error::other)?.len();
        let data_end = chunked_data_end(record)?;
        for (block_index, &(logical_start, physical_start)) in blocks.iter().enumerate() {
            if block_index == 0 {
                if logical_start != 0 {
                    return Err(invalid_chunked_data(format!(
                        "chunked file first block starts at logical offset {logical_start}"
                    )));
                }
            } else if logical_start <= blocks[block_index - 1].0 {
                return Err(invalid_chunked_data(format!(
                    "chunked file block {block_index} has a non-increasing logical offset"
                )));
            }

            let physical_end = blocks
                .get(block_index + 1)
                .map(|(_, next_physical)| *next_physical)
                .unwrap_or(data_end);
            chunked_block_data_range(
                record,
                mapped_len,
                block_index,
                physical_start,
                physical_end,
            )?;
        }

        Ok(Self {
            reader,
            record,
            record_index,
            position: 0,
            cache: BlockCache::default(),
            segment,
            blocks,
            current_block: None,
        })
    }

    /// Create a new chunked file reader with a custom cache capacity.
    pub fn with_cache_capacity(
        reader: &'a BoxFileReader,
        record: &'a ChunkedFileRecord<'a>,
        record_index: RecordIndex,
        cache_capacity: usize,
    ) -> std::io::Result<Self> {
        let mut r = Self::new(reader, record, record_index)?;
        r.cache = BlockCache::new(cache_capacity);
        Ok(r)
    }

    /// Get the current position within the file.
    pub fn position(&self) -> u64 {
        self.position
    }

    /// Get the total decompressed file size.
    pub fn len(&self) -> u64 {
        self.record.decompressed_length
    }

    /// Check if the file is empty.
    pub fn is_empty(&self) -> bool {
        self.record.decompressed_length == 0
    }

    /// Get the block size used for this chunked file.
    pub fn block_size(&self) -> u32 {
        self.record.block_size
    }

    /// Number of decompressed blocks currently retained by this reader.
    pub fn cached_block_count(&self) -> usize {
        self.cache.len()
    }

    /// Whether the block beginning at `logical_offset` is currently cached.
    ///
    /// This query does not refresh the block's LRU position.
    pub fn is_block_cached(&self, logical_offset: u64) -> bool {
        self.cache.contains(self.record_index.get(), logical_offset)
    }

    /// Read bytes at a specific offset without changing the reader's position.
    ///
    /// This is the primary random access method - like `pread(2)` or indexing a memory map.
    /// Uses the block cache for efficiency on repeated/nearby accesses.
    ///
    /// # Arguments
    /// * `offset` - Byte offset within the decompressed file
    /// * `buf` - Buffer to read into
    ///
    /// # Returns
    /// Number of bytes read (may be less than buf.len() at EOF)
    // [spec:box:sem:chunked-io.root.seek-reader]
    pub async fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() || offset >= self.record.decompressed_length {
            return Ok(0);
        }

        // Clamp read to file size
        let available =
            usize::try_from(self.record.decompressed_length - offset).unwrap_or(usize::MAX);
        let to_read = buf.len().min(available);
        let mut total_read = 0;
        let mut current_offset = offset;

        while total_read < to_read {
            // Find block containing current_offset
            let Some(block_idx) = find_block_index(&self.blocks, current_offset) else {
                return Err(invalid_chunked_data(
                    "chunked file has no block covering the requested offset",
                ));
            };

            let (block_logical, block_physical) = self.blocks[block_idx];

            // Get decompressed block (from cache or decompress)
            let block_data = self
                .get_block(block_idx, block_logical, block_physical)
                .await?;

            // Calculate how much to copy from this block
            let offset_in_block = current_offset
                .checked_sub(block_logical)
                .and_then(|value| usize::try_from(value).ok())
                .ok_or_else(|| invalid_chunked_data("chunked-file block offset is invalid"))?;
            let block_remaining =
                block_data
                    .len()
                    .checked_sub(offset_in_block)
                    .ok_or_else(|| {
                        invalid_chunked_data(
                            "chunked-file block does not cover the requested offset",
                        )
                    })?;
            if block_remaining == 0 {
                return Err(invalid_chunked_data(
                    "chunked-file block does not advance the read position",
                ));
            }
            let copy_len = (to_read - total_read).min(block_remaining);

            let destination = buf
                .get_mut(total_read..total_read + copy_len)
                .ok_or_else(|| invalid_chunked_data("chunked-file destination range is invalid"))?;
            let source = block_data
                .get(offset_in_block..offset_in_block + copy_len)
                .ok_or_else(|| invalid_chunked_data("chunked-file source range is invalid"))?;
            destination.copy_from_slice(source);

            total_read += copy_len;
            current_offset = current_offset
                .checked_add(copy_len as u64)
                .ok_or_else(|| invalid_chunked_data("chunked-file read offset overflows u64"))?;
        }

        Ok(total_read)
    }

    /// Get a decompressed block, using cache if available.
    ///
    /// Uses synchronous decompression (sans-IO) for simplicity and portability.
    fn get_block_sync(
        &mut self,
        block_idx: usize,
        block_logical: u64,
        block_physical: u64,
    ) -> std::io::Result<Vec<u8>> {
        // Check cache first
        if let Some(cached) = self.cache.get(self.record_index.get(), block_logical) {
            return Ok(cached.to_vec());
        }

        // Extract compressed block data
        let all_data = self.segment.as_slice().map_err(std::io::Error::other)?;

        let compressed_end = if block_idx + 1 < self.blocks.len() {
            self.blocks[block_idx + 1].1
        } else {
            chunked_data_end(self.record)?
        };

        let block_range = chunked_block_data_range(
            self.record,
            all_data.len(),
            block_idx,
            block_physical,
            compressed_end,
        )?;
        let block_data = all_data.get(block_range).ok_or_else(|| {
            invalid_chunked_data(format!(
                "chunked file block {block_idx} is outside the mapped record data"
            ))
        })?;

        // Use sync decompression (sans-IO)
        let decompressed = crate::compression::decompress_bytes_sync(
            block_data,
            self.record.compression,
            self.reader.core.dictionary(),
        )?;
        let expected_logical_end = self
            .blocks
            .get(block_idx + 1)
            .map(|(next_logical, _)| *next_logical)
            .unwrap_or(self.record.decompressed_length);
        let expected_len = expected_logical_end
            .checked_sub(block_logical)
            .and_then(|value| usize::try_from(value).ok())
            .ok_or_else(|| {
                invalid_chunked_data(format!(
                    "chunked file block {block_idx} has an invalid logical range"
                ))
            })?;
        if decompressed.len() != expected_len {
            return Err(invalid_chunked_data(format!(
                "chunked file block {block_idx} decompressed to {} bytes, expected {expected_len}",
                decompressed.len()
            )));
        }

        // Cache it
        self.cache.insert(
            self.record_index.get(),
            block_logical,
            decompressed.clone().into_boxed_slice(),
        );

        Ok(decompressed)
    }

    /// Get a decompressed block, using cache if available (async wrapper).
    async fn get_block(
        &mut self,
        block_idx: usize,
        block_logical: u64,
        block_physical: u64,
    ) -> std::io::Result<Vec<u8>> {
        self.get_block_sync(block_idx, block_logical, block_physical)
    }
}

/// Find the block index that contains the given logical offset.
fn find_block_index(blocks: &[(u64, u64)], offset: u64) -> Option<usize> {
    // Binary search for the block containing this offset
    match blocks.binary_search_by(|(logical, _)| logical.cmp(&offset)) {
        Ok(idx) => Some(idx),
        Err(0) => None, // offset is before first block
        Err(idx) => Some(idx - 1),
    }
}

/// Read bytes from current block at the given position.
fn read_from_block(current_block: &Option<CurrentBlock>, position: u64, buf: &mut [u8]) -> usize {
    let Some(block) = current_block else {
        return 0;
    };

    // Check if current position is within this block
    if position < block.logical_offset {
        return 0;
    }

    let Ok(offset_in_block) = usize::try_from(position - block.logical_offset) else {
        return 0;
    };
    if offset_in_block >= block.data.len() {
        return 0;
    }

    let available = block.data.len() - offset_in_block;
    let to_copy = buf.len().min(available);

    buf[..to_copy].copy_from_slice(&block.data[offset_in_block..offset_in_block + to_copy]);
    to_copy
}

pub(super) fn checked_seek_position(
    position: SeekFrom,
    current: u64,
    file_length: u64,
) -> std::io::Result<u64> {
    let candidate = match position {
        SeekFrom::Start(position) => i128::from(position),
        SeekFrom::End(offset) => i128::from(file_length) + i128::from(offset),
        SeekFrom::Current(offset) => i128::from(current) + i128::from(offset),
    };

    if candidate < 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "cannot seek to negative position",
        ));
    }
    if candidate > i128::from(file_length) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("cannot seek past end of file ({candidate} > {file_length})"),
        ));
    }

    u64::try_from(candidate).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "seek position does not fit in a 64-bit file offset",
        )
    })
}

// [spec:box:sem:chunked-io.root.seek-reader]
impl tokio::io::AsyncRead for ChunkedReader<'_> {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();

        // Check if we're at EOF
        if this.position >= this.record.decompressed_length {
            return Poll::Ready(Ok(()));
        }

        // Try to read from current block
        if this.current_block.is_some() {
            let remaining = usize::try_from(this.record.decompressed_length - this.position)
                .unwrap_or(usize::MAX);
            let unfilled = buf.initialize_unfilled();
            let slice_len = unfilled.len().min(remaining);
            let slice = &mut unfilled[..slice_len];
            let n = read_from_block(&this.current_block, this.position, slice);
            if n > 0 {
                buf.advance(n);
                this.position = this
                    .position
                    .checked_add(u64::try_from(n).map_err(|_| {
                        invalid_chunked_data("chunked-file read length does not fit in u64")
                    })?)
                    .ok_or_else(|| invalid_chunked_data("chunked-file position overflows u64"))?;
                return Poll::Ready(Ok(()));
            }
        }

        // Need new block
        let Some(block_idx) = find_block_index(&this.blocks, this.position) else {
            return Poll::Ready(Ok(())); // EOF
        };

        let (logical_offset, physical_offset) = this.blocks[block_idx];

        // Get decompressed block (from cache or decompress synchronously)
        let data = match this.get_block_sync(block_idx, logical_offset, physical_offset) {
            Ok(d) => d,
            Err(e) => return Poll::Ready(Err(e)),
        };

        this.current_block = Some(CurrentBlock {
            logical_offset,
            data,
        });

        // Read from the newly loaded block
        let remaining =
            usize::try_from(this.record.decompressed_length - this.position).unwrap_or(usize::MAX);
        let unfilled = buf.initialize_unfilled();
        let slice_len = unfilled.len().min(remaining);
        let slice = &mut unfilled[..slice_len];
        let n = read_from_block(&this.current_block, this.position, slice);
        buf.advance(n);
        this.position = this
            .position
            .checked_add(u64::try_from(n).map_err(|_| {
                invalid_chunked_data("chunked-file read length does not fit in u64")
            })?)
            .ok_or_else(|| invalid_chunked_data("chunked-file position overflows u64"))?;
        Poll::Ready(Ok(()))
    }
}

// [spec:box:sem:chunked-io.root.seek-reader]
impl tokio::io::AsyncSeek for ChunkedReader<'_> {
    fn start_seek(self: Pin<&mut Self>, position: SeekFrom) -> std::io::Result<()> {
        let this = self.get_mut();

        let new_pos =
            checked_seek_position(position, this.position, this.record.decompressed_length)?;

        this.position = new_pos;

        // Invalidate current block if position is outside it
        if let Some(block) = &this.current_block {
            let block_end = u64::try_from(block.data.len())
                .ok()
                .and_then(|length| block.logical_offset.checked_add(length))
                .unwrap_or(u64::MAX);
            if new_pos < block.logical_offset || new_pos >= block_end {
                this.current_block = None;
            }
        }

        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<u64>> {
        Poll::Ready(Ok(self.get_mut().position))
    }
}

// ============================================================================
// CHUNKED SLICE (Deref to &[u8])
// ============================================================================

/// Transparent slice access to chunked file data.
///
/// This struct decompresses the entire chunked file into memory and provides
/// direct `&[u8]` access via `Deref`. Useful when you need to access the file
/// contents as a contiguous slice.
///
/// # Example
/// ```ignore
/// let slice = bf.chunked_slice(&record, record_index).await?;
/// let data: &[u8] = &*slice;
/// println!("First byte: {}", data[0]);
/// ```
// [spec:box:sem:chunked-io.root.slice-extraction]
pub struct ChunkedSlice {
    data: Box<[u8]>,
}

impl ChunkedSlice {
    /// Create a new ChunkedSlice by decompressing the entire chunked file.
    pub async fn new(
        reader: &BoxFileReader,
        record: &ChunkedFileRecord<'_>,
        record_index: RecordIndex,
    ) -> std::io::Result<Self> {
        let mut data = logical_file_buffer(record.decompressed_length)?;
        reader
            .decompress_chunked(record, record_index, &mut data)
            .await?;
        Ok(Self {
            data: data.into_boxed_slice(),
        })
    }

    /// Get the length of the decompressed data.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if the data is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Consume self and return the underlying boxed slice.
    pub fn into_boxed_slice(self) -> Box<[u8]> {
        self.data
    }

    /// Consume self and return the data as a Vec.
    pub fn into_vec(self) -> Vec<u8> {
        self.data.into_vec()
    }
}

impl Deref for ChunkedSlice {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl AsRef<[u8]> for ChunkedSlice {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl std::borrow::Borrow<[u8]> for ChunkedSlice {
    fn borrow(&self) -> &[u8] {
        &self.data
    }
}
