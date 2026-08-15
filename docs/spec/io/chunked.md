# Chunked-file I/O

## Block storage and indexing

> [spec:box:sem:chunked-io.root]
> Chunked-file I/O treats one logical file as an ordered sequence of
> independently compressed blocks. The file record supplies block size, total
> compressed and decompressed lengths, first-data offset, codec, and attributes;
> a separate block FST relates logical block starts to physical archive offsets.

> [spec:box:sem:chunked-io.root.explicit-insert]
> The explicit async `insert_chunked` operation requires a positive block size
> and non-empty source, fills one block at a time, compresses each block
> independently with the selected compression algorithm's default
> configuration, writes the results consecutively, and records aggregate
> lengths before inserting a `ChunkedFile` record. This low-level operation is
> caller-selected; it does not choose files for chunking automatically.

> [spec:box:req:chunked-io.root.automatic-creation]
> High-level async path creation and the CLI `create` command MUST encode every
> non-empty regular source whose initial metadata length exceeds the 2,097,152
> byte default block size as a `ChunkedFile`, using that same logical block
> size; smaller and empty sources remain ordinary `File` records. Each block is
> compressed independently with the file's selected compression configuration
> and emitted in logical order, while an enabled Blake3 checksum covers the
> complete uncompressed file as one stream. Parallel creation MUST share its
> effective `jobs` bound across file and block compression work so that blocks
> of one large file can run concurrently without exceeding that bound; metadata
> insertion and archive writes remain sequential, and one file contributes one
> unit to file-level progress and statistics.

> [spec:box:syn:chunked-io.root.block-index-entry]
> A pending block-index entry is a 16-byte key containing the non-zero record
> index as big-endian u64 followed by the decompressed logical block offset as
> big-endian u64, paired with the block's physical archive offset. Finalization
> sorts these keys before building the block FST.

> [spec:box:sem:chunked-io.root.block-queries]
> Block enumeration yields `(logical_offset, physical_offset)` in key order.
> Predecessor lookup yields `(physical_offset, block_logical_offset)` for the
> greatest logical start not exceeding the requested offset, while successor
> lookup yields `(next_logical_offset, physical_offset)` for the first later
> block.

> [spec:box:sem:chunked-io.root.block-decompression]
> Readers derive a compressed block's end from the next block's physical offset
> or, for the last block, from record data offset plus compressed length. They
> decompress that byte span independently with the record codec and archive
> dictionary and concatenate whole-file output in logical block order; a
> chunked file with no block entries is invalid for decompression.

> [spec:box:sem:chunked-io.root.async-range]
> Async range reads reject a requested half-open range that extends beyond the
> decompressed length, return an empty vector for a zero-length request, locate
> the predecessor block, and decompress successive blocks until the requested
> number of bytes has been copied. Absence of a starting block is invalid
> archive data.

> [spec:box:sem:chunked-io.root.sync-range]
> Sync range reads clamp both endpoints to the decompressed file length and
> return an empty vector when the clamped start is not before the end. Otherwise
> they obtain the predecessor and subsequent block entries, decompress the
> selected spans, and slice the assembled buffer to the requested interval;
> the single-block helper derives its logical start from block index times
> block size and requires an entry at that start.

> [spec:box:sem:chunked-io.root.seek-reader]
> `ChunkedReader` implements async sequential reads and seeks over decompressed
> coordinates. `read_at` clamps at end of file and does not change the current
> position; seeks reject negative positions and positions beyond the
> decompressed length, retaining a loaded block only while the new position
> remains within it.

> [spec:box:sem:chunked-io.root.block-cache]
> A chunked reader caches decompressed blocks in an LRU keyed by record index
> and logical block offset. Capacity is measured in a positive number of
> blocks, the default is eight, a hit refreshes recency, and insertion may evict
> the least-recently-used block.

> [spec:box:sem:chunked-io.root.slice-extraction]
> Chunked slice access decompresses the complete logical file into an owned
> contiguous byte buffer that supports slice views and ownership conversion.
> Async and sync extraction likewise reconstruct a chunked payload by
> decompressing its indexed blocks in order before applying normal file
> metadata and optional checksum handling.
