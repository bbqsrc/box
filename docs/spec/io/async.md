# Asynchronous archive I/O

## Async frontend

> [spec:box:sem:async-io.root]
> The async frontend drives archive file operations with Tokio while delegating
> header, metadata, lookup, positioning, and codec state to the archive core.
> Its public reader and writer own the filesystem path and the resources that
> keep their archive state valid.

> [spec:box:sem:async-io.root.open]
> Opening a reader canonicalizes and opens the archive path, reads a header at
> the requested archive base, requires a non-zero trailer pointer, checks base
> plus trailer offset for overflow and end-of-file bounds, and parses the
> memory-mapped trailer through end of file. The reader retains that mapping
> for the lifetime of metadata borrowed from it.

> [spec:box:sem:async-io.root.read]
> A regular-file raw read seeks to archive base plus the record data offset and
> limits the stream to the compressed record length. Payload mapping uses the
> same absolute range. Decompression copies stored bytes or drives the recorded
> Zstd/XZ decoder, including the archive Zstd dictionary when present; an
> unknown compression identifier is an invalid-input error.

> [spec:box:sem:async-io.root.writer-lifecycle]
> Creating a writer uses create-new semantics and writes a provisional header.
> Opening one for append parses the existing header and trailer, requires the
> trailer to lie within the current file, and requires every regular and
> chunked payload start and checked end to lie between the header end and the
> prior trailer. It resumes at the greatest payload endpoint, or after the
> header when none exists, only when the next aligned address is representable
> and does not pass the prior trailer. Finalization preserves and merges all
> existing path- and block-FST mappings with records added during the append
> session, including children added beneath FST-indexed directories. Finishing
> flushes payload data, finalizes core metadata, rewrites the header, writes the
> trailer, flushes again, and marks the writer finished; dropping an unfinished
> writer emits a warning.

> [spec:box:sem:async-io.root.streaming-insert]
> Async insertion streams source bytes through the selected stored, Zstd, or
> XZ path, records the compressed and decompressed byte counts, advances the
> core cursor, and then inserts the file record. Checksum-enabled streaming
> hashes the uncompressed bytes as they are read and stores the completed digest
> under the checksum type's non-empty U256 attribute name.

> [spec:box:sem:async-io.root.parallel-compression+1]
> Parallel file addition coordinates every input job behind one global
> semaphore whose effective permit count is normalized to at least one and
> bounded by the runtime semaphore limit. An ordinary file holds one permit for
> its compression work, while a chunked file acquires permits from the same pool
> per block, so one large file can compress several blocks concurrently without
> exceeding the global jobs bound. Each job prepares its complete memory- or
> temporary-file-backed result without mutating the archive; every preparation
> MUST succeed before publication begins. Prepared results retain their input
> positions and one sequential writer publishes them in input order. Only that
> writer assigns aligned data offsets and record indices, so archive payload and
> metadata mutation never occur concurrently.
>
> Parallel addition reports `Started` with the input count, `Compressing` and
> `Compressed` around each input job's complete preparation, `Written` with the
> cumulative written count after each sequential write, and `Finished` after
> normal completion. Progress delivery is best-effort and a disconnected
> receiver does not fail the archive operation.

## Extraction

> [spec:box:req:extraction.root]
> Extraction APIs MUST resolve archive records and join their Box paths to the
> caller-supplied output path before materializing the corresponding filesystem
> objects. Lookup, creation, decompression, link resolution, and verification
> failures MUST be reported as path-bearing extraction errors.

> [spec:box:req:extraction.root.safety-options]
> Convenience and single-record extraction MUST reject an archive whose header
> enables escaped paths or external links. Option-bearing extraction MUST also
> reject such an archive unless the caller explicitly enables the matching
> `allow_escapes` or `allow_external_symlinks` option, and MUST perform this
> gate before materializing records.

> [spec:box:sem:extraction.root.selection+2]
> Single-record extraction resolves and materializes one indexed record.
> Extract-all consumes the metadata iterator. Recursive extraction seeds the
> traversal with the selected full path and index, then resolves each
> directory's direct children from hierarchy vectors when present or from the
> path FST after finalization. Encountering the same record index more than once
> terminates recursive extraction with a path-bearing invalid-hierarchy error,
> covering cycles and aliased records in malformed metadata. FST traversal
> retains each indexed child key and validates it against the child record name;
> legacy traversal accepts only validated single-component child names.

> [spec:box:req:extraction.root.materialization]
> Extraction MUST create needed parent directories, create or truncate output
> files, and write each regular or chunked record's decompressed payload. On
> Unix it MUST attempt to apply the resolved mode to extracted files; when
> extended attributes are requested it MUST attempt to restore archived Linux
> xattrs, while unsupported platforms and individual metadata failures MAY
> leave them unset.

> [spec:box:req:extraction.root.internal-symlink]
> An internal symlink MUST resolve its target record index to the target's full
> archive path and compute a relative target from the link's parent directory.
> Extraction MUST fail link resolution when the target record or its archive
> path cannot be found, and MUST report filesystem link-creation errors with
> both link and target paths.

> [spec:box:req:extraction.root.external-symlink]
> Once external-link extraction has been explicitly enabled, the extractor
> MUST pass the stored target path directly to the platform symlink operation.
> It MUST NOT require that target to exist, because a dangling external symlink
> is a valid extracted object.

> [spec:box:sem:extraction.root.parallel-ordering]
> Parallel extraction normalizes its effective concurrency to at least one and
> at most the number of file jobs. It first creates
> directories sequentially, then keeps at most that many regular or chunked
> file extraction tasks active against one shared archive mapping, and finally
> creates links sequentially. Checksum work may be pipelined with later file
> extraction, but links begin only after file and checksum tasks finish.

> [spec:box:req:extraction.root.checksum-verification]
> When checksum verification is enabled and a file or chunked-file record has a
> U256 Blake3 attribute, extraction MUST hash the materialized decompressed file
> and compare it with that attribute. A mismatch MUST increment
> `checksum_failures` and MUST NOT by itself turn an otherwise successful
> extraction into an error.

> [spec:box:sem:extraction.root.progress]
> Parallel extraction reports archive totals at `Started`, then directory,
> file-start, file-completion, and link-completion events, followed by
> `Finished` on success. File-completion events carry a cumulative extracted
> count; progress sends are best-effort.

## Validation

> [spec:box:req:validation.root]
> Async archive validation MUST examine `File` and `ChunkedFile` records without
> writing them to the filesystem, compare every present U256 Blake3 attribute
> with the corresponding complete decompressed payload, and return validation
> statistics.

> [spec:box:sem:validation.root.payload-hash]
> Regular-file validation maps and decompresses the record's single compressed
> byte range. Chunked-file validation verifies that its block entries form a
> contiguous logical stream inside the record's compressed-data range,
> decompresses the blocks in logical order, and feeds every block into one
> Blake3 stream whose byte count MUST equal the record's decompressed length.
> Mapping, malformed chunk metadata, or decompression failure terminates
> validation with a verification error; a digest mismatch is recorded in
> statistics.

> [spec:box:sem:validation.root.results]
> Sequential and parallel validation count every regular or chunked file as
> checked, separately count records without a correctly typed checksum, and
> count digest mismatches as checksum failures.

> [spec:box:sem:validation.root.parallel]
> Parallel validation normalizes its effective concurrency to at least one and
> at most both the number of checksum-bearing jobs and the runtime semaphore
> limit. It shares one archive mapping,
> runs checksum-bearing regular and chunked files behind a semaphore, and emits
> `Started`, per-file `Validating` and `Validated`, and terminal `Finished`
> progress. Progress totals and cumulative counts cover checksum-bearing jobs;
> final validation statistics also include checksum-less records.
