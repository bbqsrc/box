# Synchronous archive I/O

## Blocking frontend

> [spec:box:sem:sync-io.root]
> The synchronous frontend exposes blocking reader and writer workflows over
> standard `Read`, `Write`, and `Seek` operations while using the same archive
> core, metadata model, path lookup, alignment, and codec state as the async
> frontend.

> [spec:box:sem:sync-io.root.open]
> A sync reader canonicalizes the path, reads a header at the requested archive
> base, validates the trailer pointer with checked arithmetic and file bounds,
> and retains the mapped trailer that backs borrowed metadata. A sync writer
> opens at archive base zero, owns decoded existing metadata, requires the
> trailer to lie within the current file, and requires every regular and
> chunked payload start and checked end to lie between the header end and prior
> trailer. It reconstructs its append cursor from the greatest payload endpoint,
> or the header end when no payload exists, and rejects an unrepresentable
> aligned cursor or one beyond the prior trailer as invalid archive data.

> [spec:box:sem:sync-io.root.read]
> Sync payload reads map the compressed range at archive base plus record data
> offset. Regular-file decompression copies stored bytes or drives the recorded
> Zstd/XZ decoder into a blocking writer, using the archive Zstd dictionary when
> present and rejecting unknown compression identifiers.

> [spec:box:sem:sync-io.root.write]
> Sync creation uses create-new semantics and writes a provisional header.
> Insertions synchronously stream one regular payload, record byte counts, and
> update metadata and cursor with checked arithmetic. Finalization preserves
> and merges all existing path- and block-FST mappings with records added during
> the append session, including children added beneath FST-indexed directories.
> Finishing flushes data, finalizes the core, rewrites the header, writes and
> flushes the trailer, and marks the writer finished; dropping it earlier emits
> a warning.

> [spec:box:sem:sync-io.root.extraction-validation+2]
> Sync extraction processes selected records sequentially with the shared
> extraction options and statistics; internal links require both a target
> record and its indexed full path, then use a relative path from the link to
> that target, while external links use their stored target verbatim on
> supported platforms. Recursive extraction reports a path-bearing invalid-data
> error when a record index occurs more than once, so cyclic or aliased
> hierarchies terminate. Every record path and internal-link target is validated
> against its index and record name before filesystem materialization. Sync
> validation decompresses and hashes both regular and chunked file records,
> counting missing checksums and mismatches separately.
