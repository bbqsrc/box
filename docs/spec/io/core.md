# Archive I/O core

## Archive state

> [spec:box:def:archive-state.root]
> Archive state is the in-memory combination of a Box header, decoded archive
> metadata, and the position information needed to translate between archive
> records and file bytes. Reader state carries an archive base offset; writer
> state carries the next data position and any pending chunk-block index
> entries.

> [spec:box:def:archive-state.root.reader]
> Reader state consists of a parsed header, metadata that may borrow from its
> input buffer, and the byte offset at which the archive is embedded. A
> frontend that retains borrowed metadata also retains the backing storage for
> at least as long as the reader state.

> [spec:box:def:archive-state.root.writer]
> Writer state consists of the header configuration, owned mutable metadata, a
> next-write cursor, and pending block-index entries. A new core writer starts
> at the end of the 32-byte header; a writer reconstructed from an archive uses
> the header, metadata, and cursor supplied by its frontend only after the
> cursor can be aligned without overflow and the existing indexes pass
> finalization validation.

> [spec:box:sem:archive-state.root.record-index]
> Record indices are non-zero and one-based. Index `n` addresses metadata
> vector element `n - 1`; inserting a record appends it and returns the new
> vector length as its index. Unknown indices yield no record.

## Sans-I/O behavior

> [spec:box:sem:sans-io.root]
> The sans-I/O core parses and encodes supplied byte buffers, manages archive
> metadata and write positions, performs buffer-to-buffer decompression, and
> reports byte locations. It does not open, seek, read, write, map, or truncate
> archive files itself.

> [spec:box:sem:sans-io.root.header-metadata]
> Header parsing consumes a complete 32-byte header and reports how many bytes
> are missing when the buffer is shorter. Metadata parsing dispatches by the
> header version and preserves the decoded header flags, alignment, trailer
> pointer, records, attributes, dictionaries, and indexes in reader state.

> [spec:box:sem:sans-io.root.data-location]
> A regular or chunked file record's data offset is relative to its archive
> base. Reader location queries add the reader's base offset and pair the
> result with the record's compressed length, rejecting an addition that
> overflows u64 as invalid archive data; decompression uses the record's
> compression identifier and the archive dictionary, when present.

> [spec:box:sem:sans-io.root.lookup+1]
> Path lookup first uses an exact path-FST match when one is present. On an exact
> miss it starts at the deepest FST-indexed ancestor and traverses that
> directory's in-memory entries, allowing descendants appended to reopened v1
> archives to be found before finalization; otherwise it traverses the root
> entries. Metadata iteration traverses the populated root tree when available,
> falls back to all FST entries when the root is empty, and is empty when neither
> representation is present.

> [spec:box:sem:sans-io.root.alignment]
> A writer's next address is its current cursor when alignment is zero or the
> smallest address at or above that cursor divisible by the configured
> alignment. Alignment, explicit positioning, and advancement use checked
> arithmetic and reject positions that overlap the header, overflow u64, or
> leave no address space for a metadata trailer. Advancing after a payload sets
> the cursor to that aligned address plus the number of bytes written.

> [spec:box:req:sans-io.root.hierarchy]
> A non-root record insertion MUST name an existing directory parent. When a
> parent is not supplied by index and path lookup cannot find it, insertion
> MUST fail; recursive directory creation MUST create missing ancestors before
> the requested directory. An internal-link insertion MUST fail when its target
> record index does not exist.

> [spec:box:sem:sans-io.root.finalization]
> Finalization merges every existing path-FST mapping with paths collected from
> legacy roots, new roots, and children attached beneath FST-indexed
> directories. It likewise merges every existing block-FST mapping with pending
> chunk-block entries. Missing, repeated, or cyclic hierarchy indices and
> conflicting path or block mappings are invalid archive data. Each chunked
> record's merged block sequence starts at logical and physical offset zero
> relative to that record, advances by its logical block size and by strictly
> increasing physical offsets, and reaches the expected final logical block.
> The merged maps are sorted into replacement FSTs, the next aligned address
> becomes the trailer offset, and the header and returned metadata bytes use v1.
> Writing those bytes and the updated header remains the frontend's
> responsibility.
