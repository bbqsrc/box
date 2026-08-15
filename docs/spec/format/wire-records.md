# Archive wire format and records

## Archive envelope

> [spec:box:syn:wire.root]
> A Box archive is a 32-byte header at an archive-relative base offset, followed by file-data bytes and alignment padding, followed at the header's nonzero `trailer` offset by versioned metadata. Fixed-width multibyte fields are little-endian unless a field is explicitly defined otherwise. Version 1 metadata ends with a length-delimited path FST and, when encoded or present, a length-delimited block FST; consequently the production readers map the trailer from its starting offset through end of file.

> [spec:box:def:wire.root.primitives]
> The wire primitives are `u8`, little-endian `u32` and `u64`, FastVint `Vu64`, a byte string encoded as `Vu64(byte_length)` followed by that many bytes, a String with the same envelope whose payload is UTF-8, and a vector encoded as `Vu64(element_count)` followed by that many element encodings. A FastVint uses the leading-zero count of its first byte to select a one- through nine-byte representation. The unmasked payload bits in the prefix form the high-order part, subsequent payload bytes store the lower-order part least-significant byte first, and a length-specific cumulative offset makes the value ranges disjoint so the representation is canonical.

> [spec:box:req:wire.root.header]
> Writers MUST emit exactly 32 header bytes: `FF 42 4F 58` at `0x00`, the version at `0x04`, flags at `0x05`, little-endian alignment at `0x08`, and the little-endian trailer offset at `0x10`; all reserved bytes MUST be zero. Flag bit 0 denotes external symlinks and bit 1 denotes escaped path spelling. Readers MUST reject a short header or bad magic, MUST read only those two flag bits, and MUST ignore reserved bytes and other flag bits. A zero alignment means no alignment; any nonzero value is used as the modulus for the next data offset, without a power-of-two validation step.

> [spec:box:req:wire.root.bounds]
> Opening an archive MUST fail when the trailer offset is zero, when adding an
> embedded archive base to it overflows `u64`, or when the resulting offset is
> beyond end of file.

> [spec:box:req:wire.root.bounds.lengths-and-counts]
> Trailer decoding MUST reject a byte or string length that does not fit the
> host `usize`, an endpoint addition that overflows, a declared payload that
> exceeds the remaining trailer bytes, or invalid UTF-8. A declared element
> count MUST fit `usize` and MUST NOT exceed the remaining byte count, because
> each encoded element consumes at least one byte.

> [spec:box:req:wire.root.bounds.record-scalars]
> Trailer decoding MUST reject a zero `RecordIndex`, a zero File or ChunkedFile
> data offset, or an unsupported record kind.

> [spec:box:req:wire.root.bounds.fst-envelope]
> A nonempty path- or block-FST envelope MUST have a byte length that fits
> `usize`, whose endpoint does not overflow, and whose complete payload remains
> available in the trailer.

> [spec:box:req:wire.root.bounds.attrmap-envelope]
> An attribute-map byte count MUST equal the bytes consumed by its encoded entry
> count and entries; truncated keys or values remain ordinary trailer-decoding
> failures.

## Record model

> [spec:box:def:records.root]
> A Record is one of Directory, File, ChunkedFile, Link, or ExternalLink. Every variant has a UTF-8 `name` and an attribute map. A `RecordIndex` is a nonzero `Vu64`; value `n` names `records[n - 1]`, and lookup of an index beyond the record vector yields no record.

> [spec:box:def:records.root.file-records]
> A version 1 File record is its combined type/compression byte followed by little-endian `length`, `decompressed_length`, and nonzero `data` `u64` fields, then String `name` and AttrMap `attrs`. `length` is the exact encoded byte extent and `decompressed_length` is the logical content size. A ChunkedFile has the same fields and meanings but inserts a little-endian `u32 block_size` immediately after the type/compression byte; its data extent contains independently encoded blocks described by the block FST.

> [spec:box:def:records.root.non-file-records]
> A version 1 Directory is type `0x01`, String `name`, and AttrMap `attrs`; child indices are not serialized and are recovered from the path FST. An internal Link is type `0x03`, String `name`, nonzero RecordIndex `target`, and AttrMap `attrs`. An ExternalLink is type `0x0B`, String `name`, String `target`, and AttrMap `attrs`; the target is an ordinary UTF-8 platform-path spelling passed to symlink creation, not a RecordIndex or a BoxPath validated by metadata decoding.

> [spec:box:req:records.root.type-byte]
> Version 1 encoders MUST combine the low-nibble record identifiers Directory `0x01`, File `0x02`, Link `0x03`, ChunkedFile `0x0A`, and ExternalLink `0x0B` with the high-nibble compression identifiers Stored `0x00`, Zstd `0x10`, or XZ `0x20`. Decoders MUST reject an unknown low-nibble record identifier. They MUST preserve an unknown or compile-time-unavailable high nibble as `Compression::Unknown` so a later compression operation can return an unsupported-compression error; compression bits on non-file records are otherwise ignored.

> [spec:box:req:records.root.references]
> Record decoders MUST reject zero internal-link indices and zero File or
> ChunkedFile data offsets.

> [spec:box:req:records.root.references.insertion-target]
> Internal-link insertion MUST reject a target index that is not already present
> in the writer's record vector.

> [spec:box:req:records.root.references.resolution]
> Internal-link resolution or extraction MUST fail if a decoded target is
> outside the record vector or has no indexed path.

> [spec:box:req:records.root.references.external-header-flag]
> External-link insertion MUST set the external-symlink header flag before the
> archive header is serialized.

> [spec:box:sem:records.root.references.deferred-relationships]
> Metadata decoding does not cross-check the external-symlink header flag
> against ExternalLink records and does not reject link-to-link targets or link
> cycles; those relationships are interpreted only when a caller resolves or
> extracts the link.
