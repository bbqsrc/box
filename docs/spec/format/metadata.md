# Metadata support

## Attributes

> [spec:box:def:attributes.root]
> Version 1 metadata begins with an attribute-key schema and an archive-level AttrMap; every record also ends with an AttrMap. The schema is `Vu64(key_count)` followed, in zero-based symbol order, by `u8(type_tag)` and String `name` for each key. An AttrMap is little-endian `u64(byte_count_after_this_field)`, `Vu64(entry_count)`, then pairs of `Vu64(key_index)` and length-prefixed raw bytes. Attribute values remain raw bytes until their schema type is requested.

> [spec:box:def:attributes.root.types]
> Attribute type tags are Bytes `0`, String `1`, JSON `2`, U8 `3`, zigzag Vi32 `4`, FastVint Vu32 `5`, zigzag Vi64 `6`, FastVint Vu64 `7`, fixed 16-byte U128 `8`, fixed 32-byte U256 `9`, and DateTime `10`, where DateTime is a zigzag i64 count of minutes from `2026-01-01T00:00:00Z`. Tags `11..=255` are reserved and are not accepted by the version 1 decoder.

> [spec:box:def:attributes.root.standard-keys]
> The recognized timestamp keys are `created`, `modified`, and `accessed` as DateTime, their `.seconds` companions as U8, and their `.nanoseconds` companions as Vu64. `unix.mode`, `unix.uid`, and `unix.gid` are Vu32; `blake3` is U256; and names beginning `linux.xattr.` carry raw extended-attribute bytes. When `unix.mode` is absent or has an unusable type, extraction uses `0100644` for File and ChunkedFile, `0040755` for Directory, and `0777` for either link variant; record values take precedence over archive-level defaults.

> [spec:box:req:attributes.root.integrity]
> Writers MUST intern each attribute name once and MUST reject assigning a second type to an existing name. Readers MUST reject an unknown schema tag, malformed schema String, truncated entry, or AttrMap whose byte count differs from its decoded contents. An AttrMap key outside the schema MAY remain in the raw map but MUST be omitted by name-resolving iteration. Typed access MUST fall back to raw bytes for invalid UTF-8 or wrong fixed-width payloads; valid UTF-8 but invalid JSON is exposed as a String rather than making the archive unreadable.

## Checksums

> [spec:box:req:checksums.root]
> Checksum-enabled regular-file insertion MUST compute Blake3 incrementally over
> the original byte stream as it is read, before compression changes its
> representation.

> [spec:box:req:checksums.root.attachment]
> After successful streaming or parallel compression, a nonempty checksum
> result MUST be attached to the inserted record under a U256 attribute whose
> name is supplied by the checksum implementation.

> [spec:box:req:checksums.root.disabled]
> Disabling checksums through the null checksum MUST produce no checksum result
> and MUST attach no checksum attribute to the record.

> [spec:box:def:checksums.root.logical-content-domain]
> A checksum attached to either a File or ChunkedFile describes the complete
> decompressed logical content, never compressed frames or alignment padding.

> [spec:box:def:checksums.root.attribute]
> A recognized checksum is the `blake3` attribute declared as U256 with exactly 32 payload bytes. An absent key, a key declared with another type, or a payload that cannot decode as U256 is treated as “without checksum” by validation rather than as an archive parse error.

> [spec:box:req:checksums.root.verification]
> Whenever library verification is requested for a recognized checksum, the
> verifier MUST decompress the complete logical content, hash it with Blake3,
> compare all 32 bytes with the attribute, and surface a mismatch in
> `checksum_failures`.

> [spec:box:sem:checksums.root.verification.extraction-statistics]
> Checksum mismatch during extraction increments `checksum_failures` but does
> not by itself prevent successful materialization or make extraction return an
> error.

> [spec:box:sem:checksums.root.verification.checksum-less]
> Validation skips File and ChunkedFile records without a recognized checksum
> comparison while counting them in `files_without_checksum` and
> `files_checked`.

> [spec:box:req:checksums.root.verification.cli-failure]
> The validation command MUST return an unsuccessful process status when final
> validation statistics contain one or more checksum failures.

## Compression dictionaries

> [spec:box:sem:dictionaries.root]
> Version 1 stores the optional compression dictionary as `Vu64(length)` followed by that many bytes between the archive AttrMap and record count; zero length yields no dictionary. Archive readers pass a present dictionary to every Zstd whole-file or chunk-block decompressor and pass none when it is absent. Stored and XZ operations ignore this metadata. A compression configuration can likewise initialize a Zstd compressor with dictionary bytes, but it does not affect Stored or XZ.

> [spec:box:req:dictionaries.root.training]
> Dictionary training MUST ignore empty samples, truncate each accepted sample to 4096 bytes, and report no dictionary when there are no samples, when total collected bytes are less than 100 times the requested dictionary size, or when the Zstd trainer fails. The default requested dictionary size MUST be 32 KiB. Successful training MUST return the bytes produced by the Zstd trainer without changing their contents.
