// Box Archive Format Specification
// Typst source

#set document(
  title: "Box Archive Format Specification",
  author: "Brendan Molloy",
)

#set page(
  paper: "a4",
  margin: (x: 2.5cm, y: 2.5cm),
  numbering: "1",
)

#set text(
  font: "New Computer Modern",
  size: 11pt,
)

#set heading(numbering: "1.1")

#show link: underline

#show raw.where(block: false): box.with(
  fill: luma(240),
  inset: (x: 3pt, y: 0pt),
  outset: (y: 3pt),
  radius: 2pt,
)

#show raw.where(block: true): block.with(
  fill: luma(240),
  inset: 10pt,
  radius: 4pt,
  width: 100%,
)

// Title
#block(inset: (y: 2em))[
  #text(size: 24pt, weight: "bold")[Box Archive Format Specification]
  
  *Version:* 0.2.0 \
  *Status:* Draft
]

#outline(indent: auto)

= Introduction <sec:introduction>

The Box format is an open archive format designed for storing files with support for compression, checksums, extended attributes, and memory-mapped access. It provides platform-independent path encoding and efficient metadata storage through string interning.

== Design Goals <sec:design-goals>

- *Platform Independence:* Paths are encoded in a platform-neutral format that can be safely represented on all major operating systems.
- *Compression:* Multiple compression algorithms are supported, selectable per-file.
- *Integrity:* Optional Blake3 checksums for content verification.
- *Efficiency:* String interning for attribute keys reduces metadata size. Memory mapping enables zero-copy file access for uncompressed files. Block-level compression enables efficient random access for large compressed files.
- *Extensibility:* The attribute system allows arbitrary metadata without format changes.

== File Extension <sec:file-extension>

Box archives SHOULD use the `.box` file extension.

= Terminology <sec:terminology>

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be interpreted as described in #link("https://www.rfc-editor.org/rfc/rfc2119")[RFC 2119].

*Archive:* A Box format file containing zero or more entries.

*Entry:* A file, directory, or symbolic link stored in the archive.

*Record:* The metadata structure describing an entry.

*Trailer:* The metadata section at the end of the archive containing all records and attributes.

*Chunked File:* A file stored with block-level compression, enabling random access without decompressing the entire file.

= File Structure Overview <sec:file-structure>

A Box archive consists of five sections laid out sequentially:

#figure(
  table(
    columns: (auto, 1fr, auto),
    align: (right, center, left),
    stroke: 0.5pt,
    inset: 8pt,
    fill: (x, y) => if x == 1 { (rgb("#e8f4e8"), rgb("#fff8e8"), rgb("#e8f0ff"), rgb("#f0e8ff"), rgb("#ffe8f0")).at(y) } else { none },
    [Offset 0x00 →], [*Header*], [32 bytes, fixed size],
    [Offset 0x20 →], [*Data Section*], [Variable size],
    [header.trailer →], [*Trailer*], [Variable size (BoxMetadata)],
    [], [*Path FST*], [Variable size],
    [EOF →], [*Block FST*], [Variable size (optional)],
  ),
  caption: [Box archive structure],
) <fig:archive-structure>

The Header is always 32 bytes and located at offset 0. The Header contains a pointer to the Trailer, which stores record metadata. File content data is stored between the Header and Trailer. The Path FST follows the Trailer and provides path-to-record mapping (see @sec:path-fst). The Block FST, if present, follows the Path FST and provides logical-to-physical offset mapping for chunked files (see @sec:block-fst). Both FSTs are prefixed with a Vu64 length, enabling efficient seeking past the Path FST to detect the Block FST's presence.

All multi-byte integers in the Box format are stored in *little-endian* byte order unless otherwise specified.

= Data Types <sec:data-types>

== Fixed-Size Integers <sec:fixed-integers>

#table(
  columns: (auto, auto, 1fr),
  align: (left, left, left),
  table.header([*Type*], [*Size*], [*Description*]),
  [`u8`], [1 byte], [Unsigned 8-bit integer],
  [`u32`], [4 bytes], [Unsigned 32-bit integer, little-endian],
  [`u64`], [8 bytes], [Unsigned 64-bit integer, little-endian],
  [`i64`], [8 bytes], [Signed 64-bit integer, little-endian],
)

== Vu64 Encoding (FastVint) <sec:vu64>

Variable-size unsigned 64-bit integers (Vu64) are encoded using FastVint, a prefix-based variable-length encoding where the number of leading zeros in the first byte determines the total byte count.

*Length Determination:*

The number of leading zero bits in the first byte, plus one, gives the total byte count:

```
1xxx_xxxx  →  1 byte   (7 data bits)
01xx_xxxx  →  2 bytes  (14 data bits: 6 + 8)
001x_xxxx  →  3 bytes  (21 data bits: 5 + 16)
0001_xxxx  →  4 bytes  (28 data bits: 4 + 24)
0000_1xxx  →  5 bytes  (35 data bits: 3 + 32)
0000_01xx  →  6 bytes  (42 data bits: 2 + 40)
0000_001x  →  7 bytes  (49 data bits: 1 + 48)
0000_0001  →  8 bytes  (56 data bits: 0 + 56)
0000_0000  →  9 bytes  (64 data bits: 0 + 64)
```

*Offset-Based Encoding:*

FastVint uses offset-based encoding for denser packing. Each length tier has a base offset:

#table(
  columns: (auto, auto, auto),
  align: (left, left, left),
  table.header([*Bytes*], [*Value Range*], [*Offset*]),
  [1], [0 – 127], [0],
  [2], [128 – 16,511], [128],
  [3], [16,512 – 2,113,663], [16,512],
  [4], [2,113,664 – 270,549,119], [2,113,664],
  [5+], [...], [(continues exponentially)],
  [9], [(full u64 range)], [72,624,976,668,147,840],
)

*Data Byte Order:*

After the prefix byte, remaining data bytes are stored in *big-endian* order.

*Key Properties:*

- Length is known from the first byte alone (no continuation scanning required)
- Maximum 9 bytes for `u64` values
- This encoding does NOT enforce minimal representations; the same value MAY have multiple valid encodings
- Decoders MUST accept all valid encodings of a value, including non-minimal representations

== String <sec:string>

Strings are encoded as:

```
[Vu64: byte length]
[UTF-8 bytes]
```

All strings MUST be valid UTF-8. Implementations MUST reject invalid UTF-8 sequences.

== Vec\<T\> <sec:vec>

Vectors (arrays) are encoded as:

```
[Vu64: element count]
[T encoding] * count
```

The following vector types are used in the format:

#table(
  columns: (auto, auto, 1fr),
  align: (left, left, left),
  table.header([*Type*], [*Usage*], [*Element Encoding*]),
  [`Vec<Record>`], [`BoxMetadata.records`], [Record (see @sec:record-types)],
  [`Vec<AttrKey>`], [`attr_keys`], [Type tag + String (see @sec:attr-keys)],
)

== Vec\<u8\> (Byte Array) <sec:vec-u8>

Byte arrays are encoded identically to strings:

```
[Vu64: byte length]
[raw bytes]
```

== RecordIndex <sec:record-index>

A RecordIndex is a 1-based index into the records array, encoded as Vu64:

```
[Vu64: index value]
```

Index values MUST be non-zero. An index value of `n` refers to `records[n-1]`.

== AttrMap <sec:attrmap>

Attribute maps are encoded as:

```
[u64: total byte count of this section, including this field]
[Vu64: entry count]
For each entry:
    [Vu64: key index into attr_keys]
    [Vec<u8>: value]
```

The leading byte count allows implementations to skip the entire attribute map without parsing individual entries.

= Header <sec:header>

The Header is located at byte offset 0 and is exactly 32 bytes.

== Header Structure <sec:header-structure>

#table(
  columns: (auto, auto, auto, 1fr),
  align: (left, left, left, left),
  table.header([*Offset*], [*Size*], [*Field*], [*Description*]),
  [`0x00`], [4], [`magic`], [Magic bytes: `0xFF 0x42 0x4F 0x58` (`\xFFBOX`)],
  [`0x04`], [1], [`version`], [Format version number],
  [`0x05`], [1], [`flags`], [Feature flags (see @sec:flags)],
  [`0x06`], [2], [`reserved1`], [Reserved bytes],
  [`0x08`], [4], [`alignment`], [Data alignment boundary (`u32`)],
  [`0x0C`], [4], [`reserved2`], [Reserved bytes],
  [`0x10`], [8], [`trailer`], [Byte offset to trailer (`u64`)],
  [`0x18`], [8], [`reserved3`], [Reserved bytes],
)

*Total Size:* 32 bytes

== Magic Bytes <sec:magic>

The magic bytes MUST be exactly `0xFF 0x42 0x4F 0x58` (the byte `0xFF` followed by ASCII `BOX`).

The leading `0xFF` byte is invalid UTF-8, causing any attempt to parse the file as a text string to fail immediately at byte 0.

== Version <sec:version>

The current format version is `0x02` (2). Implementations SHOULD reject archives with unknown major versions.

== Flags <sec:flags>

The flags byte is a bitfield:

#table(
  columns: (auto, auto, 1fr),
  align: (left, left, left),
  table.header([*Bit*], [*Name*], [*Description*]),
  [0], [`EXTERNAL_SYMLINKS`], [Archive contains external symlinks (see @sec:external-symlink)],
  [1-7], [Reserved], [Must be 0],
)

== Alignment <sec:alignment>

The `alignment` field specifies the byte boundary to which file data is aligned. Valid values are powers of 2 from 1 to 65536. A value of 0 indicates no alignment (equivalent to 1).

When alignment is specified, padding bytes (value `0x00`) are inserted before file data to ensure each file's data starts at an offset that is a multiple of the alignment value.

Common alignment values:
- `0` or `1`: No alignment (most compact)
- `4096`: Page alignment (optimal for memory mapping)
- `512`: Sector alignment

== Reserved Fields <sec:reserved-fields>

All reserved fields (`reserved1`, `reserved2`, `reserved3`) MUST be set to zero when writing and MUST be ignored when reading to allow for future format extensions.

== Binary Layout <sec:binary-layout>

#figure(
  {
    set text(font: "Courier New", size: 9pt)
    table(
      columns: (auto,) + (1.2em,) * 16,
      align: center + horizon,
      stroke: 0.5pt,
      fill: (x, y) => if y == 0 { rgb("#e8e8e8") } else { white },
      [*Off*], [*00*], [*01*], [*02*], [*03*], [*04*], [*05*], [*06*], [*07*], [*08*], [*09*], [*0A*], [*0B*], [*0C*], [*0D*], [*0E*], [*0F*],
      [0x00], [FF], [42], [4F], [58], [VV], [XX], [RR], [RR], [AA], [AA], [AA], [AA], [RR], [RR], [RR], [RR],
      [0x10], [TT], [TT], [TT], [TT], [TT], [TT], [TT], [TT], [RR], [RR], [RR], [RR], [RR], [RR], [RR], [RR],
    )
  },
  caption: [Header binary layout],
) <fig:header-binary>

#text(size: 10pt)[
  *Legend:* `FF 42 4F 58` = magic, `VV` = version, `XX` = flags, `RR` = reserved, `AA` = alignment, `TT` = trailer offset
]

= Data Section <sec:data-section>

The Data Section immediately follows the Header and contains the actual file content.

== Layout <sec:data-layout>

File data is stored contiguously, with optional padding between files when alignment is enabled. The Data Section starts at offset `0x20` (32 bytes) and extends to the trailer offset.

#figure(
  table(
    columns: (auto, 1fr),
    align: (right, center),
    stroke: 0.5pt,
    inset: 8pt,
    [Offset 0x20 →], [*File 1 Data*],
    [], [\[Padding\] _(if alignment > 1)_],
    [], [*File 2 Data*],
    [], [\[Padding\]],
    [], [...],
    [header.trailer →], [],
  ),
  caption: [Data section layout],
) <fig:data-layout>

== Alignment Padding <sec:alignment-padding>

When `header.alignment > 1`, padding bytes (`0x00`) are inserted before file data such that:

```
file_data_offset % alignment == 0
```

The amount of padding before each file is:

```
padding = (alignment - (current_offset % alignment)) % alignment
```

= Trailer (Metadata) <sec:trailer>

The Trailer contains all archive metadata encoded as `BoxMetadata`.

== BoxMetadata Structure <sec:boxmetadata>

```
[Vec<AttrKey>: attr_keys]
[AttrMap: attrs]
[Vec<Record>: records]
```

Fields are encoded in the order shown, using the data type encodings from @sec:data-types.

== Attribute Key Table (attr_keys) <sec:attr-keys>

The attribute key table provides string interning for attribute keys. Each entry consists of:

```
[u8: type tag]
[String: key name]
```

The type tag indicates the expected value type for this attribute key. See @sec:attr-value-types for type tag values.

Keys are referenced by their 0-based index in this table.

== Attribute Value Types <sec:attr-value-types>

*Type Tag Values:*

#table(
  columns: (auto, auto, 1fr),
  align: (left, left, left),
  table.header([*Value*], [*Type*], [*Description*]),
  [0], [Bytes], [Raw bytes, no interpretation],
  [1], [String], [UTF-8 string],
  [2], [Json], [UTF-8 JSON],
  [3], [U8], [Single byte (u8)],
  [4], [Vi32], [Zigzag-encoded i32],
  [5], [Vu32], [FastVint-encoded u32],
  [6], [Vi64], [Zigzag-encoded i64],
  [7], [Vu64], [FastVint-encoded u64],
  [8], [U128], [Fixed 16 bytes (128 bits)],
  [9], [U256], [Fixed 32 bytes (256 bits)],
  [10], [DateTime], [Minutes since Box epoch (2026-01-01 00:00:00 UTC), zigzag-encoded i64],
  [11-255], [Reserved], [Reserved for future use],
)

When serializing: keys are written in symbol order (0, 1, 2, ...).

When deserializing: strings are assigned symbols in the order read. The type tag determines how the attribute value is interpreted.

== Archive Attributes (attrs) <sec:archive-attrs>

The `attrs` field stores archive-level attributes as an `AttrMap`. Common archive attributes include creation tools, comments, or custom metadata.

== Dictionary <sec:dictionary>

The `dictionary` field contains a Zstd dictionary used for compression. When present (non-empty), all records compressed with Zstd MUST use this dictionary for both compression and decompression. The dictionary is ignored for other compression methods (Stored, XZ).

Dictionary training is typically performed at archive creation time using `ZDICT_trainFromBuffer()` or equivalent. A recommended dictionary size is 32KB-128KB, trained on representative samples from the archive contents totaling approximately 100× the dictionary size.

When `dictionary` is empty, Zstd compression operates without a dictionary.

The dictionary provides two benefits:
1. Improved compression ratio, especially for small files with shared patterns
2. More predictable compression ratios, which aids in estimating compressed block sizes for chunked files

= Record Types <sec:record-types>

Records describe entries in the archive. Each record begins with a 1-byte type/compression identifier.

== Type/Compression Byte <sec:type-compression>

The first byte of each record encodes both the record type and compression method:

```
[type_compression: u8]
```

*Layout:*

```
Bits 0-3: Record type
Bits 4-7: Compression method
```

*Record Type Values:*

#table(
  columns: (auto, auto, 1fr),
  align: (left, left, left),
  table.header([*Value*], [*Type*], [*Description*]),
  [`0x00`], [Directory], [Directory entry],
  [`0x02`], [File], [Regular file],
  [`0x03`], [Symlink], [Internal symbolic link],
  [`0x0A`], [Chunked File], [File with block-level compression],
  [`0x0B`], [External Symlink], [Symbolic link to external target],
)

*Compression Values:*

#table(
  columns: (auto, auto, 1fr),
  align: (left, left, left),
  table.header([*Value*], [*Method*], [*Description*]),
  [`0x00`], [Stored], [No compression],
  [`0x10`], [Zstd], [Zstandard compression],
  [`0x20`], [XZ], [XZ/LZMA2 compression],
)

*Combined Examples:*

#table(
  columns: (auto, 1fr),
  align: (left, left),
  table.header([*Byte*], [*Meaning*]),
  [`0x00`], [Directory (compression ignored)],
  [`0x02`], [File, stored (uncompressed)],
  [`0x12`], [File, Zstd compressed],
  [`0x22`], [File, XZ compressed],
  [`0x1A`], [Chunked file, Zstd compressed blocks],
)

== Directory Record (Type 0x00) <sec:directory-record>

Directories have no associated data in the Data Section.

*Structure:*

```
[type_compression: u8]  // Always 0x00
[String: name]
[AttrMap: attrs]
```

*Binary Layout:*

#block(inset: (left: 1em))[
  #set text(font: "Courier New", size: 10pt)
  #table(
    columns: (auto, 1fr),
    stroke: none,
    inset: 3pt,
    [`00`], [type_compression (directory)],
    [...], [String: name],
    [...], [AttrMap: attrs],
  )
]

== File Record (Type 0x02) <sec:file-record>

Files store their content in the Data Section.

*Structure:*

```
[type_compression: u8]
[length: u64]              // Compressed length in data section
[decompressed_length: u64] // Original file size
[data: u64]                // Byte offset in archive
[String: name]
[AttrMap: attrs]
```

*Binary Layout:*

#block(inset: (left: 1em))[
  #set text(font: "Courier New", size: 10pt)
  #table(
    columns: (auto, 1fr),
    stroke: none,
    inset: 3pt,
    [`TT`], [type_compression],
    [`LL LL LL LL LL LL LL LL`], [length (u64)],
    [`DD DD DD DD DD DD DD DD`], [decompressed_length (u64)],
    [`OO OO OO OO OO OO OO OO`], [data offset (u64)],
    [...], [String: name],
    [...], [AttrMap: attrs],
  )
]

When `compression == Stored`, `length == decompressed_length`.

== Symlink Record (Type 0x03) <sec:symlink-record>

Symbolic links point to other entries within the archive.

*Structure:*

```
[type_compression: u8]  // Always 0x03
[String: name]
[Vu64: target (RecordIndex)]
[AttrMap: attrs]
```

*Constraints:*

- Target MUST be a valid RecordIndex pointing to an existing record
- Circular symlinks are not permitted
- The target record MUST NOT be another symlink

*Binary Layout:*

#block(inset: (left: 1em))[
  #set text(font: "Courier New", size: 10pt)
  #table(
    columns: (auto, 1fr),
    stroke: none,
    inset: 3pt,
    [`03`], [type_compression (symlink)],
    [...], [String: name],
    [...], [Vu64: target (RecordIndex)],
    [...], [AttrMap: attrs],
  )
]

== External Symlink Record (Type 0x0B) <sec:external-symlink>

External symlinks point to paths outside the archive. This record type is only valid when the `EXTERNAL_SYMLINKS` flag is set in the header.

*Structure:*

```
[type_compression: u8]  // Always 0x0B
[String: name]
[String: target]
[AttrMap: attrs]
```

*Target Path Format:*

The target path uses BoxPath encoding (see @sec:path-encoding) with one relaxation: `..` components are permitted to allow the symlink to escape the archive root. The path is stored with `\x1F` (Unit Separator) as the component separator.

*Constraints:*

- The header MUST have the `EXTERNAL_SYMLINKS` flag (bit 0) set
- Target is a relative path from the symlink's location
- Implementations MUST validate `..` does not escape the extraction directory without explicit user consent

*Binary Layout:*

#block(inset: (left: 1em))[
  #set text(font: "Courier New", size: 10pt)
  #table(
    columns: (auto, 1fr),
    stroke: none,
    inset: 3pt,
    [`0B`], [type_compression (external symlink)],
    [...], [String: name],
    [...], [String: target],
    [...], [AttrMap: attrs],
  )
]

*Extraction Behavior:*

When extracting an external symlink record, implementations:

1. Convert the stored target path from BoxPath format (`\x1F` separators) to platform path format
2. Create a symbolic link at the record's location using the converted relative target path

*Security Considerations:*

External symlinks can point to arbitrary locations outside the extraction directory. This presents security risks:

- Implementations MUST require explicit user consent before extracting archives containing external symlinks.
- This feature is opt-in at both archive creation time (setting the header flag) and extraction time.
- Extraction tools SHOULD warn users when processing archives with external symlinks.

== Chunked File Record (Type 0x0A) <sec:chunked-file>

Chunked files store content in fixed-size blocks, each independently compressed. This enables random access to large compressed files without decompressing the entire file.

*Structure:*

```
[type_compression: u8]     // 0x0A | compression_method
[block_size: u64]          // Size of each block before compression
[length: u64]              // Total compressed length
[decompressed_length: u64] // Total original file size
[data: u64]                // Byte offset to first block
[String: name]
[AttrMap: attrs]
```

*Binary Layout:*

#block(inset: (left: 1em))[
  #set text(font: "Courier New", size: 10pt)
  #table(
    columns: (auto, 1fr),
    stroke: none,
    inset: 3pt,
    [`TT`], [type_compression],
    [`BB BB BB BB BB BB BB BB`], [block_size (u64)],
    [`LL LL LL LL LL LL LL LL`], [length (u64)],
    [`DD DD DD DD DD DD DD DD`], [decompressed_length (u64)],
    [`OO OO OO OO OO OO OO OO`], [data offset (u64)],
    [...], [String: name],
    [...], [AttrMap: attrs],
  )
]

*Decompression:*

To read bytes at logical offset `O` within a chunked file, use the Block FST lookup procedure described in @sec:block-lookup.

*Recommended Block Size:*

A block size of 2MB (2,097,152 bytes) is RECOMMENDED. This aligns with common hugepage sizes and provides a good balance between compression ratio and random access granularity.

= Path Encoding <sec:path-encoding>

Paths in Box archives use a platform-independent encoding called BoxPath.

== BoxPath Format <sec:boxpath>

BoxPath paths are sequences of components separated by `U+001F` UNIT SEPARATOR (hex `\x1F`).

*Properties:*

- Paths are always relative (no leading separator)
- No trailing separator
- Empty components are not permitted
- Components `.` and `..` are not permitted
- Components cannot contain platform path separators (`/` or `\`)
- Components cannot contain null bytes

== Separator Choice <sec:separator-choice>

The `U+001F` separator was chosen because:
1. It forces explicit handling - developers cannot be lazy and treat paths as platform-native strings
2. It avoids confusion with URL scheme prefixes (`file://`, `http://`) that would occur with `/`
3. It is truly platform-agnostic - neither Unix nor Windows "owns" this separator
4. It is illegal in filenames on all major platforms, preventing ambiguity

== Platform Conversion <sec:platform-conversion>

When extracting archives, implementations MUST convert BoxPath paths to platform-native paths:

#table(
  columns: (auto, auto),
  align: (left, left),
  table.header([*Platform*], [*Separator*]),
  [Unix/Linux/macOS], [`/`],
  [Windows], [`\`],
)

*Example:*

```
BoxPath:  foo\x1Fbar\x1Fbaz.txt
Unix:     foo/bar/baz.txt
Windows:  foo\bar\baz.txt
```

== Unicode Normalization <sec:unicode-normalization>

All paths MUST be stored in NFC (Canonical Decomposition, followed by Canonical Composition) normalized form. Implementations MUST normalize paths before storage and comparison.

= Compression <sec:compression>

Box supports multiple compression methods, selectable per-file.

== Supported Methods <sec:compression-methods>

#table(
  columns: (auto, auto, auto, 1fr),
  align: (left, left, left, left),
  table.header([*Value*], [*Name*], [*Library*], [*Notes*]),
  [`0x00`], [Stored], [None], [No compression],
  [`0x10`], [Zstd], [zstd], [Recommended default],
  [`0x20`], [XZ], [liblzma], [High compression ratio],
)

== Zstd Configuration <sec:zstd-config>

When using Zstd:

- Compression level: Implementations SHOULD allow user-configurable levels (1-22)
- Default level: 3 (good balance of speed and ratio)
- Dictionary: If `BoxMetadata.dictionary` is non-empty, it MUST be used

== XZ Configuration <sec:xz-config>

When using XZ:

- Preset: Implementations SHOULD use preset 6 by default
- Integrity check: CRC64 is RECOMMENDED

== Stored (Uncompressed) <sec:stored>

When compression is `Stored` (`0x00`):

- `length` equals `decompressed_length`
- Data is stored verbatim
- Useful for already-compressed files (images, videos) or when memory mapping is desired

== Dictionary Usage <sec:dictionary-usage>

When a dictionary is present in `BoxMetadata.dictionary` (@sec:dictionary), it MUST be used for all Zstd-compressed content in the archive, including:

- Whole-file compressed files
- Individual blocks in chunked files

The dictionary is loaded once when opening the archive and reused for all Zstd decompression operations. The dictionary has no effect on Stored or XZ-compressed content.

= Checksums <sec:checksums>

Box archives support content checksums for integrity verification.

== Supported Algorithms <sec:checksum-algorithms>

#table(
  columns: (auto, auto, auto),
  align: (left, left, left),
  table.header([*Name*], [*Attribute*], [*Output Size*]),
  [Blake3], [`blake3`], [32 bytes],
)

== Checksum Storage <sec:checksum-storage>

Checksums are stored as record attributes:

```
attrs["blake3"] = <32-byte hash>
```

The checksum is computed over the *decompressed* content.

== Verification <sec:checksum-verification>

Implementations SHOULD verify checksums when:
1. Extracting files (if checksums are present)
2. Explicitly requested by the user

Verification failures MUST be reported to the user. Implementations MAY offer options to proceed despite failures.

= Standard Attributes <sec:standard-attrs>

This section defines standard attribute keys that implementations SHOULD recognize.

== Timestamps <sec:timestamps>

#table(
  columns: (auto, auto, 1fr),
  align: (left, left, left),
  table.header([*Attribute*], [*Type*], [*Description*]),
  [`created`], [Vi64], [Creation time (minutes since Box epoch)],
  [`modified`], [Vi64], [Modification time (minutes since Box epoch)],
  [`accessed`], [Vi64], [Access time (minutes since Box epoch)],
)

*Box Epoch:* 2026-01-01 00:00:00 UTC

Timestamps are OPTIONAL and stored as signed variable-length integers (Vi64, zigzag encoded) representing minutes since the Box epoch. The signed format supports dates before 2026. Minute precision is sufficient for typical archive use cases.

== Sub-Minute Precision <sec:sub-minute>

For applications requiring higher precision:

#table(
  columns: (auto, auto, 1fr),
  align: (left, left, left),
  table.header([*Attribute*], [*Type*], [*Description*]),
  [`created.seconds`], [U8], [Additional seconds (0-59)],
  [`modified.seconds`], [U8], [Additional seconds (0-59)],
  [`accessed.seconds`], [U8], [Additional seconds (0-59)],
  [`created.nanoseconds`], [Vu64], [Sub-minute precision in nanoseconds (0-59,999,999,999)],
  [`modified.nanoseconds`], [Vu64], [Sub-minute precision in nanoseconds (0-59,999,999,999)],
  [`accessed.nanoseconds`], [Vu64], [Sub-minute precision in nanoseconds (0-59,999,999,999)],
)

The `.seconds` attributes are deprecated in favor of `.nanoseconds`. New implementations SHOULD use `.nanoseconds` for sub-minute precision. The nanoseconds value represents the full sub-minute component (0 to 59,999,999,999), not just the sub-second portion.

== Unix Permissions <sec:unix-permissions>

#table(
  columns: (auto, auto, 1fr),
  align: (left, left, left),
  table.header([*Attribute*], [*Type*], [*Extraction Default*]),
  [`unix.mode`], [Vu32], [0o100644 (files), 0o40755 (dirs)],
  [`unix.uid`], [Vu32], [Archive attr, then current user],
  [`unix.gid`], [Vu32], [Archive attr, then current user],
)

*unix.mode Storage:*

The mode value includes the file type bits in the standard Unix format:
- Regular file: `0o100000` + permissions
- Directory: `0o040000` + permissions
- Symlink: `0o120000` + permissions

*Extraction Behavior:*

If `unix.mode` is not present:
- Files: `0o100644` (rw-r--r--)
- Directories: `0o040755` (rwxr-xr-x)
- Symlinks: `0o120777` (rwxrwxrwx)

If `unix.uid`/`unix.gid` are not present, implementations SHOULD:
1. Check archive-level `unix.uid`/`unix.gid` attributes
2. Fall back to current user's UID/GID

== Attribute Encoding <sec:attr-encoding>

Attribute values are stored as raw bytes (`Vec<u8>`). The type tag stored with the attribute key (see @sec:attr-keys) determines how to interpret the value.

*Standard Attribute Types:*

#table(
  columns: (1fr, auto, 1fr),
  align: (left, left, left),
  table.header([*Attribute*], [*Type Tag*], [*Encoding*]),
  [`created`, `modified`, `accessed`], [DateTime (10)], [Zigzag-encoded i64 (minutes since Box epoch)],
  [`created.seconds`, `modified.seconds`, `accessed.seconds`], [U8 (3)], [Single byte (0-59)],
  [`created.nanoseconds`, `modified.nanoseconds`, `accessed.nanoseconds`], [Vu64 (7)], [FastVint-encoded u64],
  [`unix.mode`, `unix.uid`, `unix.gid`], [Vu32 (5)], [FastVint-encoded u32],
  [`blake3`], [U256 (9)], [Fixed 32 bytes],
)

Applications MAY define custom attributes with any type tag.

= Implementation Guidance <sec:implementation-guidance>

This section provides non-normative guidance for implementers.

== Memory Mapping <sec:memory-mapping>

Box archives are designed to support memory-mapped access:

1. Use appropriate `alignment` (e.g., 4096 for page alignment)
2. Store files with `compression = Stored` for direct access
3. The FST structure enables O(m) path lookups where m is path length

== Streaming Creation <sec:streaming-creation>

Archives can be created in a streaming fashion:

1. Write header with placeholder trailer offset
2. Write file data sequentially, recording offsets
3. Write trailer with all records
4. Build and write Path FST (and Block FST if needed)
5. Seek back and update header with trailer offset

== Large File Handling <sec:large-file-handling>

For files larger than available memory:

1. Use chunked file records for random access
2. Stream decompression for sequential access
3. Consider block size trade-offs (smaller = more overhead, larger = more memory)

== Error Recovery <sec:error-recovery>

The trailer-at-end design means truncated archives lose metadata. Implementations MAY:

1. Store periodic checkpoints during creation
2. Implement recovery tools that scan for record boundaries
3. Use file system features (copy-on-write, journaling) for durability

= Path FST <sec:path-fst>

The Path FST (Finite State Transducer) provides efficient path-to-record mapping.

== Overview <sec:fst-overview>

The FST maps BoxPath strings to RecordIndex values. It supports:

- O(m) exact lookups where m is path length
- Prefix queries (enumerate all paths under a directory)
- Memory-efficient storage through prefix sharing

== FST Layout <sec:fst-layout>

The Path FST is located immediately after the Trailer:

#figure(
  table(
    columns: (auto, 1fr, auto),
    align: (right, center, left),
    stroke: 0.5pt,
    inset: 8pt,
    [trailer_end →], [*Length (Vu64)*], [1–9 bytes],
    [], [*Header*], [24 bytes],
    [], [*Node Index*], [node_count × 8 bytes],
    [], [*Hot Section*], [Variable size (compact node headers)],
    [], [*Cold Section*], [Variable size (edge data)],
  ),
  caption: [FST section layout],
) <fig:fst-layout>

The length prefix encodes the total size of the FST data (Header through Cold Section, not including the length prefix itself).

The Block FST (if present) uses the same structure (length prefix followed by FST data) and follows immediately after the Path FST.

== FST Header <sec:fst-header>

The FST Header is 24 bytes and located at the start of the FST section.

#table(
  columns: (auto, auto, auto, 1fr),
  align: (left, left, left, left),
  table.header([*Offset*], [*Size*], [*Field*], [*Description*]),
  [`0x00`], [4], [`magic`], [Magic bytes: `BFST` (0x42 0x46 0x53 0x54)],
  [`0x04`], [1], [`version`], [Format version (currently 1)],
  [`0x05`], [1], [`flags`], [Reserved flags (must be 0)],
  [`0x06`], [2], [`reserved`], [Reserved bytes (must be 0)],
  [`0x08`], [4], [`node_count`], [Total number of nodes (`u32`)],
  [`0x0C`], [8], [`entry_count`], [Number of entries indexed (`u64`)],
  [`0x14`], [4], [`cold_offset`], [Cold section start offset (`u32`)],
)

The Hot Section starts at offset `24 + node_count × 8` (immediately after the Node Index). The root node is always node 0.

Implementations MUST reject FST data where magic bytes do not match or version is unsupported.

== Node Index <sec:node-index>

The Node Index is an array of `node_count` entries, with each entry being 8 bytes.

*Entry Format:*

#table(
  columns: (auto, auto, auto, 1fr),
  align: (left, left, left, left),
  table.header([*Offset*], [*Size*], [*Field*], [*Description*]),
  [`0x00`], [4], [`hot_offset`], [Offset within Hot Section (`u32`)],
  [`0x04`], [4], [`cold_offset`], [Offset within Cold Section (`u32`)],
)

Offsets are relative to the start of their respective sections. Node IDs are indices into this array (node 0 = first entry, node 1 = second entry, etc.).

== Hot Section <sec:hot-section>

The Hot Section contains compact node headers optimized for cache efficiency. Each node's hot data has the following structure:

#block(inset: (left: 1em))[
  #set text(font: "Courier New", size: 10pt)
  #table(
    columns: (auto, 1fr),
    stroke: none,
    inset: 3pt,
    [...], [flags: u8],
    [...], [edge_count: Vu64],
    [...], [lookup_data: variable],
    [...], [offsets: edge_count × u16],
  )
]

*Flags Byte:*

#table(
  columns: (auto, auto, 1fr),
  align: (left, left, left),
  table.header([*Bit*], [*Name*], [*Description*]),
  [0], [`IS_FINAL`], [Node is an accepting state (has an output value)],
  [1], [`INDEXED`], [Node uses 256-byte lookup table (17+ edges)],
  [2-7], [Reserved], [Must be 0],
)

*Lookup Data:*

The lookup data format depends on the `INDEXED` flag:

- *If INDEXED (≥17 edges):* 256-byte lookup table where `table[byte]` returns the edge index for that byte value, or `0xFF` if no edge exists for that byte.

- *If not INDEXED (\<17 edges):* Array of `edge_count` bytes containing the first byte of each edge label, sorted in ascending order. Used for binary search or SIMD-accelerated lookups.

*Offsets Array:*

Array of `edge_count` entries, each a `u16`. Each offset points to the corresponding edge's data within this node's cold section data block.

== Cold Section <sec:cold-section>

The Cold Section contains edge data and final output values. Each node's cold data has the following structure:

#block(inset: (left: 1em))[
  #set text(size: 10pt)
  *For each edge (0 to edge_count-1):*
  #block(inset: (left: 1em))[
    #set text(font: "Courier New")
    #table(
      columns: (auto, 1fr),
      stroke: none,
      inset: 2pt,
      [...], [label_len: Vu64],
      [...], [label: label_len bytes],
      [...], [output: Vu64],
      [...], [target_node: u32],
    )
  ]
  *If IS_FINAL flag is set:*
  #block(inset: (left: 1em))[
    #set text(font: "Courier New")
    [...] final_output: Vu64
  ]
]

*Edge Fields:*

#table(
  columns: (auto, auto, 1fr),
  align: (left, left, left),
  table.header([*Field*], [*Type*], [*Description*]),
  [`label_len`], [Vu64], [Length of edge label in bytes],
  [`label`], [bytes], [Edge label (arbitrary byte sequence)],
  [`output`], [Vu64], [Output value to accumulate (`u64`)],
  [`target_node`], [`u32`], [Target node ID],
)

*Final Output:*

If the `IS_FINAL` flag is set, the final output value is stored at the end of the node's cold data block. This value is added to the accumulated output when the traversal terminates at this node.

== Path Encoding in FST <sec:fst-path-encoding>

Paths stored in the FST use the same encoding as BoxPath (@sec:path-encoding):

- Components are separated by `U+001F` UNIT SEPARATOR (`\x1F`)
- Paths are stored without leading separators
- Example: `foo/bar/baz.txt` → `foo\x1Fbar\x1Fbaz.txt`

*Build Requirements:*

- Keys MUST be inserted in strict lexicographic byte order
- Duplicate keys are not permitted
- Values are non-zero `u64` record indices

== Output Value Accumulation <sec:output-accumulation>

The FST stores output values along edges and at final nodes. The final lookup value is computed by:

1. Starting with accumulated value = 0
2. For each edge traversed: `accumulated = accumulated.wrapping_add(edge.output)`
3. If the final node has `IS_FINAL` set: `accumulated = accumulated.wrapping_add(final_output)`

For the Path FST, the result is a `RecordIndex` value. For the Block FST, the result is a physical byte offset.

== FST Implementation Notes <sec:fst-implementation>

*Adaptive Node Format:*

The FST uses an adaptive node format inspired by Adaptive Radix Trees (ART):

- Nodes with fewer than 17 edges use compact format (sorted first-bytes array)
- Nodes with 17 or more edges use indexed format (256-byte lookup table)

The threshold of 17 balances memory usage against lookup performance.

*Hot/Cold Separation:*

Data is separated into "hot" (frequently accessed during traversal) and "cold" (accessed only when needed) sections. This improves cache utilization during lookups.

*SIMD Optimization:*

Implementations MAY use SIMD instructions for compact node lookups:
- x86_64: SSE2 `_mm_cmpeq_epi8` for 16-byte parallel comparison
- aarch64: NEON `vceqq_u8` for similar parallel comparison

== FST Constants <sec:fst-constants>

These constants apply to both Path FST and Block FST:

#table(
  columns: (auto, auto, 1fr),
  align: (left, left, left),
  table.header([*Constant*], [*Value*], [*Description*]),
  [Magic], [`BFST`], [FST section identifier],
  [Version], [1], [Current format version],
  [Header size], [24 bytes], [Fixed header size],
  [Index entry size], [8 bytes], [Per-node index entry],
  [Indexed threshold], [17], [Minimum edges for indexed format],
)

= Block FST <sec:block-fst>

The Block FST provides logical-to-physical offset mapping for chunked files. It is located immediately after the Path FST.

*Presence Detection:*

Read the Path FST length prefix, then compute `path_fst_end = trailer_end + sizeof(length_prefix) + path_fst_length`. If `path_fst_end` \< `EOF`, the Block FST begins at that position (starting with its own length prefix). If `path_fst_end` = `EOF`, no Block FST is present.

== Block FST Overview <sec:block-fst-overview>

The Block FST maps (record_index, logical_offset) pairs to physical offsets within the archive's data section. Block lookups use predecessor queries to find the block containing any arbitrary byte offset.

== Block FST Format <sec:block-fst-format>

The Block FST uses the same structure as the Path FST: a Vu64 length prefix followed by the FST data (@sec:fst-header -- @sec:cold-section, @sec:output-accumulation -- @sec:fst-constants). @sec:fst-path-encoding (Path Encoding) does not apply; Block FST uses fixed-width keys as described below.

== Block FST Key Format <sec:block-key-format>

Keys are 16-byte fixed-width values:

#block(inset: (left: 1em))[
  #set text(font: "Courier New", size: 10pt)
  #table(
    columns: (auto, auto, 1fr),
    stroke: none,
    inset: 3pt,
    [Bytes 0–7:], [record_index], [(u64, big-endian)],
    [Bytes 8–15:], [logical_offset], [(u64, big-endian)],
  )
]

- `record_index`: 1-based index of the chunked file record
- `logical_offset`: Logical byte offset of the block start (always a multiple of `block_size`)

Big-endian encoding ensures lexicographic byte order matches numeric order.

*Example keys for record 5 with 2MB blocks:*

#table(
  columns: (auto, 1fr),
  align: (left, left),
  table.header([*Key (hex)*], [*Meaning*]),
  [`0000000000000005 0000000000000000`], [Record 5, block at offset 0],
  [`0000000000000005 0000000000200000`], [Record 5, block at offset 2MB],
  [`0000000000000005 0000000000400000`], [Record 5, block at offset 4MB],
)

== Block FST Value Format <sec:block-value-format>

Values are physical byte offsets (`u64`) pointing to the start of the compressed block data within the archive.

== Block Lookup <sec:block-lookup>

To find the compressed data for a logical offset `O` in a chunked file with record index `R`:

1. Construct key: `[R as u64, big-endian][O as u64, big-endian]`
2. Query Block FST for predecessor (largest key ≤ query key)
3. Verify the result key has matching record index `R`
4. Result value is the physical offset of the compressed block
5. Read and decompress the frame (compression frames are self-delimiting)
6. The offset within the decompressed block is `O - block_logical_offset` where `block_logical_offset` is from the result key

*Example:* Record 5 with 2MB (2,097,152 byte) block size. Block FST contains:

#block(inset: (left: 1em))[
  #set text(font: "Courier New", size: 10pt)
  #table(
    columns: (auto, auto, auto),
    stroke: none,
    inset: 3pt,
    align: (left, center, left),
    [\[5\]\[0\]], [→], [physical_offset_a],
    [\[5\]\[2097152\]], [→], [physical_offset_b],
    [\[5\]\[4194304\]], [→], [physical_offset_c],
  )
]

*Query: 17KB (17,408 bytes)*

1. Construct key: `[5][17408]`
2. Predecessor result: `[5][0]` (since 0 ≤ 17408 \< 2097152)
3. Read from `physical_offset_a`, decompress
4. Offset within block: 17408 - 0 = 17408

*Query: 2MB exactly (2,097,152 bytes)*

1. Construct key: `[5][2097152]`
2. Predecessor result: `[5][2097152]` (exact match)
3. Read from `physical_offset_b`, decompress
4. Offset within block: 2097152 - 2097152 = 0

*Query: 2MB + 17KB (2,114,560 bytes)*

1. Construct key: `[5][2114560]`
2. Predecessor result: `[5][2097152]` (since 2097152 ≤ 2114560 \< 4194304)
3. Read from `physical_offset_b`, decompress
4. Offset within block: 2114560 - 2097152 = 17408

== Block FST Build Requirements <sec:block-build-requirements>

- Keys MUST be inserted in strict lexicographic byte order
- For a single chunked file, this means blocks are inserted in logical offset order
- Across multiple files, files are ordered by record index

== Streaming Decompression <sec:streaming-decompression>

Compression frames are self-delimiting, so implementations can decompress blocks without knowing their compressed size in advance. The Block FST provides the starting offset; the decompressor determines where the frame ends.

== Block FST Constants <sec:block-fst-constants>

In addition to the shared constants in @sec:fst-constants:

#table(
  columns: (auto, auto, 1fr),
  align: (left, left, left),
  table.header([*Constant*], [*Value*], [*Description*]),
  [Key size], [16 bytes], [Fixed: 8 bytes record index + 8 bytes logical offset],
)
