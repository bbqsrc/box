# Box Archive Format Specification

**Version:** 0.1.0  
**Status:** Draft

## Table of Contents

1. [Introduction](#1-introduction)
2. [Terminology](#2-terminology)
3. [File Structure Overview](#3-file-structure-overview)
4. [Data Types](#4-data-types)
5. [Header](#5-header)
6. [Data Section](#6-data-section)
7. [Trailer (Metadata)](#7-trailer-metadata)
8. [Record Types](#8-record-types)
9. [Path Encoding](#9-path-encoding)
10. [Compression](#10-compression)
11. [Checksums](#11-checksums)
12. [Standard Attributes](#12-standard-attributes)
13. [Implementation Guidance](#13-implementation-guidance)
14. [FST Index](#14-fst-index)

---

## 1. Introduction

The Box format is an open archive format designed for storing files with support for compression, checksums, extended attributes, and memory-mapped access. It provides platform-independent path encoding and efficient metadata storage through string interning.

### 1.1 Design Goals

- **Platform Independence:** Paths are encoded in a platform-neutral format that can be safely represented on all major operating systems.
- **Compression:** Multiple compression algorithms are supported, selectable per-file.
- **Integrity:** Optional Blake3 checksums for content verification.
- **Efficiency:** String interning for attribute keys reduces metadata size. Memory mapping enables zero-copy file access.
- **Extensibility:** The attribute system allows arbitrary metadata without format changes.

### 1.2 File Extension

Box archives SHOULD use the `.box` file extension.

---

## 2. Terminology

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be interpreted as described in [RFC 2119](https://www.rfc-editor.org/rfc/rfc2119).

**Archive:** A Box format file containing zero or more entries.

**Entry:** A file, directory, or symbolic link stored in the archive.

**Record:** The metadata structure describing an entry.

**Trailer:** The metadata section at the end of the archive containing all records and attributes.

---

## 3. File Structure Overview

A Box archive consists of four sections laid out sequentially:

```
+------------------+  Offset 0x00
|     Header       |  32 bytes, fixed size
+------------------+  Offset 0x20
|                  |
|   Data Section   |  Variable size
|                  |
+------------------+  Offset = header.trailer
|                  |
|     Trailer      |  Variable size (BoxMetadata)
|                  |
+------------------+
|   FST Index      |  Variable size
+------------------+  EOF
```

The Header is always 32 bytes and located at offset 0. The Header contains a pointer to the Trailer, which stores record metadata. File content data is stored between the Header and Trailer. The FST Index follows the Trailer and provides path-to-record mapping (see Section 14).

All multi-byte integers in the Box format are stored in **little-endian** byte order unless otherwise specified.

---

## 4. Data Types

### 4.1 Fixed-Size Integers

| Type | Size | Description |
|------|------|-------------|
| `u8` | 1 byte | Unsigned 8-bit integer |
| `u32` | 4 bytes | Unsigned 32-bit integer, little-endian |
| `u64` | 8 bytes | Unsigned 64-bit integer, little-endian |
| `i64` | 8 bytes | Signed 64-bit integer, little-endian |

### 4.2 FastVLQ Encoding

Variable-size unsigned integers are encoded using FastVLQ, a prefix-based variable-length encoding where the number of leading zeros in the first byte determines the total byte count.

**Length Determination:**

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

**Offset-Based Encoding:**

FastVLQ uses offset-based encoding for denser packing. Each length tier has a base offset:

| Bytes | Value Range | Offset |
|-------|-------------|--------|
| 1 | 0 – 127 | 0 |
| 2 | 128 – 16,511 | 128 |
| 3 | 16,512 – 2,113,663 | 16,512 |
| 4 | 2,113,664 – 270,549,119 | 2,113,664 |
| 5+ | ... | (continues exponentially) |
| 9 | (full u64 range) | 72,624,976,668,147,840 |

**Data Byte Order:**

After the prefix byte, remaining data bytes are stored in **big-endian** order.

**Key Properties:**

- Length is known from the first byte alone (no continuation scanning required)
- Maximum 9 bytes for `u64` values
- This encoding does NOT enforce minimal representations; the same value MAY have multiple valid encodings
- Decoders MUST accept all valid encodings of a value, including non-minimal representations

### 4.3 String

Strings are encoded as:

```
[VLQ: byte length]
[UTF-8 bytes]
```

All strings MUST be valid UTF-8. Implementations MUST reject invalid UTF-8 sequences.

### 4.4 Vec\<T\>

Vectors (arrays) are encoded as:

```
[VLQ: element count]
[T encoding] * count
```

The following vector types are used in the format:

| Type | Usage | Element Encoding |
|------|-------|------------------|
| `Vec<Record>` | `BoxMetadata.records` | Record (see Section 8) |
| `Vec<AttrKey>` | `attr_keys` | Type tag + String (see Section 7.3) |

### 4.5 Vec\<u8\> (Byte Array)

Byte arrays are encoded identically to strings:

```
[VLQ: byte length]
[raw bytes]
```

### 4.6 RecordIndex

A RecordIndex is a 1-based index into the records array, encoded as VLQ:

```
[VLQ: index value]
```

Index values MUST be non-zero. An index value of `n` refers to `records[n-1]`.

### 4.7 AttrMap

Attribute maps are encoded as:

```
[u64: total byte count of this section, including this field]
[VLQ: entry count]
For each entry:
    [VLQ: key index into attr_keys]
    [Vec<u8>: value]
```

The leading byte count allows implementations to skip the entire attribute map without parsing individual entries.

---

## 5. Header

The Header is located at byte offset 0 and is exactly 32 bytes.

### 5.1 Header Structure

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0x00 | 4 | `magic` | Magic bytes: `0xFF 0x42 0x4F 0x58` (`\xFFBOX`) |
| 0x04 | 1 | `version` | Format version number |
| 0x05 | 3 | `reserved1` | Reserved bytes |
| 0x08 | 4 | `alignment` | Data alignment boundary (`u32`) |
| 0x0C | 4 | `reserved2` | Reserved bytes |
| 0x10 | 8 | `trailer` | Byte offset to trailer (`u64`) |
| 0x18 | 8 | `reserved3` | Reserved bytes |

**Total Size:** 32 bytes

### 5.2 Magic Bytes

The magic bytes MUST be exactly `0xFF 0x42 0x4F 0x58` (the byte `0xFF` followed by ASCII `BOX`).

The leading `0xFF` byte serves two purposes:
1. Distinguishes Box archives from plain text files
2. Causes immediate failure in UTF-8 parsers (0xFF is an invalid UTF-8 lead byte)

Implementations MUST reject files where the first 4 bytes do not match the magic bytes.

### 5.3 Version

The version field indicates the format version. This specification defines version **1**.

Implementations SHOULD reject archives with version numbers they do not support.

Implementations MAY support reading version 0 archives for backward compatibility. Version 0 archives include `root` in BoxMetadata and `entries` in DirectoryRecord; version 1 archives do not.

### 5.4 Reserved Fields

The `reserved1` (3 bytes), `reserved2` (4 bytes), and `reserved3` (8 bytes) fields are reserved for future use.

Writers MUST set these fields to zero.

Readers SHOULD ignore these fields but MAY reject archives where reserved fields are non-zero.

### 5.5 Alignment

The alignment field specifies the byte boundary for file data alignment.

- Value `0`: No alignment; file data is written at the next available offset.
- Value `n > 0`: File data offsets MUST be multiples of `n`.

When alignment is non-zero, padding bytes MAY be inserted before file data. Padding bytes MUST be zero.

### 5.6 Trailer Offset

The trailer field contains the byte offset to the start of the Trailer (BoxMetadata) section. The Trailer begins at byte offset `n`.

A valid, non-empty archive MUST have a non-zero trailer offset.

### 5.7 Binary Layout

```
Offset  00 01 02 03 04 05 06 07 08 09 0A 0B 0C 0D 0E 0F
        FF 42 4F 58 VV RR RR RR AA AA AA AA RR RR RR RR
        └─ magic ─┘ │  └ res1 ┘ └ alignmt ┘ └─ res2 ──┘
                 version

Offset  10 11 12 13 14 15 16 17 18 19 1A 1B 1C 1D 1E 1F
        TT TT TT TT TT TT TT TT RR RR RR RR RR RR RR RR
        └── trailer offset ───┘ └───── reserved3 ─────┘
```

---

## 6. Data Section

The Data Section begins immediately after the Header (offset 0x20 / 32) and extends to the Trailer.

### 6.1 File Data Storage

File content data is stored sequentially in the Data Section. Each file's compressed (or stored) data is written contiguously.

### 6.2 Data Alignment

If `header.alignment` is non-zero, each file's data MUST begin at an offset that is a multiple of the alignment value.

**Alignment Calculation:**

```
if alignment == 0:
    write_offset = current_position
else:
    remainder = current_position % alignment
    if remainder == 0:
        write_offset = current_position
    else:
        write_offset = current_position + (alignment - remainder)
```

Padding bytes between the previous data and the aligned offset MUST be zero.

### 6.3 Data Offset Requirements

File record data offsets MUST be non-zero. A zero data offset indicates a corrupt or invalid record.

---

## 7. Trailer (Metadata)

The Trailer contains record metadata, serialized as a BoxMetadata structure. Path-to-record mapping is stored in the FST Index (Section 14).

### 7.1 BoxMetadata Structure

The `BoxMetadata` is serialized in the following order:

1. **`records`** (`Vec<Record>`): All record definitions
2. **`attr_keys`** (`Vec<String>`): Interned attribute key names
3. **`attrs`** (`AttrMap`): Archive-level attributes

### 7.2 Records

The `records` field contains all `Record` structures. Records are referenced by 1-based index; index `n` corresponds to `records[n-1]`.

### 7.3 Attribute Keys (attr_keys)

Attribute keys are interned to reduce storage size. Each key stores both its name and its value type. The `attr_keys` field is encoded as:

```
[VLQ: count]
For each key:
    [u8: type_tag]
    [VLQ: string length]
    [UTF-8 bytes]
```

**Type Tag Values:**

| Value | Type | Description |
|-------|------|-------------|
| 0 | Bytes | Raw bytes, no interpretation |
| 1 | String | UTF-8 string |
| 2 | Json | UTF-8 JSON |
| 3 | U8 | Single byte (u8) |
| 4 | Vi32 | Zigzag-encoded i32 |
| 5 | Vu32 | VLQ-encoded u32 |
| 6 | Vi64 | Zigzag-encoded i64 |
| 7 | Vu64 | VLQ-encoded u64 |
| 8 | U128 | Fixed 16 bytes (128 bits) |
| 9 | U256 | Fixed 32 bytes (256 bits) |
| 10 | DateTime | Minutes since Box epoch (2026-01-01 00:00:00 UTC), zigzag-encoded i64 |
| 11-255 | Reserved | Reserved for future use |

When serializing: keys are written in symbol order (0, 1, 2, ...).

When deserializing: strings are assigned symbols in the order read. The type tag determines how the attribute value is interpreted.

### 7.4 Archive Attributes (attrs)

The `attrs` field contains attributes that apply to the entire archive, using the same `AttrMap` format as record attributes.

---

## 8. Record Types

Records describe entries in the archive. Each record begins with a 1-byte type identifier.

### 8.1 Record Type Identifiers

| ID | Type |
|----|------|
| 0x00 | File |
| 0x01 | Directory |
| 0x02 | Symbolic Link |

Implementations MUST reject records with unknown type identifiers.

### 8.2 File Record (Type 0x00)

File records describe regular file entries.

**Structure:**

| Field | Type | Description |
|-------|------|-------------|
| `type` | `u8` | Record type: `0x00` |
| `compression` | `u8` | Compression algorithm ID |
| `length` | `u64` | Compressed data size in bytes |
| `decompressed_length` | `u64` | Original file size (advisory) |
| `data` | `u64` | Byte offset to file data |
| `name` | `String` | Filename (not full path) |
| `attrs` | `AttrMap` | File attributes |

**Requirements:**

- `data` MUST be non-zero.
- `name` MUST be a valid filename component (no path separators).
- `decompressed_length` is advisory; implementations MUST NOT rely on it for buffer allocation without validation.

**Binary Layout:**

```
00                          - type (file)
CC                          - compression ID
LL LL LL LL LL LL LL LL     - length (u64)
DD DD DD DD DD DD DD DD     - decompressed_length (u64)
OO OO OO OO OO OO OO OO     - data offset (u64)
[String: name]
[AttrMap: attrs]
```

### 8.3 Directory Record (Type 0x01)

Directory records describe directory entries. Directory children are located via FST prefix queries using the directory's path (see Section 14).

**Structure:**

| Field | Type | Description |
|-------|------|-------------|
| `type` | `u8` | Record type: `0x01` |
| `name` | `String` | Directory name |
| `attrs` | `AttrMap` | Directory attributes |

**Requirements:**

- `name` MUST be a valid directory name component.

**Binary Layout:**

```
01                          - type (directory)
[String: name]
[AttrMap: attrs]
```

### 8.4 Link Record (Type 0x02)

Link records describe symbolic links.

**Structure:**

| Field | Type | Description |
|-------|------|-------------|
| `type` | `u8` | Record type: `0x02` |
| `name` | `String` | Link name |
| `target` | `RecordIndex` | Target record index |
| `attrs` | `AttrMap` | Link attributes |

**Requirements:**

- `name` MUST be a valid filename component.
- `target` MUST be a valid RecordIndex pointing to an existing record in the archive.

**Binary Layout:**

```
02                          - type (symlink)
[String: name]
[VLQ: target (RecordIndex)]
[AttrMap: attrs]
```

**Extraction Behavior:**

When extracting a link record, implementations MUST:

1. Resolve the target RecordIndex to obtain the target's full path
2. Compute the relative path from the link's parent directory to the target
3. Create a symbolic link with the computed relative path

This ensures that relative symlinks work correctly regardless of where the archive is extracted.

---

## 9. Path Encoding

Paths in Box archives use a platform-independent encoding called BoxPath.

### 9.1 Path Separator

BoxPath uses **U+001F UNIT SEPARATOR** (`\x1F`) as the path component delimiter.

This character:
- Is not used as a path separator on any major platform
- Is a control character unlikely to appear in filenames
- Avoids ambiguity with `/` (Unix) and `\` (Windows)

### 9.2 Parsing Platform Paths

When converting platform paths to BoxPath:

1. **Absolute Paths:** Leading path separators MUST be stripped (`/` on Unix-like systems, `\` on Windows).
2. **Current Directory:** `.` components MUST be removed.
3. **Parent Directory:** `..` components MUST be resolved by removing the preceding component.
4. **Platform-Specific Restrictions:**
   - On Unix-like systems: Backslash (`\`) MUST be rejected.
   - On Windows: Forward slash (`/`) MUST be rejected.

### 9.3 BoxPath Storage Rules

Path components stored in a BoxPath MUST conform to:

1. **Unicode Normalization:** All components MUST be in NFC (Normalization Form Composed).
2. **No Control Characters:** Control characters (except space U+0020) MUST be rejected.
3. **No Separator Characters:** Characters in Unicode General Category "Separator" (Zs, Zl, Zp) MUST be rejected, except U+0020 SPACE which is allowed.
4. **Trimmed Whitespace:** Leading and trailing whitespace MUST be trimmed from each component.
5. **Non-Empty:** Components MUST NOT be empty after trimming.

### 9.4 Path Validation Errors

Implementations MUST reject paths that:
- Are empty after parsing
- Contain prohibited characters (control characters, separators)
- Are not in NFC normalized form

### 9.5 Examples

| Input | BoxPath Internal Representation |
|-------|--------------------------------|
| `test/string.txt` | `test\x1Fstring.txt` |
| `/something/../else/./foo.txt` | `else\x1Ffoo.txt` |
| `./self` | `self` |
| `/` | Error (empty path) |
| `\0` | Error (control character) |

---

## 10. Compression

File data MAY be compressed. The compression algorithm is specified per-file in the FileRecord.

### 10.1 Compression Algorithm Identifiers

| ID | Name | Reference |
|----|------|-----------|
| 0x00 | Stored | No compression (raw data) |
| 0x10 | DEFLATE | RFC 1951 |
| 0x20 | Zstandard | RFC 8878 |
| 0x30 | XZ | LZMA2 (XZ format) |
| 0x40 | Snappy | Google Snappy framing format |
| 0x50 | Brotli | RFC 7932 |

Implementations MUST support reading `Stored` (0x00) compression.

Implementations SHOULD support at least one compression algorithm beyond Stored.

Unknown compression IDs MUST cause an error when attempting to read the file data.

### 10.2 Minimum Compressible Size

Files smaller than **96 bytes** SHOULD be stored uncompressed, as compression overhead typically exceeds any space savings for small files.

### 10.3 Compression Stream Format

Each compression algorithm uses its standard stream format:

- **DEFLATE:** Raw DEFLATE stream (no zlib/gzip wrapper)
- **Zstandard:** Standard Zstandard frame format
- **XZ:** XZ container format
- **Snappy:** Snappy framing format
- **Brotli:** Brotli stream format

### 10.4 Compression Parameters

Compression algorithms MAY support implementation-specific parameters (e.g., compression level, window size). These parameters:

- Are NOT stored in the archive
- Do not affect decompression (only the algorithm ID matters)
- Are implementation-specific and not part of this specification

---

## 11. Checksums

Box archives support content checksums for integrity verification.

### 11.1 Supported Algorithms

| Name | Attribute | Output Size |
|------|-----------|-------------|
| Blake3 | `blake3` | 32 bytes |

### 11.2 Checksum Storage

Checksums are stored as file record attributes. The attribute name indicates the algorithm, and the value contains the raw hash bytes.

### 11.3 Checksum Computation

Checksums MUST be computed on the **uncompressed** file content.

### 11.4 Verification

When verifying checksums:

1. Decompress the file data
2. Compute the hash of the decompressed data
3. Compare against the stored checksum attribute

Mismatches indicate data corruption or modification.

---

## 12. Standard Attributes

This section defines well-known attributes. Implementations MAY define additional attributes.

### 12.1 Timestamp Attributes

| Attribute | Format | Description |
|-----------|--------|-------------|
| `created` | Vi64 | Creation time (minutes since Box epoch) |
| `modified` | Vi64 | Modification time (minutes since Box epoch) |
| `accessed` | Vi64 | Access time (minutes since Box epoch) |

**Box Epoch:** 2026-01-01 00:00:00 UTC (Unix timestamp 1767225600).

Timestamps are OPTIONAL and stored as signed variable-length integers (Vi64, zigzag encoded) representing minutes since the Box epoch. The signed format supports dates before 2026. Minute precision is sufficient for typical archive use cases.

When timestamps are absent, implementations SHOULD use the archive file's own creation/modification time as a fallback.

Writers MAY omit timestamps to reduce archive size. The `box` CLI omits timestamps by default; use `--timestamps` to include them, or `--archive`/`-A` to include all metadata.

#### Optional Precision Extensions

For applications requiring finer precision, the following extension attributes are supported:

| Attribute | Format | Description |
|-----------|--------|-------------|
| `created.seconds` | u8 | Seconds component (0-59) |
| `modified.seconds` | u8 | Seconds component (0-59) |
| `accessed.seconds` | u8 | Seconds component (0-59) |
| `created.nanoseconds` | Vu64 | Sub-minute precision in nanoseconds (0-59,999,999,999) |
| `modified.nanoseconds` | Vu64 | Sub-minute precision in nanoseconds (0-59,999,999,999) |
| `accessed.nanoseconds` | Vu64 | Sub-minute precision in nanoseconds (0-59,999,999,999) |

**Precedence:** `.nanoseconds` takes precedence over `.seconds`. If `.nanoseconds` is present, `.seconds` MUST be ignored. The nanoseconds value encodes the full sub-minute precision including seconds (e.g., 30.5 seconds = 30,500,000,000 nanoseconds).

Implementations are NOT REQUIRED to support these extensions. Writers SHOULD only include them when the application explicitly requires sub-minute precision.

### 12.2 Unix Permission Attributes

| Attribute | Format | Description | Default |
|-----------|--------|-------------|---------|
| `unix.mode` | Vu32 | Unix file mode/permissions | 0o100644 (files), 0o40755 (dirs) |
| `unix.uid` | Vu32 | Unix user ID | Archive attr, then current user |
| `unix.gid` | Vu32 | Unix group ID | Archive attr, then current user |

These attributes are OPTIONAL. When absent, implementations SHOULD apply the defaults listed above.

**Fallback Chain for `unix.uid` and `unix.gid`:**

1. Check the record's attribute
2. Check the archive-level attribute (in `BoxMetadata.attrs`)
3. Use the current user's UID/GID

Writers SHOULD store `unix.uid` and `unix.gid` as archive-level attributes and only store per-record values when they differ from the archive default. This significantly reduces trailer size for archives where all files have the same owner.

Writers MAY omit `unix.uid` and `unix.gid` entirely to reduce archive size. The `box` CLI omits ownership by default; use `--ownership` to include them, or `--archive`/`-A` to include all metadata.

Writers SHOULD omit `unix.mode` when it matches the default for the record type (0o100644 for files, 0o40755 for directories).

### 12.3 Checksum Attributes

| Attribute | Format | Description |
|-----------|--------|-------------|
| `blake3` | 32 bytes raw | Blake3 hash of uncompressed content |

### 12.4 Attribute Value Encoding

Attribute values are stored as raw bytes (`Vec<u8>`). The type tag stored with the attribute key (see Section 7.3) determines how to interpret the value.

**Standard Attribute Types:**

| Attribute | Type Tag | Encoding |
|-----------|----------|----------|
| `created`, `modified`, `accessed` | DateTime (10) | Zigzag-encoded i64 (minutes since Box epoch) |
| `created.seconds`, `modified.seconds`, `accessed.seconds` | U8 (3) | Single byte (0-59) |
| `created.nanoseconds`, `modified.nanoseconds`, `accessed.nanoseconds` | Vu64 (7) | VLQ-encoded u64 |
| `unix.mode`, `unix.uid`, `unix.gid` | Vu32 (5) | VLQ-encoded u32 |
| `blake3` | U256 (9) | Fixed 32 bytes |

Applications MAY define custom attributes with any type tag.

---

## 13. Implementation Guidance

This section provides non-normative guidance for implementers.

### 13.1 Memory Mapping

Box archives are designed to support memory-mapped access:

- The fixed-size header allows quick validation
- The trailer offset enables direct seeking to metadata
- File data offsets enable direct access to compressed content
- Alignment (when non-zero) enables page-aligned memory mapping

Implementations SHOULD support memory-mapped reading for large archives.

### 13.2 Alignment Recommendations

Alignment serves different purposes depending on the value:

**64-bit Offset Alignment (value: 8)**

- Ensures file data offsets are 64-bit aligned
- Required for mmap to work correctly on some platforms (32-bit ARM will segfault on unaligned 64-bit access)
- Minimal size overhead

**Page-Size Alignment (value: 4096, 8192, 65536)**

- Enables page-aligned memory mapping for efficient direct access
- Useful for large archives where zero-copy access is important
- Higher size overhead due to padding

**No Alignment (value: 0)**

- Minimizes archive size
- May require copying data for mmap on some platforms
- Suitable when size is the priority over memory-mapped access

### 13.3 Error Handling

Implementations MUST validate:

1. Magic bytes match exactly
2. Version number is supported
3. Trailer offset is within file bounds
4. Record indices are within bounds
5. Data offsets are within bounds and non-zero
6. Strings are valid UTF-8
7. Paths meet normalization requirements
8. Checksums match (when verification is enabled)

When validation fails, implementations SHOULD:

- Report the specific error (invalid magic, unsupported version, etc.)
- Fail fast rather than attempt partial recovery
- NOT expose potentially corrupted data to callers

Archives with structural errors (invalid offsets, malformed records) SHOULD be rejected entirely rather than partially processed.

### 13.4 Parallel Operations

The format supports parallel processing:

- **Reading:** Multiple files can be read concurrently (independent data offsets)
- **Writing:** Files can be compressed in parallel, then written sequentially
- **Verification:** Checksums can be verified in parallel

### 13.5 Large File Handling

For files that exceed available memory:

- Use streaming compression/decompression
- Use temporary files for parallel compression when memory is constrained
- Consider checksum computation during streaming rather than as a separate pass

### 13.6 Security Considerations

Implementations MUST:

- Validate all offsets are within file bounds before seeking
- Limit memory allocation based on reported sizes (decompressed_length is advisory)
- Reject paths containing `..` after normalization
- Handle symbolic links carefully to prevent path traversal attacks during extraction

---

## 14. FST Index

The FST (Finite State Transducer) Index provides the path-to-record mapping for the archive. It is located immediately after the Trailer section and extends to the end of the archive file.

### 14.1 Overview

The FST provides:

- **Fast path lookups:** O(m) time complexity where m = path length in bytes
- **Prefix iteration:** Efficiently enumerate all paths with a given prefix
- **Directory traversal:** Find directory children via prefix queries

The FST maps full paths (using `\x1F` unit separator between components, as described in Section 9) to `RecordIndex` values.

### 14.2 FST Layout

```
+------------------+  Offset = trailer_end
|     Header       |  24 bytes
+------------------+
|    Node Index    |  node_count × 8 bytes
+------------------+
|   Hot Section    |  Variable size (compact node headers)
+------------------+
|   Cold Section   |  Variable size (edge data)
+------------------+  EOF
```

### 14.3 Header

The FST Header is 24 bytes and located at the start of the FST section.

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0x00 | 4 | `magic` | Magic bytes: `BFST` (0x42 0x46 0x53 0x54) |
| 0x04 | 1 | `version` | Format version (currently 1) |
| 0x05 | 1 | `flags` | Reserved flags (must be 0) |
| 0x06 | 2 | `reserved` | Reserved bytes (must be 0) |
| 0x08 | 4 | `node_count` | Total number of nodes (`u32`) |
| 0x0C | 8 | `entry_count` | Number of paths indexed (`u64`) |
| 0x14 | 4 | `cold_offset` | Cold section start offset (`u32`) |

The Hot Section starts at offset `24 + node_count × 8` (immediately after the Node Index). The root node is always node 0.

Implementations MUST reject FST data where magic bytes do not match or version is unsupported.

### 14.4 Node Index

The Node Index is an array of `node_count` entries, with each entry being 8 bytes.

**Entry Format:**

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0x00 | 4 | `hot_offset` | Offset within Hot Section (`u32`) |
| 0x04 | 4 | `cold_offset` | Offset within Cold Section (`u32`) |

Offsets are relative to the start of their respective sections. Node IDs are indices into this array (node 0 = first entry, node 1 = second entry, etc.).

### 14.5 Hot Section

The Hot Section contains compact node headers optimized for cache efficiency. Each node's hot data has the following structure:

```
[flags: u8]
[edge_count: VLQ]
[lookup_data: variable]
[offsets: edge_count × u16]
```

**Flags Byte:**

| Bit | Name | Description |
|-----|------|-------------|
| 0 | `IS_FINAL` | Node is an accepting state (has an output value) |
| 1 | `INDEXED` | Node uses 256-byte lookup table (17+ edges) |
| 2-7 | Reserved | Must be 0 |

**Lookup Data:**

The lookup data format depends on the `INDEXED` flag:

- **If INDEXED (≥17 edges):** 256-byte lookup table where `table[byte]` returns the edge index for that byte value, or `0xFF` if no edge exists for that byte.

- **If not INDEXED (<17 edges):** Array of `edge_count` bytes containing the first byte of each edge label, sorted in ascending order. Used for binary search or SIMD-accelerated lookups.

**Offsets Array:**

Array of `edge_count` entries, each a `u16`. Each offset points to the corresponding edge's data within this node's cold section data block.

### 14.6 Cold Section

The Cold Section contains edge data and final output values. Each node's cold data has the following structure:

```
For each edge (0 to edge_count-1):
    [label_len: VLQ]
    [label: label_len bytes]
    [output: VLQ]
    [target_node: u32]

If IS_FINAL flag is set:
    [final_output: VLQ]
```

**Edge Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `label_len` | VLQ | Length of edge label in bytes |
| `label` | bytes | Edge label (arbitrary byte sequence) |
| `output` | VLQ | Output value to accumulate (`u64`) |
| `target_node` | `u32` | Target node ID |

**Final Output:**

If the `IS_FINAL` flag is set, the final output value is stored at the end of the node's cold data block. This value is added to the accumulated output when the traversal terminates at this node.

### 14.7 Path Encoding in FST

Paths stored in the FST use the same encoding as BoxPath (Section 9):

- Components are separated by `U+001F` UNIT SEPARATOR (`\x1F`)
- Paths are stored without leading separators
- Example: `foo/bar/baz.txt` → `foo\x1Fbar\x1Fbaz.txt`

**Build Requirements:**

- Keys MUST be inserted in strict lexicographic byte order
- Duplicate keys are not permitted
- Values are `u64` record indices (as returned by `RecordIndex.get()`)

### 14.8 Output Value Accumulation

The FST stores output values along edges and at final nodes. The final lookup value is computed by:

1. Starting with accumulated value = 0
2. For each edge traversed: `accumulated = accumulated.wrapping_add(edge.output)`
3. If the final node has `IS_FINAL` set: `accumulated = accumulated.wrapping_add(final_output)`

The result is the `RecordIndex` value for the path.

### 14.9 Implementation Notes

**Adaptive Node Format:**

The FST uses an adaptive node format inspired by Adaptive Radix Trees (ART):

- Nodes with fewer than 17 edges use compact format (sorted first-bytes array)
- Nodes with 17 or more edges use indexed format (256-byte lookup table)

The threshold of 17 balances memory usage against lookup performance.

**Hot/Cold Separation:**

Data is separated into "hot" (frequently accessed during traversal) and "cold" (accessed only when needed) sections. This improves cache utilization during lookups.

**SIMD Optimization:**

Implementations MAY use SIMD instructions for compact node lookups:
- x86_64: SSE2 `_mm_cmpeq_epi8` for 16-byte parallel comparison
- aarch64: NEON `vceqq_u8` for similar parallel comparison

### 14.10 Constants

| Constant | Value | Description |
|----------|-------|-------------|
| Magic | `BFST` | FST section identifier |
| Version | 1 | Current format version |
| Header size | 24 bytes | Fixed header size |
| Index entry size | 8 bytes | Per-node index entry |
| Indexed threshold | 17 | Minimum edges for indexed format |
