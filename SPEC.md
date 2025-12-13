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

A Box archive consists of three sections laid out sequentially:

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
+------------------+  EOF
```

The Header is always 32 bytes and located at offset 0. The Header contains a pointer to the Trailer, which stores all metadata. File content data is stored between the Header and Trailer.

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
| `Vec<RecordIndex>` | `root`, `DirectoryRecord.entries` | VLQ (1-based index) |
| `Vec<Record>` | `BoxMetadata.records` | Record (see Section 8) |
| `Vec<String>` | `attr_keys` | String (VLQ length + UTF-8) |

**Example: Vec\<RecordIndex\> with 3 elements [1, 2, 5]:**

```
03              - VLQ: count = 3
01              - VLQ: index 1
02              - VLQ: index 2
05              - VLQ: index 5
```

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

The Trailer contains all archive metadata, serialized as a BoxMetadata structure.

### 7.1 BoxMetadata Structure

The `BoxMetadata` is serialized in the following order:

1. **`root`** (`Vec<RecordIndex>`): Indices of root-level entries
2. **`records`** (`Vec<Record>`): All record definitions
3. **`attr_keys`** (`Vec<String>`): Interned attribute key names
4. **`attrs`** (`AttrMap`): Archive-level attributes

### 7.2 Root

The `root` field contains `RecordIndex` values for all top-level entries (files and directories not contained within another directory).

### 7.3 Records

The `records` field contains all `Record` structures. Records are referenced by 1-based index; index `n` corresponds to `records[n-1]`.

### 7.4 Attribute Keys (attr_keys)

Attribute keys are interned to reduce storage size. The `attr_keys` field is a vector of strings where the index corresponds to the key's symbol value.

When serializing: keys are written in symbol order (0, 1, 2, ...).

When deserializing: strings are assigned symbols in the order read.

### 7.5 Archive Attributes (attrs)

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

Directory records describe directory entries.

**Structure:**

| Field | Type | Description |
|-------|------|-------------|
| `type` | `u8` | Record type: `0x01` |
| `name` | `String` | Directory name |
| `entries` | `Vec<RecordIndex>` | Child record indices |
| `attrs` | `AttrMap` | Directory attributes |

**Requirements:**

- `name` MUST be a valid directory name component.
- `entries` contains indices of records that are direct children of this directory.

**Binary Layout:**

```
01                          - type (directory)
[String: name]
[Vec<RecordIndex>: entries]
[AttrMap: attrs]
```

### 8.4 Link Record (Type 0x02)

Link records describe symbolic links.

**Structure:**

| Field | Type | Description |
|-------|------|-------------|
| `type` | `u8` | Record type: `0x02` |
| `name` | `String` | Link name |
| `target` | `BoxPath` | Target path |
| `attrs` | `AttrMap` | Link attributes |

**Requirements:**

- `name` MUST be a valid filename component.
- `target` MUST be a valid BoxPath (see Section 9).

**Binary Layout:**

```
02                          - type (symlink)
[String: name]
[String: target (BoxPath)]
[AttrMap: attrs]
```

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
| `created` | `i64` | Creation time (Unix timestamp, seconds) |
| `modified` | `i64` | Modification time (Unix timestamp, seconds) |
| `accessed` | `i64` | Access time (Unix timestamp, seconds) |

Timestamps are stored as signed 64-bit integers representing seconds since the Unix epoch (1970-01-01 00:00:00 UTC). The signed format supports dates before 1970. Sub-second precision is not supported. Implementations that require higher precision MAY define custom attributes.

### 12.2 Unix Permission Attributes

| Attribute | Format | Description |
|-----------|--------|-------------|
| `unix.mode` | `u32` | Unix file mode/permissions |
| `unix.uid` | `u32` | Unix user ID |
| `unix.gid` | `u32` | Unix group ID |

These attributes are OPTIONAL and typically only present for archives created on Unix-like systems.

### 12.3 Checksum Attributes

| Attribute | Format | Description |
|-----------|--------|-------------|
| `blake3` | 32 bytes raw | Blake3 hash of uncompressed content |

### 12.4 Attribute Value Encoding

Attribute values are stored as raw bytes (`Vec<u8>`). The interpretation depends on the attribute:

| Attribute Type | Encoding |
|----------------|----------|
| Timestamps (`created`, `modified`, `accessed`) | `i64` little-endian (8 bytes) |
| Unix permissions (`unix.mode`, `unix.uid`, `unix.gid`) | `u32` little-endian (4 bytes) |
| Checksums (`blake3`) | Raw hash bytes (32 bytes for Blake3) |
| Custom attributes | Application-defined (typically UTF-8 or raw bytes) |

Implementations MAY interpret UTF-8 attribute values as JSON for display purposes, but this is not required.

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
