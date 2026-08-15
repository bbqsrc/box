# Linux kernel integration

## Kernel archive parser

> [spec:box:syn:kernel-parser.root]
> A kernel-readable Box archive consists of a well-formed 32-byte version-1
> header followed at its declared trailer offset by an attribute-key table,
> archive attributes, an optional dictionary, a record vector, a length-prefixed
> path FST, and a following length-prefixed block FST. Truncated fields, invalid
> UTF-8 strings, unknown record kinds, and invalid required framing are malformed.

> [spec:box:syn:kernel-parser.root.header]
> A kernel header is well-formed when it is at least 32 bytes, begins with
> `\xFFBOX`, has version byte 1, and contains a nonzero little-endian trailer
> offset at `0x10`. Flag bits 0 and 1 determine external-symlink and escaped-path
> state, and the little-endian word at `0x08` is retained as alignment.

> [spec:box:syn:kernel-parser.root.trailer]
> A kernel trailer is well-formed when every FastVint-prefixed string and byte
> array lies within the supplied bytes, every attribute map's little-endian byte
> count encloses its declared entries, and dictionary and FST lengths remain in
> bounds. Attribute keys precede skipped archive attributes and dictionary data;
> parsed path-FST entries establish directory children, and the next FST payload
> supplies chunk block lookup data.

> [spec:box:syn:kernel-parser.root.records]
> A kernel record begins with a low-nibble kind and high-nibble compression.
> Kinds `0x01`, `0x02`, `0x03`, `0x0A`, and `0x0B` carry respectively directory,
> ordinary-file, internal-link, chunked-file, and external-link fields in the
> implemented parser order. Stored, Zstandard, and XZ compression identifiers
> are recognized; other compression nibbles remain `Unknown`, while other record
> kinds make the trailer malformed. Record indices remain one-based.

## Kernel VFS

> [spec:box:req:kernel-vfs.root]
> The Rust VFS entry layer MUST export `boxfs_rust_fill_super` as a C-ABI entry
> point that initializes one block-backed Box archive into Rust-owned metadata.
> It MUST return zero after successful initialization and the negative
> `KernelError` mapping when initialization fails. The retained metadata supplies
> the namespace, data, link, and xattr operations defined by the descendant rules.

> [spec:box:req:kernel-vfs.root.mount-lifecycle]
> Rust mount initialization MUST reject a missing backing device, read and
> validate the archive header, read an in-range trailer through device end, parse
> it, retain the resulting metadata allocation, and publish archive size, trailer
> offset, and root inode identity through the declared helper bindings. Metadata
> ownership MUST transfer only after parsing succeeds. `boxfs_rust_put_super`
> MUST recover and drop a present Rust metadata allocation, clear the stored
> pointer, and leave an already-empty pointer unchanged.

> [spec:box:req:kernel-vfs.root.namespace]
> Record identities MUST be packed into kernel inode numbers using the implemented
> archive/local composite mapping. Lookup MUST resolve a child through the merged
> path index with directory-record fallback, directory iteration MUST resume from
> `dir_context` position and stop when `dir_emit` fills its buffer, and inode
> attribute lookup MUST return the parsed mode, decompressed size, and block count
> for the selected record.

> [spec:box:req:kernel-vfs.root.data]
> Rust read paths MUST clamp requests at decompressed EOF. Stored ordinary and
> chunked data MUST be copied from acquired device blocks; Zstandard and XZ
> ordinary files MUST call the declared decompression helpers, while compressed
> chunked files MUST locate predecessor blocks through the block FST, decompress
> intersecting blocks, and reuse the byte-limited LRU block cache. Rust readahead
> MUST fill, zero, mark, and unlock each supplied folio through the same read
> paths.

> [spec:box:req:kernel-vfs.root.links-and-xattrs]
> Rust link resolution MUST resolve an internal target index within the same
> archive and return its path, while an external link MUST return its stored
> target. The exported readlink result MUST be NUL-terminated or return
> `ENAMETOOLONG` when the supplied buffer cannot hold it. Rust extended-attribute
> access MUST expose only record attributes whose keys begin with
> `linux.xattr.`, strip that prefix from returned names, support size-only probes,
> use NUL-separated lists, and distinguish missing data from a short buffer.

## Kernel ABI

> [spec:box:req:kernel-abi.root]
> The Rust ABI surface MUST expose its `boxfs_rust_*` functions as unmangled
> `extern "C"` entry points and MUST declare the opaque C-compatible types and
> external helper signatures used by those functions. A buffer pointer with an
> explicit length MUST be accessed only within that length, a NUL-terminated name
> pointer MUST be consumed only during its entry-point call, and no borrowed
> foreign pointer MAY be retained after the call. This rule covers the Rust ABI
> declarations and exports; it does not assert foreign header or helper
> implementation agreement.

> [spec:box:req:kernel-abi.root.ownership-and-errors]
> Successful mount initialization MUST transfer the parsed metadata allocation
> through the metadata setter as a raw pointer; `boxfs_rust_put_super` MUST recover
> and drop that allocation at most once and clear the stored pointer. Rust
> operation failures MUST retain the negative sign of the `KernelError` errno
> mapping, including signed-size read and xattr results, while lookup's not-found
> convention MUST remain inode zero.

> [spec:box:req:kernel-abi.root.helpers-and-buffers]
> Rust helper declarations MUST cover block-buffer acquisition and release,
> superblock metadata accessors, directory emission, allocation and free,
> Zstandard and XZ decompression, and folio operations with the argument and
> return types consumed by the Rust implementation. `read_block` MUST return
> `None` for a null handle and otherwise pair the borrowed bytes with the handle
> required by `release_block`. Rust data-read paths MUST release acquired buffer
> heads after copying, free temporary decompression buffers after codec success
> and failure, and pair mapped folios with unmapping and unlocking.
