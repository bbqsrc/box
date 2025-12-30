# boxfs: Linux Kernel Filesystem for Box Archives

This document describes the plan for implementing a Linux kernel filesystem driver for the Box archive format, similar to how SquashFS works for `.squashfs` files.

## Overview

**Goal:** Enable direct mounting of `.box` files without extraction, providing efficient read-only access to archived content directly from the kernel.

**Status:** Planning / Not yet implemented

## Why a Kernel Filesystem?

The Box format already has userspace filesystem implementations:
- `fusebox` - FUSE driver for Linux/macOS
- `projfsbox` - Windows ProjFS driver
- `fskitbox` - macOS FSKit driver (macOS 15.4+)

A kernel filesystem provides advantages:
- **Performance:** No userspace context switches for file operations
- **Simplicity:** No FUSE daemon process to manage
- **Integration:** Works with all tools expecting real filesystems
- **Boot:** Can be used early in boot before userspace is available

## Rust for Linux

As of December 2025, Rust is an official kernel language:
- Promoted from experimental at the 2025 Kernel Maintainer Summit in Tokyo
- ~25k lines of Rust in kernel currently (vs 34M lines of C)

### VFS Abstractions Status

**The VFS `FileSystem` trait shown below is NOT in mainline Linux.** It exists only as RFC patches.

| Component | Status | Location |
|-----------|--------|----------|
| Basic file abstractions (`File`, `LocalFile`, `Kiocb`) | **In mainline** | `rust/kernel/fs.rs` |
| VFS `FileSystem` trait (`super_params`, `init_root`, etc.) | **RFC only** | [Patches by Wedson Almeida Filho](https://lore.kernel.org/all/ZbCRF%2FOkpqESeQpC@wedsonaf-dev/T/) |

**To use the full VFS abstractions today, you must:**
1. Build against a forked kernel with RFC patches applied (e.g., [PuzzleFS's puzzlefs_dependencies branch](https://github.com/ariel-miculas/linux/tree/puzzlefs_rfc_v2))
2. Or wait for mainline integration (~12-18 months estimated)

Reference implementations using RFC patches:
- PuzzleFS (Cisco) - container filesystem
- Tarfs - tar-based filesystem
- Rust EXT2 driver (WIP by Microsoft)

### Resources

- [Rust for Linux](https://rust-for-linux.com/)
- [Kernel Rust Quick Start](https://docs.kernel.org/rust/quick-start.html)
- [Out-of-tree Module Template](https://github.com/Rust-for-Linux/rust-out-of-tree-module)
- [VFS RFC Patches](https://lore.kernel.org/all/ZbCRF%2FOkpqESeQpC@wedsonaf-dev/T/)
- [PuzzleFS](https://github.com/project-machine/puzzlefs)
- [LWN: Rust for filesystems](https://lwn.net/Articles/978738/)

## Architecture

### Project Structure

```
box/
├── Cargo.toml          # Workspace config (existing)
├── box-core/           # NEW: no_std core (format parsing, no I/O)
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs
│       ├── header.rs   # Header parsing
│       ├── record.rs   # Record types (File, Directory, Link)
│       ├── metadata.rs # Trailer parsing
│       └── fst.rs      # FST index
├── box-format/         # Existing: userspace with async I/O
│   └── ...             # Will depend on box-core
├── box-fst/            # Existing: FST implementation
├── box-kmod/           # NEW: kernel module
│   ├── Cargo.toml
│   ├── Kbuild
│   ├── Makefile
│   └── src/
│       ├── lib.rs      # Module entry point
│       ├── fs.rs       # FileSystem trait impl
│       ├── inode.rs    # INode operations
│       └── decompress.rs
└── ...
```

### Code Sharing Strategy

Following PuzzleFS's approach of "sharing the same code between user space and kernel space":

1. **`box-core`** - Pure format parsing in `#![no_std]`
   - No I/O operations
   - No heap allocation (or optional `alloc` crate)
   - Trait-based abstractions for reading bytes

2. **`box-format`** - Userspace implementation
   - Depends on `box-core`
   - Adds async I/O with Tokio
   - Full compression support (all 6 algorithms)

3. **`box-kmod`** - Kernel implementation
   - Depends on `box-core`
   - Uses kernel I/O APIs
   - Kernel-native compression only

## VFS Trait Implementation

> **Note:** This API is from RFC patches, not mainline Linux. See [VFS Abstractions Status](#vfs-abstractions-status) above.

The Rust VFS abstractions require implementing the `FileSystem` trait:

```rust
use kernel::prelude::*;
use kernel::fs::{self, FileSystem, INode, SuperBlock};

module_fs! {
    type: BoxFs,
    name: "boxfs",
    license: "GPL",
}

struct BoxFs;

impl FileSystem for BoxFs {
    // Mount: configure superblock parameters
    fn super_params(sb: &NewSuperBlock<Self>) -> Result<SuperParams<Self::Data>>;

    // Mount: initialize root inode
    fn init_root(sb: &SuperBlock<Self>) -> Result<ARef<INode<Self>>>;

    // List directory contents
    fn read_dir(inode: &INode<Self>, emitter: &mut DirEmitter) -> Result;

    // Find child by name
    fn lookup(parent: &INode<Self>, name: &[u8]) -> Result<ARef<INode<Self>>>;

    // Read file pages
    fn read_folio(inode: &INode<Self>, folio: LockedFolio<'_>) -> Result;

    // Optional: filesystem statistics
    fn statfs(sb: &SuperBlock<Self>) -> Result<Stat>;
}
```

### Box Operations to VFS Mapping

| Box Operation | VFS Method | Description |
|---------------|------------|-------------|
| Parse header | `super_params()` | Read 32-byte header, validate magic |
| Load trailer | `super_params()` | Memory-map metadata section |
| Get root | `init_root()` | Create inode from root record |
| List directory | `read_dir()` | Iterate DirectoryRecord children via DirEmitter |
| Find by name | `lookup()` | Use FST index for O(m) lookup |
| Read file | `read_folio()` | Decompress and fill folio pages |
| Get stats | `statfs()` | Return file/block counts |

## Compression Support

### Kernel-Native (Supported)

These compression algorithms are available in the Linux kernel:

| Algorithm | Kernel Config | Notes |
|-----------|---------------|-------|
| Stored | N/A | No compression, direct copy |
| DEFLATE | `CONFIG_ZLIB_INFLATE` | zlib implementation |
| Zstd | `CONFIG_ZSTD_DECOMPRESS` | Facebook's algorithm |
| XZ | `CONFIG_XZ_DEC` | LZMA2-based |

### Userspace Only (Not Supported)

These algorithms are not in the kernel and would require porting:

| Algorithm | Status |
|-----------|--------|
| Brotli | Not in kernel |
| Snappy | Not in kernel |

**Behavior:** The kernel driver will reject mounting archives containing Brotli or Snappy compressed files with an error message. Users must extract such archives with userspace tools first, or recompress using a supported algorithm.

## Build Requirements

### Kernel Configuration

The kernel must be built with Rust support:

```
CONFIG_RUST=y
CONFIG_ZLIB_INFLATE=y      # For DEFLATE
CONFIG_ZSTD_DECOMPRESS=y   # For Zstd
CONFIG_XZ_DEC=y            # For XZ
```

### Toolchain

- Linux 6.12+ (latest stable with best Rust support)
- LLVM toolchain (Clang, LLD)
- Rust nightly (for kernel development)
- `bindgen` for generating Rust bindings

### Building

```bash
# Build the kernel module (out-of-tree)
cd box-kmod
make KDIR=/path/to/linux-source LLVM=1

# Load the module
sudo insmod boxfs.ko

# Mount a box archive
sudo mount -t boxfs /path/to/archive.box /mnt/box

# Unmount
sudo umount /mnt/box

# Unload the module
sudo rmmod boxfs
```

## Implementation Phases

### Phase A: Now (Preparatory Work)

This work can be done while waiting for VFS abstractions to land in mainline:

1. **Create `box-core` as a `no_std` crate**
   - Extract format parsing from `box-format`
   - Make it `#![no_std]` compatible using `alloc` crate
   - Abstract I/O behind traits

2. **Refactor `box-format` to use `box-core`**
   - Depend on `box-core`
   - Implement I/O traits with Tokio async
   - Ensure existing functionality unchanged

3. **Add kernel-compatible compression abstraction**
   - Create decompression trait for both contexts
   - Userspace: existing crate implementations
   - Kernel: (future) wrap kernel APIs

### Phase B: When VFS Lands in Mainline

Once the Rust VFS `FileSystem` trait is in mainline Linux (~12-18 months):

4. **Create `box-kmod` module skeleton**
   - Set up Kbuild/Makefile for out-of-tree build
   - Implement `module_fs!` registration

5. **Implement mount operations**
   - `super_params()` - parse header
   - `init_root()` - create root inode

6. **Implement directory operations**
   - `lookup()` using FST index
   - `read_dir()` via DirEmitter

7. **Implement file reading**
   - Wrap kernel compression APIs
   - `read_folio()` with decompression

8. **Testing & validation**
   - Compare with FUSE extraction
   - Performance benchmarks
   - Edge cases

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| VFS abstractions are RFC status | Start read-only, track upstream patches |
| Kernel API changes | Pin to specific kernel version initially |
| Compression algorithm missing | Document limitations, support subset |
| GPL licensing | Box is already open source |

## Future Work

- **Write support:** Add `write_folio()`, `create()`, `unlink()` etc.
- **Brotli/Snappy:** Port decompressors to kernel (complex)
- **Mainline submission:** Once stable, submit patches upstream
- **Block device backing:** Support mounting from loop devices
- **fs-verity integration:** Cryptographic file integrity (like PuzzleFS)

## License

The kernel module must be GPL-licensed (all Rust kernel symbols are `EXPORT_SYMBOL_GPL`). The Box format and existing crates are already open source.

## References

- [Rust for Linux](https://rust-for-linux.com/)
- [Kernel Rust Quick Start](https://docs.kernel.org/rust/quick-start.html)
- [Out-of-tree Module Template](https://github.com/Rust-for-Linux/rust-out-of-tree-module)
- [VFS RFC Patches](https://lore.kernel.org/all/ZbCRF%2FOkpqESeQpC@wedsonaf-dev/T/)
- [PuzzleFS](https://github.com/project-machine/puzzlefs)
- [Linux VFS Overview](https://docs.kernel.org/next/filesystems/vfs.html)
