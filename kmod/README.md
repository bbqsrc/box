# boxfs - Linux Kernel Filesystem Module

This is a Linux kernel module implementing read-only support for the Box archive format.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Linux Kernel VFS                         │
├─────────────────────────────────────────────────────────────┤
│  C shim layer (super.c)                                      │
│  - file_system_type registration                            │
│  - inode_operations, file_operations                        │
│  - Calls into Rust via C-ABI functions                      │
├─────────────────────────────────────────────────────────────┤
│  rust_helpers.c                                              │
│  - Wrappers for inline kernel functions                     │
│  - Called by Rust code                                      │
├─────────────────────────────────────────────────────────────┤
│  Rust core (rust/*.rs)                                       │
│  - lib.rs: C-ABI exports (boxfs_rust_*)                     │
│  - bindings.rs: Kernel type definitions                     │
│  - metadata.rs: Parsed archive data structures              │
│  - error.rs: Kernel error codes                             │
├─────────────────────────────────────────────────────────────┤
│  box-format core (../src/core/)                              │
│  - #![no_std] + alloc compatible                            │
│  - Zero-copy parsing from byte slices                       │
└─────────────────────────────────────────────────────────────┘
```

## Files

- `Kconfig` - Kernel configuration options
- `Makefile` - Build configuration for in-tree/out-of-tree builds
- `super.c` - Main C code: mount, superblock, inode, file ops
- `rust_helpers.c` - C wrappers for inline functions
- `include/boxfs.h` - Shared header with Rust FFI declarations
- `rust/lib.rs` - Rust entry point with C-ABI exports
- `rust/bindings.rs` - Kernel type bindings
- `rust/metadata.rs` - Archive metadata structures
- `rust/error.rs` - Error type definitions

## Building

### Requirements

- Linux kernel 6.1+ with `CONFIG_RUST=y`
- Rust toolchain compatible with kernel (see kernel docs)
- LLVM/Clang (kernel Rust requires LLVM)

### Out-of-tree build

```bash
# Ensure kernel headers are installed
make KDIR=/path/to/kernel/build
```

### In-tree build

Copy or symlink `kmod/` to `fs/boxfs/` in the kernel tree:

```bash
ln -s /path/to/box/kmod /path/to/linux/fs/boxfs

# Add to fs/Kconfig:
# source "fs/boxfs/Kconfig"

# Add to fs/Makefile:
# obj-$(CONFIG_BOXFS_FS) += boxfs/

# Configure kernel
make menuconfig
# Enable: File systems -> Box archive filesystem support

# Build
make LLVM=1
```

## Usage

```bash
# Load module
modprobe boxfs

# Mount a box archive (on a block device)
mount -t boxfs /dev/loop0 /mnt/box

# Or with a loopback file
losetup /dev/loop0 archive.box
mount -t boxfs /dev/loop0 /mnt/box

# Unmount
umount /mnt/box
losetup -d /dev/loop0
```

## Status

This is a skeleton implementation. Current state:

- [x] Module registration
- [x] Superblock initialization
- [x] Inode allocation/freeing
- [x] Basic VFS operations structure
- [ ] Archive trailer parsing
- [ ] Directory listing
- [ ] File reading
- [ ] Decompression (zstd, xz)
- [ ] Symlink support
- [ ] Extended attributes

## License

GPL-2.0-only (required for Linux kernel modules)
