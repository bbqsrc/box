# boxfs - Linux kernel filesystem module

Read-only Linux filesystem driver for the Box archive format.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Linux kernel VFS                         │
├─────────────────────────────────────────────────────────────┤
│  super.c                                                     │
│  - file_system_type, super/inode/file/address_space ops      │
│  - calls boxfs_rust_* across the C ABI                       │
├─────────────────────────────────────────────────────────────┤
│  rust_helpers.c                                              │
│  - non-inline wrappers for the kernel's inline helpers        │
│  - zstd/xz decompression, kvmalloc, buffer heads, folios     │
├─────────────────────────────────────────────────────────────┤
│  rust/ (cargo staticlib, no_std + alloc)                     │
│  - lib.rs:      C-ABI exports (boxfs_rust_*)                 │
│  - bindings.rs: hand-written kernel type/helper declarations  │
│  - parser.rs:   header, trailer, record and FST parsing      │
│  - metadata.rs: in-memory namespace and decompressed-block   │
│                 LRU cache                                    │
│  - error.rs:    KernelError -> negative errno                │
├─────────────────────────────────────────────────────────────┤
│  box-fst (crates/fst, no_std) + hashbrown + fastvint         │
└─────────────────────────────────────────────────────────────┘
```

The module does **not** use the kernel's own Rust support. `rust/` is compiled
by cargo into a freestanding `staticlib`, and kbuild links the resulting
archive into `boxfs.ko` as one relocatable object. `CONFIG_RUST` is irrelevant.

## Build requirements

- Linux 6.12 kernel headers (`/lib/modules/$(uname -r)/build`).
- A **nightly** Rust toolchain with the `rust-src` component and the
  `x86_64-unknown-none` target. `-Zbuild-std` is mandatory: rustup ships
  `core` and `alloc` for that target as position-independent rlibs, whose
  `R_X86_64_GOTPCREL` relocations the module loader rejects.

  ```bash
  rustup toolchain install nightly
  rustup component add rust-src --toolchain nightly
  rustup target add x86_64-unknown-none --toolchain nightly
  ```

- Network access for the `fastvint` git dependency. The build exports
  `CARGO_NET_GIT_FETCH_WITH_CLI=true` because cargo's built-in git client
  cannot authenticate against that repository.

x86-64 only. `SRCARCH` values other than `x86` stop the build with an explicit
error rather than producing a mis-compiled object.

### Codegen constraints

`rust/.cargo/config.toml` pins the flags that make the output loadable. The
`x86_64-unknown-none` target already supplies soft-float, no SSE/AVX,
`code-model=kernel` and no red zone. On top of that:

| flag | why |
| --- | --- |
| `-Crelocation-model=static` | no GOT-relative relocations |
| `-Zplt=y` | otherwise external calls go through `*sym@GOTPCREL(%rip)` |
| `-Zdirect-access-external-data=y` | external data addressed directly |
| `-Zcf-protection=branch` | `CONFIG_X86_KERNEL_IBT`: ENDBR on indirect targets |
| `-Cllvm-args=--min-jump-table-entries=…` | IBT has NOTRACK disabled, and jump tables carry no ENDBR |
| `-Zfunction-return=thunk-extern` | `CONFIG_MITIGATION_RETHUNK` |
| `-Zretpoline-external-thunk=y` | `CONFIG_MITIGATION_RETPOLINE` |
| `-Cforce-unwind-tables=n`, `-Cpanic=abort` | no unwinder in the kernel |

`compiler_builtins` supplies the weak `memcpy`/`memset`/`memmove`/`memcmp`
used by the Rust half. On x86-64 those are `rep movsb`/`rep movsq`, so the
module stays self-contained instead of depending on the kernel's exports.

The partial link seeds from the exported `boxfs_rust_*` symbols and runs
`--gc-sections`, which keeps the unreachable half of `compiler_builtins`
(soft-float, libm) out of the module.

### Out-of-tree build

```bash
cd kmod
make                 # KDIR=/lib/modules/$(uname -r)/build by default
make KDIR=/path/to/kernel/build
make clean           # also removes rust/target
```

`make` runs cargo and then kbuild; no `CONFIG_BOXFS_FS` needs to be set.

One objtool warning is expected and benign:

```
boxfs.o: warning: objtool: .text.unlikely._…handle_alloc_error: unexpected end of section
```

objtool has no `__noreturn` annotation for Rust's allocation-error handler, so
it cannot see that the function does not return. The chain ends in
`boxfs_panic()`, which is `__noreturn` and calls `BUG()`.

### Host install

```bash
cd kmod
make
sudo make install    # modules_install
sudo depmod -a
```

### DKMS

`dkms.conf` builds through the same flow. Because the Rust crate has path
dependencies on the surrounding box checkout, which DKMS does not copy into
its build tree, `dkms-prebuild.sh` links them in. Point the DKMS source
directory at the checkout's `kmod/`:

```bash
sudo ln -s /path/to/box/kmod /usr/src/boxfs-0.1.0
sudo dkms install -m boxfs -v 0.1.0
```

`dkms-prebuild.sh` resolves the checkout from that symlink; set
`BOXFS_CHECKOUT` to override. The nightly toolchain and `rust-src` must be
reachable by the user DKMS builds as (root, normally), and cargo needs network
access on the first build.

### In-tree

`Kconfig` is kept for a future in-kernel build. `fs/boxfs/` would use the same
cargo-built staticlib; nothing here depends on `CONFIG_RUST`.

## Usage

The archive is attached as a block device.

```bash
sudo insmod boxfs.ko                 # or: sudo modprobe boxfs
sudo losetup --find --show --read-only archive.box
sudo mount -t boxfs -o ro /dev/loopN /mnt/box
...
sudo umount /mnt/box
sudo losetup -d /dev/loopN
sudo rmmod boxfs
```

**A loop device exposes whole 512-byte sectors.** A Box trailer runs to the end
of the file, so an archive whose length is not a multiple of 512 loses the tail
of its trailer and will not mount (`EBADMSG`, with `boxfs: cannot parse archive
on …` in `dmesg`). Pad a copy first:

```bash
size=$(stat -c %s archive.box)
cp --reflink=auto archive.box padded.box
truncate -s $(( (size + 511) / 512 * 512 )) padded.box
```

Trailing zero padding is ignored by both this module and the box library.

## Smoke test

`smoke.sh` does the whole cycle — insmod, losetup, mount, `box extract`,
byte-identity diff of contents, symlink targets and modes, unmount, rmmod —
with per-step timing and a PASS/FAIL verdict. It pads the archive itself when
needed and cleans up on every exit path.

```bash
sudo ./smoke.sh /path/to/archive.box
# BOXFS_KO=... BOX=... TMPDIR=... override the module, CLI and scratch paths
```

The scratch directory holds a full extraction of the archive, so `TMPDIR` needs
room for the uncompressed tree.

## Userspace tests

`rust/kernel_sim.rs` implements every helper `rust_helpers.c` provides, backed
by an in-memory image that reproduces the kernel's buffer-head behaviour
(including refusing the trailing partial sector). `rust/archive_tests.rs`
drives the exported C ABI against an archive written by the box library and
checks the namespace, modes, symlink targets, xattrs and byte-identical reads
across chunk boundaries.

```bash
cd rust && cargo test
```

## Status

- [x] Module registration, superblock, inode lifecycle
- [x] Header and trailer parsing (v1: attribute keys, records, path FST,
      block FST)
- [x] Directory listing (path FST, resumable `iterate_shared`, dots)
- [x] Lookup, `getattr`, `statfs`
- [x] File reading: stored, zstd and xz, single-blob and chunked
- [x] Chunked block lookup through the block FST with a byte-bounded LRU of
      decompressed blocks
- [x] Symlinks: internal links resolved to a path relative to the link, and
      external links
- [x] Extended attributes (`linux.xattr.*`)
- [x] `read_folio` and `readahead`
- [x] Builds out of tree against stock 6.12 headers, MODPOST clean, loadable
      relocation types only
- [ ] Loaded and mounted on real hardware — the port has been verified in
      userspace only (see *Userspace tests*); `smoke.sh` is the first load
- [ ] Multiple archives merged into one mount (the metadata layer supports it;
      nothing mounts a second archive yet)
- [ ] Compression dictionaries
- [ ] Timestamps from record attributes (inodes get mount time)
- [ ] Mount options
- [ ] arm64

## License

GPL-2.0-only (required for Linux kernel modules)
