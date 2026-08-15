# FUSE integration

## Read-only mount

> [spec:box:req:fuse-mount.root]
> `fusebox` MUST mount one or more successfully opened Box archives through the
> `fuser` filesystem interface using read-only mount options. The binary MUST add
> archives in command-line order, use the requested size-bounded decompression
> cache, and expose lookup, attributes, directory enumeration, file reads,
> symlink reads, filesystem statistics, extended attributes, and access checks.

> [spec:box:req:fuse-mount.root.read-only-and-attributes]
> The mount MUST report ordinary and chunked records as regular files,
> directories as directories, and internal and external links as symlinks.
> Record `unix.mode`, `unix.uid`, `unix.gid`, and timestamp attributes MUST be
> used when decodable, with type-appropriate permission, current-user/group, and
> epoch fallbacks. Opens MUST request persistent kernel caching, and access
> checks MUST reject all write requests while applying owner, group, or other
> read/execute bits to non-root callers.

> [spec:box:req:fuse-mount.root.file-data]
> Reads of an ordinary file MUST decompress and cache the whole file under its
> composite identity, then return the requested range clamped to the file size.
> Reads of a chunked file MUST identify every intersecting logical block,
> decompress missing blocks individually, concatenate only the requested slices,
> and cache blocks independently. The cache MUST enforce its byte capacity by
> least-recently-used eviction, MUST NOT retain an entry larger than its total
> capacity, and MUST survive file release.

> [spec:box:req:fuse-mount.root.links-and-xattrs]
> Reading an internal link MUST return a path relative from the link's parent to
> its target in the same archive; reading an external link MUST return its stored
> target bytes. FUSE xattr lookup and listing MUST map native names to record or
> root attributes prefixed by `linux.xattr.`, support size probes, return
> `ERANGE` for short buffers, and emit listed names as NUL-terminated entries.

## Multi-archive namespace

> [spec:box:sem:multi-archive.root]
> Each added archive receives a monotonically increasing archive identifier.
> Records are identified internally by `(archive_id << 64) | local_record_index`,
> while FUSE inode numbers reserve inode 1 for the root and pack a 16-bit archive
> identifier plus a 48-bit local index into the remaining inode space. A merged
> path FST and reverse path index connect names to these identities.

> [spec:box:sem:multi-archive.root.precedence]
> Adding a later archive replaces the merged path value for a conflicting
> non-directory entry, so the later record shadows the earlier record. When both
> conflicting records are directories, the earlier directory identity remains
> at that path and direct children from all archives remain discoverable through
> the merged prefix namespace.

> [spec:box:sem:multi-archive.root.navigation]
> Child lookup constructs a Box path from the parent reverse index and performs
> a merged-FST lookup, with directory-record scanning as a fallback where used.
> Directory listings retain only direct prefix children, root listings retain
> only keys without a separator, and filesystem totals sum record counts and
> decompressed ordinary/chunked file sizes across every loaded archive.
