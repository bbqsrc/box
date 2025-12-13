# fusebox - FUSE Driver for Box Archives

**Status: Demonstration / Experimental**

A FUSE filesystem driver that allows mounting `.box` archives as read-only filesystems.

## Requirements

- Linux or macOS with FUSE support
- `libfuse` development libraries

## Usage

```bash
fusebox archive.box /mnt/point
```

To unmount:

```bash
fusermount -u /mnt/point  # Linux
umount /mnt/point         # macOS
```

## Limitations

- Read-only access only
- Experimental - not recommended for production use
