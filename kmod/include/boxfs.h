/* SPDX-License-Identifier: GPL-2.0-only */
/*
 * boxfs filesystem declarations
 */

#ifndef _BOXFS_H
#define _BOXFS_H

#include <linux/fs.h>
#include <linux/pagemap.h>
#include <linux/types.h>

/* Magic number for boxfs superblock */
#define BOXFS_MAGIC	0x424F5846	/* "BOXF" */

/* Box file header magic bytes */
#define BOX_MAGIC_0	0xB0
#define BOX_MAGIC_1	0x0B
#define BOX_MAGIC_2	0x00
#define BOX_MAGIC_3	0x01

/*
 * In-memory superblock info
 */
struct boxfs_sb_info {
	/* Block device or backing file */
	struct block_device *bdev;

	/* Total archive size in bytes */
	u64 archive_size;

	/* Offset to trailer (metadata) section */
	u64 trailer_offset;

	/* Pointer to parsed metadata (owned by Rust) */
	void *metadata;

	/* Root inode number */
	u64 root_ino;
};

/*
 * In-memory inode info
 */
struct boxfs_inode_info {
	/* Index into the records array */
	u64 record_index;

	/* Cached attributes from the record */
	u64 data_offset;
	u64 data_length;
	u64 decompressed_length;
	u8 compression;

	/* VFS inode (must be last for container_of) */
	struct inode vfs_inode;
};

static inline struct boxfs_sb_info *BOXFS_SB(struct super_block *sb)
{
	return sb->s_fs_info;
}

static inline struct boxfs_inode_info *BOXFS_I(struct inode *inode)
{
	return container_of(inode, struct boxfs_inode_info, vfs_inode);
}

/*
 * Rust FFI declarations
 */

/* Initialize boxfs from the given superblock, returns 0 on success */
int boxfs_rust_fill_super(struct super_block *sb, void *data, int silent);

/* Clean up Rust-allocated metadata */
void boxfs_rust_put_super(struct super_block *sb);

/* Get filesystem statistics */
int boxfs_rust_statfs(struct super_block *sb, struct kstatfs *buf);

/* Look up a name in a directory, returns inode number or 0 if not found */
u64 boxfs_rust_lookup(struct super_block *sb, u64 dir_ino, const char *name, size_t name_len);

/* Iterate directory entries, calls emit_fn for each entry */
int boxfs_rust_iterate_dir(struct super_block *sb, u64 dir_ino,
			   struct dir_context *ctx);

/* Read file data into buffer, returns bytes read or negative error */
ssize_t boxfs_rust_read(struct super_block *sb, u64 ino,
			char *buf, size_t len, loff_t offset);

/* Get inode attributes */
int boxfs_rust_getattr(struct super_block *sb, u64 ino,
		       umode_t *mode, u64 *size, u64 *blocks);

/* Read symlink target */
int boxfs_rust_readlink(struct super_block *sb, u64 ino,
			char *buf, size_t buflen);

/* Readahead for sequential read optimization */
void boxfs_rust_readahead(struct super_block *sb, u64 ino,
			  struct readahead_control *ractl);

/* Get extended attribute, returns size or negative error */
ssize_t boxfs_rust_getxattr(struct super_block *sb, u64 ino,
			    const char *name, void *value, size_t size);

/* List extended attributes, returns size or negative error */
ssize_t boxfs_rust_listxattr(struct super_block *sb, u64 ino,
			     char *list, size_t size);

/* Inode getter */
struct inode *boxfs_iget(struct super_block *sb, u64 ino);

#endif /* _BOXFS_H */
