/* SPDX-License-Identifier: GPL-2.0-only */
/*
 * boxfs filesystem declarations
 */

#ifndef _BOXFS_H
#define _BOXFS_H

#include <linux/compiler_attributes.h>
#include <linux/fs.h>
#include <linux/mutex.h>
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

	/* Serialises the Rust block cache, which has no interior locking */
	struct mutex meta_lock;
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

/*
 * Helpers called from Rust (rust_helpers.c)
 */

struct buffer_head *boxfs_sb_bread(struct super_block *sb, sector_t block);
struct buffer_head *boxfs_sb_bread_unmovable(struct super_block *sb, sector_t block);
void boxfs_brelse(struct buffer_head *bh);
void boxfs_put_bh(struct buffer_head *bh);
void *boxfs_bh_data(struct buffer_head *bh);
size_t boxfs_bh_size(struct buffer_head *bh);

struct boxfs_sb_info *boxfs_get_sb_info(struct super_block *sb);
void boxfs_set_metadata(struct super_block *sb, void *metadata);
void *boxfs_get_metadata(struct super_block *sb);
void boxfs_set_archive_size(struct super_block *sb, u64 size);
void boxfs_set_trailer_offset(struct super_block *sb, u64 offset);
void boxfs_set_root_ino(struct super_block *sb, u64 ino);
u64 boxfs_get_root_ino(struct super_block *sb);
void boxfs_meta_lock(struct super_block *sb);
void boxfs_meta_unlock(struct super_block *sb);

struct block_device *boxfs_sb_bdev(struct super_block *sb);
unsigned int boxfs_sb_blocksize(struct super_block *sb);
unsigned char boxfs_sb_blocksize_bits(struct super_block *sb);
loff_t boxfs_bdev_nr_bytes(struct block_device *bdev);

bool boxfs_dir_emit(struct dir_context *ctx, const char *name, int namelen,
		    u64 ino, unsigned int type);
bool boxfs_dir_emit_dot(struct file *file, struct dir_context *ctx);
bool boxfs_dir_emit_dotdot(struct file *file, struct dir_context *ctx);
loff_t boxfs_dir_ctx_pos(struct dir_context *ctx);
void boxfs_dir_ctx_set_pos(struct dir_context *ctx, loff_t pos);

void *boxfs_kmalloc(size_t size, gfp_t flags);
void *boxfs_kzalloc(size_t size, gfp_t flags);
void boxfs_kfree(void *ptr);
void *boxfs_kvmalloc(size_t size, gfp_t flags);
void boxfs_kvfree(void *ptr);

void boxfs_pr_info(const char *msg);
void boxfs_pr_err(const char *msg);
void boxfs_pr_warn(const char *msg);
void boxfs_pr_debug(const char *msg);
void __noreturn boxfs_panic(const char *msg);

loff_t boxfs_readahead_pos(struct readahead_control *ractl);
size_t boxfs_readahead_length(struct readahead_control *ractl);
struct folio *boxfs_readahead_folio(struct readahead_control *ractl);
loff_t boxfs_folio_pos(struct folio *folio);
size_t boxfs_folio_size(struct folio *folio);
void *boxfs_kmap_local_folio(struct folio *folio, size_t offset);
void boxfs_kunmap_local(void *addr);
void boxfs_folio_mark_uptodate(struct folio *folio);
void boxfs_folio_unlock(struct folio *folio);
void boxfs_folio_zero_segment(struct folio *folio, size_t start, size_t end);

int boxfs_zstd_decompress(const void *src, size_t src_len,
			  void *dst, size_t dst_len, size_t *out_len);
int boxfs_xz_decompress(const void *src, size_t src_len,
			void *dst, size_t dst_len, size_t *out_len);

#endif /* _BOXFS_H */
