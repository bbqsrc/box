// SPDX-License-Identifier: GPL-2.0-only
/*
 * boxfs - Rust helper wrappers for inline functions
 *
 * Many kernel functions are defined as static inline, which means
 * they cannot be called directly from Rust via bindgen. This file
 * provides non-inline wrapper functions that Rust can call.
 */

#include <linux/fs.h>
#include <linux/buffer_head.h>
#include <linux/pagemap.h>
#include <linux/slab.h>
#include <linux/uaccess.h>
#include <linux/zstd.h>
#include <linux/xz.h>

#include "include/boxfs.h"

/*
 * Buffer head operations
 */

struct buffer_head *boxfs_sb_bread(struct super_block *sb, sector_t block)
{
	return sb_bread(sb, block);
}

struct buffer_head *boxfs_sb_bread_unmovable(struct super_block *sb, sector_t block)
{
	return sb_bread_unmovable(sb, block);
}

void boxfs_brelse(struct buffer_head *bh)
{
	brelse(bh);
}

void boxfs_put_bh(struct buffer_head *bh)
{
	put_bh(bh);
}

void *boxfs_bh_data(struct buffer_head *bh)
{
	return bh->b_data;
}

size_t boxfs_bh_size(struct buffer_head *bh)
{
	return bh->b_size;
}

/*
 * Superblock info accessors
 */

struct boxfs_sb_info *boxfs_get_sb_info(struct super_block *sb)
{
	return BOXFS_SB(sb);
}

void boxfs_set_metadata(struct super_block *sb, void *metadata)
{
	struct boxfs_sb_info *sbi = BOXFS_SB(sb);
	sbi->metadata = metadata;
}

void *boxfs_get_metadata(struct super_block *sb)
{
	struct boxfs_sb_info *sbi = BOXFS_SB(sb);
	return sbi->metadata;
}

void boxfs_set_archive_size(struct super_block *sb, u64 size)
{
	struct boxfs_sb_info *sbi = BOXFS_SB(sb);
	sbi->archive_size = size;
}

void boxfs_set_trailer_offset(struct super_block *sb, u64 offset)
{
	struct boxfs_sb_info *sbi = BOXFS_SB(sb);
	sbi->trailer_offset = offset;
}

void boxfs_set_root_ino(struct super_block *sb, u64 ino)
{
	struct boxfs_sb_info *sbi = BOXFS_SB(sb);
	sbi->root_ino = ino;
}

u64 boxfs_get_root_ino(struct super_block *sb)
{
	struct boxfs_sb_info *sbi = BOXFS_SB(sb);
	return sbi->root_ino;
}

/*
 * Block device access
 */

struct block_device *boxfs_sb_bdev(struct super_block *sb)
{
	return sb->s_bdev;
}

unsigned int boxfs_sb_blocksize(struct super_block *sb)
{
	return sb->s_blocksize;
}

unsigned char boxfs_sb_blocksize_bits(struct super_block *sb)
{
	return sb->s_blocksize_bits;
}

loff_t boxfs_bdev_nr_bytes(struct block_device *bdev)
{
	return bdev_nr_bytes(bdev);
}

/*
 * Directory emission helpers
 */

bool boxfs_dir_emit(struct dir_context *ctx, const char *name, int namelen,
		    u64 ino, unsigned int type)
{
	return dir_emit(ctx, name, namelen, ino, type);
}

bool boxfs_dir_emit_dot(struct file *file, struct dir_context *ctx)
{
	return dir_emit_dot(file, ctx);
}

bool boxfs_dir_emit_dotdot(struct file *file, struct dir_context *ctx)
{
	return dir_emit_dotdot(file, ctx);
}

loff_t boxfs_dir_ctx_pos(struct dir_context *ctx)
{
	return ctx->pos;
}

void boxfs_dir_ctx_set_pos(struct dir_context *ctx, loff_t pos)
{
	ctx->pos = pos;
}

/*
 * Memory allocation
 */

void *boxfs_kmalloc(size_t size, gfp_t flags)
{
	return kmalloc(size, flags);
}

void *boxfs_kzalloc(size_t size, gfp_t flags)
{
	return kzalloc(size, flags);
}

void boxfs_kfree(void *ptr)
{
	kfree(ptr);
}

/*
 * Printk helpers
 */

void boxfs_pr_info(const char *msg)
{
	pr_info("boxfs: %s\n", msg);
}

void boxfs_pr_err(const char *msg)
{
	pr_err("boxfs: %s\n", msg);
}

void boxfs_pr_warn(const char *msg)
{
	pr_warn("boxfs: %s\n", msg);
}

void boxfs_pr_debug(const char *msg)
{
	pr_debug("boxfs: %s\n", msg);
}

/*
 * Readahead helpers
 */

loff_t boxfs_readahead_pos(struct readahead_control *ractl)
{
	return readahead_pos(ractl);
}

size_t boxfs_readahead_length(struct readahead_control *ractl)
{
	return readahead_length(ractl);
}

struct folio *boxfs_readahead_folio(struct readahead_control *ractl)
{
	return readahead_folio(ractl);
}

loff_t boxfs_folio_pos(struct folio *folio)
{
	return folio_pos(folio);
}

size_t boxfs_folio_size(struct folio *folio)
{
	return folio_size(folio);
}

void *boxfs_kmap_local_folio(struct folio *folio, size_t offset)
{
	return kmap_local_folio(folio, offset);
}

void boxfs_kunmap_local(void *addr)
{
	kunmap_local(addr);
}

void boxfs_folio_mark_uptodate(struct folio *folio)
{
	folio_mark_uptodate(folio);
}

void boxfs_folio_unlock(struct folio *folio)
{
	folio_unlock(folio);
}

void boxfs_folio_zero_segment(struct folio *folio, size_t start, size_t end)
{
	folio_zero_segment(folio, start, end);
}

/*
 * Zstd decompression
 */

/**
 * boxfs_zstd_decompress - Decompress zstd-compressed data
 * @src: Source (compressed) data
 * @src_len: Length of compressed data
 * @dst: Destination buffer for decompressed data
 * @dst_len: Size of destination buffer
 * @out_len: Output: actual decompressed length
 *
 * Returns 0 on success, negative errno on failure.
 */
int boxfs_zstd_decompress(const void *src, size_t src_len,
			  void *dst, size_t dst_len,
			  size_t *out_len)
{
	size_t wksp_size;
	void *wksp;
	zstd_dctx *dctx;
	size_t ret;

	/* Get workspace size needed for decompression */
	wksp_size = zstd_dctx_workspace_bound();
	wksp = kvmalloc(wksp_size, GFP_KERNEL);
	if (!wksp)
		return -ENOMEM;

	/* Initialize decompression context */
	dctx = zstd_init_dctx(wksp, wksp_size);
	if (!dctx) {
		kvfree(wksp);
		return -EINVAL;
	}

	/* Decompress */
	ret = zstd_decompress_dctx(dctx, dst, dst_len, src, src_len);
	kvfree(wksp);

	if (zstd_is_error(ret)) {
		pr_debug("boxfs: zstd decompression failed: %s\n",
			 zstd_get_error_name(ret));
		return -EIO;
	}

	*out_len = ret;
	return 0;
}

/*
 * XZ decompression
 */

/**
 * boxfs_xz_decompress - Decompress xz-compressed data
 * @src: Source (compressed) data
 * @src_len: Length of compressed data
 * @dst: Destination buffer for decompressed data
 * @dst_len: Size of destination buffer
 * @out_len: Output: actual decompressed length
 *
 * Returns 0 on success, negative errno on failure.
 */
int boxfs_xz_decompress(const void *src, size_t src_len,
			void *dst, size_t dst_len,
			size_t *out_len)
{
	struct xz_dec *dec;
	struct xz_buf buf;
	enum xz_ret ret;

	/* Initialize decoder with default dict size (same as xzminidec) */
	dec = xz_dec_init(XZ_DYNALLOC, (u32)-1);
	if (!dec)
		return -ENOMEM;

	/* Set up input/output buffers */
	buf.in = src;
	buf.in_pos = 0;
	buf.in_size = src_len;
	buf.out = dst;
	buf.out_pos = 0;
	buf.out_size = dst_len;

	/* Decompress */
	ret = xz_dec_run(dec, &buf);
	xz_dec_end(dec);

	if (ret != XZ_STREAM_END) {
		pr_debug("boxfs: xz decompression failed: %d\n", ret);
		return -EIO;
	}

	*out_len = buf.out_pos;
	return 0;
}
