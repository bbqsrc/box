// SPDX-License-Identifier: GPL-2.0-only
/*
 * boxfs - Box archive filesystem
 *
 * Copyright (C) 2025 Brendan Molloy
 */

#include <linux/module.h>
#include <linux/fs.h>
#include <linux/fs_context.h>
#include <linux/pagemap.h>
#include <linux/statfs.h>
#include <linux/buffer_head.h>
#include <linux/slab.h>
#include <linux/xattr.h>

#include "include/boxfs.h"

static struct kmem_cache *boxfs_inode_cachep;

/*
 * Inode operations
 */

static struct dentry *boxfs_lookup(struct inode *dir, struct dentry *dentry,
				   unsigned int flags)
{
	struct super_block *sb = dir->i_sb;
	struct boxfs_inode_info *dir_info = BOXFS_I(dir);
	struct inode *inode = NULL;
	u64 ino;

	ino = boxfs_rust_lookup(sb, dir_info->record_index,
				dentry->d_name.name, dentry->d_name.len);
	if (ino != 0) {
		inode = boxfs_iget(sb, ino);
		if (IS_ERR(inode))
			return ERR_CAST(inode);
	}

	return d_splice_alias(inode, dentry);
}

static ssize_t boxfs_listxattr(struct dentry *dentry, char *buffer, size_t size);

static const struct inode_operations boxfs_dir_inode_operations = {
	.lookup = boxfs_lookup,
	.listxattr = boxfs_listxattr,
};

/*
 * Extended attribute operations
 */

static ssize_t boxfs_listxattr(struct dentry *dentry, char *buffer, size_t size)
{
	struct inode *inode = d_inode(dentry);
	struct super_block *sb = inode->i_sb;
	struct boxfs_inode_info *info = BOXFS_I(inode);

	return boxfs_rust_listxattr(sb, info->record_index, buffer, size);
}

static int boxfs_xattr_get(const struct xattr_handler *handler,
			   struct dentry *dentry, struct inode *inode,
			   const char *name, void *buffer, size_t size)
{
	struct super_block *sb = inode->i_sb;
	struct boxfs_inode_info *info = BOXFS_I(inode);
	char *full_name;
	ssize_t ret;

	/* Build the full xattr name with prefix */
	full_name = kasprintf(GFP_KERNEL, "%s%s", handler->prefix, name);
	if (!full_name)
		return -ENOMEM;

	ret = boxfs_rust_getxattr(sb, info->record_index, full_name, buffer, size);
	kfree(full_name);

	return ret;
}

static const struct xattr_handler boxfs_xattr_user_handler = {
	.prefix = XATTR_USER_PREFIX,
	.get = boxfs_xattr_get,
};

static const struct xattr_handler boxfs_xattr_trusted_handler = {
	.prefix = XATTR_TRUSTED_PREFIX,
	.get = boxfs_xattr_get,
};

static const struct xattr_handler boxfs_xattr_security_handler = {
	.prefix = XATTR_SECURITY_PREFIX,
	.get = boxfs_xattr_get,
};

static const struct xattr_handler * const boxfs_xattr_handlers[] = {
	&boxfs_xattr_user_handler,
	&boxfs_xattr_trusted_handler,
	&boxfs_xattr_security_handler,
	NULL,
};

/*
 * File inode operations
 */

static int boxfs_getattr(struct mnt_idmap *idmap, const struct path *path,
			 struct kstat *stat, u32 request_mask,
			 unsigned int query_flags)
{
	struct inode *inode = d_inode(path->dentry);

	generic_fillattr(idmap, request_mask, inode, stat);
	return 0;
}

static const struct inode_operations boxfs_file_inode_operations = {
	.getattr = boxfs_getattr,
	.listxattr = boxfs_listxattr,
};

/*
 * Directory operations
 */

static int boxfs_iterate_shared(struct file *file, struct dir_context *ctx)
{
	struct inode *inode = file_inode(file);
	struct super_block *sb = inode->i_sb;
	struct boxfs_inode_info *info = BOXFS_I(inode);

	return boxfs_rust_iterate_dir(sb, info->record_index, ctx);
}

static const struct file_operations boxfs_dir_operations = {
	.read = generic_read_dir,
	.iterate_shared = boxfs_iterate_shared,
	.llseek = generic_file_llseek,
};

/*
 * File operations
 */

static int boxfs_read_folio(struct file *file, struct folio *folio)
{
	struct inode *inode = folio->mapping->host;
	struct super_block *sb = inode->i_sb;
	struct boxfs_inode_info *info = BOXFS_I(inode);
	loff_t offset = folio_pos(folio);
	size_t len = folio_size(folio);
	char *buf;
	ssize_t ret;

	buf = kmap_local_folio(folio, 0);
	ret = boxfs_rust_read(sb, info->record_index, buf, len, offset);
	kunmap_local(buf);

	if (ret < 0) {
		folio_set_error(folio);
		folio_unlock(folio);
		return ret;
	}

	if (ret < len)
		folio_zero_segment(folio, ret, len);

	folio_mark_uptodate(folio);
	folio_unlock(folio);
	return 0;
}

static void boxfs_readahead(struct readahead_control *ractl)
{
	struct inode *inode = ractl->mapping->host;
	struct super_block *sb = inode->i_sb;
	struct boxfs_inode_info *info = BOXFS_I(inode);

	boxfs_rust_readahead(sb, info->record_index, ractl);
}

static const struct address_space_operations boxfs_aops = {
	.read_folio = boxfs_read_folio,
	.readahead = boxfs_readahead,
};

static const struct file_operations boxfs_file_operations = {
	.read_iter = generic_file_read_iter,
	.mmap = generic_file_readonly_mmap,
	.llseek = generic_file_llseek,
};

/*
 * Symlink operations
 */

static const char *boxfs_get_link(struct dentry *dentry, struct inode *inode,
				  struct delayed_call *callback)
{
	struct boxfs_inode_info *info = BOXFS_I(inode);
	struct super_block *sb = inode->i_sb;
	char *buf;
	int ret;

	buf = kmalloc(PATH_MAX, GFP_KERNEL);
	if (!buf)
		return ERR_PTR(-ENOMEM);

	ret = boxfs_rust_readlink(sb, info->record_index, buf, PATH_MAX);
	if (ret < 0) {
		kfree(buf);
		return ERR_PTR(ret);
	}

	set_delayed_call(callback, kfree_link, buf);
	return buf;
}

static const struct inode_operations boxfs_symlink_inode_operations = {
	.get_link = boxfs_get_link,
};

/*
 * Inode management
 */

static struct inode *boxfs_alloc_inode(struct super_block *sb)
{
	struct boxfs_inode_info *info;

	info = alloc_inode_sb(sb, boxfs_inode_cachep, GFP_KERNEL);
	if (!info)
		return NULL;

	return &info->vfs_inode;
}

static void boxfs_free_inode(struct inode *inode)
{
	kmem_cache_free(boxfs_inode_cachep, BOXFS_I(inode));
}

struct inode *boxfs_iget(struct super_block *sb, u64 ino)
{
	struct inode *inode;
	struct boxfs_inode_info *info;
	umode_t mode;
	u64 size, blocks;
	int ret;

	inode = iget_locked(sb, ino);
	if (!inode)
		return ERR_PTR(-ENOMEM);

	if (!(inode->i_state & I_NEW))
		return inode;

	info = BOXFS_I(inode);
	info->record_index = ino;

	ret = boxfs_rust_getattr(sb, ino, &mode, &size, &blocks);
	if (ret < 0) {
		iget_failed(inode);
		return ERR_PTR(ret);
	}

	inode->i_mode = mode;
	inode->i_size = size;
	inode->i_blocks = blocks;
	set_nlink(inode, 1);
	inode->i_uid = GLOBAL_ROOT_UID;
	inode->i_gid = GLOBAL_ROOT_GID;
	inode->i_atime = inode->i_mtime = inode_set_ctime_current(inode);

	if (S_ISREG(mode)) {
		inode->i_op = &boxfs_file_inode_operations;
		inode->i_fop = &boxfs_file_operations;
		inode->i_mapping->a_ops = &boxfs_aops;
	} else if (S_ISDIR(mode)) {
		inode->i_op = &boxfs_dir_inode_operations;
		inode->i_fop = &boxfs_dir_operations;
	} else if (S_ISLNK(mode)) {
		inode->i_op = &boxfs_symlink_inode_operations;
	}

	unlock_new_inode(inode);
	return inode;
}

/*
 * Superblock operations
 */

static int boxfs_statfs(struct dentry *dentry, struct kstatfs *buf)
{
	return boxfs_rust_statfs(dentry->d_sb, buf);
}

static void boxfs_put_super(struct super_block *sb)
{
	struct boxfs_sb_info *sbi = BOXFS_SB(sb);

	if (sbi) {
		boxfs_rust_put_super(sb);
		kfree(sbi);
		sb->s_fs_info = NULL;
	}
}

static const struct super_operations boxfs_super_operations = {
	.alloc_inode = boxfs_alloc_inode,
	.free_inode = boxfs_free_inode,
	.statfs = boxfs_statfs,
	.put_super = boxfs_put_super,
};

/*
 * Mount/unmount
 */

static int boxfs_fill_super(struct super_block *sb, struct fs_context *fc)
{
	struct boxfs_sb_info *sbi;
	struct inode *root_inode;
	int ret;

	sbi = kzalloc(sizeof(*sbi), GFP_KERNEL);
	if (!sbi)
		return -ENOMEM;

	sb->s_fs_info = sbi;
	sb->s_magic = BOXFS_MAGIC;
	sb->s_op = &boxfs_super_operations;
	sb->s_xattr = boxfs_xattr_handlers;
	sb->s_flags |= SB_RDONLY;
	sb->s_maxbytes = MAX_LFS_FILESIZE;
	sb->s_time_gran = 1;

	/* Call into Rust to parse the archive and set up metadata */
	ret = boxfs_rust_fill_super(sb, fc->fs_private, fc->sb_flags & SB_SILENT);
	if (ret < 0) {
		kfree(sbi);
		sb->s_fs_info = NULL;
		return ret;
	}

	/* Create root inode */
	root_inode = boxfs_iget(sb, sbi->root_ino);
	if (IS_ERR(root_inode)) {
		boxfs_rust_put_super(sb);
		kfree(sbi);
		sb->s_fs_info = NULL;
		return PTR_ERR(root_inode);
	}

	sb->s_root = d_make_root(root_inode);
	if (!sb->s_root) {
		boxfs_rust_put_super(sb);
		kfree(sbi);
		sb->s_fs_info = NULL;
		return -ENOMEM;
	}

	return 0;
}

static int boxfs_get_tree(struct fs_context *fc)
{
	return get_tree_bdev(fc, boxfs_fill_super);
}

static void boxfs_free_fc(struct fs_context *fc)
{
	/* Nothing to free for now */
}

static const struct fs_context_operations boxfs_context_ops = {
	.get_tree = boxfs_get_tree,
	.free = boxfs_free_fc,
};

static int boxfs_init_fs_context(struct fs_context *fc)
{
	fc->ops = &boxfs_context_ops;
	return 0;
}

static struct file_system_type boxfs_fs_type = {
	.owner = THIS_MODULE,
	.name = "boxfs",
	.init_fs_context = boxfs_init_fs_context,
	.kill_sb = kill_block_super,
	.fs_flags = FS_REQUIRES_DEV,
};
MODULE_ALIAS_FS("boxfs");

/*
 * Module init/exit
 */

static void boxfs_inode_init_once(void *ptr)
{
	struct boxfs_inode_info *info = ptr;
	inode_init_once(&info->vfs_inode);
}

static int __init boxfs_init(void)
{
	int err;

	boxfs_inode_cachep = kmem_cache_create("boxfs_inode_cache",
					       sizeof(struct boxfs_inode_info),
					       0,
					       SLAB_RECLAIM_ACCOUNT | SLAB_ACCOUNT,
					       boxfs_inode_init_once);
	if (!boxfs_inode_cachep)
		return -ENOMEM;

	err = register_filesystem(&boxfs_fs_type);
	if (err) {
		kmem_cache_destroy(boxfs_inode_cachep);
		return err;
	}

	pr_info("boxfs: Box archive filesystem registered\n");
	return 0;
}

static void __exit boxfs_exit(void)
{
	unregister_filesystem(&boxfs_fs_type);
	kmem_cache_destroy(boxfs_inode_cachep);
	pr_info("boxfs: Box archive filesystem unregistered\n");
}

module_init(boxfs_init);
module_exit(boxfs_exit);

MODULE_DESCRIPTION("Box archive filesystem");
MODULE_AUTHOR("Brendan Molloy");
MODULE_LICENSE("GPL");
