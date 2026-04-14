/*
 * ext4_fast_alloc.c - Kernel-side ext4 patch for FALLOC_FL_NO_HIDE_STALE
 *
 * This file contains the kernel modifications needed to support
 * FALLOC_FL_NO_HIDE_STALE (0x04) on ext4 filesystems.
 *
 * Integration points (kernel ~5.10+):
 *   - include/uapi/linux/falloc.h   : add FALLOC_FL_NO_HIDE_STALE definition
 *   - fs/ext4/extents.c             : add ext4_alloc_file_blocks_no_hide()
 *   - fs/ext4/extents.c             : modify ext4_fallocate() to handle the flag
 *
 * DISCLAIMER: This is a reference implementation. Before applying to production,
 * review and test against your specific kernel version. The code below is based
 * on Linux 5.10 ext4 structure.
 */

#include <linux/falloc.h>
#include <linux/fs.h>
#include <linux/ext4.h>
#include <linux/ext4_fs.h>

/*
 * Step 1: Add flag definition in include/uapi/linux/falloc.h
 *
 * --- a/include/uapi/linux/falloc.h
 * +++ b/include/uapi/linux/falloc.h
 * @@
 *  #define FALLOC_FL_KEEP_SIZE             0x01
 *  #define FALLOC_FL_PUNCH_HOLE            0x02
 * +#define FALLOC_FL_NO_HIDE_STALE         0x04
 *  #define FALLOC_FL_COLLAPSE_RANGE        0x08
 *  #define FALLOC_FL_ZERO_RANGE            0x10
 *  #define FALLOC_FL_INSERT_RANGE          0x20
 *  #define FALLOC_FL_UNSHARE_RANGE         0x40
 */

#ifndef FALLOC_FL_NO_HIDE_STALE
#define FALLOC_FL_NO_HIDE_STALE		0x04
#endif

/*
 * Step 2: Implement ext4_alloc_file_blocks_no_hide()
 *
 * This function directly creates written extents without going through the
 * unwritten extent conversion path, thereby avoiding the jdb2 metadata
 * overhead of extent state transitions.
 *
 * Key difference from standard path:
 *
 * Standard path:
 *   ext4_map_blocks with EXT4_GET_BLOCKS_CREATE_UNWRIT_EXT
 *   -> creates unwritten extent
 *   -> first write triggers conversion to written
 *   -> TWO jdb2 transactions (create + convert)
 *
 * NO_HIDE_STALE path:
 *   ext4_map_blocks with EXT4_GET_BLOCKS_CREATE
 *   | EXT4_GET_BLOCKS_CONVERT_UNWRITTEN
 *   -> directly creates written extent
 *   -> no conversion needed at write time
 *   -> ONE jdb2 transaction (allocate only)
 */
static int ext4_alloc_file_blocks_no_hide(struct file *file,
					  loff_t offset,
					  loff_t len,
					  int mode)
{
	struct inode *inode = file_inode(file);
	struct ext4_map_blocks map;
	handle_t *handle;
	int ret;
	int blkbits = inode->i_blkbits;
	loff_t new_size = 0;

	if (offset < 0 || len <= 0)
		return -EINVAL;

	/* Check for overflow */
	if ((offset + len) < offset)
		return -EINVAL;

	/* Convert byte offsets to block offsets */
	map.m_lblk = offset >> blkbits;
	map.m_len  = len >> blkbits;

	if (map.m_len == 0)
		return 0;

	/*
	 * Calculate number of transaction credits needed.
	 * ext4_chunk_trans_blocks estimates credits for the extent tree
	 * operations (node splits, leaf insertions).
	 */
	handle = ext4_journal_start(inode, EXT4_HT_MAP_BLOCKS,
				ext4_chunk_trans_blocks(inode, map.m_len));
	if (IS_ERR(handle))
		return PTR_ERR(handle);

	/*
	 * CRITICAL FLAGS:
	 *
	 * EXT4_GET_BLOCKS_CREATE:
	 *   Create new blocks if they don't exist (allocate physical blocks).
	 *
	 * EXT4_GET_BLOCKS_CONVERT_UNWRITTEN:
	 *   If an unwritten extent already exists in this range, convert it
	 *   to written. This handles the case where a partial pre-allocation
	 *   was done previously.
	 *
	 * Together, these flags create written extents directly, bypassing
	 * the unwritten extent state entirely.
	 */
	ret = ext4_map_blocks(handle, inode, &map,
			EXT4_GET_BLOCKS_CREATE |
			EXT4_GET_BLOCKS_CONVERT_UNWRITTEN);

	if (ret < 0)
		goto out_stop;

	/*
	 * Update i_size if the allocation extends beyond current size.
	 * When file_size is fixed (CommitLog pre-allocation), this is
	 * a one-time cost. For sequential write workloads, the file
	 * should be fully pre-allocated upfront, so subsequent writes
	 * don't trigger i_size updates.
	 */
	new_size = offset + len;
	if (new_size > inode->i_size) {
		ret = ext4_update_i_size(inode, new_size);
		if (ret)
			goto out_stop;
	}

out_stop:
	ext4_journal_stop(handle);
	return ret;
}

/*
 * Step 3: Modify ext4_fallocate() to handle FALLOC_FL_NO_HIDE_STALE
 *
 * This patch intercepts the fallocate call and routes NO_HIDE_STALE
 * requests to the fast allocation path.
 *
 * --- a/fs/ext4/extents.c
 * +++ b/fs/ext4/extents.c
 * @@ ext4_fallocate @@
 */
static long ext4_fallocate(struct file *file, int mode, loff_t offset,
			   loff_t len)
{
	int ret;

	/*
	 * Handle FALLOC_FL_NO_HIDE_STALE: direct written extent allocation
	 *
	 * This must be checked BEFORE the standard validation, as
	 * NO_HIDE_STALE has different requirements:
	 *   - Cannot be combined with PUNCH_HOLE or COLLAPSE_RANGE
	 *   - Caller must guarantee no read of un-written regions
	 */
	if (mode & FALLOC_FL_NO_HIDE_STALE) {
		/* Reject incompatible flag combinations */
		if (mode & (FALLOC_FL_PUNCH_HOLE |
			    FALLOC_FL_COLLAPSE_RANGE |
			    FALLOC_FL_ZERO_RANGE))
			return -EOPNOTSUPP;

		/*
		 * Security check: NO_HIDE_STALE skips zero-fill, which could
		 * leak stale data from previously allocated blocks.
		 * Only allow for regular files.
		 */
		if (!S_ISREG(file_inode(file)->i_mode))
			return -EOPNOTSUPP;

		/*
		 * Capability check (optional, recommended for production):
		 * Require CAP_SYS_ADMIN to prevent unprivileged users from
		 * bypassing the zero-fill safety mechanism.
		 */
		if (!capable(CAP_SYS_ADMIN))
			return -EPERM;

		ret = ext4_alloc_file_blocks_no_hide(file, offset, len, mode);
		return ret;
	}

	/*
	 * FALL THROUGH to standard ext4_fallocate logic.
	 *
	 * In the real kernel, this would be the existing implementation
	 * handling FALLOC_FL_KEEP_SIZE, FALLOC_FL_PUNCH_HOLE,
	 * FALLOC_FL_ZERO_RANGE, etc.
	 */
	return ext4_alloc_file_blocks(file, offset, len, mode);
}

/*
 * Safety notes for production deployment:
 *
 * 1. CAP_SYS_ADMIN check:
 *    The NO_HIDE_STALE flag bypasses zero-fill, which could expose stale
 *    block data. Production kernels should restrict this to privileged
 *    callers. For embedded/controlled environments (e.g., Alibaba Cloud),
 *    this check can be relaxed.
 *
 * 2. Filesystem mount option (alternative approach):
 *    Instead of requiring CAP_SYS_ADMIN, add a mount option:
 *      mount -o allow_no_hide_stale /dev/sda1 /data
 *    Then check EXT4_SB(sb)->s_mount_opt & EXT4_MOUNT_ALLOW_NO_HIDE_STALE
 *    instead of capable(CAP_SYS_ADMIN).
 *
 * 3. kProbe alternative (no kernel rebuild):
 *    If recompiling the kernel is not feasible, a kProbe-based approach
 *    can intercept and modify the ext4_map_blocks behavior at runtime:
 *
 *    #include <linux/kprobe.h>
 *
 *    static struct kprobe kp = {
 *        .symbol_name = "ext4_map_blocks",
 *    };
 *
 *    static int kprobe_pre_handler(struct kprobe *p, struct pt_regs *regs) {
 *        // Modify the flags argument to include EXT4_GET_BLOCKS_CREATE
 *        // when the calling context matches our pre-allocated file.
 *        return 0;
 *    }
 *
 *    kp.pre_handler = kprobe_pre_handler;
 *    register_kprobe(&kp);
 */
