/*
 * ext4_fast_alloc.c - Kernel module: FALLOC_FL_NO_HIDE_STALE via kprobe
 *
 * Problem:
 *   ext4 standard fallocate creates unwritten extents.
 *   First write triggers unwritten->written conversion via ext4_map_blocks.
 *   This conversion costs a jbd2 transaction at runtime = write amplification.
 *
 * Solution:
 *   1. kprobe on ext4_fallocate: detect FALLOC_FL_NO_HIDE_STALE (0x04),
 *      clear the flag so it doesn't get rejected, set thread-local context.
 *   2. kprobe on ext4_map_blocks: when thread context is set and the call
 *      is creating unwritten extents, modify flags to:
 *        EXT4_GET_BLOCKS_CREATE | EXT4_GET_BLOCKS_CONVERT_UNWRITTEN
 *      This forces immediate conversion to written extents in the SAME
 *      jbd2 transaction, eliminating the separate conversion transaction.
 *
 * This achieves the NO_HIDE_STALE optimization WITHOUT recompiling the kernel.
 *
 * Build:
 *   make -C /usr/src/kernels/$(uname -r) M=$(pwd) modules
 *
 * Load:
 *   insmod ext4_fast_alloc.ko
 *   (unsigned OK: CONFIG_MODULE_SIG=y but CONFIG_MODULE_SIG_FORCE=n)
 *
 * Unload:
 *   rmmod ext4_fast_alloc
 *
 * Verify:
 *   dmesg | grep ext4_fast_alloc
 */

#include <linux/module.h>
#include <linux/kernel.h>
#include <linux/kprobes.h>
#include <linux/fs.h>
#include <linux/falloc.h>
#include <linux/sched.h>
#include <linux/version.h>

MODULE_LICENSE("GPL");
MODULE_AUTHOR("fast-commitlog");
MODULE_DESCRIPTION("kprobe module: FALLOC_FL_NO_HIDE_STALE for ext4");
MODULE_VERSION("1.0");

#ifndef FALLOC_FL_NO_HIDE_STALE
#define FALLOC_FL_NO_HIDE_STALE	0x04
#endif

/*
 * ext4 internal flag definitions (from fs/ext4/ext4.h).
 * Must match the kernel source exactly.
 */
#define EXT4_GET_BLOCKS_CREATE		0x0002
#define EXT4_GET_BLOCKS_CONVERT		0x0010
#define EXT4_GET_BLOCKS_CONVERT_UNWRITTEN	0x0020
#define EXT4_GET_BLOCKS_CREATE_UNWRIT_EXT	0x0040

/*
 * Thread-local context tracking.
 *
 * We use current->journal_info as per-task storage since it's already
 * per-task and available in struct task_struct. We store a magic pointer
 * to indicate "this thread is in a NO_HIDE_STALE fallocate context".
 */
#define NO_HIDE_MAGIC	((void *)0xDEADBEEF00000042ULL)

static inline void set_no_hide_context(void)
{
	current->journal_info = NO_HIDE_MAGIC;
}

static inline void clear_no_hide_context(void)
{
	if (current->journal_info == NO_HIDE_MAGIC)
		current->journal_info = NULL;
}

static inline bool in_no_hide_context(void)
{
	return current->journal_info == NO_HIDE_MAGIC;
}

/*
 * kprobe on ext4_fallocate: detect NO_HIDE_STALE and set thread context.
 *
 * We don't block the call - we clear the flag and let it proceed
 * with standard allocate (mode=0), which creates unwritten extents.
 * The real optimization happens in ext4_map_blocks kprobe below.
 */
static int __kprobes kprobe_fallocate_pre(struct kprobe *p, struct pt_regs *regs)
{
	int mode = (int)regs->si;

	if (mode & FALLOC_FL_NO_HIDE_STALE) {
		/*
		 * Clear NO_HIDE_STALE so ext4_fallocate doesn't reject it
		 * with EOPNOTSUPP. Set mode to 0 (standard allocate).
		 *
		 * This creates unwritten extents, but they'll be immediately
		 * converted to written by the ext4_map_blocks kprobe below.
		 */
		regs->si = mode & ~FALLOC_FL_NO_HIDE_STALE;
		set_no_hide_context();

		pr_debug("ext4_fast_alloc: NO_HIDE_STALE fallocate intercepted "
			 "pid=%d mode=0x%x -> 0x%lx\n",
			 current->pid, mode, regs->si);
	}

	return 0;
}

static int __kprobes kretprobe_fallocate_ret(struct kretprobe_instance *ri,
					      struct pt_regs *regs)
{
	clear_no_hide_context();
	return 0;
}

/*
 * kprobe on ext4_map_blocks: force immediate extent conversion.
 *
 * When we're in a NO_HIDE_STALE context (thread-local flag set by
 * ext4_fallocate kprobe), and ext4_map_blocks is about to create
 * unwritten extents, we modify the flags to force immediate conversion
 * to written extents.
 *
 * This eliminates the second jbd2 transaction that would normally
 * happen on first write to convert unwritten -> written.
 *
 * ext4_map_blocks signature:
 *   int ext4_map_blocks(handle_t *handle, struct inode *inode,
 *                        struct ext4_map_blocks *map, int flags);
 *
 * On x86_64 calling convention:
 *   rdi = handle (arg1)
 *   rsi = inode (arg2)
 *   rdx = map (arg3)
 *   rcx = flags (arg4)
 */
static int __kprobes kprobe_map_blocks_pre(struct kprobe *p, struct pt_regs *regs)
{
	int flags;

	if (!in_no_hide_context())
		return 0;

	flags = (int)regs->cx;

	/*
	 * Check if this call is creating unwritten extents.
	 * EXT4_GET_BLOCKS_CREATE_UNWRIT_EXT = 0x0040
	 */
	if (flags & EXT4_GET_BLOCKS_CREATE_UNWRIT_EXT) {
		/*
		 * Replace with flags that create AND convert immediately:
		 *   EXT4_GET_BLOCKS_CREATE (0x0002)
		 *   | EXT4_GET_BLOCKS_CONVERT_UNWRITTEN (0x0020)
		 *
		 * This creates written extents in ONE jbd2 transaction
		 * instead of two (create unwritten + convert later).
		 */
		regs->cx = (flags & ~EXT4_GET_BLOCKS_CREATE_UNWRIT_EXT)
			   | EXT4_GET_BLOCKS_CREATE
			   | EXT4_GET_BLOCKS_CONVERT_UNWRITTEN;

		pr_debug("ext4_fast_alloc: forced immediate conversion "
			 "pid=%d flags 0x%x -> 0x%lx\n",
			 current->pid, flags, regs->cx);
	}

	return 0;
}

/*
 * Kprobe structures
 */
static struct kprobe kp_fallocate = {
	.symbol_name = "ext4_fallocate",
	.pre_handler = kprobe_fallocate_pre,
};

static struct kretprobe krp_fallocate = {
	.kp.symbol_name = "ext4_fallocate",
	.handler = kretprobe_fallocate_ret,
	.maxactive = 64,
};

static struct kprobe kp_map_blocks = {
	.symbol_name = "ext4_map_blocks",
	.pre_handler = kprobe_map_blocks_pre,
};

static int __init ext4_fast_alloc_init(void)
{
	int ret;

	pr_info("ext4_fast_alloc: loading module (pid=%d)\n", current->pid);

	/* Register kprobe on ext4_fallocate */
	ret = register_kprobe(&kp_fallocate);
	if (ret < 0) {
		pr_err("ext4_fast_alloc: register_kprobe(ext4_fallocate) failed: %d\n", ret);
		return ret;
	}
	pr_info("ext4_fast_alloc: kprobe registered on ext4_fallocate\n");

	/* Register kretprobe on ext4_fallocate */
	ret = register_kretprobe(&krp_fallocate);
	if (ret < 0) {
		pr_err("ext4_fast_alloc: register_kretprobe(ext4_fallocate) failed: %d\n", ret);
		unregister_kprobe(&kp_fallocate);
		return ret;
	}
	pr_info("ext4_fast_alloc: kretprobe registered on ext4_fallocate\n");

	/* Register kprobe on ext4_map_blocks (the key optimization) */
	ret = register_kprobe(&kp_map_blocks);
	if (ret < 0) {
		pr_err("ext4_fast_alloc: register_kprobe(ext4_map_blocks) failed: %d\n", ret);
		unregister_kretprobe(&krp_fallocate);
		unregister_kprobe(&kp_fallocate);
		return ret;
	}
	pr_info("ext4_fast_alloc: kprobe registered on ext4_map_blocks\n");

	pr_info("ext4_fast_alloc: module loaded successfully\n");
	pr_info("ext4_fast_alloc: FALLOC_FL_NO_HIDE_STALE (0x%02x) is now active\n",
		FALLOC_FL_NO_HIDE_STALE);

	return 0;
}

static void __exit ext4_fast_alloc_exit(void)
{
	unregister_kprobe(&kp_map_blocks);
	unregister_kretprobe(&krp_fallocate);
	unregister_kprobe(&kp_fallocate);
	pr_info("ext4_fast_alloc: module unloaded\n");
}

module_init(ext4_fast_alloc_init);
module_exit(ext4_fast_alloc_exit);
