/*
 * fast_commitlog.c - User-space implementation of fast-commitlog API
 *
 * Links against programs using include/fast_commitlog.h
 */

#define _GNU_SOURCE
#include "fast_commitlog.h"

#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <sys/syscall.h>
#include <stdio.h>
#include <string.h>

/*
 * glibc may not define FALLOC_FL_NO_HIDE_STALE, so use the
 * value directly (must match kernel-side definition).
 */
#ifndef FALLOC_FL_NO_HIDE_STALE
#define FALLOC_FL_NO_HIDE_STALE  0x04
#endif

/*
 * fast_commitlog_alloc - Pre-allocate written extents without zero-fill.
 *
 * Requires kernel patch (kernel/ext4_fast_alloc.c).
 * Without the patch, this returns -1 with errno=EOPNOTSUPP.
 */
int fast_commitlog_alloc(int fd, off_t offset, off_t len)
{
	int mode = FALLOC_FL_NO_HIDE_STALE;

	return fallocate(fd, mode, offset, len);
}

/*
 * fast_commitlog_zero_range_alloc - Pre-allocate written extents with zero-fill.
 *
 * No kernel patch required - uses standard FALLOC_FL_ZERO_RANGE.
 * Creates written extents upfront so no extent conversion happens at write
 * time, but there is a one-time zero-fill cost.
 */
int fast_commitlog_zero_range_alloc(int fd, off_t offset, off_t len)
{
	int mode = FALLOC_FL_ZERO_RANGE;

	return fallocate(fd, mode, offset, len);
}

/*
 * fast_commitlog_init - Initialize a fixed-size CommitLog file.
 *
 * This is the recommended entry point for CommitLog setup:
 *   1. Set file size via ftruncate
 *   2. Pre-allocate written extents via NO_HIDE_STALE
 *   3. If NO_HIDE_STALE fails (no kernel patch), fall back to ZERO_RANGE
 *
 * The file should be fully pre-allocated so that subsequent writes
 * don't trigger extent state conversions or i_size updates.
 */
int fast_commitlog_init(int fd, off_t file_size)
{
	int ret;

	/* Step 1: Set the file size */
	ret = ftruncate(fd, file_size);
	if (ret != 0)
		return -1;

	/* Step 2: Try NO_HIDE_STALE first (optimal, no zero-fill) */
	ret = fast_commitlog_alloc(fd, 0, file_size);
	if (ret == 0)
		return 0;

	/*
	 * NO_HIDE_STALE failed (likely EOPNOTSUPP - kernel patch not installed).
	 * Fall back to ZERO_RANGE (still eliminates runtime extent conversion,
	 * but has a one-time zero-fill cost).
	 */
	if (errno == EOPNOTSUPP) {
		ret = fast_commitlog_zero_range_alloc(fd, 0, file_size);
		if (ret == 0)
			return 0;
	}

	/*
	 * Both methods failed. As last resort, try standard fallocate.
	 * This creates unwritten extents, which will be converted on write.
	 * Write amplification NOT eliminated, but file is still pre-allocated.
	 */
	ret = fallocate(fd, 0, 0, file_size);
	return ret;
}

/*
 * fast_commitlog_check_support - Probe for FALLOC_FL_NO_HIDE_STALE support.
 */
int fast_commitlog_check_support(int fd)
{
	int ret;

	ret = fallocate(fd, FALLOC_FL_NO_HIDE_STALE, 0, 4096);
	if (ret == 0) {
		/* Clean up: deallocate the test range */
		fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
			  0, 4096);
		return 1;
	}

	if (errno == EOPNOTSUPP)
		return 0;

	return -1;
}
