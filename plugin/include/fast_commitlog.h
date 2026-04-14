/*
 * fast_commitlog.h - FALLOC_FL_NO_HIDE_STALE flag definition and API declarations
 *
 * This header defines the FALLOC_FL_NO_HIDE_STALE flag (0x04) which, when used
 * with fallocate(), directly allocates written extents on ext4 without the
 * zero-fill step. This eliminates the jdb2 write amplification caused by
 * unwritten-to-written extent conversions during runtime.
 *
 * Usage:
 *   1. Kernel: apply the corresponding kernel patch (see kernel/ext4_fast_alloc.c)
 *   2. User-space: include this header and call fast_commitlog_alloc()
 */

#ifndef _FAST_COMMITLOG_H
#define _FAST_COMMITLOG_H

#include <sys/types.h>

/*
 * fallocate mode flags.
 * FALLOC_FL_NO_HIDE_STALE (0x04) must match the kernel-side definition.
 *
 * This flag tells ext4 to:
 *   - Directly allocate written extents (not unwritten)
 *   - Skip the zero-fill step
 *   - Caller guarantees no read of un-written regions
 *
 * Compatible with existing flags:
 *   FALLOC_FL_KEEP_SIZE     0x01
 *   FALLOC_FL_PUNCH_HOLE    0x02
 *   FALLOC_FL_NO_HIDE_STALE 0x04  <-- new
 *   FALLOC_FL_ZERO_RANGE    0x10
 */
#ifndef FALLOC_FL_NO_HIDE_STALE
#define FALLOC_FL_NO_HIDE_STALE  0x04
#endif

#ifndef FALLOC_FL_ZERO_RANGE
#define FALLOC_FL_ZERO_RANGE     0x10
#endif

/*
 * Fast pre-allocation that bypasses the zero-fill path.
 *
 * @fd:        file descriptor (opened with O_WRONLY or O_RDWR)
 * @offset:    start offset
 * @len:       length to pre-allocate
 *
 * Returns 0 on success, -1 on error (errno set).
 *
 * NOTE: This requires the kernel patch in kernel/ext4_fast_alloc.c.
 *       Without the patch, fallocate will reject FALLOC_FL_NO_HIDE_STALE
 *       with EOPNOTSUPP.
 */
int fast_commitlog_alloc(int fd, off_t offset, off_t len);

/*
 * Standard fallocate-based pre-allocation (no kernel patch required).
 * Uses FALLOC_FL_ZERO_RANGE which creates written extents but zeros them.
 *
 * @fd:        file descriptor
 * @offset:    start offset
 * @len:       length to pre-allocate
 *
 * Returns 0 on success, -1 on error (errno set).
 *
 * NOTE: Without the NO_HIDE_STALE patch, this is the best standard-kernel
 *       alternative. Written extents are created upfront, so no extent
 *       conversion happens at write time, but there is a one-time zero-fill cost.
 */
int fast_commitlog_zero_range_alloc(int fd, off_t offset, off_t len);

/*
 * Combined initialization for a fixed-size CommitLog file.
 *
 * Steps:
 *   1. ftruncate to FILE_SIZE (set file length)
 *   2. fast_commitlog_alloc to pre-allocate written extents (no zero-fill)
 *   3. mmap the file for sequential write (optional)
 *
 * @fd:        file descriptor
 * @file_size: total file size to allocate
 *
 * Returns 0 on success, -1 on error (errno set).
 */
int fast_commitlog_init(int fd, off_t file_size);

/*
 * Check if the kernel supports FALLOC_FL_NO_HIDE_STALE.
 *
 * @fd:        file descriptor on ext4 filesystem
 *
 * Returns 1 if supported, 0 if not, -1 on error.
 */
int fast_commitlog_check_support(int fd);

#endif /* _FAST_COMMITLOG_H */
