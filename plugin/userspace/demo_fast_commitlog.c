/*
 * demo_fast_commitlog.c - Demonstration of fast-commitlog usage
 *
 * This program shows how to use the fast-commitlog API to create
 * a CommitLog file with pre-allocated written extents, eliminating
 * ext4 jdb2 write amplification during sequential writes.
 *
 * Build:
 *   make
 *
 * Run:
 *   ./demo_fast_commitlog /data/commitlog/test.log 1073741824
 *   (creates a 1GB CommitLog file)
 */

#define _GNU_SOURCE
#include "../include/fast_commitlog.h"

#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <errno.h>
#include <time.h>

#define MSG_SIZE  256
#define BATCH_SIZE 1024

static void print_usage(const char *prog)
{
	fprintf(stderr, "Usage: %s <commitlog_path> <file_size_bytes> [iterations]\n", prog);
	fprintf(stderr, "\n");
	fprintf(stderr, "  commitlog_path    Path to the CommitLog file (must be on ext4)\n");
	fprintf(stderr, "  file_size_bytes   Pre-allocated file size (e.g. 1073741824 for 1GB)\n");
	fprintf(stderr, "  iterations        Number of write batches (default: 10)\n");
	fprintf(stderr, "\n");
	fprintf(stderr, "Examples:\n");
	fprintf(stderr, "  %s /data/commitlog/00000000001073741824 1073741824\n", prog);
	fprintf(stderr, "  %s /tmp/test.log 104857600 100\n", prog);
}

static double get_time_ms(void)
{
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return tv.tv_sec * 1000.0 + tv.tv_usec / 1000.0;
}

static int write_sequential(int fd, off_t file_size, int iterations)
{
	char buf[MSG_SIZE];
	off_t written = 0;
	int batch;
	double start, elapsed, throughput;
	void *mapped;

	/* Use mmap for sequential write (optional, but typical for CommitLog) */
	mapped = mmap(NULL, file_size, PROT_READ | PROT_WRITE,
			  MAP_SHARED, fd, 0);
	if (mapped == MAP_FAILED) {
		fprintf(stderr, "mmap failed: %s\n", strerror(errno));
		return -1;
	}

	start = get_time_ms();

	for (batch = 0; batch < iterations; batch++) {
		int i;
		for (i = 0; i < BATCH_SIZE; i++) {
			if (written + MSG_SIZE > file_size) {
				fprintf(stderr, "File full after %lld bytes\n",
					(long long)written);
				goto done;
			}

			/* Simulate CommitLog message: [header][payload] */
			snprintf(buf, MSG_SIZE,
				 "batch=%d seq=%d ts=%ld data=xxxxxxxxxxxxxxxx",
				 batch, i, (long)time(NULL));

			memcpy((char *)mapped + written, buf, MSG_SIZE);
			written += MSG_SIZE;
		}

		/* fsync after each batch to simulate CommitLog sync write */
		if (fsync(fd) != 0) {
			fprintf(stderr, "fsync failed: %s\n", strerror(errno));
			goto done;
		}
	}


done:
	elapsed = get_time_ms() - start;
	throughput = (written / (1024.0 * 1024.0)) / (elapsed / 1000.0);

	printf("Wrote %lld bytes in %.2f ms (%.2f MB/s)\n",
	       (long long)written, elapsed, throughput);

	munmap(mapped, file_size);
	return 0;
}

int main(int argc, char *argv[])
{
	const char *path;
	off_t file_size;
	int iterations = 10;
	int fd;
	int support;

	if (argc < 3) {
		print_usage(argv[0]);
		return EXIT_FAILURE;
	}

	path = argv[1];
	file_size = atoll(argv[2]);
	if (argc >= 4)
		iterations = atoi(argv[3]);

	if (file_size <= 0) {
		fprintf(stderr, "file_size must be positive\n");
		return EXIT_FAILURE;
	}

	printf("=== Fast CommitLog Demo ===\n");
	printf("Path: %s\n", path);
	printf("File size: %lld bytes (%.2f MB)\n", (long long)file_size, file_size / (1024.0 * 1024.0));
	printf("Iterations: %d\n\n", iterations);

	/*
	 * Step 1: Open the file (O_RDWR | O_CREAT | O_DSYNC for sync writes).
	 *
	 * O_DSYNC ensures data is flushed to disk on each write.
	 * This is typical for CommitLog but causes the jdb2 amplification
	 * we're trying to eliminate.
	 */
	fd = open(path, O_RDWR | O_CREAT | O_DSYNC, 0644);
	if (fd < 0) {
		fprintf(stderr, "open failed: %s\n", strerror(errno));
		return EXIT_FAILURE;
	}

	/*
	 * Step 2: Check kernel support for FALLOC_FL_NO_HIDE_STALE.
	 */
	support = fast_commitlog_check_support(fd);
	switch (support) {
	case 1:
		printf("[OK] FALLOC_FL_NO_HIDE_STALE is supported\n");
		break;
	case 0:
		printf("[WARN] FALLOC_FL_NO_HIDE_STALE not supported (kernel patch not installed)\n");
		printf("       Falling back to FALLOC_FL_ZERO_RANGE\n");
		printf("       To enable full optimization, apply kernel patch:\n");
		printf("         cp fast-commitlog/kernel/ext4_fast_alloc.c /usr/src/linux/fs/ext4/\n");
		printf("         Rebuild and install the kernel\n");
		break;
	default:
		fprintf(stderr, "[ERROR] Failed to check kernel support: %s\n", strerror(errno));
		close(fd);
		return EXIT_FAILURE;
	}

	/*
	 * Step 3: Initialize the CommitLog file with pre-allocated written extents.
	 *
	 * This is the key step that eliminates jdb2 write amplification:
	 * - With NO_HIDE_STALE patch: written extents created without zero-fill
	 * - Without patch: written extents created with zero-fill (one-time cost)
	 * - Last resort: standard fallocate (unwritten extents, partial benefit)
	 */
	double alloc_start = get_time_ms();
	if (fast_commitlog_init(fd, file_size) != 0) {
		fprintf(stderr, "fast_commitlog_init failed: %s\n", strerror(errno));
		close(fd);
		return EXIT_FAILURE;
	}
	double alloc_ms = get_time_ms() - alloc_start;
	printf("Pre-allocation completed in %.2f ms\n\n", alloc_ms);

	/*
	 * Step 4: Simulate sequential CommitLog writes.
	 *
	 * Because the file is pre-allocated with written extents:
	 * - No unwritten->written extent conversion during writes
	 * - No i_size updates (file size is fixed)
	 * - jdb2 metadata writes are minimized to ~0
	 */
	printf("Running write benchmark...\n\n");
	int ret = write_sequential(fd, file_size, iterations);

	close(fd);

	if (ret == 0) {
		printf("\n=== Done ===\n");
		printf("Check disk I/O with: iostat -x 1 /dev/sda\n");
		printf("Compare jdb2 write bytes with and without the optimization.\n");
	}

	return ret == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
