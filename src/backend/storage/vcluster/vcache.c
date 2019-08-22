/*-------------------------------------------------------------------------
 *
 * vcache.c
 *    Cache implementation for storing version cluster pages in memory
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/vcluster/vcache.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/vcache.h"
#include "storage/vcache_hash.h"

VCacheDescPadded *VCacheDescriptors;
char	   *VCacheBlocks;

/*
 * VCacheShmemSize
 *
 * compute the size of shared memory for the vcache including
 * vcluster pages, vcache descriptors, hash tables, etc.
 */
Size
VCacheShmemSize(void)
{
	Size		size = 0;

	/* size of vcache descriptors */
	size = add_size(size, mul_size(NVCache, sizeof(VCacheDescPadded)));
	/* to allow aligning buffer descriptors */
	size = add_size(size, PG_CACHE_LINE_SIZE);

	/* size of data pages */
	size = add_size(size, mul_size(NVCache, BLCKSZ));

	/* size of vcache_hash */
	size = add_size(size, VCacheHashShmemSize(NVCache + NUM_VCACHE_PARTITIONS));

	return size;
}

/*
 * VCacheInit
 *
 * Initialize vcache, vcache_desc, vcache_hash in shared memory
 *
 */
void
VCacheInit(void)
{
	bool		foundBufs,
				foundDescs;

	/* Align descriptors to a cacheline boundary. */
	VCacheDescriptors = (VCacheDescPadded *)
		ShmemInitStruct("VCache Descriptors",
						NVCache * sizeof(VCacheDescPadded),
						&foundDescs);

	VCacheBlocks = (char *)
		ShmemInitStruct("VCache Blocks",
						NVCache * (Size) BLCKSZ, &foundBufs);

	/* Initialize the shared vcache lookup hashtable */
	VCacheHashInit(NVCache + NUM_VCACHE_PARTITIONS);

}

