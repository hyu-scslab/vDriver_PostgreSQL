/*-------------------------------------------------------------------------
 *
 * vcache_hash.c
 *
 * hash table implementation for mapping VCacheTag to vcache indexes
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/vcluster/vcache_hash.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/vcache.h"
#include "storage/vcache_hash.h"
#include "utils/hsearch.h"

/* entry for vcache lookup hashtable */
typedef struct
{
	VCacheTag	key;			/* Tag of a disk page */
	int			id;				/* Associated cache ID */
} VCacheLookupEnt;

static HTAB *SharedVCacheHash;


/*
 * VCacheHashShmemSize
 *
 * compute the size of shared memory for vcache_hash
 */
Size
VCacheHashShmemSize(int size)
{
	return hash_estimate_size(size, sizeof(VCacheLookupEnt));
}

/*
 * VCacheHashInit
 *
 * Initialize vcache_hash in shared memory
 */
void
VCacheHashInit(int size)
{
	HASHCTL		info;

	/* VCacheTag maps to VCache */
	info.keysize = sizeof(VCacheTag);
	info.entrysize = sizeof(VCacheLookupEnt);
	info.num_partitions = NUM_VCACHE_PARTITIONS;

	SharedVCacheHash = ShmemInitHash("Shared VCache Lookup Table",
								     size, size,
								     &info,
								     HASH_ELEM | HASH_BLOBS | HASH_PARTITION);
}
