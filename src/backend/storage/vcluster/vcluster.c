/*-------------------------------------------------------------------------
 *
 * vcluster.c
 *
 * Version Cluster Implementation
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/vcluster/vcluster.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/shmem.h"
#include "storage/vcache.h"
#include "storage/vcluster.h"

/*
 * VClusterShmemSize
 *
 * compute the size of shared memory for the vcluster
 */
Size
VClusterShmemSize(void)
{
	Size		size = 0;

	size = add_size(size, VCacheShmemSize());

	return size;
}

/*
 * VClusterShmemInit
 *
 * Initialize shared data structures related to vcluster in shared memory
 */
void
VClusterShmemInit(void)
{
	VCacheInit();
}

