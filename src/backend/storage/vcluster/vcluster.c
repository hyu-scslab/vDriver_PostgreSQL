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

#include <fcntl.h>
#include <unistd.h>

#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/vcache.h"
#include "storage/vcluster.h"

/* vcluster descriptor in shared memory*/
VClusterDesc	*vclusters;

/* dsa_area of vcluster for this process */
dsa_area		*dsa_vcluster;

/* decls for local routines only used within this module */
static dsa_pointer AllocNewSegmentInternal(dsa_area *dsa);
static dsa_pointer AllocNewSegment(void);
static void PrepareSegmentFile(VSegmentId seg_id);

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
	bool		foundDesc;

	VCacheInit();
	
	vclusters = (VClusterDesc *)
		ShmemInitStruct("VCluster Descriptor",
						sizeof(VClusterDesc), &foundDesc);
	pg_atomic_init_u32(&vclusters->next_seg_id, 0);
}

void
VClusterDsaInit(void)
{
	dsa_area	*dsa;
	
	/* Initialize dsa area for vcluster */
	dsa = dsa_create(LWTRANCHE_VCLUSTER);
	ProcGlobal->vcluster_dsa_handle = dsa_get_handle(dsa);
	dsa_pin(dsa);

	/*
	 * Pre-allocate and initialize the first segment descriptors, files
	 * for each vcluster.
	 * assign segment id 0, 1, 2 for HOT, COLD, LLT segment, respectively.
	 */
	for (int i = 0; i < VCLUSTER_NUM; i++)
	{
		vclusters->head[i] = AllocNewSegmentInternal(dsa);
	}

	ereport(LOG, (errmsg("VClusterDsaInit, dsa: %p", dsa)));
	dsa_detach(dsa);
}

void
VClusterAttachDsa(void)
{
	if (dsa_vcluster != NULL) return;
	//Assert(dsa_vcluster == NULL);
	
	dsa_vcluster = dsa_attach(ProcGlobal->vcluster_dsa_handle);
	ereport(LOG, (errmsg("VClusterAttachDsa, dsa: %p", dsa_vcluster)));

	dsa_pin_mapping(dsa_vcluster);
}

void
VClusterDetachDsa(void)
{
	if (dsa_vcluster == NULL) return;
	//Assert(dsa_vcluster != NULL);
	
	ereport(LOG, (errmsg("VClusterDetachDsa, dsa: %p", dsa_vcluster)));

	dsa_detach(dsa_vcluster);
	dsa_vcluster = NULL;
}

/*
 * VClusterAppendTuple
 *
 * Append a given tuple into the vcluster whose type is cluster_type
 */
void
VClusterAppendTuple(VCLUSTER_TYPE cluster_type,
					Size tuple_size,
					void *tuple)
{
	VSegmentDesc 	*seg_desc;
	int				alloc_seg_offset;
	
	Assert(cluster_type == VCLUSTER_HOT ||
		   cluster_type == VCLUSTER_COLD ||
		   cluster_type == VCLUSTER_LLT);

	Assert(dsa_vcluster != NULL);

retry:
	seg_desc = (VSegmentDesc *)dsa_get_address(
				dsa_vcluster, vclusters->head[cluster_type]);
	
	/* Pre-check whether the current segment is full */
	if (pg_atomic_read_u32(&seg_desc->seg_offset) > VCLUSTER_SEGSIZE)
	{
		/*
		 * Current segment is full, someone might(must) opening a
		 * new segmnet. Backoff and waiting it.
		 */
		pg_usleep(1000L);
		goto retry;
	}

	alloc_seg_offset = pg_atomic_fetch_add_u32(
			&seg_desc->seg_offset, tuple_size);
	if (alloc_seg_offset + tuple_size <= VCLUSTER_SEGSIZE)
	{
		/* Success to allocate segment space */
		
		/* TODO: just append the tuple into the corresponding buffer page.. */
		/* Also need to think about where to store fd.. */
		char filename[128];
		int fd;

		sprintf(filename, "vsegment.%08d", seg_desc->seg_id);
		fd = open(filename, O_RDWR);
		pwrite(fd, tuple, tuple_size, alloc_seg_offset);
		close(fd);

	}
	else if (alloc_seg_offset <= VCLUSTER_SEGSIZE &&
			 alloc_seg_offset + tuple_size > VCLUSTER_SEGSIZE)
	{
		/* Responsible for making a new segment for this cluster */
		
		/* TODO: If multiple process(foreground/background) prepare
		 * a new segment concurrently, need to add some arbitration code */
		vclusters->head[cluster_type] = AllocNewSegment();
		pg_memory_barrier();
		goto retry;
	}
	else
	{
		/*
		 * Current segment is full, someone might(must) opening a
		 * new segmnet. Backoff and waiting it.
		 */
		pg_usleep(1000L);
		goto retry;
	}
}

/*
 * AllocateNewSegmentInternal
 *
 * Internal implementation of AllocateNewSegment
 * Postmaster exceptionally call this function directly,
 * using its own dsa_area.
 */
static dsa_pointer AllocNewSegmentInternal(dsa_area *dsa)
{
	dsa_pointer ret;
	VSegmentDesc *seg_desc;
	int new_seg_id;

	/* Allocate a new segment descriptor in shared memory */
	ret = dsa_allocate_extended(
			dsa, sizeof(VSegmentDesc), DSA_ALLOC_ZERO);

	/* Get a new segment id */
	new_seg_id = pg_atomic_fetch_add_u32(&vclusters->next_seg_id, 1);

	/* Initialize the segment descriptor */
	seg_desc = (VSegmentDesc *)dsa_get_address(dsa, ret);
	seg_desc->seg_id = new_seg_id;
	seg_desc->xmin = 0;
	seg_desc->xmax = 0;
	seg_desc->next = 0;
	pg_atomic_init_u32(&seg_desc->seg_offset, 0);

	/* Allocate a new segment file */
	PrepareSegmentFile(new_seg_id);

	/* Returns dsa_pointer for the new VSegmentDesc */
	return ret;
}

/*
 * AllocateNewSegment
 *
 * Make a new segment file and its index
 * Returns the dsa_pointer of the new segment descriptor
 */
static dsa_pointer AllocNewSegment(void)
{
	Assert(dsa_vcluster != NULL);

	return AllocNewSegmentInternal(dsa_vcluster);
}

/*
 * PrepareSegmentFile
 *
 * Make a new file for corresponding seg_id
 */
static void PrepareSegmentFile(VSegmentId seg_id)
{
	int fd;
	char filename[128];

	sprintf(filename, "vsegment.%08d", seg_id);
	fd = open(filename, O_RDWR | O_CREAT | O_DIRECT, (mode_t)0600);

	/* pre-allocate the segment file*/
	fallocate(fd, 0, 0, VCLUSTER_SEGSIZE);
	close(fd);
}

