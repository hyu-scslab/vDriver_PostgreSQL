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
#ifndef HYU_LLT
#include "postgres.h"

#include <fcntl.h>
#include <unistd.h>

#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/vcache.h"
#include "storage/vchain.h"
#include "storage/vcluster.h"

/* vcluster descriptor in shared memory*/
VClusterDesc	*vclusters;

/* dsa_area of vcluster for this process */
dsa_area		*dsa_vcluster;

/*
 * Temporal fd array (private) for each segment.
 * TODO: replace it to hash table or something..
 */
int seg_fds[VCLUSTER_MAX_SEGMENTS];

/* decls for local routines only used within this module */
static dsa_pointer AllocNewSegmentInternal(dsa_area *dsa,
										   VCLUSTER_TYPE type);
static dsa_pointer AllocNewSegment(VCLUSTER_TYPE type);
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
	size = add_size(size, VChainShmemSize());

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
	VChainInit();
	
	vclusters = (VClusterDesc *)
		ShmemInitStruct("VCluster Descriptor",
						sizeof(VClusterDesc), &foundDesc);

	/* We use segment id from 1, 0 is reserved for exceptional cases */
	for (int i = 0; i < VCLUSTER_NUM; i++)
	{
		vclusters->reserved_seg_id[i] = i + 1;
	}
	pg_atomic_init_u32(&vclusters->next_seg_id, VCLUSTER_NUM + 1);
}

/*
 * VClusterDsaInit
 *
 * Initialize a dsa area for vcluster.
 * This function should only be called after the initialization of
 * the dynamic shared memory facilities.
 */
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
		vclusters->head[i] = AllocNewSegmentInternal(dsa, i);
	}

	dsa_detach(dsa);
}

/*
 * VClusterDsaInit
 *
 * Attach to the dsa area for vcluster.
 */
void
VClusterAttachDsa(void)
{
	if (dsa_vcluster != NULL)
		return;
	
	dsa_vcluster = dsa_attach(ProcGlobal->vcluster_dsa_handle);
	dsa_pin_mapping(dsa_vcluster);
}

/*
 * VClusterDsaInit
 *
 * Detach from the dsa area for vcluster.
 */
void
VClusterDetachDsa(void)
{
	if (dsa_vcluster == NULL)
		return;

	dsa_detach(dsa_vcluster);
	dsa_vcluster = NULL;
}

#if 0
/*
 * VClusterLookupTuple
 *
 * Find a visible version tuple with given primary key and snapshot.
 * Returns true if found. Returns false if not found.
 */
bool
VClusterLookupTuple(PrimaryKey primary_key,
					Size size,
					Snapshot snapshot,
					void *ret_tuple)
{
	VLocator *locator;
	if (!VChainLookupLocator(primary_key, snapshot, &locator))
	{
		/* Couldn't find the visible locator for the primary key */
		return false;
	}

	VCacheReadTuple(locator->seg_id,
					locator->seg_offset,
					size,
					ret_tuple);
	
	return true;
}
#endif
/*
 * VClusterLookupTuple
 *
 * Find a visible version tuple with given primary key and snapshot.
 * Set ret_tuple to the pointer of the tuple in the vcache entry, and
 * return the pinned vcache id. Caller should unpin the cache after using.
 * Return InvalidVCache if not found.
 */
int
VClusterLookupTuple(PrimaryKey primary_key,
					Snapshot snapshot,
					void **ret_tuple)
{
	VLocator *locator;
	int cache_id;

	if (!VChainLookupLocator(primary_key, snapshot, &locator))
	{
		/* Couldn't find the visible locator for the primary key */
		return InvalidVCache;
	}

	cache_id = VCacheReadTupleRef(locator->seg_id,
								  locator->seg_offset,
								  ret_tuple);
	return cache_id;
}
/*
 * VClusterAppendTuple
 *
 * Append a given tuple into the vcluster whose type is cluster_type
 */
void
VClusterAppendTuple(VCLUSTER_TYPE cluster_type,
					PrimaryKey primary_key,
					TransactionId xmin,
					Size tuple_size,
					const void *tuple)
{
	VSegmentDesc 	*seg_desc;
	int				alloc_seg_offset;
	dsa_pointer		dsap_new_seg;
	VSegmentDesc	*new_seg_desc;
	VSegmentId		reserved_seg_id;
	int				vidx;

	/*
	 * Alined size with power of 2. This is needed because
	 * the current version only support fixed size tuple with power of 2
	 */
	Size			aligned_tuple_size;
	
	Assert(cluster_type == VCLUSTER_HOT ||
		   cluster_type == VCLUSTER_COLD ||
		   cluster_type == VCLUSTER_LLT);

	Assert(dsa_vcluster != NULL);

	aligned_tuple_size = 1 << my_log2(tuple_size);

retry:
	/*
	 * The order of reading the segment id and the reserved segment id
	 * is important for making happen-before relation.
	 * If we read the reserved segment id first, then the segment id and
	 * the reserved segment id could be same.
	 */
	seg_desc = (VSegmentDesc *)dsa_get_address(
			dsa_vcluster, vclusters->head[cluster_type]);
	pg_memory_barrier();
	reserved_seg_id = vclusters->reserved_seg_id[cluster_type];
	
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

	/* Allocate tuple space with aligned size with power of 2 */
	alloc_seg_offset = pg_atomic_fetch_add_u32(
			&seg_desc->seg_offset, aligned_tuple_size);

	if (alloc_seg_offset + aligned_tuple_size <= VCLUSTER_SEGSIZE)
	{
		/* Success to allocate segment space */

		/*
		 * NOTE: We are assuming that the tuple would not be overlapped
		 * between two pages
		 */
		VCacheAppendTuple(seg_desc->seg_id,
						  reserved_seg_id,
						  alloc_seg_offset,
						  tuple_size,
						  tuple);
		
		/* Set vlocator of the version tuple */
		vidx = alloc_seg_offset / aligned_tuple_size;
		seg_desc->locators[vidx].xmin = xmin;
		seg_desc->locators[vidx].seg_id = seg_desc->seg_id;
		seg_desc->locators[vidx].seg_offset = alloc_seg_offset;
		seg_desc->locators[vidx].dsap_prev = 0;
		seg_desc->locators[vidx].dsap_next = 0;

		/* Append the locator into the version chain */
		VChainAppendLocator(primary_key, &seg_desc->locators[vidx]);
	}
	else if (alloc_seg_offset <= VCLUSTER_SEGSIZE &&
			 alloc_seg_offset + aligned_tuple_size > VCLUSTER_SEGSIZE)
	{
		/* Responsible for making a new segment for this cluster */
		
		/* TODO: If multiple process(foreground/background) prepare
		 * a new segment concurrently, need to add some arbitration code */
		dsap_new_seg = AllocNewSegment(cluster_type);

		/* Segment descriptor chain remains new to old */
		new_seg_desc =
				(VSegmentDesc *)dsa_get_address(dsa_vcluster, dsap_new_seg);
		new_seg_desc->next = vclusters->head[cluster_type];
		vclusters->head[cluster_type] = dsap_new_seg;
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
static dsa_pointer
AllocNewSegmentInternal(dsa_area *dsa, VCLUSTER_TYPE cluster_type)
{
	dsa_pointer		ret;
	VSegmentDesc   *seg_desc;
	VSegmentId		new_seg_id;

	/* Allocate a new segment descriptor in shared memory */
	ret = dsa_allocate_extended(
			dsa, sizeof(VSegmentDesc), DSA_ALLOC_ZERO);

	/* Get a new segment id */
	new_seg_id = vclusters->reserved_seg_id[cluster_type];
	vclusters->reserved_seg_id[cluster_type] =
			pg_atomic_fetch_add_u32(&vclusters->next_seg_id, 1);

	/* Initialize the segment descriptor */
	seg_desc = (VSegmentDesc *)dsa_get_address(dsa, ret);
	seg_desc->dsap = ret;
	seg_desc->seg_id = new_seg_id;
	seg_desc->xmin = 0;
	seg_desc->xmax = 0;
	seg_desc->next = 0;
	pg_atomic_init_u32(&seg_desc->seg_offset, 0);

	/* Initialize the all vlocators in the segment index */
	for (int i = 0; i < VCLUSTER_SEG_NUM_ENTRY; i++)
	{
		/* Each vlocator contains its dsa_pointer itself */
		seg_desc->locators[i].dsap =
				ret + offsetof(VSegmentDesc, locators[i]);
	}

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
static dsa_pointer
AllocNewSegment(VCLUSTER_TYPE cluster_type)
{
	Assert(dsa_vcluster != NULL);

	return AllocNewSegmentInternal(dsa_vcluster, cluster_type);
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
	//fd = open(filename, O_RDWR | O_CREAT | O_DIRECT, (mode_t)0600);
	fd = open(filename, O_RDWR | O_CREAT, (mode_t)0600);
	/* TODO: need to confirm if we need to use O_DIRECT */

	/* pre-allocate the segment file*/
	fallocate(fd, 0, 0, VCLUSTER_SEGSIZE);

	seg_fds[seg_id] = fd;
}

#endif /* HYU_LLT */
