/*-------------------------------------------------------------------------
 *
 * vcache.h
 *	  hash table definitions for mapping VCacheTag to vcache indexes.
 *
 *
 *
 * src/include/storage/vcache.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VCACHE_H
#define VCACHE_H

#include "port/atomics.h"
#include "storage/vcluster.h"

typedef struct VCacheTag
{
	VSegmentId		seg_id;
	VSegmentPageId	page_id;
} VCacheTag;

typedef struct VCacheDesc
{
	/* ID of the cached page. seg_id 0 means this entry is unused */
	VCacheTag	tag;

	bool		is_dirty;		/* whether the page is not yet synced */

	/*
	 * Written bytes on this cache entry. If it gets same with the page size,
	 * it can be unpinned and ready to flush.
	 *
	 * NOTE: At this time, we don't care about tuples overlapped between
	 * two pages.
	 */ 
	pg_atomic_uint32	written_bytes;
	
	/*
	 * Cache entry with refcount > 0 cannot be evicted.
	 * We use refcount as a pin. The refcount of appending page
	 * should be kept above 1, and the last transaction which filled up
	 * the page should decrease it for unpinning it.
	 */
	pg_atomic_uint32	refcount;
} VCacheDesc;

#define VCACHEDESC_PAD_TO_SIZE	(SIZEOF_VOID_P == 8 ? 64 : 1)

typedef union VCacheDescPadded
{
	VCacheDesc	vcachedesc;
	char		pad[VCACHEDESC_PAD_TO_SIZE];
} VCacheDescPadded;

/* Metadata for vcache */
typedef struct VCacheMeta
{
	/*
	 * Indicate the cache entry which might be a victim for allocating
	 * a new page. Need to use fetch-and-add on this so that multiple
	 * transactions can allocate/evict cache entries concurrently.
	 */
	pg_atomic_uint64	eviction_rr_idx;

} VCacheMeta;

#define GetVCacheDescriptor(id) (&VCacheDescriptors[(id)].vcachedesc)

/* in globals.c ... this duplicates miscadmin.h */
extern PGDLLIMPORT int NVCache;

extern Size VCacheShmemSize(void);
extern void VCacheInit(void);

extern void VCacheAppendTuple(VSegmentId seg_id,
							  VSegmentId reserved_seg_id,
							  VSegmentOffset seg_offset,
				  			  Size tuple_size,
				  			  const void *tuple);

extern void VCacheReadTuple(VSegmentId seg_id,
							VSegmentOffset seg_offset,
							Size tuple_size,
							void *ret_tuple);


#endif							/* VCACHE_H */
